import { withExecutor, withSchemaClient, type SqlExecutor } from "../db/client.ts";
import type { UserRole } from "../auth/types.ts";
import type { JsonObject } from "../validation/ingestion.ts";
import {
  findActiveObjectMutationByObjectWithExecutor,
  tryCreateArchiveRequestWithExecutor,
  type ArchiveRequestStatus,
} from "./archive-request-repo.ts";
import { createArchiveRequestSourceWithExecutor } from "./archive-request-source-repo.ts";
import {
  isObjectEditAuthorized,
  mapObjectEdit,
  selectObjectEditRow,
  type ObjectEditRecord,
} from "./object-edit-repo.ts";

const PACKAGE_SCHEMA_VERSION = "1.0";

interface ObjectRevisionSubmissionRow {
  submission_id: string;
  request_id: string;
  tenant_id: string;
  object_id: string;
  edit_revision: number;
  package_schema_version: string;
  package_checksum_sha256: string;
  submitted_by: string | null;
  submission_note: string | null;
  archive_result: JsonObject | null;
  applied_at: Date | null;
  created_at: Date;
  updated_at: Date;
  request_status?: ArchiveRequestStatus;
  request_created_at?: Date;
  request_completed_at?: Date | null;
  request_failure_reason?: string | null;
  source_purged_at?: Date | null;
}

export interface SubmittedObjectRevisionRequestRecord {
  id: string;
  actionType: "object_revision_apply";
  status: ArchiveRequestStatus;
  createdAt: Date;
  requestedBy: string;
}

export interface ObjectRevisionSubmissionRecord {
  submissionId: string;
  requestId: string;
  tenantId: string;
  objectId: string;
  editRevision: number;
  packageSchemaVersion: string;
  packageChecksumSha256: string;
  submittedBy: string | null;
  submissionNote: string | null;
  archiveResult: JsonObject | null;
  appliedAt: Date | null;
  createdAt: Date;
  updatedAt: Date;
  requestStatus?: ArchiveRequestStatus;
  requestCreatedAt?: Date;
  requestCompletedAt?: Date | null;
  requestFailureReason?: string | null;
  sourcePurgedAt?: Date | null;
}

export const objectRevisionPackageSchemaVersion = (): string => PACKAGE_SCHEMA_VERSION;

export function buildObjectRevisionApplyDedupeKey(params: {
  tenantId: string;
  objectId: string;
  editRevision: number;
}): string {
  return `object_revision_apply:${params.tenantId}:${params.objectId}:editrev-${params.editRevision}:pkg-v1`;
}

function mapSubmission(
  row: ObjectRevisionSubmissionRow,
): ObjectRevisionSubmissionRecord {
  return {
    submissionId: row.submission_id,
    requestId: row.request_id,
    tenantId: row.tenant_id,
    objectId: row.object_id,
    editRevision: row.edit_revision,
    packageSchemaVersion: row.package_schema_version,
    packageChecksumSha256: row.package_checksum_sha256,
    submittedBy: row.submitted_by,
    submissionNote: row.submission_note,
    archiveResult: row.archive_result,
    appliedAt: row.applied_at,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    requestStatus: row.request_status,
    requestCreatedAt: row.request_created_at,
    requestCompletedAt: row.request_completed_at,
    requestFailureReason: row.request_failure_reason,
    sourcePurgedAt: row.source_purged_at,
  };
}

export type CreateObjectRevisionSubmissionResult =
  | { status: "not_found" }
  | { status: "unauthorized" }
  | { status: "locked"; lockedBy: string; lockedUntil: Date }
  | { status: "revision_conflict"; latestRevision: number }
  | { status: "deduped"; record: ObjectEditRecord; request: SubmittedObjectRevisionRequestRecord; submission: ObjectRevisionSubmissionRecord }
  | { status: "mutation_active"; request: SubmittedObjectRevisionRequestRecord }
  | { status: "submitted"; record: ObjectEditRecord; request: SubmittedObjectRevisionRequestRecord };

export async function createObjectRevisionSubmission(params: {
  tenantId: string;
  objectId: string;
  actorUserId: string;
  actorRole: UserRole;
  revision: number;
  requestId: string;
  submissionId: string;
  submissionNote: string | null;
  source: {
    storageKey: string;
    contentType: string;
    sizeBytes: number;
    checksumSha256: string;
  };
  actionPayload: JsonObject;
}): Promise<CreateObjectRevisionSubmissionResult> {
  return await withSchemaClient(async (sql) => {
    return await sql.begin(async (transaction) => {
      const currentRow = await selectObjectEditRow(transaction, {
        tenantId: params.tenantId,
        objectId: params.objectId,
        forUpdate: true,
      });

      if (!currentRow) {
        return { status: "not_found" };
      }

      if (!await isObjectEditAuthorized(transaction, {
        tenantId: params.tenantId,
        objectId: params.objectId,
        userId: params.actorUserId,
        role: params.actorRole,
        accessLevel: currentRow.access_level,
      })) {
        return { status: "unauthorized" };
      }

      if (
        currentRow.locked_by &&
        currentRow.locked_until &&
        currentRow.locked_until > new Date() &&
        currentRow.locked_by !== params.actorUserId
      ) {
        return {
          status: "locked",
          lockedBy: currentRow.locked_by,
          lockedUntil: currentRow.locked_until,
        };
      }

      await transaction`
        INSERT INTO object_edits (object_id, revision)
        VALUES (${params.objectId}, 0)
        ON CONFLICT (object_id) DO NOTHING
      `;

      const revisionRows = await transaction<{ revision: number }[]>`
        SELECT revision
        FROM object_edits
        WHERE object_id = ${params.objectId}
        LIMIT 1
      `;
      const currentRevision = revisionRows[0]?.revision ?? 0;
      if (currentRevision !== params.revision) {
        return {
          status: "revision_conflict",
          latestRevision: currentRevision,
        };
      }

      const exactSubmission = await findObjectRevisionSubmissionByIdentityWithExecutor(
        transaction,
        {
          tenantId: params.tenantId,
          objectId: params.objectId,
          editRevision: params.revision,
          packageSchemaVersion: PACKAGE_SCHEMA_VERSION,
        },
      );
      if (exactSubmission) {
        const row = await selectObjectEditRow(transaction, {
          tenantId: params.tenantId,
          objectId: params.objectId,
        });
        return {
          status: "deduped",
          record: mapObjectEdit(row!),
          request: {
            id: exactSubmission.requestId,
            actionType: "object_revision_apply",
            status: exactSubmission.requestStatus ?? "PENDING",
            createdAt: exactSubmission.requestCreatedAt ?? exactSubmission.createdAt,
            requestedBy: exactSubmission.submittedBy ?? params.actorUserId,
          },
          submission: exactSubmission,
        };
      }

      const activeMutation = await findActiveObjectMutationByObjectWithExecutor(
        transaction,
        {
          tenantId: params.tenantId,
          objectId: params.objectId,
        },
      );
      if (activeMutation) {
        return {
          status: "mutation_active",
          request: {
            id: activeMutation.id,
            actionType: "object_revision_apply",
            status: activeMutation.status,
            createdAt: activeMutation.createdAt,
            requestedBy: activeMutation.requestedBy,
          },
        };
      }

      const dedupeKey = buildObjectRevisionApplyDedupeKey({
        tenantId: params.tenantId,
        objectId: params.objectId,
        editRevision: params.revision,
      });

      const createdRequest = await tryCreateArchiveRequestWithExecutor(transaction, {
        requestId: params.requestId,
        tenantId: params.tenantId,
        targetType: "object",
        targetId: params.objectId,
        actionType: "object_revision_apply",
        actionPayload: params.actionPayload,
        requestedBy: params.actorUserId,
        dedupeKey,
      });

      if (!createdRequest) {
        const concurrentMutation = await findActiveObjectMutationByObjectWithExecutor(
          transaction,
          {
            tenantId: params.tenantId,
            objectId: params.objectId,
          },
        );
        if (!concurrentMutation) {
          throw new Error("Active object mutation conflict did not resolve to a request.");
        }

        return {
          status: "mutation_active",
          request: {
            id: concurrentMutation.id,
            actionType: "object_revision_apply",
            status: concurrentMutation.status,
            createdAt: concurrentMutation.createdAt,
            requestedBy: concurrentMutation.requestedBy,
          },
        };
      }

      await createArchiveRequestSourceWithExecutor(transaction, {
        requestId: createdRequest.id,
        tenantId: params.tenantId,
        sourceName: "object_revision_package",
        storageKey: params.source.storageKey,
        contentType: params.source.contentType,
        sizeBytes: params.source.sizeBytes,
        checksumSha256: params.source.checksumSha256,
      });

      await transaction`
        INSERT INTO object_revision_submissions (
          submission_id, request_id, tenant_id, object_id, edit_revision,
          package_schema_version, package_checksum_sha256, submitted_by,
          submission_note
        ) VALUES (
          ${params.submissionId}, ${createdRequest.id}, ${params.tenantId},
          ${params.objectId}, ${params.revision}, ${PACKAGE_SCHEMA_VERSION},
          ${params.source.checksumSha256}, ${params.actorUserId},
          ${params.submissionNote}
        )
      `;

      await transaction`
        INSERT INTO object_edit_events (
          id,
          object_id,
          tenant_id,
          type,
          actor_user_id,
          revision_before,
          revision_after,
          payload
        )
        VALUES (
          ${crypto.randomUUID()},
          ${params.objectId},
          ${params.tenantId},
          ${"CHANGES_SUBMITTED"},
          ${params.actorUserId},
          ${currentRevision},
          ${currentRevision},
          ${<JsonObject>{
            submission_id: params.submissionId,
            request_id: createdRequest.id,
            edit_revision: params.revision,
            package_schema_version: PACKAGE_SCHEMA_VERSION,
            submission_note: params.submissionNote,
          }}
        )
      `;

      const updatedRow = await selectObjectEditRow(transaction, {
        tenantId: params.tenantId,
        objectId: params.objectId,
      });

      return {
        status: "submitted",
        record: mapObjectEdit(updatedRow!),
        request: {
          id: createdRequest.id,
          actionType: "object_revision_apply",
          status: createdRequest.status,
          createdAt: createdRequest.createdAt,
          requestedBy: createdRequest.requestedBy,
        },
      };
    });
  });
}

async function findObjectRevisionSubmissionByIdentityWithExecutor(
  executor: SqlExecutor,
  params: {
    tenantId: string;
    objectId: string;
    editRevision: number;
    packageSchemaVersion: string;
  },
): Promise<ObjectRevisionSubmissionRecord | undefined> {
  const rows = await executor<ObjectRevisionSubmissionRow[]>`
    SELECT submission.*, request.status AS request_status,
           request.created_at AS request_created_at,
           request.completed_at AS request_completed_at,
           request.failure_reason AS request_failure_reason,
           source.purged_at AS source_purged_at
    FROM object_revision_submissions submission
    INNER JOIN archive_requests request ON request.id = submission.request_id
    LEFT JOIN archive_request_sources source ON source.request_id = submission.request_id
    WHERE submission.tenant_id = ${params.tenantId}
      AND submission.object_id = ${params.objectId}
      AND submission.edit_revision = ${params.editRevision}
      AND submission.package_schema_version = ${params.packageSchemaVersion}
    LIMIT 1
  `;

  const row = rows[0];
  return row ? mapSubmission(row) : undefined;
}

export async function findObjectRevisionSubmissionByIdentity(params: {
  tenantId: string;
  objectId: string;
  editRevision: number;
  packageSchemaVersion: string;
}): Promise<ObjectRevisionSubmissionRecord | undefined> {
  return await withSchemaClient(async (sql) => {
    return await findObjectRevisionSubmissionByIdentityWithExecutor(sql, params);
  });
}

export async function findObjectRevisionSubmissionByRequestId(params: {
  requestId: string;
  executor?: SqlExecutor;
}): Promise<ObjectRevisionSubmissionRecord | undefined> {
  const rows = await withExecutor(params.executor, async (sql) => {
    return await sql<ObjectRevisionSubmissionRow[]>`
      SELECT submission.*, request.status AS request_status,
             request.created_at AS request_created_at,
             request.completed_at AS request_completed_at,
             request.failure_reason AS request_failure_reason,
             source.purged_at AS source_purged_at
      FROM object_revision_submissions submission
      INNER JOIN archive_requests request ON request.id = submission.request_id
      LEFT JOIN archive_request_sources source ON source.request_id = submission.request_id
      WHERE submission.request_id = ${params.requestId}
      LIMIT 1
    `;
  });

  const row = rows[0];
  return row ? mapSubmission(row) : undefined;
}

export async function findLatestObjectRevisionSubmission(params: {
  tenantId: string;
  objectId: string;
}): Promise<ObjectRevisionSubmissionRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<ObjectRevisionSubmissionRow[]>`
      SELECT submission.*, request.status AS request_status,
             request.created_at AS request_created_at,
             request.completed_at AS request_completed_at,
             request.failure_reason AS request_failure_reason,
             source.purged_at AS source_purged_at
      FROM object_revision_submissions submission
      INNER JOIN archive_requests request ON request.id = submission.request_id
      LEFT JOIN archive_request_sources source ON source.request_id = submission.request_id
      WHERE submission.tenant_id = ${params.tenantId}
        AND submission.object_id = ${params.objectId}
      ORDER BY
        (request.status IN ('PENDING', 'PROCESSING')) DESC,
        submission.created_at DESC,
        submission.submission_id DESC
      LIMIT 1
    `;
  });

  const row = rows[0];
  return row ? mapSubmission(row) : undefined;
}

export async function findLatestAppliedObjectRevisionSubmission(params: {
  tenantId: string;
  objectId: string;
}): Promise<ObjectRevisionSubmissionRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<ObjectRevisionSubmissionRow[]>`
      SELECT submission.*, request.status AS request_status,
             request.created_at AS request_created_at,
             request.completed_at AS request_completed_at,
             request.failure_reason AS request_failure_reason,
             source.purged_at AS source_purged_at
      FROM object_revision_submissions submission
      INNER JOIN archive_requests request ON request.id = submission.request_id
      LEFT JOIN archive_request_sources source ON source.request_id = submission.request_id
      WHERE submission.tenant_id = ${params.tenantId}
        AND submission.object_id = ${params.objectId}
        AND request.status = 'COMPLETED'
        AND submission.applied_at IS NOT NULL
      ORDER BY submission.edit_revision DESC
      LIMIT 1
    `;
  });

  const row = rows[0];
  return row ? mapSubmission(row) : undefined;
}

export async function findLatestActiveObjectRevisionSubmission(params: {
  tenantId: string;
  objectId: string;
}): Promise<ObjectRevisionSubmissionRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<ObjectRevisionSubmissionRow[]>`
      SELECT submission.*, request.status AS request_status,
             request.created_at AS request_created_at,
             request.completed_at AS request_completed_at,
             request.failure_reason AS request_failure_reason,
             source.purged_at AS source_purged_at
      FROM object_revision_submissions submission
      INNER JOIN archive_requests request ON request.id = submission.request_id
      LEFT JOIN archive_request_sources source ON source.request_id = submission.request_id
      WHERE submission.tenant_id = ${params.tenantId}
        AND submission.object_id = ${params.objectId}
        AND request.status IN ('PENDING', 'PROCESSING')
      ORDER BY submission.created_at DESC, submission.submission_id DESC
      LIMIT 1
    `;
  });

  const row = rows[0];
  return row ? mapSubmission(row) : undefined;
}

export type CompleteObjectRevisionApplyResult =
  | { status: "not_found" }
  | { status: "lease_inactive" }
  | { status: "result_mismatch"; reason: string }
  | { status: "already_completed"; record: ObjectRevisionSubmissionRecord }
  | { status: "completed"; record: ObjectRevisionSubmissionRecord };

export async function completeObjectRevisionApply(params: {
  requestId: string;
  leaseId: string;
  leaseTokenId: string;
  result: {
    submissionId: string;
    objectId: string;
    objectRevision: number;
    packageSha256: string;
    archiveRevisionId: string;
    appliedAt: string;
  };
}): Promise<CompleteObjectRevisionApplyResult> {
  return await withSchemaClient(async (sql) => {
    return await sql.begin(async (transaction) => {
      const requestRows = await transaction<Array<{
        id: string;
        tenant_id: string;
        target_id: string;
        action_type: string;
        status: ArchiveRequestStatus;
        lease_id: string | null;
        lease_token_id: string | null;
        released_at: Date | null;
        lease_expires_at: Date | null;
      }>>`
        SELECT id, tenant_id, target_id, action_type, status, lease_id,
               lease_token_id, released_at, lease_expires_at
        FROM archive_requests
        WHERE id = ${params.requestId}
        FOR UPDATE
        LIMIT 1
      `;
      const request = requestRows[0];
      if (!request) {
        return { status: "not_found" };
      }
      if (request.action_type !== "object_revision_apply") {
        return { status: "result_mismatch", reason: "request action is not object_revision_apply" };
      }

      const submission = await findObjectRevisionSubmissionByRequestId({
        requestId: params.requestId,
        executor: transaction,
      });
      if (!submission) {
        return { status: "not_found" };
      }

      if (submission.requestStatus === "COMPLETED" && submission.appliedAt) {
        return { status: "already_completed", record: submission };
      }

      if (
        request.status !== "PROCESSING" ||
        request.lease_id !== params.leaseId ||
        request.lease_token_id !== params.leaseTokenId ||
        request.released_at !== null ||
        (request.lease_expires_at !== null && request.lease_expires_at <= new Date())
      ) {
        return { status: "lease_inactive" };
      }

      const mismatchReason = (() => {
        if (params.result.submissionId !== submission.submissionId) {
          return "submission_id does not match the request submission";
        }
        if (params.result.objectId !== submission.objectId || params.result.objectId !== request.target_id) {
          return "object_id does not match the request target";
        }
        if (params.result.objectRevision !== submission.editRevision) {
          return "object_revision does not match the submission revision";
        }
        if (params.result.packageSha256 !== submission.packageChecksumSha256) {
          return "package_sha256 does not match the submission package";
        }
        return null;
      })();
      if (mismatchReason) {
        return { status: "result_mismatch", reason: mismatchReason };
      }

      const archiveResult = <JsonObject>{
        schema_version: "1.0",
        submission_id: params.result.submissionId,
        object_id: params.result.objectId,
        object_revision: params.result.objectRevision,
        package_sha256: params.result.packageSha256,
        disposition: "applied",
        archive_revision_id: params.result.archiveRevisionId,
        applied_at: params.result.appliedAt,
      };

      await transaction`
        UPDATE object_revision_submissions
        SET archive_result = ${archiveResult},
            applied_at = now(),
            updated_at = now()
        WHERE request_id = ${params.requestId}
      `;

      await transaction`
        UPDATE archive_requests
        SET status = 'COMPLETED',
            completed_at = COALESCE(completed_at, now()),
            released_at = now(),
            lease_expires_at = NULL,
            failure_reason = NULL,
            failure_details = NULL,
            updated_at = now()
        WHERE id = ${params.requestId}
      `;

      await transaction`
        UPDATE archive_request_sources
        SET cleanup_eligible_at = COALESCE(cleanup_eligible_at, now() + interval '24 hours')
        WHERE request_id = ${params.requestId}
      `;

      await transaction`
        INSERT INTO object_edit_events (
          id,
          object_id,
          tenant_id,
          type,
          actor_user_id,
          revision_before,
          revision_after,
          payload
        )
        VALUES (
          ${crypto.randomUUID()},
          ${submission.objectId},
          ${submission.tenantId},
          ${"CHANGES_SYNCHRONIZED"},
          ${submission.submittedBy},
          ${submission.editRevision},
          ${submission.editRevision},
          ${<JsonObject>{
            submission_id: submission.submissionId,
            request_id: submission.requestId,
            edit_revision: submission.editRevision,
            archive_revision_id: params.result.archiveRevisionId,
          }}
        )
      `;

      const completed = await findObjectRevisionSubmissionByRequestId({
        requestId: params.requestId,
        executor: transaction,
      });

      return {
        status: "completed",
        record: completed ?? submission,
      };
    });
  });
}

export type RetryObjectRevisionSubmissionResult =
  | { status: "not_found" }
  | { status: "unauthorized" }
  | { status: "locked"; lockedBy: string; lockedUntil: Date }
  | { status: "superseded"; latestAppliedRevision: number }
  | { status: "source_unavailable" }
  | { status: "mutation_active" }
  | { status: "not_retryable" }
  | { status: "unchanged"; record: ObjectRevisionSubmissionRecord }
  | { status: "requeued"; record: ObjectRevisionSubmissionRecord };

export async function retryObjectRevisionSubmission(params: {
  tenantId: string;
  objectId: string;
  requestId: string;
  actorUserId: string;
  actorRole: UserRole;
  retryReason: string | null;
}): Promise<RetryObjectRevisionSubmissionResult> {
  return await withSchemaClient(async (sql) => {
    return await sql.begin(async (transaction) => {
      const currentRow = await selectObjectEditRow(transaction, {
        tenantId: params.tenantId,
        objectId: params.objectId,
        forUpdate: true,
      });

      if (!currentRow) {
        return { status: "not_found" };
      }

      if (!await isObjectEditAuthorized(transaction, {
        tenantId: params.tenantId,
        objectId: params.objectId,
        userId: params.actorUserId,
        role: params.actorRole,
        accessLevel: currentRow.access_level,
      })) {
        return { status: "unauthorized" };
      }

      if (
        currentRow.locked_by &&
        currentRow.locked_until &&
        currentRow.locked_until > new Date() &&
        currentRow.locked_by !== params.actorUserId
      ) {
        return {
          status: "locked",
          lockedBy: currentRow.locked_by,
          lockedUntil: currentRow.locked_until,
        };
      }

      const submission = await findObjectRevisionSubmissionByRequestId({
        requestId: params.requestId,
        executor: transaction,
      });
      if (!submission || submission.objectId !== params.objectId || submission.tenantId !== params.tenantId) {
        return { status: "not_found" };
      }

      if (
        submission.requestStatus === "PENDING" ||
        submission.requestStatus === "PROCESSING" ||
        submission.requestStatus === "COMPLETED"
      ) {
        return { status: "unchanged", record: submission };
      }

      if (submission.requestStatus !== "FAILED") {
        return { status: "not_retryable" };
      }

      const failureDetails = await transaction<Array<{ failure_details: JsonObject | null }>>`
        SELECT failure_details
        FROM archive_requests
        WHERE id = ${params.requestId}
        LIMIT 1
      `;
      const retryable = failureDetails[0]?.failure_details?.retryable === true;
      if (!retryable) {
        return { status: "not_retryable" };
      }

      const latestApplied = await findLatestAppliedObjectRevisionSubmission({
        tenantId: params.tenantId,
        objectId: params.objectId,
      });
      if (latestApplied && latestApplied.editRevision > submission.editRevision) {
        return { status: "superseded", latestAppliedRevision: latestApplied.editRevision };
      }

      if (submission.sourcePurgedAt) {
        return { status: "source_unavailable" };
      }

      const activeMutation = await findActiveObjectMutationByObjectWithExecutor(
        transaction,
        {
          tenantId: params.tenantId,
          objectId: params.objectId,
        },
      );
      if (activeMutation && activeMutation.id !== params.requestId) {
        return { status: "mutation_active" };
      }

      const requeuedRows = await transaction<Array<{ id: string }>>`
        UPDATE archive_requests
        SET status = 'PENDING',
            released_at = now(),
            lease_expires_at = NULL,
            failure_reason = NULL,
            failure_details = NULL,
            updated_at = now()
        WHERE id = ${params.requestId}
          AND status = 'FAILED'
        RETURNING id
      `;
      if (requeuedRows.length === 0) {
        return { status: "not_retryable" };
      }

      await transaction`
        UPDATE archive_request_sources
        SET cleanup_eligible_at = NULL,
            cleanup_claim_id = NULL,
            cleanup_claimed_at = NULL,
            cleanup_attempt_count = 0,
            cleanup_next_attempt_at = NULL,
            cleanup_last_error = NULL
        WHERE request_id = ${params.requestId}
      `;

      await transaction`
        INSERT INTO object_edit_events (
          id,
          object_id,
          tenant_id,
          type,
          actor_user_id,
          revision_before,
          revision_after,
          payload
        )
        VALUES (
          ${crypto.randomUUID()},
          ${params.objectId},
          ${params.tenantId},
          ${"CHANGES_RETRY_REQUESTED"},
          ${params.actorUserId},
          ${submission.editRevision},
          ${submission.editRevision},
          ${<JsonObject>{
            submission_id: submission.submissionId,
            request_id: params.requestId,
            edit_revision: submission.editRevision,
            retry_reason: params.retryReason,
          }}
        )
      `;

      const updated = await findObjectRevisionSubmissionByRequestId({
        requestId: params.requestId,
        executor: transaction,
      });

      return {
        status: "requeued",
        record: updated ?? submission,
      };
    });
  });
}
