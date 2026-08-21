import { withExecutor, withSchemaClient, type SqlExecutor } from "../db/client.ts";
import type { JsonObject } from "../validation/ingestion.ts";

export type ArchiveRequestSqlExecutor = SqlExecutor;

export type ArchiveRequestStatus =
    | "PENDING"
    | "PROCESSING"
    | "COMPLETED"
    | "FAILED"
    | "CANCELED";

export type ArchiveRequestTargetType = "object" | "ingestion";

export type ArchiveRequestActionType =
    | "object_resync"
    | "artifact_fetch"
    | "curation_apply";

interface ArchiveRequestRow {
    id: string;
    tenant_id: string;
    target_type: ArchiveRequestTargetType;
    target_id: string;
    action_type: ArchiveRequestActionType;
    action_payload: JsonObject;
    requested_by: string;
    dedupe_key: string | null;
    status: ArchiveRequestStatus;
    failure_reason: string | null;
    failure_details: JsonObject | null;
    lease_id: string | null;
    lease_token_id: string | null;
    lease_expires_at: Date | null;
    leased_by: string | null;
    released_at: Date | null;
    created_at: Date;
    updated_at: Date;
    completed_at: Date | null;
    artifact_upload_token_id?: string | null;
    artifact_upload_storage_key?: string | null;
    artifact_upload_content_type?: string | null;
    artifact_upload_size_bytes?: bigint | null;
    artifact_upload_checksum_sha256?: string | null;
}

export interface ArchiveRequestRecord {
    id: string;
    tenantId: string;
    targetType: ArchiveRequestTargetType;
    targetId: string;
    actionType: ArchiveRequestActionType;
    actionPayload: JsonObject;
    requestedBy: string;
    dedupeKey: string | null;
    status: ArchiveRequestStatus;
    failureReason: string | null;
    failureDetails: JsonObject | null;
    leaseId: string | null;
    leaseTokenId: string | null;
    leaseExpiresAt: Date | null;
    leasedBy: string | null;
    releasedAt: Date | null;
    createdAt: Date;
    updatedAt: Date;
    completedAt: Date | null;
}

export interface LeasedArchiveRequestRecord {
    request: ArchiveRequestRecord;
    leaseId: string;
    leaseTokenId: string;
    leaseExpiresAt: Date;
}

export interface ArchiveArtifactUploadRecord {
    requestId: string;
    tenantId: string;
    targetId: string;
    actionType: ArchiveRequestActionType;
    status: ArchiveRequestStatus;
    uploadTokenId?: string;
    storageKey?: string;
    contentType?: string;
    sizeBytes?: number;
    checksumSha256?: string;
}

interface CountRow {
    count: number;
}

export interface ListArchiveRequestsParams {
    tenantId: string;
    limit: number;
    cursorCreatedAt?: string;
    cursorRequestId?: string;
    targetType?: ArchiveRequestTargetType;
    targetId?: string;
    actionType?: ArchiveRequestActionType;
    statuses?: ArchiveRequestStatus[];
}

export interface ListArchiveRequestsResult {
    requests: ArchiveRequestRecord[];
    filteredCount: number;
}

export interface CreateArchiveRequestParams {
    tenantId: string;
    targetType: ArchiveRequestTargetType;
    targetId: string;
    actionType: ArchiveRequestActionType;
    actionPayload: JsonObject;
    requestedBy: string;
    dedupeKey?: string | null;
    requestId?: string;
}

export interface FindActiveArchiveRequestByDedupeKeyParams {
    tenantId: string;
    actionType: ArchiveRequestActionType;
    dedupeKey: string;
}

export interface FindActiveCurationApplyByObjectParams {
    tenantId: string;
    objectId: string;
}

function mapArchiveRequest(row: ArchiveRequestRow): ArchiveRequestRecord {
    return {
        id: row.id,
        tenantId: row.tenant_id,
        targetType: row.target_type,
        targetId: row.target_id,
        actionType: row.action_type,
        actionPayload: row.action_payload,
        requestedBy: row.requested_by,
        dedupeKey: row.dedupe_key,
        status: row.status,
        failureReason: row.failure_reason,
        failureDetails: row.failure_details,
        leaseId: row.lease_id,
        leaseTokenId: row.lease_token_id,
        leaseExpiresAt: row.lease_expires_at,
        leasedBy: row.leased_by,
        releasedAt: row.released_at,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
        completedAt: row.completed_at,
    };
}

export async function createArchiveRequestWithExecutor(
    executor: ArchiveRequestSqlExecutor,
    params: CreateArchiveRequestParams,
): Promise<ArchiveRequestRecord> {
    const rows = await executor<ArchiveRequestRow[]>`
      INSERT INTO archive_requests (
        id,
        tenant_id,
        target_type,
        target_id,
        action_type,
        action_payload,
        requested_by,
        dedupe_key,
        status
      )
      VALUES (
        ${params.requestId ?? crypto.randomUUID()},
        ${params.tenantId},
        ${params.targetType}::archive_request_target_type,
        ${params.targetId},
        ${params.actionType}::archive_request_action_type,
        ${params.actionPayload},
        ${params.requestedBy},
        ${params.dedupeKey ?? null},
        'PENDING'
      )
      RETURNING id, tenant_id, target_type, target_id, action_type, action_payload,
                requested_by, dedupe_key, status, failure_reason, failure_details,
                lease_id, lease_token_id, lease_expires_at, leased_by, released_at,
                created_at, updated_at, completed_at
    `;

    return mapArchiveRequest(rows[0]!);
}

export async function tryCreateCurationApplyArchiveRequestWithExecutor(
    executor: ArchiveRequestSqlExecutor,
    params: CreateArchiveRequestParams & { actionType: "curation_apply" },
): Promise<ArchiveRequestRecord | undefined> {
    const rows = await executor<ArchiveRequestRow[]>`
      INSERT INTO archive_requests (
        id, tenant_id, target_type, target_id, action_type, action_payload,
        requested_by, dedupe_key, status
      )
      VALUES (
        ${params.requestId ?? crypto.randomUUID()},
        ${params.tenantId},
        ${params.targetType}::archive_request_target_type,
        ${params.targetId},
        ${params.actionType}::archive_request_action_type,
        ${params.actionPayload},
        ${params.requestedBy},
        ${params.dedupeKey ?? null},
        'PENDING'
      )
      ON CONFLICT DO NOTHING
      RETURNING id, tenant_id, target_type, target_id, action_type, action_payload,
                requested_by, dedupe_key, status, failure_reason, failure_details,
                lease_id, lease_token_id, lease_expires_at, leased_by, released_at,
                created_at, updated_at, completed_at
    `;

    const row = rows[0];
    return row ? mapArchiveRequest(row) : undefined;
}

export async function createArchiveRequest(
    params: CreateArchiveRequestParams,
): Promise<ArchiveRequestRecord> {
    return await withSchemaClient(async (sql) => {
        return await createArchiveRequestWithExecutor(sql, params);
    });
}

export async function findArchiveRequestById(params: {
    requestId: string;
    executor?: SqlExecutor;
}): Promise<ArchiveRequestRecord | undefined> {
    const rows = await withExecutor(params.executor, async (sql) => {
        return await sql<ArchiveRequestRow[]>`
      SELECT id, tenant_id, target_type, target_id, action_type, action_payload,
             requested_by, dedupe_key, status, failure_reason, failure_details,
             lease_id, lease_token_id, lease_expires_at, leased_by, released_at,
             created_at, updated_at, completed_at
      FROM archive_requests
      WHERE id = ${params.requestId}
      LIMIT 1
    `;
    });

    const row = rows[0];
    return row ? mapArchiveRequest(row) : undefined;
}

export async function findArchiveRequestByIdForUpdate(params: {
    requestId: string;
    executor: SqlExecutor;
}): Promise<ArchiveRequestRecord | undefined> {
    const rows = await params.executor<ArchiveRequestRow[]>`
      SELECT id, tenant_id, target_type, target_id, action_type, action_payload,
             requested_by, dedupe_key, status, failure_reason, failure_details,
             lease_id, lease_token_id, lease_expires_at, leased_by, released_at,
             created_at, updated_at, completed_at
      FROM archive_requests
      WHERE id = ${params.requestId}
      FOR UPDATE
    `;
    const row = rows[0];
    return row ? mapArchiveRequest(row) : undefined;
}

export async function findActiveArchiveRequestByDedupeKeyWithExecutor(
    executor: ArchiveRequestSqlExecutor,
    params: FindActiveArchiveRequestByDedupeKeyParams,
): Promise<ArchiveRequestRecord | undefined> {
    const rows = await executor<ArchiveRequestRow[]>`
      SELECT id, tenant_id, target_type, target_id, action_type, action_payload,
             requested_by, dedupe_key, status, failure_reason, failure_details,
             lease_id, lease_token_id, lease_expires_at, leased_by, released_at,
             created_at, updated_at, completed_at
      FROM archive_requests req
      WHERE req.tenant_id = ${params.tenantId}
        AND req.action_type = ${params.actionType}::archive_request_action_type
        AND req.dedupe_key = ${params.dedupeKey}
        AND req.status IN ('PENDING', 'PROCESSING')
      ORDER BY req.created_at DESC, req.id DESC
      LIMIT 1
    `;

    const row = rows[0];
    return row ? mapArchiveRequest(row) : undefined;
}

export async function findActiveArchiveRequestByDedupeKey(
    params: FindActiveArchiveRequestByDedupeKeyParams,
): Promise<ArchiveRequestRecord | undefined> {
    return await withSchemaClient(async (sql) => {
        return await findActiveArchiveRequestByDedupeKeyWithExecutor(
            sql,
            params,
        );
    });
}

export async function findActiveCurationApplyByObjectWithExecutor(
    executor: ArchiveRequestSqlExecutor,
    params: FindActiveCurationApplyByObjectParams,
): Promise<ArchiveRequestRecord | undefined> {
    const rows = await executor<ArchiveRequestRow[]>`
      SELECT id, tenant_id, target_type, target_id, action_type, action_payload,
             requested_by, dedupe_key, status, failure_reason, failure_details,
             lease_id, lease_token_id, lease_expires_at, leased_by, released_at,
             created_at, updated_at, completed_at
      FROM archive_requests req
      WHERE req.tenant_id = ${params.tenantId}
        AND req.target_type = 'object'
        AND req.target_id = ${params.objectId}
        AND req.action_type = 'curation_apply'
        AND req.status IN ('PENDING', 'PROCESSING')
      ORDER BY req.created_at DESC, req.id DESC
      LIMIT 1
    `;

    const row = rows[0];
    return row ? mapArchiveRequest(row) : undefined;
}

export async function findCurrentCurationApplyByObject(params: {
    tenantId: string;
    objectId: string;
}): Promise<ArchiveRequestRecord | undefined> {
    return await withSchemaClient(async (sql) => {
        const rows = await sql<ArchiveRequestRow[]>`
          SELECT id, tenant_id, target_type, target_id, action_type, action_payload,
                 requested_by, dedupe_key, status, failure_reason, failure_details,
                 lease_id, lease_token_id, lease_expires_at, leased_by, released_at,
                 created_at, updated_at, completed_at
          FROM archive_requests req
          WHERE req.tenant_id = ${params.tenantId}
            AND req.target_type = 'object'
            AND req.target_id = ${params.objectId}
            AND req.action_type = 'curation_apply'
          ORDER BY
            (req.status IN ('PENDING', 'PROCESSING')) DESC,
            req.created_at DESC,
            req.id DESC
          LIMIT 1
        `;

        const row = rows[0];
        return row ? mapArchiveRequest(row) : undefined;
    });
}

export async function listArchiveRequestsByTarget(params: {
    tenantId: string;
    targetType: ArchiveRequestTargetType;
    targetId: string;
}): Promise<ArchiveRequestRecord[]> {
    const rows = await withSchemaClient(async (sql) => {
        return await sql<ArchiveRequestRow[]>`
      SELECT id, tenant_id, target_type, target_id, action_type, action_payload,
             requested_by, dedupe_key, status, failure_reason, failure_details,
             lease_id, lease_token_id, lease_expires_at, leased_by, released_at,
             created_at, updated_at, completed_at
      FROM archive_requests req
      WHERE req.tenant_id = ${params.tenantId}
        AND req.target_type = ${params.targetType}::archive_request_target_type
        AND req.target_id = ${params.targetId}
      ORDER BY req.created_at DESC, req.id DESC
    `;
    });

    return rows.map(mapArchiveRequest);
}

export async function listArchiveRequests(
    params: ListArchiveRequestsParams,
): Promise<ListArchiveRequestsResult> {
    return await withSchemaClient(async (sql) => {
        const hasStatuses = (params.statuses?.length ?? 0) > 0;
        const statuses = hasStatuses ? params.statuses! : ["PENDING"];

        const filteredCountRows = await sql<CountRow[]>`
      SELECT COUNT(*)::int AS count
      FROM archive_requests req
      WHERE req.tenant_id = ${params.tenantId}
        AND (${params.targetType ?? null}::archive_request_target_type IS NULL OR req.target_type = ${params.targetType ?? null}::archive_request_target_type)
        AND (${params.targetId ?? null}::text IS NULL OR req.target_id = ${params.targetId ?? null}::text)
        AND (${params.actionType ?? null}::archive_request_action_type IS NULL OR req.action_type = ${params.actionType ?? null}::archive_request_action_type)
        AND (
          ${hasStatuses}::boolean = false
          OR req.status IN ${sql(statuses)}
        )
    `;

        const rows = await sql<ArchiveRequestRow[]>`
      SELECT id, tenant_id, target_type, target_id, action_type, action_payload,
             requested_by, dedupe_key, status, failure_reason, failure_details,
             lease_id, lease_token_id, lease_expires_at, leased_by, released_at,
             created_at, updated_at, completed_at
      FROM archive_requests req
      WHERE req.tenant_id = ${params.tenantId}
        AND (${params.targetType ?? null}::archive_request_target_type IS NULL OR req.target_type = ${params.targetType ?? null}::archive_request_target_type)
        AND (${params.targetId ?? null}::text IS NULL OR req.target_id = ${params.targetId ?? null}::text)
        AND (${params.actionType ?? null}::archive_request_action_type IS NULL OR req.action_type = ${params.actionType ?? null}::archive_request_action_type)
        AND (
          ${hasStatuses}::boolean = false
          OR req.status IN ${sql(statuses)}
        )
        AND (
          (${params.cursorCreatedAt ?? null}::timestamptz IS NULL OR ${params.cursorRequestId ?? null}::uuid IS NULL)
          OR (req.created_at, req.id) < (${params.cursorCreatedAt ?? null}::timestamptz, ${params.cursorRequestId ?? null}::uuid)
        )
      ORDER BY req.created_at DESC, req.id DESC
      LIMIT ${params.limit}
    `;

        return {
            requests: rows.map(mapArchiveRequest),
            filteredCount: filteredCountRows[0]?.count ?? 0,
        };
    });
}

export async function leaseNextPendingArchiveRequest(params: {
    workerId?: string;
    leaseDurationSeconds: number;
    actionType?: ArchiveRequestActionType;
}): Promise<LeasedArchiveRequestRecord | undefined> {
    return await withSchemaClient(async (sql) => {
        return await sql.begin(async (transaction) => {
            const candidates = await transaction<ArchiveRequestRow[]>`
        SELECT req.id, req.tenant_id, req.target_type, req.target_id, req.action_type,
               req.action_payload, req.requested_by, req.dedupe_key, req.status,
               req.failure_reason, req.failure_details, req.lease_id, req.lease_token_id,
               req.lease_expires_at, req.leased_by, req.released_at,
               req.created_at, req.updated_at, req.completed_at
        FROM archive_requests req
        WHERE req.status = 'PENDING'
          AND (${params.actionType ?? null}::archive_request_action_type IS NULL OR req.action_type = ${params.actionType ?? null}::archive_request_action_type)
        ORDER BY req.updated_at ASC, req.created_at ASC, req.id ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1
      `;

            const candidate = candidates[0];
            if (!candidate) {
                return undefined;
            }

            const leaseId = crypto.randomUUID();
            const leaseTokenId = crypto.randomUUID();

            const updatedRows = await transaction<ArchiveRequestRow[]>`
        UPDATE archive_requests req
        SET status = 'PROCESSING',
            lease_id = ${leaseId},
            lease_token_id = ${leaseTokenId},
            lease_expires_at = now() + (${params.leaseDurationSeconds}::int * interval '1 second'),
            leased_by = ${params.workerId ?? null},
            released_at = NULL,
            updated_at = now()
        WHERE req.id = ${candidate.id}
          AND req.status = 'PENDING'
        RETURNING id, tenant_id, target_type, target_id, action_type, action_payload,
                  requested_by, dedupe_key, status, failure_reason, failure_details,
                  lease_id, lease_token_id, lease_expires_at, leased_by, released_at,
                  created_at, updated_at, completed_at
      `;

            const updated = updatedRows[0];
            if (
                !updated ||
                !updated.lease_id ||
                !updated.lease_token_id ||
                !updated.lease_expires_at
            ) {
                return undefined;
            }

            return {
                request: mapArchiveRequest(updated),
                leaseId: updated.lease_id,
                leaseTokenId: updated.lease_token_id,
                leaseExpiresAt: updated.lease_expires_at,
            };
        });
    });
}

export async function extendArchiveRequestLease(params: {
    requestId: string;
    leaseId: string;
    leaseTokenId: string;
    leaseDurationSeconds: number;
}): Promise<LeasedArchiveRequestRecord | undefined> {
    const rows = await withSchemaClient(async (sql) => {
        return await sql<ArchiveRequestRow[]>`
      UPDATE archive_requests req
      SET lease_expires_at = now() + (${params.leaseDurationSeconds}::int * interval '1 second'),
          updated_at = now()
      WHERE req.id = ${params.requestId}
        AND req.status = 'PROCESSING'
        AND req.lease_id = ${params.leaseId}
        AND req.lease_token_id = ${params.leaseTokenId}
        AND req.released_at IS NULL
        AND req.lease_expires_at > now()
      RETURNING id, tenant_id, target_type, target_id, action_type, action_payload,
                requested_by, dedupe_key, status, failure_reason, failure_details,
                lease_id, lease_token_id, lease_expires_at, leased_by, released_at,
                created_at, updated_at, completed_at
    `;
    });

    const row = rows[0];
    if (!row || !row.lease_id || !row.lease_token_id || !row.lease_expires_at) {
        return undefined;
    }

    return {
        request: mapArchiveRequest(row),
        leaseId: row.lease_id,
        leaseTokenId: row.lease_token_id,
        leaseExpiresAt: row.lease_expires_at,
    };
}

export async function releaseArchiveRequestLease(params: {
    requestId: string;
    leaseId: string;
    leaseTokenId: string;
}): Promise<ArchiveRequestRecord | undefined> {
    return await withSchemaClient(async (sql) => sql.begin(async (transaction) => {
        const rows = await transaction<ArchiveRequestRow[]>`
      UPDATE archive_requests req
      SET status = 'PENDING',
          released_at = now(),
          lease_expires_at = NULL,
          updated_at = now()
      WHERE req.id = ${params.requestId}
        AND req.status = 'PROCESSING'
        AND req.lease_id = ${params.leaseId}
        AND req.lease_token_id = ${params.leaseTokenId}
        AND req.released_at IS NULL
      RETURNING id, tenant_id, target_type, target_id, action_type, action_payload,
                requested_by, dedupe_key, status, failure_reason, failure_details,
                lease_id, lease_token_id, lease_expires_at, leased_by, released_at,
                created_at, updated_at, completed_at
    `;
        const row = rows[0];
        if (!row) return undefined;
        await transaction`
          UPDATE archive_artifact_upload_attempts
          SET invalidated_at = now(), updated_at = now()
          WHERE request_id = ${params.requestId}
            AND state = 'AUTHORIZED'
            AND invalidated_at IS NULL
        `;
        return mapArchiveRequest(row);
    }));
}

export async function completeArchiveRequest(params: {
    requestId: string;
    leaseId: string;
    leaseTokenId: string;
    executor?: SqlExecutor;
}): Promise<ArchiveRequestRecord | undefined> {
    const rows = await withExecutor(params.executor, async (sql) => {
        return await sql<ArchiveRequestRow[]>`
      WITH completed AS (
        UPDATE archive_requests req
        SET status = 'COMPLETED',
            completed_at = COALESCE(req.completed_at, now()),
            released_at = now(),
            lease_expires_at = NULL,
            failure_reason = NULL,
            failure_details = NULL,
            updated_at = now()
        WHERE req.id = ${params.requestId}
          AND req.status = 'PROCESSING'
          AND req.lease_id = ${params.leaseId}
          AND req.lease_token_id = ${params.leaseTokenId}
          AND req.released_at IS NULL
          AND req.lease_expires_at > now()
        RETURNING id, tenant_id, target_type, target_id, action_type, action_payload,
                  requested_by, dedupe_key, status, failure_reason, failure_details,
                  lease_id, lease_token_id, lease_expires_at, leased_by, released_at,
                  created_at, updated_at, completed_at
      ), cleanup AS (
        UPDATE curation_publications publication
        SET cleanup_eligible_at = COALESCE(
              publication.cleanup_eligible_at,
              now() + interval '24 hours'
            )
        FROM completed
        WHERE publication.request_id = completed.id
        RETURNING publication.request_id
      )
      SELECT * FROM completed
    `;
    });

    const row = rows[0];
    return row ? mapArchiveRequest(row) : undefined;
}

export async function transferArchiveRequestUploadToBackend(params: {
    requestId: string;
    leaseId: string;
    leaseTokenId: string;
    executor: SqlExecutor;
}): Promise<ArchiveRequestRecord | undefined> {
    const rows = await params.executor<ArchiveRequestRow[]>`
      UPDATE archive_requests req
      SET released_at = now(),
          lease_expires_at = NULL,
          updated_at = now()
      WHERE req.id = ${params.requestId}
        AND req.action_type = 'artifact_fetch'
        AND req.status = 'PROCESSING'
        AND req.lease_id = ${params.leaseId}
        AND req.lease_token_id = ${params.leaseTokenId}
        AND req.released_at IS NULL
        AND req.lease_expires_at > now()
      RETURNING id, tenant_id, target_type, target_id, action_type, action_payload,
                requested_by, dedupe_key, status, failure_reason, failure_details,
                lease_id, lease_token_id, lease_expires_at, leased_by, released_at,
                created_at, updated_at, completed_at
    `;
    const row = rows[0];
    return row ? mapArchiveRequest(row) : undefined;
}

export async function completeVerifiedArtifactRequest(params: {
    requestId: string;
    executor: SqlExecutor;
}): Promise<ArchiveRequestRecord | undefined> {
    const rows = await params.executor<ArchiveRequestRow[]>`
      UPDATE archive_requests req
      SET status = 'COMPLETED',
          completed_at = COALESCE(req.completed_at, now()),
          released_at = COALESCE(req.released_at, now()),
          lease_expires_at = NULL,
          failure_reason = NULL,
          failure_details = NULL,
          updated_at = now()
      WHERE req.id = ${params.requestId}
        AND req.action_type = 'artifact_fetch'
        AND req.status = 'PROCESSING'
      RETURNING id, tenant_id, target_type, target_id, action_type, action_payload,
                requested_by, dedupe_key, status, failure_reason, failure_details,
                lease_id, lease_token_id, lease_expires_at, leased_by, released_at,
                created_at, updated_at, completed_at
    `;
    const row = rows[0];
    return row ? mapArchiveRequest(row) : undefined;
}

export async function prepareArchiveArtifactUpload(params: {
    requestId: string;
    leaseId: string;
    leaseTokenId: string;
    uploadTokenId: string;
    storageKey: string;
    contentType: string;
    sizeBytes: number;
}): Promise<boolean> {
    const rows = await withSchemaClient(async (sql) => {
        return await sql<Array<{ id: string }>>`
      UPDATE archive_requests
      SET artifact_upload_token_id = ${params.uploadTokenId},
          artifact_upload_storage_key = ${params.storageKey},
          artifact_upload_content_type = ${params.contentType},
          artifact_upload_size_bytes = ${params.sizeBytes},
          artifact_upload_checksum_sha256 = NULL,
          updated_at = now()
      WHERE id = ${params.requestId}
        AND action_type = 'artifact_fetch'
        AND status = 'PROCESSING'
        AND lease_id = ${params.leaseId}
        AND lease_token_id = ${params.leaseTokenId}
      RETURNING id
    `;
    });

    return rows.length > 0;
}

export async function findArchiveArtifactUpload(params: {
    requestId: string;
}): Promise<ArchiveArtifactUploadRecord | undefined> {
    const rows = await withSchemaClient(async (sql) => {
        return await sql<Array<{
            id: string;
            tenant_id: string;
            target_id: string;
            action_type: ArchiveRequestActionType;
            status: ArchiveRequestStatus;
            artifact_upload_token_id: string | null;
            artifact_upload_storage_key: string | null;
            artifact_upload_content_type: string | null;
            artifact_upload_size_bytes: bigint | null;
            artifact_upload_checksum_sha256: string | null;
        }>>`
      SELECT id, tenant_id, target_id, action_type, status,
             artifact_upload_token_id, artifact_upload_storage_key,
             artifact_upload_content_type, artifact_upload_size_bytes,
             artifact_upload_checksum_sha256
      FROM archive_requests
      WHERE id = ${params.requestId}
      LIMIT 1
    `;
    });
    const row = rows[0];

    if (!row) {
        return undefined;
    }

    return {
        requestId: row.id,
        tenantId: row.tenant_id,
        targetId: row.target_id,
        actionType: row.action_type,
        status: row.status,
        uploadTokenId: row.artifact_upload_token_id ?? undefined,
        storageKey: row.artifact_upload_storage_key ?? undefined,
        contentType: row.artifact_upload_content_type ?? undefined,
        sizeBytes: row.artifact_upload_size_bytes === null
            ? undefined
            : Number(row.artifact_upload_size_bytes),
        checksumSha256: row.artifact_upload_checksum_sha256 ?? undefined,
    };
}

export async function recordArchiveArtifactUpload(params: {
    requestId: string;
    uploadTokenId: string;
    storageKey: string;
    checksumSha256: string;
}): Promise<boolean> {
    const rows = await withSchemaClient(async (sql) => {
        return await sql<Array<{ id: string }>>`
      UPDATE archive_requests
      SET artifact_upload_checksum_sha256 = ${params.checksumSha256},
          updated_at = now()
      WHERE id = ${params.requestId}
        AND action_type = 'artifact_fetch'
        AND status = 'PROCESSING'
        AND artifact_upload_token_id = ${params.uploadTokenId}
        AND artifact_upload_storage_key = ${params.storageKey}
      RETURNING id
    `;
    });

    return rows.length > 0;
}

export async function failArchiveRequest(params: {
    requestId: string;
    leaseId: string;
    leaseTokenId: string;
    failureReason: string;
    failureDetails: JsonObject;
}): Promise<ArchiveRequestRecord | undefined> {
    return await withSchemaClient(async (sql) => sql.begin(async (transaction) => {
        const rows = await transaction<ArchiveRequestRow[]>`
      UPDATE archive_requests req
      SET status = 'FAILED',
          failure_reason = ${params.failureReason},
          failure_details = ${params.failureDetails},
          released_at = now(),
          lease_expires_at = NULL,
          updated_at = now()
      WHERE req.id = ${params.requestId}
        AND req.status = 'PROCESSING'
        AND req.lease_id = ${params.leaseId}
        AND req.lease_token_id = ${params.leaseTokenId}
        AND req.released_at IS NULL
        AND req.lease_expires_at > now()
      RETURNING id, tenant_id, target_type, target_id, action_type, action_payload,
                requested_by, dedupe_key, status, failure_reason, failure_details,
                lease_id, lease_token_id, lease_expires_at, leased_by, released_at,
                created_at, updated_at, completed_at
    `;
        const row = rows[0];
        if (!row) return undefined;
        await transaction`
          UPDATE curation_publications
          SET cleanup_eligible_at = COALESCE(
                cleanup_eligible_at,
                now() + interval '7 days'
              )
          WHERE request_id = ${row.id}
        `;
        await transaction`
          UPDATE archive_artifact_upload_attempts
          SET invalidated_at = now(), updated_at = now()
          WHERE request_id = ${params.requestId}
            AND state = 'AUTHORIZED'
            AND invalidated_at IS NULL
        `;
        return mapArchiveRequest(row);
    }));
}

export async function findActiveArchiveRequestLeaseByToken(params: {
    requestId: string;
    leaseId: string;
    leaseTokenId: string;
}): Promise<ArchiveRequestRecord | undefined> {
    const rows = await withSchemaClient(async (sql) => {
        return await sql<ArchiveRequestRow[]>`
      SELECT id, tenant_id, target_type, target_id, action_type, action_payload,
             requested_by, dedupe_key, status, failure_reason, failure_details,
             lease_id, lease_token_id, lease_expires_at, leased_by, released_at,
             created_at, updated_at, completed_at
      FROM archive_requests req
      WHERE req.id = ${params.requestId}
        AND req.status = 'PROCESSING'
        AND req.lease_id = ${params.leaseId}
        AND req.lease_token_id = ${params.leaseTokenId}
        AND req.released_at IS NULL
        AND req.lease_expires_at > now()
      LIMIT 1
    `;
    });

    const row = rows[0];
    return row ? mapArchiveRequest(row) : undefined;
}

export async function sweepExpiredArchiveRequestLeases(): Promise<number> {
    return await withSchemaClient(async (sql) => sql.begin(async (transaction) => {
        const candidates = await transaction<Array<{ id: string }>>`
          SELECT req.id
          FROM archive_requests req
          WHERE req.status = 'PROCESSING'
            AND req.released_at IS NULL
            AND req.lease_expires_at <= now()
          FOR UPDATE SKIP LOCKED
        `;
        if (candidates.length === 0) return 0;
        const ids = candidates.map((candidate) => candidate.id);
        await transaction`
          UPDATE archive_artifact_upload_attempts
          SET invalidated_at = now(), updated_at = now()
          WHERE request_id IN ${sql(ids)}
            AND state = 'AUTHORIZED'
            AND invalidated_at IS NULL
        `;
        const rows = await transaction<Array<{ id: string }>>`
          UPDATE archive_requests
          SET status = 'PENDING', released_at = now(), lease_expires_at = NULL, updated_at = now()
          WHERE id IN ${sql(ids)}
            AND status = 'PROCESSING'
            AND released_at IS NULL
          RETURNING id
        `;
        return rows.length;
    }));
}
