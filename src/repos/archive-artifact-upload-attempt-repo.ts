import { withExecutor, withSchemaClient } from "../db/client.ts";
import type { SqlExecutor } from "../db/client.ts";
import { toSafeNumberFromDbInt, type DbInt } from "../db/number.ts";

export type ArchiveArtifactUploadAttemptState =
  | "AUTHORIZED"
  | "VERIFIED"
  | "MATERIALIZED";

interface ArchiveArtifactUploadAttemptRow {
  upload_token_id: string;
  request_id: string;
  authorized_lease_id: string;
  authorized_lease_token_id: string;
  storage_key: string;
  content_type: string;
  size_bytes: DbInt;
  expected_sha256: string | null;
  computed_sha256: string | null;
  state: ArchiveArtifactUploadAttemptState;
  created_at: Date;
  expires_at: Date | null;
  updated_at: Date;
  verified_at: Date | null;
  artifact_id: string | null;
  materialized_at: Date | null;
  invalidated_at: Date | null;
  finalization_claim_token: string | null;
  finalization_claimed_at: Date | null;
  finalization_attempt_count: DbInt;
  finalization_next_retry_at: Date | null;
  finalization_last_error: string | null;
}

export interface ArchiveArtifactUploadAttemptRecord {
  uploadTokenId: string;
  requestId: string;
  authorizedLeaseId: string;
  authorizedLeaseTokenId: string;
  storageKey: string;
  contentType: string;
  sizeBytes: number;
  expectedSha256: string | null;
  computedSha256: string | null;
  state: ArchiveArtifactUploadAttemptState;
  createdAt: Date;
  expiresAt: Date | null;
  updatedAt: Date;
  verifiedAt: Date | null;
  artifactId: string | null;
  materializedAt: Date | null;
  invalidatedAt: Date | null;
  finalizationClaimToken: string | null;
  finalizationClaimedAt: Date | null;
  finalizationAttemptCount: number;
  finalizationNextRetryAt: Date | null;
  finalizationLastError: string | null;
}

export interface CreateAuthorizedArchiveArtifactUploadAttemptParams {
  requestId: string;
  leaseId: string;
  leaseTokenId: string;
  uploadTokenId: string;
  storageKey: string;
  contentType: string;
  sizeBytes: number;
  expectedSha256?: string | null;
  expiresAt?: Date | null;
}

function mapAttempt(
  row: ArchiveArtifactUploadAttemptRow,
): ArchiveArtifactUploadAttemptRecord {
  return {
    uploadTokenId: row.upload_token_id,
    requestId: row.request_id,
    authorizedLeaseId: row.authorized_lease_id,
    authorizedLeaseTokenId: row.authorized_lease_token_id,
    storageKey: row.storage_key,
    contentType: row.content_type,
    sizeBytes: toSafeNumberFromDbInt(
      row.size_bytes,
      "archive_artifact_upload_attempts.size_bytes",
    ),
    expectedSha256: row.expected_sha256,
    computedSha256: row.computed_sha256,
    state: row.state,
    createdAt: row.created_at,
    expiresAt: row.expires_at,
    updatedAt: row.updated_at,
    verifiedAt: row.verified_at,
    artifactId: row.artifact_id,
    materializedAt: row.materialized_at,
    invalidatedAt: row.invalidated_at,
    finalizationClaimToken: row.finalization_claim_token,
    finalizationClaimedAt: row.finalization_claimed_at,
    finalizationAttemptCount: toSafeNumberFromDbInt(
      row.finalization_attempt_count,
      "archive_artifact_upload_attempts.finalization_attempt_count",
    ),
    finalizationNextRetryAt: row.finalization_next_retry_at,
    finalizationLastError: row.finalization_last_error,
  };
}

export async function createAuthorizedArchiveArtifactUploadAttempt(
  executor: SqlExecutor,
  params: CreateAuthorizedArchiveArtifactUploadAttemptParams,
): Promise<ArchiveArtifactUploadAttemptRecord | undefined> {
  const requests = await executor<Array<{ id: string }>>`
    SELECT id
    FROM archive_requests
    WHERE id = ${params.requestId}
      AND action_type = 'artifact_fetch'
      AND status = 'PROCESSING'
      AND lease_id = ${params.leaseId}
      AND lease_token_id = ${params.leaseTokenId}
      AND released_at IS NULL
      AND lease_expires_at > now()
    FOR UPDATE
  `;
  if (!requests[0]) {
    return undefined;
  }

  await executor`
    UPDATE archive_artifact_upload_attempts
    SET invalidated_at = now(),
        updated_at = now()
    WHERE request_id = ${params.requestId}
      AND state = 'AUTHORIZED'
      AND invalidated_at IS NULL
  `;

  const rows = await executor<ArchiveArtifactUploadAttemptRow[]>`
    INSERT INTO archive_artifact_upload_attempts (
      upload_token_id, request_id, authorized_lease_id,
      authorized_lease_token_id, storage_key, content_type, size_bytes,
      expected_sha256, expires_at
    )
    VALUES (
      ${params.uploadTokenId}, ${params.requestId}, ${params.leaseId},
      ${params.leaseTokenId}, ${params.storageKey}, ${params.contentType},
      ${params.sizeBytes}, ${params.expectedSha256 ?? null}, ${params.expiresAt ?? null}
    )
    RETURNING *
  `;
  return mapAttempt(rows[0]!);
}

async function findAttemptById(
  params: { uploadTokenId: string; executor?: SqlExecutor },
  forUpdate: boolean,
): Promise<ArchiveArtifactUploadAttemptRecord | undefined> {
  const rows = await withExecutor(params.executor, async (sql) => {
    if (forUpdate) {
      return sql<ArchiveArtifactUploadAttemptRow[]>`
        SELECT *
        FROM archive_artifact_upload_attempts
        WHERE upload_token_id = ${params.uploadTokenId}
        FOR UPDATE
      `;
    }
    return sql<ArchiveArtifactUploadAttemptRow[]>`
      SELECT *
      FROM archive_artifact_upload_attempts
      WHERE upload_token_id = ${params.uploadTokenId}
    `;
  });
  return rows[0] ? mapAttempt(rows[0]) : undefined;
}

export async function findArchiveArtifactUploadAttemptById(params: {
  uploadTokenId: string;
  executor?: SqlExecutor;
}): Promise<ArchiveArtifactUploadAttemptRecord | undefined> {
  return findAttemptById(params, false);
}

export async function findArchiveArtifactUploadAttemptByIdForUpdate(params: {
  uploadTokenId: string;
  executor?: SqlExecutor;
}): Promise<ArchiveArtifactUploadAttemptRecord | undefined> {
  return findAttemptById(params, true);
}

async function findAttemptByRequest(
  params: { requestId: string; executor?: SqlExecutor },
  forUpdate: boolean,
): Promise<ArchiveArtifactUploadAttemptRecord | undefined> {
  const rows = await withExecutor(params.executor, async (sql) => {
    if (forUpdate) {
      return sql<ArchiveArtifactUploadAttemptRow[]>`
        SELECT *
        FROM archive_artifact_upload_attempts
        WHERE request_id = ${params.requestId}
        ORDER BY created_at DESC, upload_token_id DESC
        LIMIT 1
        FOR UPDATE
      `;
    }
    return sql<ArchiveArtifactUploadAttemptRow[]>`
      SELECT *
      FROM archive_artifact_upload_attempts
      WHERE request_id = ${params.requestId}
      ORDER BY created_at DESC, upload_token_id DESC
      LIMIT 1
    `;
  });
  return rows[0] ? mapAttempt(rows[0]) : undefined;
}

export async function findArchiveArtifactUploadAttemptByRequest(params: {
  requestId: string;
  executor?: SqlExecutor;
}): Promise<ArchiveArtifactUploadAttemptRecord | undefined> {
  return findAttemptByRequest(params, false);
}

export async function findArchiveArtifactUploadAttemptByRequestForUpdate(params: {
  requestId: string;
  executor?: SqlExecutor;
}): Promise<ArchiveArtifactUploadAttemptRecord | undefined> {
  return findAttemptByRequest(params, true);
}

export async function verifyArchiveArtifactUploadAttempt(params: {
  uploadTokenId: string;
  requestId: string;
  leaseId: string;
  leaseTokenId: string;
  computedSizeBytes: number;
  computedSha256: string;
  executor?: SqlExecutor;
}): Promise<ArchiveArtifactUploadAttemptRecord | undefined> {
  const run = async (sql: SqlExecutor): Promise<ArchiveArtifactUploadAttemptRecord | undefined> => {
    await sql`
      SELECT id
      FROM archive_requests
      WHERE id = ${params.requestId}
      FOR UPDATE
    `;
    const rows = await sql<ArchiveArtifactUploadAttemptRow[]>`
      UPDATE archive_artifact_upload_attempts attempt
      SET state = 'VERIFIED',
          computed_sha256 = ${params.computedSha256},
          verified_at = now(),
          updated_at = now()
      FROM archive_requests request
      WHERE attempt.upload_token_id = ${params.uploadTokenId}
        AND attempt.request_id = ${params.requestId}
        AND attempt.state = 'AUTHORIZED'
        AND attempt.invalidated_at IS NULL
        AND attempt.authorized_lease_id = ${params.leaseId}
        AND attempt.authorized_lease_token_id = ${params.leaseTokenId}
        AND attempt.size_bytes = ${params.computedSizeBytes}
        AND (attempt.expires_at IS NULL OR attempt.expires_at > now())
        AND (attempt.expected_sha256 IS NULL OR attempt.expected_sha256 = ${params.computedSha256})
        AND request.id = attempt.request_id
        AND request.status = 'PROCESSING'
        AND request.lease_id = ${params.leaseId}
        AND request.lease_token_id = ${params.leaseTokenId}
        AND request.released_at IS NULL
        AND request.lease_expires_at > now()
        AND NOT EXISTS (
          SELECT 1
          FROM archive_artifact_upload_attempts accepted
          WHERE accepted.request_id = attempt.request_id
            AND accepted.upload_token_id <> attempt.upload_token_id
            AND accepted.state IN ('VERIFIED', 'MATERIALIZED')
        )
      RETURNING attempt.*
    `;
    return rows[0] ? mapAttempt(rows[0]) : undefined;
  };
  if (params.executor) return run(params.executor);
  return withSchemaClient((sql) => sql.begin(run));
}

export async function claimVerifiedArchiveArtifactUploadAttempt(params: {
  uploadTokenId: string;
  claimTimeoutSeconds: number;
  ignoreRetrySchedule?: boolean;
  executor?: SqlExecutor;
}): Promise<ArchiveArtifactUploadAttemptRecord | undefined> {
  const claimToken = crypto.randomUUID();
  const rows = await withExecutor(params.executor, (sql) =>
    sql<ArchiveArtifactUploadAttemptRow[]>`
      UPDATE archive_artifact_upload_attempts
      SET finalization_claim_token = ${claimToken},
          finalization_claimed_at = now(),
          finalization_attempt_count = finalization_attempt_count + 1,
          finalization_next_retry_at = NULL,
          updated_at = now()
      WHERE upload_token_id = ${params.uploadTokenId}
        AND state = 'VERIFIED'
        AND (
          ${params.ignoreRetrySchedule ?? false}
          OR finalization_next_retry_at IS NULL
          OR finalization_next_retry_at <= now()
        )
        AND (
          finalization_claimed_at IS NULL
          OR finalization_claimed_at <= now() - (${params.claimTimeoutSeconds} * interval '1 second')
        )
      RETURNING *
    `,
  );
  return rows[0] ? mapAttempt(rows[0]) : undefined;
}

export async function claimVerifiedArchiveArtifactUploadAttemptBatch(params: {
  batchSize: number;
  claimTimeoutSeconds: number;
  executor?: SqlExecutor;
}): Promise<ArchiveArtifactUploadAttemptRecord[]> {
  const claimToken = crypto.randomUUID();
  const rows = await withExecutor(params.executor, (sql) =>
    sql<ArchiveArtifactUploadAttemptRow[]>`
      WITH candidates AS (
        SELECT upload_token_id
        FROM archive_artifact_upload_attempts
        WHERE state = 'VERIFIED'
          AND (finalization_next_retry_at IS NULL OR finalization_next_retry_at <= now())
          AND (
            finalization_claimed_at IS NULL
            OR finalization_claimed_at <= now() - (${params.claimTimeoutSeconds} * interval '1 second')
          )
        ORDER BY COALESCE(finalization_next_retry_at, verified_at) ASC,
                 verified_at ASC,
                 upload_token_id ASC
        LIMIT ${params.batchSize}
        FOR UPDATE SKIP LOCKED
      )
      UPDATE archive_artifact_upload_attempts attempt
      SET finalization_claim_token = ${claimToken},
          finalization_claimed_at = now(),
          finalization_attempt_count = attempt.finalization_attempt_count + 1,
          finalization_next_retry_at = NULL,
          updated_at = now()
      FROM candidates
      WHERE attempt.upload_token_id = candidates.upload_token_id
      RETURNING attempt.*
    `,
  );
  return rows.map(mapAttempt);
}

export async function markArchiveArtifactUploadAttemptMaterialized(
  executor: SqlExecutor,
  params: {
    uploadTokenId: string;
    claimToken: string;
    artifactId: string;
  },
): Promise<ArchiveArtifactUploadAttemptRecord | undefined> {
  const rows = await executor<ArchiveArtifactUploadAttemptRow[]>`
    UPDATE archive_artifact_upload_attempts
    SET state = 'MATERIALIZED',
        artifact_id = ${params.artifactId},
        materialized_at = now(),
        finalization_claim_token = NULL,
        finalization_claimed_at = NULL,
        finalization_next_retry_at = NULL,
        finalization_last_error = NULL,
        updated_at = now()
    WHERE upload_token_id = ${params.uploadTokenId}
      AND state = 'VERIFIED'
      AND finalization_claim_token = ${params.claimToken}
    RETURNING *
  `;
  return rows[0] ? mapAttempt(rows[0]) : undefined;
}

export async function recordArchiveArtifactUploadFinalizationFailure(params: {
  uploadTokenId: string;
  claimToken: string;
  retryDelaySeconds: number;
  lastError: string;
  executor?: SqlExecutor;
}): Promise<ArchiveArtifactUploadAttemptRecord | undefined> {
  const rows = await withExecutor(params.executor, (sql) =>
    sql<ArchiveArtifactUploadAttemptRow[]>`
      UPDATE archive_artifact_upload_attempts
      SET finalization_claim_token = NULL,
          finalization_claimed_at = NULL,
          finalization_next_retry_at = now() + (${params.retryDelaySeconds} * interval '1 second'),
          finalization_last_error = ${params.lastError},
          updated_at = now()
      WHERE upload_token_id = ${params.uploadTokenId}
        AND state = 'VERIFIED'
        AND finalization_claim_token = ${params.claimToken}
      RETURNING *
    `,
  );
  return rows[0] ? mapAttempt(rows[0]) : undefined;
}

export async function findMaterializedArchiveArtifactUploadAttempt(params: {
  requestId: string;
  executor?: SqlExecutor;
}): Promise<ArchiveArtifactUploadAttemptRecord | undefined> {
  const rows = await withExecutor(params.executor, (sql) =>
    sql<ArchiveArtifactUploadAttemptRow[]>`
      SELECT *
      FROM archive_artifact_upload_attempts
      WHERE request_id = ${params.requestId}
        AND state = 'MATERIALIZED'
      LIMIT 1
    `,
  );
  return rows[0] ? mapAttempt(rows[0]) : undefined;
}
