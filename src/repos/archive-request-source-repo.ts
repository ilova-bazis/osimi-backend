import { withExecutor, withSchemaClient, type SqlExecutor } from "../db/client.ts";

interface ArchiveRequestSourceRow {
  request_id: string;
  tenant_id: string;
  source_name: string;
  storage_key: string;
  content_type: string;
  size_bytes: bigint;
  checksum_sha256: string;
  created_at: Date;
  cleanup_eligible_at: Date | null;
  purged_at: Date | null;
}

export interface ArchiveRequestSourceRecord {
  requestId: string;
  tenantId: string;
  sourceName: string;
  storageKey: string;
  contentType: string;
  sizeBytes: number;
  checksumSha256: string;
  createdAt: Date;
  cleanupEligibleAt: Date | null;
  purgedAt: Date | null;
}

function mapSource(row: ArchiveRequestSourceRow): ArchiveRequestSourceRecord {
  return {
    requestId: row.request_id,
    tenantId: row.tenant_id,
    sourceName: row.source_name,
    storageKey: row.storage_key,
    contentType: row.content_type,
    sizeBytes: Number(row.size_bytes),
    checksumSha256: row.checksum_sha256,
    createdAt: row.created_at,
    cleanupEligibleAt: row.cleanup_eligible_at,
    purgedAt: row.purged_at,
  };
}

export async function createArchiveRequestSourceWithExecutor(
  executor: SqlExecutor,
  params: {
    requestId: string;
    tenantId: string;
    sourceName: string;
    storageKey: string;
    contentType: string;
    sizeBytes: number;
    checksumSha256: string;
  },
): Promise<void> {
  await executor`
    INSERT INTO archive_request_sources (
      request_id, tenant_id, source_name, storage_key, content_type,
      size_bytes, checksum_sha256
    ) VALUES (
      ${params.requestId}, ${params.tenantId}, ${params.sourceName},
      ${params.storageKey}, ${params.contentType}, ${params.sizeBytes},
      ${params.checksumSha256}
    )
  `;
}

export async function findArchiveRequestSourceByRequestId(params: {
  requestId: string;
  executor?: SqlExecutor;
}): Promise<ArchiveRequestSourceRecord | undefined> {
  const rows = await withExecutor(params.executor, async (sql) => {
    return await sql<ArchiveRequestSourceRow[]>`
      SELECT request_id, tenant_id, source_name, storage_key, content_type,
             size_bytes, checksum_sha256, created_at, cleanup_eligible_at, purged_at
      FROM archive_request_sources
      WHERE request_id = ${params.requestId}
      LIMIT 1
    `;
  });

  const row = rows[0];
  return row ? mapSource(row) : undefined;
}

export async function archiveRequestSourceStorageKeyExists(
  storageKey: string,
): Promise<boolean> {
  return await withSchemaClient(async (sql) => {
    const rows = await sql<Array<{ exists: boolean }>>`
      SELECT EXISTS (
        SELECT 1 FROM archive_request_sources WHERE storage_key = ${storageKey}
      ) AS exists
    `;
    return rows[0]?.exists ?? false;
  });
}

export interface ArchiveRequestSourceCleanupClaim {
  requestId: string;
  claimId: string;
  storageKey: string;
}

export async function claimArchiveRequestSourceCleanupBatch(params: {
  batchSize: number;
  claimTimeoutSeconds: number;
}): Promise<ArchiveRequestSourceCleanupClaim[]> {
  const claimId = crypto.randomUUID();
  return await withSchemaClient(async (sql) => sql.begin(async (transaction) => {
    const rows = await transaction<Array<{ request_id: string; storage_key: string }>>`
      WITH candidates AS (
        SELECT request_id
        FROM archive_request_sources
        WHERE purged_at IS NULL
          AND cleanup_eligible_at IS NOT NULL
          AND cleanup_eligible_at <= now()
          AND (cleanup_next_attempt_at IS NULL OR cleanup_next_attempt_at <= now())
          AND (
            cleanup_claimed_at IS NULL
            OR cleanup_claimed_at <= now() - (${params.claimTimeoutSeconds}::int * interval '1 second')
          )
        ORDER BY cleanup_eligible_at, created_at, request_id
        FOR UPDATE SKIP LOCKED
        LIMIT ${params.batchSize}
      )
      UPDATE archive_request_sources source
      SET cleanup_claim_id = ${claimId}, cleanup_claimed_at = now()
      FROM candidates
      WHERE source.request_id = candidates.request_id
      RETURNING source.request_id, source.storage_key
    `;
    return rows.map((row) => ({
      requestId: row.request_id,
      claimId,
      storageKey: row.storage_key,
    }));
  }));
}

export async function completeArchiveRequestSourceCleanup(
  claim: ArchiveRequestSourceCleanupClaim,
): Promise<boolean> {
  return await withSchemaClient(async (sql) => {
    const rows = await sql<Array<{ request_id: string }>>`
      UPDATE archive_request_sources
      SET purged_at = now(), cleanup_claim_id = NULL, cleanup_claimed_at = NULL,
          cleanup_last_error = NULL, cleanup_next_attempt_at = NULL
      WHERE request_id = ${claim.requestId}
        AND cleanup_claim_id = ${claim.claimId}
        AND purged_at IS NULL
      RETURNING request_id
    `;
    return rows.length === 1;
  });
}

export async function failArchiveRequestSourceCleanup(params: {
  claim: ArchiveRequestSourceCleanupClaim;
  message: string;
}): Promise<boolean> {
  return await withSchemaClient(async (sql) => {
    const rows = await sql<Array<{ request_id: string }>>`
      UPDATE archive_request_sources
      SET cleanup_claim_id = NULL,
          cleanup_claimed_at = NULL,
          cleanup_attempt_count = cleanup_attempt_count + 1,
          cleanup_last_error = ${params.message},
          cleanup_next_attempt_at = now() + (
            LEAST(3600, 30 * power(2, LEAST(cleanup_attempt_count, 7))) * interval '1 second'
          )
      WHERE request_id = ${params.claim.requestId}
        AND cleanup_claim_id = ${params.claim.claimId}
        AND purged_at IS NULL
      RETURNING request_id
    `;
    return rows.length === 1;
  });
}
