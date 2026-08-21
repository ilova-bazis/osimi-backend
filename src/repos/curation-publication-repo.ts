import { withExecutor, withSchemaClient, type SqlExecutor } from "../db/client.ts";
import type { ArchiveRequestStatus } from "./archive-request-repo.ts";

interface CurationPublicationRow {
  request_id: string;
  tenant_id: string;
  object_id: string;
  curated_kind: "ocr_curated";
  publication_revision: number;
  target_version: string;
  storage_key: string;
  content_type: string;
  size_bytes: bigint;
  checksum_sha256: string;
  created_at: Date;
  cleanup_eligible_at: Date | null;
  purged_at: Date | null;
  request_status?: ArchiveRequestStatus;
  requested_by?: string;
  request_created_at?: Date;
  request_updated_at?: Date;
  request_completed_at?: Date | null;
  failure_reason?: string | null;
}

export interface CurationPublicationRecord {
  requestId: string;
  tenantId: string;
  objectId: string;
  curatedKind: "ocr_curated";
  publicationRevision: number;
  targetVersion: string;
  storageKey: string;
  contentType: string;
  sizeBytes: number;
  checksumSha256: string;
  createdAt: Date;
  cleanupEligibleAt: Date | null;
  purgedAt: Date | null;
  requestStatus?: ArchiveRequestStatus;
  requestedBy?: string;
  requestCreatedAt?: Date;
  requestUpdatedAt?: Date;
  requestCompletedAt?: Date | null;
  failureReason?: string | null;
}

const mapPublication = (row: CurationPublicationRow): CurationPublicationRecord => ({
  requestId: row.request_id,
  tenantId: row.tenant_id,
  objectId: row.object_id,
  curatedKind: row.curated_kind,
  publicationRevision: row.publication_revision,
  targetVersion: row.target_version,
  storageKey: row.storage_key,
  contentType: row.content_type,
  sizeBytes: Number(row.size_bytes),
  checksumSha256: row.checksum_sha256,
  createdAt: row.created_at,
  cleanupEligibleAt: row.cleanup_eligible_at,
  purgedAt: row.purged_at,
  requestStatus: row.request_status,
  requestedBy: row.requested_by,
  requestCreatedAt: row.request_created_at,
  requestUpdatedAt: row.request_updated_at,
  requestCompletedAt: row.request_completed_at,
  failureReason: row.failure_reason,
});

export async function findCurationPublicationByIdentity(params: {
  tenantId: string;
  objectId: string;
  curatedKind: "ocr_curated";
  publicationRevision: number;
  executor?: SqlExecutor;
}): Promise<CurationPublicationRecord | undefined> {
  const rows = await withExecutor(params.executor, async (sql) => await sql<CurationPublicationRow[]>`
    SELECT publication.*, request.status AS request_status,
           request.requested_by, request.created_at AS request_created_at,
           request.updated_at AS request_updated_at,
           request.completed_at AS request_completed_at,
           request.failure_reason
    FROM curation_publications publication
    INNER JOIN archive_requests request ON request.id = publication.request_id
    WHERE publication.tenant_id = ${params.tenantId}
      AND publication.object_id = ${params.objectId}
      AND publication.curated_kind = ${params.curatedKind}
      AND publication.publication_revision = ${params.publicationRevision}
    LIMIT 1
  `);
  const row = rows[0];
  return row ? mapPublication(row) : undefined;
}

export async function createCurationPublicationWithExecutor(
  executor: SqlExecutor,
  params: {
    requestId: string;
    tenantId: string;
    objectId: string;
    curatedKind: "ocr_curated";
    publicationRevision: number;
    targetVersion: string;
    storageKey: string;
    contentType: string;
    sizeBytes: number;
    checksumSha256: string;
  },
): Promise<void> {
  await executor`
    INSERT INTO curation_publications (
      request_id, tenant_id, object_id, curated_kind, publication_revision,
      target_version, storage_key, content_type, size_bytes, checksum_sha256
    ) VALUES (
      ${params.requestId}, ${params.tenantId}, ${params.objectId}, ${params.curatedKind},
      ${params.publicationRevision}, ${params.targetVersion}, ${params.storageKey},
      ${params.contentType}, ${params.sizeBytes}, ${params.checksumSha256}
    )
  `;
}

export async function findCurationPublicationByRequestId(params: {
  requestId: string;
}): Promise<CurationPublicationRecord | undefined> {
  return await withSchemaClient(async (sql) => {
    const rows = await sql<CurationPublicationRow[]>`
      SELECT * FROM curation_publications WHERE request_id = ${params.requestId} LIMIT 1
    `;
    const row = rows[0];
    return row ? mapPublication(row) : undefined;
  });
}

export async function curationPublicationStorageKeyExists(storageKey: string): Promise<boolean> {
  return await withSchemaClient(async (sql) => {
    const rows = await sql<Array<{ exists: boolean }>>`
      SELECT EXISTS (
        SELECT 1 FROM curation_publications WHERE storage_key = ${storageKey}
      ) AS exists
    `;
    return rows[0]?.exists ?? false;
  });
}

export interface CurationPublicationCleanupClaim {
  requestId: string;
  claimId: string;
  storageKey: string;
}

export async function claimCurationPublicationCleanupBatch(params: {
  batchSize: number;
  claimTimeoutSeconds: number;
}): Promise<CurationPublicationCleanupClaim[]> {
  const claimId = crypto.randomUUID();
  return await withSchemaClient(async (sql) => sql.begin(async (transaction) => {
    const rows = await transaction<Array<{ request_id: string; storage_key: string }>>`
      WITH candidates AS (
        SELECT request_id
        FROM curation_publications
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
      UPDATE curation_publications publication
      SET cleanup_claim_id = ${claimId}, cleanup_claimed_at = now()
      FROM candidates
      WHERE publication.request_id = candidates.request_id
      RETURNING publication.request_id, publication.storage_key
    `;
    return rows.map((row) => ({
      requestId: row.request_id,
      claimId,
      storageKey: row.storage_key,
    }));
  }));
}

export async function completeCurationPublicationCleanup(
  claim: CurationPublicationCleanupClaim,
): Promise<boolean> {
  return await withSchemaClient(async (sql) => {
    const rows = await sql<Array<{ request_id: string }>>`
      UPDATE curation_publications
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

export async function failCurationPublicationCleanup(params: {
  claim: CurationPublicationCleanupClaim;
  message: string;
}): Promise<boolean> {
  return await withSchemaClient(async (sql) => {
    const rows = await sql<Array<{ request_id: string }>>`
      UPDATE curation_publications
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
