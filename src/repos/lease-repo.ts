import { db, qualifiedTableName } from "../db/runtime.ts";

interface LeaseRow {
  id: string;
  ingestion_id: string;
  leased_by: string | null;
  lease_token_id: string;
  lease_expires_at: Date;
  created_at: Date;
  released_at: Date | null;
}

interface QueuedIngestionRow {
  id: string;
  upload_id: string;
  tenant_id: string;
  status: "QUEUED" | "PROCESSING";
}

export interface LeaseRecord {
  id: string;
  ingestionId: string;
  leasedBy?: string;
  leaseTokenId: string;
  leaseExpiresAt: Date;
  createdAt: Date;
  releasedAt?: Date;
}

export interface LeasedIngestionRecord {
  id: string;
  uploadId: string;
  tenantId: string;
  status: "QUEUED" | "PROCESSING";
}

function mapLease(row: LeaseRow): LeaseRecord {
  return {
    id: row.id,
    ingestionId: row.ingestion_id,
    leasedBy: row.leased_by ?? undefined,
    leaseTokenId: row.lease_token_id,
    leaseExpiresAt: new Date(row.lease_expires_at),
    createdAt: new Date(row.created_at),
    releasedAt: row.released_at ? new Date(row.released_at) : undefined,
  };
}

function mapLeasedIngestion(row: QueuedIngestionRow): LeasedIngestionRecord {
  return {
    id: row.id,
    uploadId: row.upload_id,
    tenantId: row.tenant_id,
    status: row.status,
  };
}

export async function sweepExpiredLeases(): Promise<number> {
  const sql = db();
  const leasesTable = qualifiedTableName("ingestion_leases");
  const ingestionsTable = qualifiedTableName("ingestions");

  const rows = (await sql.unsafe(
    `
      WITH expired AS (
        UPDATE ${leasesTable}
        SET released_at = now()
        WHERE released_at IS NULL
          AND lease_expires_at <= now()
        RETURNING ingestion_id
      ),
      requeued AS (
        UPDATE ${ingestionsTable}
        SET status = 'QUEUED',
            updated_at = now()
        WHERE id IN (SELECT ingestion_id FROM expired)
          AND status = 'PROCESSING'
        RETURNING id
      )
      SELECT COUNT(*)::int AS count
      FROM requeued
    `,
  )) as Array<{ count: number }>;

  return Number(rows[0]?.count ?? 0);
}

export async function leaseNextQueuedIngestion(params: {
  workerId?: string;
  leaseDurationSeconds: number;
}): Promise<{ ingestion: LeasedIngestionRecord; lease: LeaseRecord } | undefined> {
  const sql = db();
  const ingestionsTable = qualifiedTableName("ingestions");
  const leasesTable = qualifiedTableName("ingestion_leases");

  return sql.begin(async transaction => {
    const candidates = (await transaction.unsafe(
      `
        SELECT i.id, i.upload_id, i.tenant_id, i.status
        FROM ${ingestionsTable} i
        WHERE i.status = 'QUEUED'
          AND NOT EXISTS (
            SELECT 1
            FROM ${leasesTable} l
            WHERE l.ingestion_id = i.id
              AND l.released_at IS NULL
              AND l.lease_expires_at > now()
          )
        ORDER BY i.created_at ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1
      `,
    )) as QueuedIngestionRow[];

    const candidate = candidates[0];

    if (!candidate) {
      return undefined;
    }

    const leaseId = crypto.randomUUID();
    const leaseTokenId = crypto.randomUUID();
    const leaseRows = (await transaction.unsafe(
      `
        INSERT INTO ${leasesTable} (
          id,
          ingestion_id,
          leased_by,
          lease_token_id,
          lease_expires_at
        )
        VALUES (
          $1,
          $2,
          $3,
          $4,
          now() + ($5::int * interval '1 second')
        )
        RETURNING id, ingestion_id, leased_by, lease_token_id, lease_expires_at, created_at, released_at
      `,
      [leaseId, candidate.id, params.workerId ?? null, leaseTokenId, params.leaseDurationSeconds],
    )) as LeaseRow[];

    await transaction.unsafe(
      `
        UPDATE ${ingestionsTable}
        SET status = 'PROCESSING',
            updated_at = now()
        WHERE id = $1
      `,
      [candidate.id],
    );

    return {
      ingestion: {
        ...mapLeasedIngestion(candidate),
        status: "PROCESSING",
      },
      lease: mapLease(leaseRows[0]!),
    };
  });
}

export async function extendLease(params: {
  ingestionId: string;
  leaseId: string;
  leaseTokenId: string;
  leaseDurationSeconds: number;
}): Promise<LeaseRecord | undefined> {
  const sql = db();
  const leasesTable = qualifiedTableName("ingestion_leases");

  const rows = (await sql.unsafe(
    `
      UPDATE ${leasesTable}
      SET lease_expires_at = now() + ($4::int * interval '1 second')
      WHERE id = $1
        AND ingestion_id = $2
        AND lease_token_id = $3
        AND released_at IS NULL
        AND lease_expires_at > now()
      RETURNING id, ingestion_id, leased_by, lease_token_id, lease_expires_at, created_at, released_at
    `,
    [params.leaseId, params.ingestionId, params.leaseTokenId, params.leaseDurationSeconds],
  )) as LeaseRow[];

  const row = rows[0];
  return row ? mapLease(row) : undefined;
}

export async function releaseLease(params: {
  ingestionId: string;
  leaseId: string;
  leaseTokenId: string;
}): Promise<LeaseRecord | undefined> {
  const sql = db();
  const leasesTable = qualifiedTableName("ingestion_leases");

  const rows = (await sql.unsafe(
    `
      UPDATE ${leasesTable}
      SET released_at = now()
      WHERE id = $1
        AND ingestion_id = $2
        AND lease_token_id = $3
        AND released_at IS NULL
      RETURNING id, ingestion_id, leased_by, lease_token_id, lease_expires_at, created_at, released_at
    `,
    [params.leaseId, params.ingestionId, params.leaseTokenId],
  )) as LeaseRow[];

  const row = rows[0];
  return row ? mapLease(row) : undefined;
}

export async function findActiveLeaseByToken(params: {
  ingestionId: string;
  leaseId: string;
  leaseTokenId: string;
}): Promise<LeaseRecord | undefined> {
  const sql = db();
  const leasesTable = qualifiedTableName("ingestion_leases");

  const rows = (await sql.unsafe(
    `
      SELECT id, ingestion_id, leased_by, lease_token_id, lease_expires_at, created_at, released_at
      FROM ${leasesTable}
      WHERE id = $1
        AND ingestion_id = $2
        AND lease_token_id = $3
        AND released_at IS NULL
        AND lease_expires_at > now()
      LIMIT 1
    `,
    [params.leaseId, params.ingestionId, params.leaseTokenId],
  )) as LeaseRow[];

  const row = rows[0];
  return row ? mapLease(row) : undefined;
}
