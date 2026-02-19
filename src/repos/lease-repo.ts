import { withSchemaClient } from "../db/client.ts";

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
  batch_label: string;
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
  batchLabel: string;
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
    batchLabel: row.batch_label,
    tenantId: row.tenant_id,
    status: row.status,
  };
}

export async function sweepExpiredLeases(): Promise<number> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<Array<{ count: number }>>`
      WITH expired AS (
        UPDATE ingestion_leases
        SET released_at = now()
        WHERE released_at IS NULL
          AND lease_expires_at <= now()
        RETURNING ingestion_id
      ),
      requeued AS (
        UPDATE ingestions
        SET status = 'QUEUED',
            updated_at = now()
        WHERE id IN (SELECT ingestion_id FROM expired)
          AND status = 'PROCESSING'
        RETURNING id
      )
      SELECT COUNT(*)::int AS count
      FROM requeued
    `;
  });

  return Number(rows[0]?.count ?? 0);
}

export async function leaseNextQueuedIngestion(params: {
  workerId?: string;
  leaseDurationSeconds: number;
}): Promise<{ ingestion: LeasedIngestionRecord; lease: LeaseRecord } | undefined> {
  return withSchemaClient(async (sql) => {
    return sql.begin(async (transaction) => {
      const candidates = await transaction<QueuedIngestionRow[]>`
        SELECT ing.id, ing.batch_label, ing.tenant_id, ing.status
        FROM ingestions ing
        WHERE ing.status = 'QUEUED'
          AND NOT EXISTS (
            SELECT 1
            FROM ingestion_leases lease
            WHERE lease.ingestion_id = ing.id
              AND lease.released_at IS NULL
              AND lease.lease_expires_at > now()
          )
        ORDER BY ing.created_at ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1
      `;

      const candidate = candidates[0];

      if (!candidate) {
        return undefined;
      }

      const leaseId = crypto.randomUUID();
      const leaseTokenId = crypto.randomUUID();
      const leaseRows = await transaction<LeaseRow[]>`
        INSERT INTO ingestion_leases (
          id,
          ingestion_id,
          leased_by,
          lease_token_id,
          lease_expires_at
        )
        VALUES (
          ${leaseId},
          ${candidate.id},
          ${params.workerId ?? null},
          ${leaseTokenId},
          now() + (${params.leaseDurationSeconds}::int * interval '1 second')
        )
        RETURNING id, ingestion_id, leased_by, lease_token_id, lease_expires_at, created_at, released_at
      `;

      await transaction`
        UPDATE ingestions
        SET status = 'PROCESSING',
            updated_at = now()
        WHERE id = ${candidate.id}
      `;

      return {
        ingestion: {
          ...mapLeasedIngestion(candidate),
          status: "PROCESSING",
        },
        lease: mapLease(leaseRows[0]!),
      };
    });
  });
}

export async function extendLease(params: {
  ingestionId: string;
  leaseId: string;
  leaseTokenId: string;
  leaseDurationSeconds: number;
}): Promise<LeaseRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<LeaseRow[]>`
      UPDATE ingestion_leases
      SET lease_expires_at = now() + (${params.leaseDurationSeconds}::int * interval '1 second')
      WHERE id = ${params.leaseId}
        AND ingestion_id = ${params.ingestionId}
        AND lease_token_id = ${params.leaseTokenId}
        AND released_at IS NULL
        AND lease_expires_at > now()
      RETURNING id, ingestion_id, leased_by, lease_token_id, lease_expires_at, created_at, released_at
    `;
  });

  const row = rows[0];
  return row ? mapLease(row) : undefined;
}

export async function releaseLease(params: {
  ingestionId: string;
  leaseId: string;
  leaseTokenId: string;
}): Promise<LeaseRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<LeaseRow[]>`
      UPDATE ingestion_leases
      SET released_at = now()
      WHERE id = ${params.leaseId}
        AND ingestion_id = ${params.ingestionId}
        AND lease_token_id = ${params.leaseTokenId}
        AND released_at IS NULL
      RETURNING id, ingestion_id, leased_by, lease_token_id, lease_expires_at, created_at, released_at
    `;
  });

  const row = rows[0];
  return row ? mapLease(row) : undefined;
}

export async function findActiveLeaseByToken(params: {
  ingestionId: string;
  leaseId: string;
  leaseTokenId: string;
}): Promise<LeaseRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<LeaseRow[]>`
      SELECT id, ingestion_id, leased_by, lease_token_id, lease_expires_at, created_at, released_at
      FROM ingestion_leases
      WHERE id = ${params.leaseId}
        AND ingestion_id = ${params.ingestionId}
        AND lease_token_id = ${params.leaseTokenId}
        AND released_at IS NULL
        AND lease_expires_at > now()
      LIMIT 1
    `;
  });

  const row = rows[0];
  return row ? mapLease(row) : undefined;
}
