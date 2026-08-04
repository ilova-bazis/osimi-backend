import { withSchemaClient } from "../db/client.ts";
import type { SqlExecutor } from "../db/client.ts";

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

interface IngestionLeaseCandidateRow {
  id: string;
  batch_label: string;
  tenant_id: string;
  status:
    | "DRAFT"
    | "UPLOADING"
    | "QUEUED"
    | "PROCESSING"
    | "COMPLETED"
    | "COMPLETED_WITH_ERRORS"
    | "FAILED"
    | "CANCELED";
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

export type LeaseByIdResult =
  | { status: "leased"; ingestion: LeasedIngestionRecord; lease: LeaseRecord }
  | { status: "not_found" }
  | { status: "not_leasable" };

export interface LeaseClaimCandidate {
  id: string;
  batchLabel: string;
  tenantId: string;
}

export interface PendingLeaseGrant {
  id: string;
  tokenId: string;
  expiresAt: Date;
}

export type ReleaseLeaseResult =
  | { status: "released"; lease: LeaseRecord }
  | { status: "not_found" }
  | { status: "inactive" };

export type LeaseClaimResult<T> =
  | { status: "claimed"; payload: T }
  | { status: "invalid"; error: unknown };

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

function createPendingLeaseGrant(leaseDurationSeconds: number): PendingLeaseGrant {
  return {
    id: crypto.randomUUID(),
    tokenId: crypto.randomUUID(),
    expiresAt: new Date(Date.now() + leaseDurationSeconds * 1000),
  };
}

async function insertLeaseAndMarkProcessing(
  transaction: SqlExecutor,
  params: {
    candidate: LeaseClaimCandidate;
    workerId?: string;
    grant: PendingLeaseGrant;
  },
): Promise<LeaseRecord> {
  const updatedRows = await transaction<Array<{ id: string }>>`
    UPDATE ingestions
    SET status = 'PROCESSING',
        updated_at = now()
    WHERE id = ${params.candidate.id}
      AND tenant_id = ${params.candidate.tenantId}
      AND status = 'QUEUED'
    RETURNING id
  `;

  if (updatedRows.length === 0) {
    throw new Error(`Queued ingestion '${params.candidate.id}' could not be claimed.`);
  }

  const leaseRows = await transaction<LeaseRow[]>`
    INSERT INTO ingestion_leases (
      id,
      ingestion_id,
      leased_by,
      lease_token_id,
      lease_expires_at
    )
    VALUES (
      ${params.grant.id},
      ${params.candidate.id},
      ${params.workerId ?? null},
      ${params.grant.tokenId},
      ${params.grant.expiresAt.toISOString()}
    )
    RETURNING id, ingestion_id, leased_by, lease_token_id, lease_expires_at, created_at, released_at
  `;

  return mapLease(leaseRows[0]!);
}

export async function claimNextQueuedIngestion<T>(params: {
  workerId?: string;
  leaseDurationSeconds: number;
  buildPayload: (params: {
    candidate: LeaseClaimCandidate;
    grant: PendingLeaseGrant;
    executor: SqlExecutor;
  }) => Promise<T>;
}): Promise<LeaseClaimResult<T> | undefined> {
  return withSchemaClient(async (sql) => {
    return sql.begin(async (transaction) => {
      const candidates = await transaction<QueuedIngestionRow[]>`
        SELECT ing.id, ing.batch_label, ing.tenant_id, ing.status
        FROM ingestions ing
        WHERE ing.status = 'QUEUED'
          AND ing.staging_purge_started_at IS NULL
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
      const candidateRow = candidates[0];
      if (!candidateRow) {
        return undefined;
      }

      const candidate: LeaseClaimCandidate = {
        id: candidateRow.id,
        batchLabel: candidateRow.batch_label,
        tenantId: candidateRow.tenant_id,
      };
      const grant = createPendingLeaseGrant(params.leaseDurationSeconds);
      let payload: T;
      try {
        payload = await params.buildPayload({ candidate, grant, executor: transaction });
      } catch (error) {
        return { status: "invalid", error };
      }
      await insertLeaseAndMarkProcessing(transaction, {
        candidate,
        workerId: params.workerId,
        grant,
      });
      return { status: "claimed", payload };
    });
  });
}

export async function claimQueuedIngestionById<T>(params: {
  ingestionId: string;
  workerId?: string;
  leaseDurationSeconds: number;
  buildPayload: (params: {
    candidate: LeaseClaimCandidate;
    grant: PendingLeaseGrant;
    executor: SqlExecutor;
  }) => Promise<T>;
}): Promise<LeaseClaimResult<T> | { status: "not_found" } | { status: "not_leasable" }> {
  return withSchemaClient(async (sql) => {
    return sql.begin(async (transaction) => {
      const candidates = await transaction<IngestionLeaseCandidateRow[]>`
        SELECT ing.id, ing.batch_label, ing.tenant_id, ing.status
        FROM ingestions ing
        WHERE ing.id = ${params.ingestionId}
        FOR UPDATE
      `;
      const candidateRow = candidates[0];
      if (!candidateRow) {
        return { status: "not_found" as const };
      }

      const activeLeaseRows = await transaction<Array<{ exists: boolean }>>`
        SELECT EXISTS(
          SELECT 1
          FROM ingestion_leases lease
          WHERE lease.ingestion_id = ${candidateRow.id}
            AND lease.released_at IS NULL
            AND lease.lease_expires_at > now()
        ) AS exists
      `;
      const purgeStartedRows = await transaction<Array<{ started: boolean }>>`
        SELECT staging_purge_started_at IS NOT NULL AS started
        FROM ingestions
        WHERE id = ${candidateRow.id}
      `;
      if (
        candidateRow.status !== "QUEUED" ||
        activeLeaseRows[0]?.exists ||
        purgeStartedRows[0]?.started
      ) {
        return { status: "not_leasable" as const };
      }

      const candidate: LeaseClaimCandidate = {
        id: candidateRow.id,
        batchLabel: candidateRow.batch_label,
        tenantId: candidateRow.tenant_id,
      };
      const grant = createPendingLeaseGrant(params.leaseDurationSeconds);
      let payload: T;
      try {
        payload = await params.buildPayload({ candidate, grant, executor: transaction });
      } catch (error) {
        return { status: "invalid", error };
      }
      await insertLeaseAndMarkProcessing(transaction, {
        candidate,
        workerId: params.workerId,
        grant,
      });
      return { status: "claimed" as const, payload };
    });
  });
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

export async function leaseQueuedIngestionById(params: {
  ingestionId: string;
  workerId?: string;
  leaseDurationSeconds: number;
}): Promise<LeaseByIdResult> {
  return withSchemaClient(async (sql) => {
    return sql.begin(async (transaction) => {
      const candidates = await transaction<IngestionLeaseCandidateRow[]>`
        SELECT ing.id, ing.batch_label, ing.tenant_id, ing.status
        FROM ingestions ing
        WHERE ing.id = ${params.ingestionId}
        FOR UPDATE
      `;

      const candidate = candidates[0];

      if (!candidate) {
        return { status: "not_found" };
      }

      const activeLeaseRows = await transaction<Array<{ exists: boolean }>>`
        SELECT EXISTS(
          SELECT 1
          FROM ingestion_leases lease
          WHERE lease.ingestion_id = ${candidate.id}
            AND lease.released_at IS NULL
            AND lease.lease_expires_at > now()
        ) AS exists
      `;

      if (activeLeaseRows[0]?.exists) {
        return { status: "not_leasable" };
      }

      if (candidate.status !== "QUEUED") {
        return { status: "not_leasable" };
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

      const updatedRows = await transaction<Array<{ id: string }>>`
        UPDATE ingestions
        SET status = 'PROCESSING',
            updated_at = now()
        WHERE id = ${candidate.id}
          AND status = 'QUEUED'
        RETURNING id
      `;

      if (updatedRows.length === 0) {
        return { status: "not_leasable" };
      }

      return {
        status: "leased",
        ingestion: {
          ...mapLeasedIngestion({
            ...candidate,
            status: "QUEUED",
          }),
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

export async function releaseLeaseAndRequeue(params: {
  tenantId: string;
  ingestionId: string;
  leaseId: string;
  leaseTokenId: string;
}): Promise<ReleaseLeaseResult> {
  return withSchemaClient(async (sql) => {
    return sql.begin(async (transaction) => {
      const ingestions = await transaction<Array<{ id: string; status: string }>>`
        SELECT id, status
        FROM ingestions
        WHERE id = ${params.ingestionId}
          AND tenant_id = ${params.tenantId}
        FOR UPDATE
      `;
      const ingestion = ingestions[0];
      if (!ingestion) {
        return { status: "not_found" };
      }

      const leaseRows = await transaction<LeaseRow[]>`
        UPDATE ingestion_leases
        SET released_at = now()
        WHERE id = ${params.leaseId}
          AND ingestion_id = ${params.ingestionId}
          AND lease_token_id = ${params.leaseTokenId}
          AND released_at IS NULL
        RETURNING id, ingestion_id, leased_by, lease_token_id, lease_expires_at, created_at, released_at
      `;
      const lease = leaseRows[0];
      if (!lease) {
        return { status: "inactive" };
      }

      if (ingestion.status === "PROCESSING") {
        const requeuedRows = await transaction<Array<{ id: string }>>`
          UPDATE ingestions
          SET status = 'QUEUED',
              updated_at = now()
          WHERE id = ${params.ingestionId}
            AND tenant_id = ${params.tenantId}
            AND status = 'PROCESSING'
          RETURNING id
        `;
        if (requeuedRows.length === 0) {
          throw new Error(`Processing ingestion '${params.ingestionId}' could not be requeued.`);
        }
      }

      return { status: "released", lease: mapLease(lease) };
    });
  });
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

export async function hasActiveLease(ingestionId: string): Promise<boolean> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<Array<{ exists: boolean }>>`
      SELECT EXISTS(
        SELECT 1
        FROM ingestion_leases
        WHERE ingestion_id = ${ingestionId}
          AND released_at IS NULL
          AND lease_expires_at > now()
      ) AS exists
    `;
  });

  return Boolean(rows[0]?.exists ?? false);
}
