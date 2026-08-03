import { withSchemaClient } from "../db/client.ts";
import type { SqlExecutor } from "../db/client.ts";

type IdempotencyState = "PROCESSING" | "COMPLETED";

interface IdempotencyRow {
  id: string;
  tenant_id: string;
  actor_user_id: string;
  endpoint: string;
  idempotency_key: string;
  request_fingerprint: string;
  state: IdempotencyState;
  owner_token: string;
  locked_until: Date;
  status_code: number | null;
  response_body: unknown;
  expires_at: Date | null;
  replayable?: boolean;
  locked?: boolean;
}

export interface IdempotencyRecord {
  id: string;
  tenantId: string;
  actorUserId: string;
  endpoint: string;
  idempotencyKey: string;
  requestFingerprint: string;
  state: IdempotencyState;
  ownerToken: string;
  lockedUntil: Date;
  statusCode: number | null;
  responseBody: unknown;
  expiresAt: Date | null;
  replayable?: boolean;
  locked?: boolean;
}

export type IdempotencyReservation =
  | { kind: "acquired"; record: IdempotencyRecord }
  | { kind: "replay"; record: IdempotencyRecord }
  | { kind: "processing"; record: IdempotencyRecord }
  | { kind: "mismatch" };

function mapRecord(row: IdempotencyRow): IdempotencyRecord {
  return {
    id: row.id,
    tenantId: row.tenant_id,
    actorUserId: row.actor_user_id,
    endpoint: row.endpoint,
    idempotencyKey: row.idempotency_key,
    requestFingerprint: row.request_fingerprint,
    state: row.state,
    ownerToken: row.owner_token,
    lockedUntil: new Date(row.locked_until),
    statusCode: row.status_code,
    responseBody: row.response_body,
    expiresAt: row.expires_at ? new Date(row.expires_at) : null,
    replayable: row.replayable,
    locked: row.locked,
  };
}

async function findRecordForUpdate(params: {
  tenantId: string;
  actorUserId: string;
  endpoint: string;
  idempotencyKey: string;
  executor: SqlExecutor;
}): Promise<IdempotencyRecord | undefined> {
  const rows = await params.executor<IdempotencyRow[]>`
    SELECT id, tenant_id, actor_user_id, endpoint, idempotency_key,
      request_fingerprint, state, owner_token, locked_until, status_code,
      response_body, expires_at, expires_at > now() AS replayable,
      locked_until > now() AS locked
    FROM ingestion_idempotency_records
    WHERE tenant_id = ${params.tenantId}
      AND actor_user_id = ${params.actorUserId}
      AND endpoint = ${params.endpoint}
      AND idempotency_key = ${params.idempotencyKey}
    FOR UPDATE
  `;

  const row = rows[0];
  return row ? mapRecord(row) : undefined;
}

export async function reserveIngestionIdempotencyKey(params: {
  tenantId: string;
  actorUserId: string;
  endpoint: string;
  idempotencyKey: string;
  requestFingerprint: string;
  ownerToken: string;
  lockSeconds: number;
  retentionDays: number;
}): Promise<IdempotencyReservation> {
  return withSchemaClient(async (sql) => {
    return sql.begin(async (executor) => {
      const inserted = await executor<IdempotencyRow[]>`
        INSERT INTO ingestion_idempotency_records (
          id, tenant_id, actor_user_id, endpoint, idempotency_key,
          request_fingerprint, state, owner_token, locked_until
        )
        VALUES (
          ${crypto.randomUUID()}, ${params.tenantId}, ${params.actorUserId},
          ${params.endpoint}, ${params.idempotencyKey},
          ${params.requestFingerprint}, 'PROCESSING', ${params.ownerToken},
          now() + (${params.lockSeconds} * interval '1 second')
        )
        ON CONFLICT (tenant_id, actor_user_id, endpoint, idempotency_key)
        DO NOTHING
        RETURNING id, tenant_id, actor_user_id, endpoint, idempotency_key,
          request_fingerprint, state, owner_token, locked_until, status_code,
          response_body, expires_at
      `;
      if (inserted[0]) {
        return { kind: "acquired" as const, record: mapRecord(inserted[0]) };
      }

      const existing = await findRecordForUpdate({ ...params, executor });
      if (!existing) {
        return { kind: "mismatch" as const };
      }
      if (existing.state === "COMPLETED" && existing.replayable) {
        if (existing.requestFingerprint !== params.requestFingerprint) {
          return { kind: "mismatch" as const };
        }
        return { kind: "replay" as const, record: existing };
      }

      const expiredCompleted = existing.state === "COMPLETED";
      if (!expiredCompleted && existing.requestFingerprint !== params.requestFingerprint) {
        return { kind: "mismatch" as const };
      }

      if (
        (existing.state === "PROCESSING" && existing.locked) ||
        (existing.state === "COMPLETED" && !existing.expiresAt)
      ) {
        return { kind: "processing" as const, record: existing };
      }

      const recoveredRows = await executor<IdempotencyRow[]>`
        UPDATE ingestion_idempotency_records
        SET state = 'PROCESSING',
            request_fingerprint = ${params.requestFingerprint},
            owner_token = ${params.ownerToken},
            locked_until = now() + (${params.lockSeconds} * interval '1 second'),
            status_code = NULL,
            response_body = NULL,
            completed_at = NULL,
            expires_at = NULL,
            updated_at = now()
        WHERE id = ${existing.id}
        RETURNING id, tenant_id, actor_user_id, endpoint, idempotency_key,
          request_fingerprint, state, owner_token, locked_until, status_code,
          response_body, expires_at
      `;

      return { kind: "acquired" as const, record: mapRecord(recoveredRows[0]!) };
    });
  });
}

export async function lockIngestionIdempotencyKey(params: {
  tenantId: string;
  actorUserId: string;
  endpoint: string;
  idempotencyKey: string;
  executor: SqlExecutor;
}): Promise<IdempotencyRecord | undefined> {
  return findRecordForUpdate(params);
}

export async function completeIngestionIdempotencyKey(params: {
  recordId: string;
  ownerToken: string;
  statusCode: number;
  responseBody: unknown;
  retentionDays: number;
  executor: SqlExecutor;
}): Promise<boolean> {
  const rows = await params.executor<Array<{ id: string }>>`
    UPDATE ingestion_idempotency_records
    SET state = 'COMPLETED',
        status_code = ${params.statusCode},
        response_body = ${params.responseBody},
        completed_at = now(),
        expires_at = now() + (${params.retentionDays} * interval '1 day'),
        updated_at = now()
    WHERE id = ${params.recordId}
      AND owner_token = ${params.ownerToken}
      AND state = 'PROCESSING'
    RETURNING id
  `;

  return rows.length > 0;
}

export async function releaseIngestionIdempotencyKey(params: {
  recordId: string;
  ownerToken: string;
}): Promise<void> {
  await withSchemaClient(async (sql) => {
    await sql`
      DELETE FROM ingestion_idempotency_records
      WHERE id = ${params.recordId}
        AND owner_token = ${params.ownerToken}
        AND state = 'PROCESSING'
    `;
  });
}
