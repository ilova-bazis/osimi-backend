import { db, qualifiedTableName } from "../db/runtime.ts";

type ObjectEventType =
  | "INGESTION_SUBMITTED"
  | "INGESTION_QUEUED"
  | "INGESTION_PROCESSING"
  | "INGESTION_COMPLETED"
  | "INGESTION_FAILED"
  | "INGESTION_CANCELED"
  | "LEASE_GRANTED"
  | "LEASE_RENEWED"
  | "LEASE_EXPIRED"
  | "LEASE_RELEASED"
  | "FILE_VALIDATED"
  | "FILE_FAILED"
  | "PIPELINE_STEP_STARTED"
  | "PIPELINE_STEP_COMPLETED"
  | "PIPELINE_STEP_FAILED"
  | "OBJECT_CREATED"
  | "ARTIFACT_CREATED";

interface InsertResult {
  id: string;
}

export async function insertObjectEvent(params: {
  eventId: string;
  tenantId: string;
  type: ObjectEventType;
  ingestionId?: string;
  objectId?: string;
  payload: Record<string, unknown>;
  actorUserId?: string;
  createdAt?: Date;
}): Promise<boolean> {
  const sql = db();
  const eventsTable = qualifiedTableName("object_events");

  const rows = (await sql.unsafe(
    `
      INSERT INTO ${eventsTable} (
        id,
        event_id,
        tenant_id,
        type,
        ingestion_id,
        object_id,
        payload,
        actor_user_id,
        created_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, COALESCE($9::timestamptz, now()))
      ON CONFLICT (event_id) DO NOTHING
      RETURNING id
    `,
    [
      crypto.randomUUID(),
      params.eventId,
      params.tenantId,
      params.type,
      params.ingestionId ?? null,
      params.objectId ?? null,
      JSON.stringify(params.payload ?? {}),
      params.actorUserId ?? null,
      params.createdAt ? params.createdAt.toISOString() : null,
    ],
  )) as InsertResult[];

  return rows.length > 0;
}
