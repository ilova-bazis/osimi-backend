import { withSchemaClient } from "../db/client.ts";

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
  const rows = await withSchemaClient(async (sql) => {
    return await sql<InsertResult[]>`
      INSERT INTO object_events (
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
      VALUES (
        ${crypto.randomUUID()},
        ${params.eventId},
        ${params.tenantId},
        ${params.type},
        ${params.ingestionId ?? null},
        ${params.objectId ?? null},
        CAST(${JSON.stringify(params.payload ?? {})} AS jsonb),
        ${params.actorUserId ?? null},
        COALESCE(${params.createdAt ? params.createdAt.toISOString() : null}::timestamptz, now())
      )
      ON CONFLICT (event_id) DO NOTHING
      RETURNING id
    `;
  });

  return rows.length > 0;
}
