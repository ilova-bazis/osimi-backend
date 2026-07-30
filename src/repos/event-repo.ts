import { withSchemaClient } from "../db/client.ts";
import type { SqlExecutor } from "../db/client.ts";

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
    | "INGESTION_ITEM_CREATED"
    | "INGESTION_ITEM_UPDATED"
    | "INGESTION_ITEM_PROCESSING"
    | "INGESTION_ITEM_COMPLETED"
    | "INGESTION_ITEM_FAILED"
    | "OBJECT_CREATED"
    | "ARTIFACT_CREATED";

interface InsertResult {
    id: string;
}

interface ObjectEventRow {
    id: string;
    tenant_id: string;
    type: ObjectEventType;
    ingestion_id: string | null;
    ingestion_item_id: string | null;
    object_id: string | null;
    payload: Record<string, unknown>;
    actor_user_id: string | null;
    created_at: Date;
}

export type ReserveObjectEventResult =
    | { status: "inserted"; id: string }
    | { status: "duplicate" }
    | { status: "conflict" };

function stableStringify(value: unknown): string {
    if (Array.isArray(value)) {
        return `[${value.map(stableStringify).join(",")}]`;
    }

    if (value && typeof value === "object") {
        const entries = Object.entries(value as Record<string, unknown>)
            .sort(([left], [right]) => left.localeCompare(right))
            .map(([key, entry]) => `${JSON.stringify(key)}:${stableStringify(entry)}`);
        return `{${entries.join(",")}}`;
    }

    return JSON.stringify(value);
}

export async function reserveObjectEventWithExecutor(
    executor: SqlExecutor,
    params: {
        eventId: string;
        tenantId: string;
        type: ObjectEventType;
        ingestionId?: string;
        ingestionItemId?: string;
        objectId?: string;
        payload: Record<string, unknown>;
        actorUserId?: string;
        createdAt: Date;
    },
): Promise<ReserveObjectEventResult> {
    const rows = await executor<InsertResult[]>`
      INSERT INTO object_events (
        id,
        event_id,
        tenant_id,
        type,
        ingestion_id,
        ingestion_item_id,
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
        ${params.ingestionItemId ?? null},
        ${null},
        ${params.payload},
        ${params.actorUserId ?? null},
        ${params.createdAt.toISOString()}
      )
      ON CONFLICT (event_id) DO NOTHING
      RETURNING id
    `;

    const inserted = rows[0];
    if (inserted) {
        return { status: "inserted", id: inserted.id };
    }

    const existingRows = await executor<ObjectEventRow[]>`
      SELECT id, tenant_id, type, ingestion_id, ingestion_item_id, object_id,
             payload, actor_user_id, created_at
      FROM object_events
      WHERE event_id = ${params.eventId}
      LIMIT 1
    `;
    const existing = existingRows[0];
    if (!existing) {
        throw new Error(`Event '${params.eventId}' could not be reserved or loaded.`);
    }

    const hasMatchingEnvelope =
        existing.tenant_id === params.tenantId &&
        existing.type === params.type &&
        existing.ingestion_id === (params.ingestionId ?? null) &&
        existing.ingestion_item_id === (params.ingestionItemId ?? null) &&
        existing.object_id === (params.objectId ?? null) &&
        stableStringify(existing.payload) === stableStringify(params.payload);

    return hasMatchingEnvelope ? { status: "duplicate" } : { status: "conflict" };
}

export async function finalizeObjectEventWithExecutor(
    executor: SqlExecutor,
    params: {
        id: string;
        objectId?: string;
    },
): Promise<void> {
    await executor`
      UPDATE object_events
      SET object_id = ${params.objectId ?? null}
      WHERE id = ${params.id}
    `;
}

export async function insertObjectEvent(params: {
    eventId: string;
    tenantId: string;
    type: ObjectEventType;
    ingestionId?: string;
    ingestionItemId?: string;
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
        ingestion_item_id,
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
        ${params.ingestionItemId ?? null},
        ${params.objectId ?? null},
        ${params.payload ?? {}},
        ${params.actorUserId ?? null},
        COALESCE(${params.createdAt ? params.createdAt.toISOString() : null}::timestamptz, now())
      )
      ON CONFLICT (event_id) DO NOTHING
      RETURNING id
    `;
    });

    return rows.length > 0;
}
