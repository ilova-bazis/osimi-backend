import { withSchemaClient } from "../db/client.ts";
import type { JsonObject } from "../validation/ingestion.ts";

interface SummaryCountsRow {
  total_ingestions: number;
  failed_count: number;
  processed_today: number;
  processed_week: number;
}

interface TotalObjectsRow {
  total_objects: number;
}

interface ActivityRow {
  id: string;
  event_id: string;
  type: string;
  ingestion_id: string | null;
  object_id: string | null;
  payload: JsonObject;
  actor_user_id: string | null;
  created_at: Date;
}

export interface DashboardSummary {
  totalIngestions: number;
  totalObjects: number;
  processedToday: number;
  processedWeek: number;
  failedCount: number;
}

export interface ActivityRecord {
  id: string;
  eventId: string;
  type: string;
  ingestionId?: string;
  objectId?: string;
  payload: JsonObject;
  actorUserId?: string;
  createdAt: Date;
}

function mapActivity(row: ActivityRow): ActivityRecord {
  return {
    id: row.id,
    eventId: row.event_id,
    type: row.type,
    ingestionId: row.ingestion_id ?? undefined,
    objectId: row.object_id ?? undefined,
    payload: row.payload,
    actorUserId: row.actor_user_id ?? undefined,
    createdAt: new Date(row.created_at),
  };
}

export async function getDashboardSummary(
  tenantId: string,
): Promise<DashboardSummary> {
  const ingestionRows = await withSchemaClient(async (sql) => {
    return await sql<SummaryCountsRow[]>`
      SELECT
        COUNT(*)::int AS total_ingestions,
        COUNT(*) FILTER (WHERE status = 'FAILED')::int AS failed_count,
        COUNT(*) FILTER (
          WHERE status = 'COMPLETED'
            AND updated_at >= date_trunc('day', now())
        )::int AS processed_today,
        COUNT(*) FILTER (
          WHERE status = 'COMPLETED'
            AND updated_at >= date_trunc('week', now())
        )::int AS processed_week
      FROM ingestions
      WHERE tenant_id = ${tenantId}
    `;
  });

  const objectRows = await withSchemaClient(async (sql) => {
    return await sql<TotalObjectsRow[]>`
      SELECT COUNT(*)::int AS total_objects
      FROM objects
      WHERE tenant_id = ${tenantId}
    `;
  });

  return {
    totalIngestions: Number(ingestionRows[0]?.total_ingestions ?? 0),
    failedCount: Number(ingestionRows[0]?.failed_count ?? 0),
    processedToday: Number(ingestionRows[0]?.processed_today ?? 0),
    processedWeek: Number(ingestionRows[0]?.processed_week ?? 0),
    totalObjects: Number(objectRows[0]?.total_objects ?? 0),
  };
}

export async function listDashboardActivity(params: {
  tenantId: string;
  limit: number;
  cursorCreatedAt?: string;
  cursorId?: string;
}): Promise<ActivityRecord[]> {
  const rows = await withSchemaClient(async (sql) => {
    if (params.cursorCreatedAt && params.cursorId) {
      return await sql<ActivityRow[]>`
        SELECT id, event_id, type, ingestion_id, object_id, payload, actor_user_id, created_at
        FROM object_events
        WHERE tenant_id = ${params.tenantId}
          AND (created_at, id) < (${params.cursorCreatedAt}::timestamptz, ${params.cursorId}::uuid)
        ORDER BY created_at DESC, id DESC
        LIMIT ${params.limit}
      `;
    }

    return await sql<ActivityRow[]>`
      SELECT id, event_id, type, ingestion_id, object_id, payload, actor_user_id, created_at
      FROM object_events
      WHERE tenant_id = ${params.tenantId}
      ORDER BY created_at DESC, id DESC
      LIMIT ${params.limit}
    `;
  });

  return rows.map(mapActivity);
}
