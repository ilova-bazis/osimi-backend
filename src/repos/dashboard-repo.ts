import { db, qualifiedTableName } from "../db/runtime.ts";

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
  payload: unknown;
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
  payload: unknown;
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

export async function getDashboardSummary(tenantId: string): Promise<DashboardSummary> {
  const sql = db();
  const ingestionsTable = qualifiedTableName("ingestions");
  const objectsTable = qualifiedTableName("objects");

  const ingestionRows = (await sql.unsafe(
    `
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
      FROM ${ingestionsTable}
      WHERE tenant_id = $1
    `,
    [tenantId],
  )) as SummaryCountsRow[];

  const objectRows = (await sql.unsafe(
    `
      SELECT COUNT(*)::int AS total_objects
      FROM ${objectsTable}
      WHERE tenant_id = $1
    `,
    [tenantId],
  )) as TotalObjectsRow[];

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
  const sql = db();
  const eventsTable = qualifiedTableName("object_events");
  const values: Array<string | number> = [params.tenantId, params.limit];
  let cursorClause = "";

  if (params.cursorCreatedAt && params.cursorId) {
    values.push(params.cursorCreatedAt, params.cursorId);
    cursorClause = "AND (created_at, id) < ($3::timestamptz, $4::uuid)";
  }

  const rows = (await sql.unsafe(
    `
      SELECT id, event_id, type, ingestion_id, object_id, payload, actor_user_id, created_at
      FROM ${eventsTable}
      WHERE tenant_id = $1
      ${cursorClause}
      ORDER BY created_at DESC, id DESC
      LIMIT $2
    `,
    values,
  )) as ActivityRow[];

  return rows.map(mapActivity);
}
