import { encodeCursor } from "../http/pagination.ts";
import type { AuthenticatedContext } from "../auth/guards.ts";
import { getDashboardSummary, listDashboardActivity } from "../repos/dashboard-repo.ts";
import {
  parseDashboardActivityPayload,
  type DashboardActivityQuery,
  type DashboardActivityResponse,
  type DashboardSummaryResponse,
} from "../validation/dashboard.ts";

export async function getDashboardSummaryForTenant(params: {
  auth: AuthenticatedContext;
}): Promise<DashboardSummaryResponse> {
  const summary = await getDashboardSummary(params.auth.tenantId);

  return {
    summary: {
      total_ingestions: summary.totalIngestions,
      total_objects: summary.totalObjects,
      processed_today: summary.processedToday,
      processed_week: summary.processedWeek,
      failed_count: summary.failedCount,
    },
  };
}

export async function getDashboardActivityForTenant(params: {
  auth: AuthenticatedContext;
  query: DashboardActivityQuery;
}): Promise<DashboardActivityResponse> {
  const pagination = params.query;

  const records = await listDashboardActivity({
    tenantId: params.auth.tenantId,
    limit: pagination.limit + 1,
    cursorCreatedAt: pagination.cursor?.created_at,
    cursorId: pagination.cursor?.id,
  });

  const hasMore = records.length > pagination.limit;
  const visible = hasMore ? records.slice(0, pagination.limit) : records;
  const lastItem = visible.at(-1);

  return {
    activity: visible.map((item) => ({
      id: item.id,
      event_id: item.eventId,
      type: item.type,
      ingestion_id: item.ingestionId ?? null,
      object_id: item.objectId ?? null,
      payload: parseDashboardActivityPayload(item.payload),
      actor_user_id: item.actorUserId ?? null,
      created_at: item.createdAt.toISOString(),
    })),
    next_cursor:
      hasMore && lastItem
        ? encodeCursor({
            created_at: lastItem.createdAt.toISOString(),
            id: lastItem.id,
          })
        : null,
  };
}
