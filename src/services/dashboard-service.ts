import { decodeCursor, encodeCursor, parsePaginationParams } from "../http/pagination.ts";
import { ValidationError } from "../http/errors.ts";
import { getDashboardSummary, listDashboardActivity } from "../repos/dashboard-repo.ts";

interface ActivityCursorPayload {
  created_at: string;
  id: string;
}

export async function getDashboardSummaryForTenant(tenantId: string): Promise<Record<string, unknown>> {
  const summary = await getDashboardSummary(tenantId);

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
  tenantId: string;
  url: URL;
}): Promise<Record<string, unknown>> {
  const pagination = parsePaginationParams(params.url);

  let cursor: ActivityCursorPayload | undefined;
  if (pagination.cursor) {
    const decoded = decodeCursor<Record<string, unknown>>(pagination.cursor);

    if (typeof decoded.created_at !== "string" || typeof decoded.id !== "string") {
      throw new ValidationError("Query parameter 'cursor' is invalid.");
    }

    cursor = {
      created_at: decoded.created_at,
      id: decoded.id,
    };
  }

  const records = await listDashboardActivity({
    tenantId: params.tenantId,
    limit: pagination.limit + 1,
    cursorCreatedAt: cursor?.created_at,
    cursorId: cursor?.id,
  });

  const hasMore = records.length > pagination.limit;
  const visible = hasMore ? records.slice(0, pagination.limit) : records;
  const lastItem = visible.at(-1);

  return {
    activity: visible.map(item => ({
      id: item.id,
      event_id: item.eventId,
      type: item.type,
      ingestion_id: item.ingestionId ?? null,
      object_id: item.objectId ?? null,
      payload: item.payload,
      actor_user_id: item.actorUserId ?? null,
      created_at: item.createdAt.toISOString(),
    })),
    next_cursor: hasMore && lastItem
      ? encodeCursor({
          created_at: lastItem.createdAt.toISOString(),
          id: lastItem.id,
        })
      : null,
  };
}
