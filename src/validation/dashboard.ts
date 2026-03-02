import { z } from "zod";

import { decodeCursor } from "../http/pagination.ts";
import type { JsonObject } from "./ingestion.ts";
import { jsonObjectSchema } from "./ingestion.ts";
import { mapZodErrorToValidation } from "./zod-errors.ts";

export const dashboardCursorPayloadSchema = z.strictObject({
  created_at: z.string(),
  id: z.string(),
});

export const dashboardActivityQuerySchema = z.strictObject({
  limit: z.coerce.number().int().min(1).max(200).default(50),
  cursor: z.string().trim().min(1).optional(),
  ingestion_id: z.uuid().optional(),
});

export const dashboardSummaryResponseSchema = z.strictObject({
  summary: z.strictObject({
    total_ingestions: z.number(),
    total_objects: z.number(),
    processed_today: z.number(),
    processed_week: z.number(),
    failed_count: z.number(),
  }),
});

export const dashboardActivityItemSchema = z.strictObject({
  id: z.string(),
  event_id: z.string(),
  type: z.string(),
  ingestion_id: z.string().nullable(),
  object_id: z.string().nullable(),
  payload: jsonObjectSchema,
  actor_user_id: z.string().nullable(),
  created_at: z.string(),
});

export const dashboardActivityResponseSchema = z.strictObject({
  activity: z.array(dashboardActivityItemSchema),
  next_cursor: z.string().nullable(),
});

export type DashboardCursorPayload = z.infer<typeof dashboardCursorPayloadSchema>;
export interface DashboardActivityQuery {
  limit: number;
  cursor?: DashboardCursorPayload;
  ingestionId?: string;
}
export type DashboardSummaryResponse = z.infer<typeof dashboardSummaryResponseSchema>;
export type DashboardActivityItem = z.infer<typeof dashboardActivityItemSchema>;
export type DashboardActivityResponse = z.infer<typeof dashboardActivityResponseSchema>;

export function parseDashboardActivityQuery(url: URL): DashboardActivityQuery {
  const parsed = dashboardActivityQuerySchema.safeParse({
    limit: url.searchParams.get("limit") ?? undefined,
    cursor: url.searchParams.get("cursor") ?? undefined,
    ingestion_id: url.searchParams.get("ingestion_id") ?? undefined,
  });

  if (!parsed.success) {
    throw mapZodErrorToValidation(parsed.error);
  }

  let cursor: DashboardCursorPayload | undefined;
  if (parsed.data.cursor) {
    const decoded = decodeCursor<JsonObject>(parsed.data.cursor);
    cursor = parseDashboardCursorPayload(decoded);
  }

  return {
    limit: parsed.data.limit,
    cursor,
    ingestionId: parsed.data.ingestion_id,
  };
}

export function parseDashboardCursorPayload(value: unknown): DashboardCursorPayload {
  const parsed = dashboardCursorPayloadSchema.safeParse(value);
  if (!parsed.success) {
    throw mapZodErrorToValidation(parsed.error);
  }

  return parsed.data;
}

export function parseDashboardActivityPayload(value: unknown): JsonObject {
  const parsed = jsonObjectSchema.safeParse(value);
  if (!parsed.success) {
    throw mapZodErrorToValidation(parsed.error);
  }

  return parsed.data;
}
