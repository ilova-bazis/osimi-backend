import { z } from "zod";

import { ingestionSummarySchema, parseIngestionSummary } from "../../validation/catalog.ts";
import type { JsonObject } from "../../validation/ingestion.ts";
import { mapZodErrorToValidation } from "../../validation/zod-errors.ts";

const catalogEnvelopeFields = new Set([
  "schema_version", "object_id", "ingestion_id", "ingestion_item_id", "item_index",
  "updated_at", "updated_by", "access", "item_kind",
]);

export function catalogSummaryFields(summary: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(summary).filter(([key]) => !catalogEnvelopeFields.has(key)));
}

function merge(base: JsonObject, patch: JsonObject): JsonObject {
  const result = { ...base };
  for (const [key, value] of Object.entries(patch)) {
    const previous = result[key];
    result[key] = value !== null && typeof value === "object" && !Array.isArray(value)
      && previous !== null && typeof previous === "object" && !Array.isArray(previous)
      ? merge(previous as JsonObject, value as JsonObject)
      : value;
  }
  return result;
}

// Item summaries permit extension fields. Validate projected fields without
// rejecting worker-specific metadata retained in the effective catalog.
const projectedSchema = z.object({
  title: ingestionSummarySchema.shape.title.passthrough(),
  classification: ingestionSummarySchema.shape.classification.passthrough(),
  people: ingestionSummarySchema.shape.people.unwrap().passthrough().optional(),
}).passthrough();

type CatalogDates = ReturnType<typeof parseIngestionSummary>["dates"];
type EffectiveDates = { [Key in keyof CatalogDates]: CatalogDates[Key] & Record<string, unknown> } & Record<string, unknown>;

export function resolveEffectiveSummary(ingestionSummary: unknown, itemSummary: JsonObject): z.infer<typeof projectedSchema> & { dates: EffectiveDates } {
  const base = parseIngestionSummary(ingestionSummary);
  const merged = merge(base as JsonObject, itemSummary);
  const parsed = projectedSchema.safeParse(merged);
  if (!parsed.success) throw mapZodErrorToValidation(parsed.error);

  const rawDates = merged.dates;
  const dates: EffectiveDates = {
    ...(rawDates && typeof rawDates === "object" && !Array.isArray(rawDates) ? rawDates : {}),
    ...base.dates,
  };
  for (const key of ["published", "created"] as const) {
    const candidate = rawDates && typeof rawDates === "object" && !Array.isArray(rawDates)
      ? rawDates[key] : null;
    if (!candidate || typeof candidate !== "object" || Array.isArray(candidate)) continue;
    const fallback = base.dates[key];
    dates[key] = {
      ...candidate,
      value: candidate.value === null || (typeof candidate.value === "string" && /^\d{4}(-\d{2})?(-\d{2})?$/.test(candidate.value)) ? candidate.value : fallback.value,
      approximate: typeof candidate.approximate === "boolean" ? candidate.approximate : fallback.approximate,
      confidence: candidate.confidence === "low" || candidate.confidence === "medium" || candidate.confidence === "high" ? candidate.confidence : fallback.confidence,
      note: candidate.note === null || typeof candidate.note === "string" ? candidate.note : fallback.note,
    };
  }
  return { ...parsed.data, dates };
}
