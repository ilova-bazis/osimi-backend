import { describe, expect, test } from "bun:test";

import { catalogSummaryFields, resolveEffectiveSummary } from "../../src/domain/ingestions/effective-summary.ts";
import type { JsonObject } from "../../src/validation/ingestion.ts";

const date = { value: null, approximate: false, confidence: "low" as const, note: null };
const base = {
  title: { primary: "Default", original_script: null, translations: [] },
  classification: { summary: "Default description", tags: ["default"] },
  dates: { published: date, created: date },
  people: { mentioned: ["Default person"], authors: ["Author"] },
};

describe("effective item summary", () => {
  test("preserves extensions without promoting envelope overrides", () => {
    const effective = resolveEffectiveSummary(base, {
      custom: { nested: { retained: true } },
      title: { edition: "first" },
      schema_version: "forged", object_id: "forged", ingestion_id: "forged",
      ingestion_item_id: "forged", item_index: 999, access: { level: "public" },
      updated_at: "forged", updated_by: "forged", item_kind: "audio",
    });
    const fields = catalogSummaryFields(effective);
    expect(fields.custom).toEqual({ nested: { retained: true } });
    expect(fields.title).toMatchObject({ primary: "Default", edition: "first" });
    for (const key of ["schema_version", "object_id", "ingestion_id", "ingestion_item_id", "item_index", "access", "updated_at", "updated_by", "item_kind"]) {
      expect(fields).not.toHaveProperty(key);
    }
  });

  test("inherits defaults and replaces arrays without mutating the source", () => {
    const result = resolveEffectiveSummary(base, {
      classification: { summary: "Item", tags: ["item"] },
      people: { mentioned: [" Person "] },
      custom: { retained: true },
    });
    expect(result.classification).toEqual({ summary: "Item", tags: ["item"] });
    expect(result.people).toEqual({ mentioned: [" Person "], authors: ["Author"] });
    expect(result.custom).toEqual({ retained: true });
    expect(base.classification.summary).toBe("Default description");
    expect(resolveEffectiveSummary(base, {})).toEqual(base);
  });

  test("clears inherited values explicitly and retains date normalization", () => {
    const result = resolveEffectiveSummary(base, {
      classification: { summary: null, tags: [] },
      people: { mentioned: [] },
      dates: { collection_note: "archive", created: { value: "bad", confidence: "high", source_label: "scan" } },
    });
    expect(result.classification).toEqual({ summary: null, tags: [] });
    expect(result.people?.mentioned).toEqual([]);
    expect(result.dates).toMatchObject({ collection_note: "archive" });
    expect(result.dates.created).toEqual({ ...date, confidence: "high", source_label: "scan" });
  });

  test.each<JsonObject>([
    { classification: null },
    { classification: { tags: "invalid" } },
    { people: { mentioned: [null] } },
  ])("rejects invalid projected fields (%j)", (patch) => {
    expect(() => resolveEffectiveSummary(base, patch)).toThrow();
  });
});
