import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, mkdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import {
  classifyArtifactSearchText,
  decodeArtifactSearchText,
  indexMaterializedArtifact,
} from "../../../src/services/artifact-search-indexing-service.ts";
import { runWithRuntimeConfig } from "../../../src/runtime/config.ts";
import type { ObjectArtifactRecord } from "../../../src/repos/object-repo.ts";

let stagingRoot = "";

afterEach(async () => {
  if (stagingRoot) await rm(stagingRoot, { recursive: true, force: true });
  stagingRoot = "";
});

function artifact(
  overrides: Partial<ObjectArtifactRecord> = {},
): ObjectArtifactRecord {
  return {
    id: "10000000-0000-4000-8000-000000000001",
    objectId: "OBJ-SEARCH-UNIT",
    kind: "ocr_text",
    variant: null,
    storageKey: "unit/artifact.txt",
    contentType: "text/plain; charset=utf-8",
    sizeBytes: 2,
    createdAt: new Date("2026-08-05T00:00:00.000Z"),
    ...overrides,
  };
}

describe("artifact search text indexing", () => {
  test("accepts only OCR/transcript text bodies within the limit", () => {
    expect(classifyArtifactSearchText({
      kind: "ocr_text",
      contentType: "text/plain; charset=utf-8",
      sizeBytes: 10,
      maxBytes: 10,
    })).toEqual({ outcome: "eligible" });
    expect(classifyArtifactSearchText({
      kind: "transcript",
      contentType: "TEXT/VTT",
      sizeBytes: 1,
      maxBytes: 10,
    })).toEqual({ outcome: "eligible" });

    for (const input of [
      { kind: "pdf" as const, contentType: "text/plain", sizeBytes: 1 },
      { kind: "ocr_text" as const, contentType: "application/json", sizeBytes: 1 },
      { kind: "ocr_text" as const, contentType: "text/plain; charset", sizeBytes: 1 },
    ]) {
      expect(classifyArtifactSearchText({ ...input, maxBytes: 10 })).toEqual({
        outcome: "skipped",
        reason: "unsupported",
      });
    }
    expect(classifyArtifactSearchText({
      kind: "ocr_text",
      contentType: "text/plain",
      sizeBytes: 11,
      maxBytes: 10,
    })).toEqual({ outcome: "skipped", reason: "over_limit" });
  });

  test("preserves decoded text and skips malformed or empty UTF-8", () => {
    const original = "  First line\nSecond line  ";
    expect(decodeArtifactSearchText(new TextEncoder().encode(original))).toEqual({
      outcome: "indexed",
      text: original,
    });
    expect(decodeArtifactSearchText(new TextEncoder().encode(" \n\t "))).toEqual({
      outcome: "skipped",
      reason: "empty",
    });
    expect(decodeArtifactSearchText(Uint8Array.from([0xc3, 0x28]))).toEqual({
      outcome: "skipped",
      reason: "malformed_utf8",
    });
  });

  test("uses the live rules and staging path when provenance is unavailable", async () => {
    stagingRoot = await mkdtemp(join(tmpdir(), "artifact-search-unit-"));
    const runtimeConfig = { stagingRoot, maxArtifactSearchTextBytes: 2 };

    expect(await runWithRuntimeConfig(runtimeConfig, () =>
      indexMaterializedArtifact({
        tenantId: "00000000-0000-4000-8000-000000000001",
        objectId: "OBJ-SEARCH-UNIT",
        artifact: artifact(),
      })),
    ).toEqual({ outcome: "skipped", reason: "unavailable" });

    const malformedPath = join(stagingRoot, "unit/malformed.txt");
    await mkdir(dirname(malformedPath), { recursive: true });
    await Bun.write(malformedPath, Uint8Array.from([0xc3, 0x28]));
    expect(await runWithRuntimeConfig(runtimeConfig, () =>
      indexMaterializedArtifact({
        tenantId: "00000000-0000-4000-8000-000000000001",
        objectId: "OBJ-SEARCH-UNIT",
        artifact: artifact({ storageKey: "unit/malformed.txt" }),
      })),
    ).toEqual({ outcome: "skipped", reason: "malformed_utf8" });

    await Bun.write(join(stagingRoot, "unit/empty.txt"), " \n");
    expect(await runWithRuntimeConfig(runtimeConfig, () =>
      indexMaterializedArtifact({
        tenantId: "00000000-0000-4000-8000-000000000001",
        objectId: "OBJ-SEARCH-UNIT",
        artifact: artifact({ storageKey: "unit/empty.txt" }),
      })),
    ).toEqual({ outcome: "skipped", reason: "empty" });

    expect(await runWithRuntimeConfig(runtimeConfig, () =>
      indexMaterializedArtifact({
        tenantId: "00000000-0000-4000-8000-000000000001",
        objectId: "OBJ-SEARCH-UNIT",
        artifact: artifact({ contentType: "application/json" }),
      })),
    ).toEqual({ outcome: "skipped", reason: "unsupported" });
    expect(await runWithRuntimeConfig(runtimeConfig, () =>
      indexMaterializedArtifact({
        tenantId: "00000000-0000-4000-8000-000000000001",
        objectId: "OBJ-SEARCH-UNIT",
        artifact: artifact({ sizeBytes: 3 }),
      })),
    ).toEqual({ outcome: "skipped", reason: "over_limit" });
  });
});
