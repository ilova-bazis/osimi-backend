import { describe, expect, test } from "bun:test";

import { backfillArtifactSearchText } from "../../../src/services/artifact-search-backfill-service.ts";
import type { ArtifactSearchBackfillCandidate } from "../../../src/repos/object-artifact-search-document-repo.ts";

function candidate(id: string): ArtifactSearchBackfillCandidate {
  return {
    tenantId: "00000000-0000-4000-8000-000000000001",
    artifact: {
      id,
      objectId: `OBJ-${id}`,
      kind: "ocr_text",
      variant: null,
      storageKey: `${id}.txt`,
      contentType: "text/plain",
      sizeBytes: 4,
      createdAt: new Date("2026-08-05T00:00:00.000Z"),
    },
  };
}

describe("artifact search text backfill", () => {
  test("processes keyset batches and advances past skipped and failed rows", async () => {
    const candidates = [
      candidate("00000000-0000-4000-8000-000000000001"),
      candidate("00000000-0000-4000-8000-000000000002"),
      candidate("00000000-0000-4000-8000-000000000003"),
      candidate("00000000-0000-4000-8000-000000000004"),
      candidate("00000000-0000-4000-8000-000000000005"),
    ];
    const cursors: Array<string | undefined> = [];
    const failures: string[] = [];

    const summary = await backfillArtifactSearchText(2, {
      listCandidates: async ({ afterArtifactId, limit }) => {
        cursors.push(afterArtifactId);
        return candidates.filter((item) =>
          afterArtifactId === undefined || item.artifact.id > afterArtifactId
        ).slice(0, limit);
      },
      indexArtifact: async (item) => {
        if (item === candidates[1]) return { outcome: "skipped", reason: "unavailable" };
        if (item === candidates[2]) throw new Error("private content must not be logged");
        if (item === candidates[3]) return { outcome: "skipped", reason: "empty" };
        if (item === candidates[4]) return { outcome: "skipped", reason: "unsupported" };
        return { outcome: "indexed", text: "private content" };
      },
      reportFailure: (item) => failures.push(item.artifact.id),
    });

    expect(cursors).toEqual([
      undefined,
      "00000000-0000-4000-8000-000000000002",
      "00000000-0000-4000-8000-000000000004",
    ]);
    expect(summary).toEqual({
      processed: 5,
      indexed: 1,
      skipped: {
        unsupported: 1,
        over_limit: 0,
        unavailable: 1,
        malformed_utf8: 0,
        empty: 1,
      },
      failures: 1,
    });
    expect(failures).toEqual(["00000000-0000-4000-8000-000000000003"]);
  });
});
