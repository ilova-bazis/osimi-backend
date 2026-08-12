import { describe, expect, test } from "bun:test";

import {
  DEFAULT_BATCH_SIZE,
  parseBatchSize,
} from "../../../src/cli/backfill-artifact-search-text.ts";

describe("artifact search backfill CLI", () => {
  test("uses the default and accepts both batch-size forms", () => {
    expect(parseBatchSize([])).toBe(DEFAULT_BATCH_SIZE);
    expect(parseBatchSize(["--batch-size", "25"])).toBe(25);
    expect(parseBatchSize(["--batch-size=50"])).toBe(50);
  });

  test("rejects invalid batch sizes and arguments", () => {
    for (const args of [
      ["--batch-size", "0"],
      ["--batch-size=-1"],
      ["--batch-size", "1.5"],
      ["--unknown"],
      ["--batch-size"],
    ]) {
      expect(() => parseBatchSize(args)).toThrow();
    }
  });

  test("returns a failing exit code only when rows failed", async () => {
    async function runWithFailures(failures: number): Promise<number> {
      const script = `
        import { mock } from "bun:test";
        mock.module("./src/services/artifact-search-backfill-service.ts", () => ({
          backfillArtifactSearchText: async () => ({
            processed: 1,
            indexed: ${failures === 0 ? 1 : 0},
            skipped: { unsupported: 0, over_limit: 0, unavailable: 0, malformed_utf8: 0, empty: 0 },
            failures: ${failures},
          }),
        }));
        const { runArtifactSearchBackfill } = await import("./src/cli/backfill-artifact-search-text.ts");
        process.exit(await runArtifactSearchBackfill(["--batch-size=2"]));
      `;
      const child = Bun.spawn(["bun", "-e", script], {
        cwd: process.cwd(),
        stdout: "ignore",
        stderr: "pipe",
      });
      return await child.exited;
    }

    expect(await runWithFailures(0)).toBe(0);
    expect(await runWithFailures(1)).toBe(1);
  });
});
