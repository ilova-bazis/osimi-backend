import { closeDatabaseClients } from "../db/runtime.ts";
import { backfillArtifactSearchText } from "../services/artifact-search-backfill-service.ts";

export const DEFAULT_BATCH_SIZE = 100;

export function parseBatchSize(args: string[]): number {
  if (args.length === 0) return DEFAULT_BATCH_SIZE;

  let value: string | undefined;
  if (args.length === 1 && args[0]?.startsWith("--batch-size=")) {
    value = args[0].slice("--batch-size=".length);
  } else if (args.length === 2 && args[0] === "--batch-size") {
    value = args[1];
  } else {
    throw new Error("Usage: bun run backfill-artifact-search-text [--batch-size <positive-integer>]");
  }

  if (!value || !/^[1-9][0-9]*$/.test(value)) {
    throw new Error("--batch-size must be a positive integer.");
  }

  const batchSize = Number(value);
  if (!Number.isSafeInteger(batchSize)) {
    throw new Error("--batch-size must be a positive safe integer.");
  }
  return batchSize;
}

export async function runArtifactSearchBackfill(args: string[]): Promise<number> {
  const summary = await backfillArtifactSearchText(parseBatchSize(args));
  console.log(
    `artifact_search_backfill_complete processed=${summary.processed} indexed=${summary.indexed} ` +
      `skipped_unsupported=${summary.skipped.unsupported} ` +
      `skipped_over_limit=${summary.skipped.over_limit} ` +
      `skipped_unavailable=${summary.skipped.unavailable} ` +
      `skipped_malformed_utf8=${summary.skipped.malformed_utf8} ` +
      `skipped_empty=${summary.skipped.empty} failures=${summary.failures}`,
  );
  return summary.failures === 0 ? 0 : 1;
}

if (import.meta.main) {
  try {
    process.exitCode = await runArtifactSearchBackfill(process.argv.slice(2));
  } catch (error) {
    console.error(
      "artifact_search_backfill_aborted",
      error instanceof Error ? error.message : "Unexpected failure",
    );
    process.exitCode = 1;
  } finally {
    await closeDatabaseClients({ timeoutMs: 1_000 });
  }
}
