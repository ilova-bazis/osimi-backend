import {
  listArtifactSearchBackfillCandidates,
  type ArtifactSearchBackfillCandidate,
} from "../repos/object-artifact-search-document-repo.ts";
import {
  indexMaterializedArtifact,
  type ArtifactSearchTextResult,
  type ArtifactSearchTextSkipReason,
} from "./artifact-search-indexing-service.ts";

export interface ArtifactSearchBackfillSummary {
  processed: number;
  indexed: number;
  skipped: Record<ArtifactSearchTextSkipReason, number>;
  failures: number;
}

interface ArtifactSearchBackfillDependencies {
  listCandidates: (params: {
    afterArtifactId?: string;
    limit: number;
  }) => Promise<ArtifactSearchBackfillCandidate[]>;
  indexArtifact: (candidate: ArtifactSearchBackfillCandidate) => Promise<ArtifactSearchTextResult>;
  reportFailure: (candidate: ArtifactSearchBackfillCandidate) => void;
}

const initialSkippedCounts = (): Record<ArtifactSearchTextSkipReason, number> => ({
  unsupported: 0,
  over_limit: 0,
  unavailable: 0,
  malformed_utf8: 0,
  empty: 0,
});

export async function backfillArtifactSearchText(
  batchSize: number,
  dependencies: Partial<ArtifactSearchBackfillDependencies> = {},
): Promise<ArtifactSearchBackfillSummary> {
  const listCandidates = dependencies.listCandidates ?? listArtifactSearchBackfillCandidates;
  const indexArtifact = dependencies.indexArtifact ?? (async (candidate) =>
    indexMaterializedArtifact({
      tenantId: candidate.tenantId,
      objectId: candidate.artifact.objectId,
      artifact: candidate.artifact,
    }));
  const reportFailure = dependencies.reportFailure ?? ((candidate) => {
    console.error(
      `artifact_search_backfill_failed artifact_id=${candidate.artifact.id} tenant_id=${candidate.tenantId}`,
    );
  });
  const summary: ArtifactSearchBackfillSummary = {
    processed: 0,
    indexed: 0,
    skipped: initialSkippedCounts(),
    failures: 0,
  };
  let afterArtifactId: string | undefined;

  while (true) {
    const candidates = await listCandidates({ afterArtifactId, limit: batchSize });
    if (candidates.length === 0) break;

    for (const candidate of candidates) {
      // Advance independently of the outcome so a permanently skipped row cannot loop.
      afterArtifactId = candidate.artifact.id;
      summary.processed += 1;
      try {
        const result = await indexArtifact(candidate);
        if (result.outcome === "indexed") {
          summary.indexed += 1;
        } else {
          summary.skipped[result.reason] += 1;
        }
      } catch {
        summary.failures += 1;
        reportFailure(candidate);
      }
    }

    if (candidates.length < batchSize) break;
  }

  return summary;
}
