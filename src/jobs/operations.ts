import { rm, stat } from "node:fs/promises";
import { dirname } from "node:path";

import {
  claimStagingPurgeBatch,
  completeStagingPurge,
  failStagingPurge,
  listStuckIngestions,
} from "../repos/ingestion-repo.ts";
import { buildIngestionStagingDirectory, resolveStagingPath } from "../storage/staging.ts";
import { claimVerifiedArchiveArtifactUploadAttemptBatch } from "../repos/archive-artifact-upload-attempt-repo.ts";
import { finalizeClaimedArchiveArtifactUpload } from "../services/archive-artifact-finalization-service.ts";
import {
  claimCurationPublicationCleanupBatch,
  completeCurationPublicationCleanup,
  curationPublicationStorageKeyExists,
  failCurationPublicationCleanup,
} from "../repos/curation-publication-repo.ts";
import { stagingRootPath } from "../storage/staging.ts";

export interface StagingRetentionConfig {
  completedRetentionDays: number;
  failedCanceledRetentionDays: number;
  batchSize?: number;
  claimTimeoutSeconds?: number;
}

export interface StuckAttentionConfig {
  thresholdMinutes: number;
}

export interface StagingRetentionResult {
  claimed: number;
  purged: number;
  missing: number;
  failed: number;
}

export interface StuckAttentionResult {
  thresholdMinutes: number;
  stuckCount: number;
  ingestions: Array<{
    ingestion_id: string;
    tenant_id: string;
    status: string;
    updated_at: string;
    created_by: string;
  }>;
}

export interface ArtifactFinalizationResult {
  claimed: number;
  completed: number;
  failed: number;
}

export interface CurationPublicationSourceCleanupResult extends StagingRetentionResult {
  orphaned: number;
}

export async function runCurationPublicationSourceCleanup(config: {
  batchSize?: number;
  claimTimeoutSeconds?: number;
  orphanMinAgeSeconds?: number;
} = {}): Promise<CurationPublicationSourceCleanupResult> {
  const claims = await claimCurationPublicationCleanupBatch({
    batchSize: config.batchSize ?? 25,
    claimTimeoutSeconds: config.claimTimeoutSeconds ?? 900,
  });
  let purged = 0;
  let missing = 0;
  let failed = 0;
  let orphaned = 0;

  for (const claim of claims) {
    const path = resolveStagingPath(claim.storageKey);
    const existed = await stat(path).then(() => true).catch(() => false);
    try {
      await rm(path, { force: true });
      if (await completeCurationPublicationCleanup(claim)) {
        purged += 1;
        if (!existed) missing += 1;
      }
    } catch (error) {
      failed += 1;
      await failCurationPublicationCleanup({
        claim,
        message: error instanceof Error ? error.message : "filesystem_error",
      });
    }
  }

  const orphanCutoff = Date.now() - (config.orphanMinAgeSeconds ?? 86_400) * 1_000;
  const sourceGlob = new Bun.Glob("tenants/*/archive-request-sources/*/*/source.*");
  for await (const storageKey of sourceGlob.scan({ cwd: stagingRootPath(), onlyFiles: true })) {
    try {
      const path = resolveStagingPath(storageKey);
      const file = await stat(path);
      if (file.mtimeMs > orphanCutoff || await curationPublicationStorageKeyExists(storageKey)) {
        continue;
      }
      await rm(dirname(path), { recursive: true, force: true });
      orphaned += 1;
    } catch {
      failed += 1;
    }
  }

  return { claimed: claims.length, purged, missing, failed, orphaned };
}

export async function runArtifactFinalizationSweep(config: {
  batchSize?: number;
  claimTimeoutSeconds?: number;
} = {}): Promise<ArtifactFinalizationResult> {
  const attempts = await claimVerifiedArchiveArtifactUploadAttemptBatch({
    batchSize: config.batchSize ?? 25,
    claimTimeoutSeconds: config.claimTimeoutSeconds ?? 300,
  });
  let completed = 0;
  let failed = 0;
  for (const attempt of attempts) {
    try {
      await finalizeClaimedArchiveArtifactUpload(attempt);
      completed += 1;
    } catch {
      failed += 1;
    }
  }
  return { claimed: attempts.length, completed, failed };
}

export async function runStagingRetentionSweep(config: StagingRetentionConfig): Promise<StagingRetentionResult> {
  const claims = await claimStagingPurgeBatch({
    completedRetentionDays: config.completedRetentionDays,
    failedCanceledRetentionDays: config.failedCanceledRetentionDays,
    batchSize: config.batchSize ?? 25,
    claimTimeoutSeconds: config.claimTimeoutSeconds ?? 900,
  });

  let purged = 0;
  let missing = 0;
  let failed = 0;

  for (const claim of claims) {
    const directory = resolveStagingPath(buildIngestionStagingDirectory({
      tenantId: claim.tenantId,
      ingestionId: claim.ingestionId,
    }));
    const existed = await stat(directory).then(() => true).catch(() => false);
    try {
      await rm(directory, { recursive: true, force: true });
      if (await completeStagingPurge(claim)) {
        purged += 1;
        if (!existed) {
          missing += 1;
        }
      }
    } catch (error) {
      failed += 1;
      await failStagingPurge({
        ...claim,
        message: "filesystem_error",
      });
    }
  }

  return {
    claimed: claims.length,
    purged,
    missing,
    failed,
  };
}

export async function runStuckAttentionCheck(config: StuckAttentionConfig): Promise<StuckAttentionResult> {
  const stuck = await listStuckIngestions({
    thresholdMinutes: config.thresholdMinutes,
  });

  return {
    thresholdMinutes: config.thresholdMinutes,
    stuckCount: stuck.length,
    ingestions: stuck.map(item => ({
      ingestion_id: item.ingestionId,
      tenant_id: item.tenantId,
      status: item.status,
      updated_at: item.updatedAt.toISOString(),
      created_by: item.createdBy,
    })),
  };
}
