import { rm, stat } from "node:fs/promises";

import {
  claimStagingPurgeBatch,
  completeStagingPurge,
  failStagingPurge,
  listStuckIngestions,
} from "../repos/ingestion-repo.ts";
import { buildIngestionStagingDirectory, resolveStagingPath } from "../storage/staging.ts";
import { claimVerifiedArchiveArtifactUploadAttemptBatch } from "../repos/archive-artifact-upload-attempt-repo.ts";
import { finalizeClaimedArchiveArtifactUpload } from "../services/archive-artifact-finalization-service.ts";

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
