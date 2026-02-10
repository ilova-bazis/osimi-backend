import { rm } from "node:fs/promises";

import { listStagingCleanupCandidates, listStuckIngestions } from "../repos/ingestion-repo.ts";
import { resolveStagingPath } from "../storage/index.ts";

export interface StagingRetentionConfig {
  completedRetentionDays: number;
  failedCanceledRetentionDays: number;
}

export interface StuckAttentionConfig {
  thresholdMinutes: number;
}

export interface StagingRetentionResult {
  scanned: number;
  deleted: number;
  missing: number;
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

export async function runStagingRetentionSweep(config: StagingRetentionConfig): Promise<StagingRetentionResult> {
  const candidates = await listStagingCleanupCandidates({
    completedRetentionDays: config.completedRetentionDays,
    failedCanceledRetentionDays: config.failedCanceledRetentionDays,
  });

  let deleted = 0;
  let missing = 0;

  for (const candidate of candidates) {
    const filePath = resolveStagingPath(candidate.storageKey);
    const file = Bun.file(filePath);

    if (!(await file.exists())) {
      missing += 1;
      continue;
    }

    await rm(filePath, { force: true });
    deleted += 1;
  }

  return {
    scanned: candidates.length,
    deleted,
    missing,
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
