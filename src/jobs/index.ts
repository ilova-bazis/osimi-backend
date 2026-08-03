import { runStagingRetentionSweep, runStuckAttentionCheck } from "./operations.ts";

const DEFAULT_COMPLETED_RETENTION_DAYS = 7;
const DEFAULT_FAILED_CANCELED_RETENTION_DAYS = 14;
const DEFAULT_STUCK_THRESHOLD_MINUTES = 60;
const DEFAULT_RETENTION_INTERVAL_SECONDS = 300;
const DEFAULT_STUCK_INTERVAL_SECONDS = 120;
const DEFAULT_RETENTION_BATCH_SIZE = 25;
const DEFAULT_RETENTION_CLAIM_TIMEOUT_SECONDS = 900;

export interface JobRuntime {
  stop: () => Promise<void>;
  activeCount: () => number;
}

const disabledJobRuntime: JobRuntime = {
  stop: () => Promise.resolve(),
  activeCount: () => 0,
};

function parseIntegerEnv(rawValue: string | undefined, fallback: number, label: string): number {
  if (!rawValue) {
    return fallback;
  }

  const parsed = Number.parseInt(rawValue, 10);

  if (!Number.isFinite(parsed) || parsed < 1) {
    throw new Error(`Environment variable '${label}' must be a positive integer.`);
  }

  return parsed;
}

function parseBooleanEnv(rawValue: string | undefined, fallback: boolean): boolean {
  if (!rawValue) {
    return fallback;
  }

  const normalized = rawValue.trim().toLowerCase();

  if (["1", "true", "yes", "on"].includes(normalized)) {
    return true;
  }

  if (["0", "false", "no", "off"].includes(normalized)) {
    return false;
  }

  return fallback;
}

function logJobEvent(level: "INFO" | "WARN" | "ERROR", event: string, fields: Record<string, unknown>): void {
  const payload = {
    timestamp: new Date().toISOString(),
    level: level.toLowerCase(),
    event,
    ...fields,
  };

  if (level === "ERROR") {
    console.error(JSON.stringify(payload));
    return;
  }

  if (level === "WARN") {
    console.warn(JSON.stringify(payload));
    return;
  }

  console.info(JSON.stringify(payload));
}

export function startBackgroundJobs(): JobRuntime {
  const enabled = parseBooleanEnv(process.env.BACKGROUND_JOBS_ENABLED, true);

  if (!enabled) {
    logJobEvent("INFO", "jobs.disabled", {
      reason: "BACKGROUND_JOBS_ENABLED=false",
    });
    return disabledJobRuntime;
  }

  const completedRetentionDays = parseIntegerEnv(
    process.env.COMPLETED_STAGING_RETENTION_DAYS,
    DEFAULT_COMPLETED_RETENTION_DAYS,
    "COMPLETED_STAGING_RETENTION_DAYS",
  );
  const failedCanceledRetentionDays = parseIntegerEnv(
    process.env.FAILED_CANCELED_STAGING_RETENTION_DAYS,
    DEFAULT_FAILED_CANCELED_RETENTION_DAYS,
    "FAILED_CANCELED_STAGING_RETENTION_DAYS",
  );
  const stuckThresholdMinutes = parseIntegerEnv(
    process.env.STUCK_ATTENTION_THRESHOLD_MINUTES,
    DEFAULT_STUCK_THRESHOLD_MINUTES,
    "STUCK_ATTENTION_THRESHOLD_MINUTES",
  );

  const retentionIntervalSeconds = parseIntegerEnv(
    process.env.STAGING_RETENTION_SWEEP_INTERVAL_SECONDS,
    DEFAULT_RETENTION_INTERVAL_SECONDS,
    "STAGING_RETENTION_SWEEP_INTERVAL_SECONDS",
  );
  const retentionBatchSize = parseIntegerEnv(
    process.env.STAGING_RETENTION_BATCH_SIZE,
    DEFAULT_RETENTION_BATCH_SIZE,
    "STAGING_RETENTION_BATCH_SIZE",
  );
  const retentionClaimTimeoutSeconds = parseIntegerEnv(
    process.env.STAGING_RETENTION_CLAIM_TIMEOUT_SECONDS,
    DEFAULT_RETENTION_CLAIM_TIMEOUT_SECONDS,
    "STAGING_RETENTION_CLAIM_TIMEOUT_SECONDS",
  );
  const stuckIntervalSeconds = parseIntegerEnv(
    process.env.STUCK_ATTENTION_INTERVAL_SECONDS,
    DEFAULT_STUCK_INTERVAL_SECONDS,
    "STUCK_ATTENTION_INTERVAL_SECONDS",
  );

  const runRetention = async (): Promise<void> => {
    try {
      const result = await runStagingRetentionSweep({
        completedRetentionDays,
        failedCanceledRetentionDays,
        batchSize: retentionBatchSize,
        claimTimeoutSeconds: retentionClaimTimeoutSeconds,
      });

      logJobEvent("INFO", "jobs.staging_retention", {
        claimed: result.claimed,
        purged: result.purged,
        missing: result.missing,
        failed: result.failed,
        completed_retention_days: completedRetentionDays,
        failed_canceled_retention_days: failedCanceledRetentionDays,
      });
    } catch (error) {
      logJobEvent("ERROR", "jobs.staging_retention.failed", {
        error: error instanceof Error ? error.message : String(error),
      });
    }
  };

  const runStuckAttention = async (): Promise<void> => {
    try {
      const result = await runStuckAttentionCheck({
        thresholdMinutes: stuckThresholdMinutes,
      });

      logJobEvent(result.stuckCount > 0 ? "WARN" : "INFO", "jobs.stuck_attention", {
        threshold_minutes: result.thresholdMinutes,
        stuck_count: result.stuckCount,
        ingestions: result.ingestions,
      });
    } catch (error) {
      logJobEvent("ERROR", "jobs.stuck_attention.failed", {
        error: error instanceof Error ? error.message : String(error),
      });
    }
  };

  const activeJobs = new Set<Promise<void>>();
  let stopping = false;
  let retentionRunning = false;
  let stopPromise: Promise<void> | undefined;

  const track = (job: () => Promise<void>): void => {
    if (stopping) {
      return;
    }
    const active = job();
    activeJobs.add(active);
    void active.finally(() => activeJobs.delete(active));
  };

  const runRetentionSingleFlight = async (): Promise<void> => {
    if (retentionRunning) {
      logJobEvent("INFO", "jobs.staging_retention.skipped", { reason: "already_running" });
      return;
    }
    retentionRunning = true;
    try {
      await runRetention();
    } finally {
      retentionRunning = false;
    }
  };

  track(runRetentionSingleFlight);
  track(runStuckAttention);

  const retentionTimer = setInterval(() => {
    track(runRetentionSingleFlight);
  }, retentionIntervalSeconds * 1000);

  const stuckTimer = setInterval(() => {
    track(runStuckAttention);
  }, stuckIntervalSeconds * 1000);

  logJobEvent("INFO", "jobs.started", {
    retention_interval_seconds: retentionIntervalSeconds,
    stuck_interval_seconds: stuckIntervalSeconds,
    completed_retention_days: completedRetentionDays,
    failed_canceled_retention_days: failedCanceledRetentionDays,
    batch_size: retentionBatchSize,
    claim_timeout_seconds: retentionClaimTimeoutSeconds,
    stuck_threshold_minutes: stuckThresholdMinutes,
  });

  return {
    stop: (): Promise<void> => {
      if (stopPromise) {
        return stopPromise;
      }
      stopping = true;
      clearInterval(retentionTimer);
      clearInterval(stuckTimer);

      logJobEvent("INFO", "jobs.stopped", {});
      stopPromise = Promise.all([...activeJobs]).then(() => undefined);
      return stopPromise;
    },
    activeCount: () => activeJobs.size,
  };
}
