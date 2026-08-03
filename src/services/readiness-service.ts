import { resolveDatabaseUrl } from "../db/client.ts";
import { checkMigrationReadiness } from "../db/migration-status.ts";
import { db, resolveDbSchema } from "../db/runtime.ts";
import {
  getRuntimeConfig,
  resolveReadinessTimeoutMs,
  validateRuntimeConfiguration,
  validateWorkerConfiguration,
} from "../runtime/config.ts";
import { LifecycleController } from "../runtime/lifecycle.ts";

export type ReadinessCheckName = "lifecycle" | "configuration" | "database" | "migrations";
export type ReadinessReason =
  | "draining"
  | "configuration_invalid"
  | "database_unavailable"
  | "check_timed_out"
  | "migration_tracking_missing"
  | "migrations_pending"
  | "migration_checksum_mismatch"
  | "unknown_migration"
  | "migration_manifest_invalid";

export interface ReadinessCheck {
  name: ReadinessCheckName;
  ready: boolean;
  reason?: ReadinessReason;
}

export interface ReadinessResult {
  ready: boolean;
  checks: ReadinessCheck[];
}

export interface ReadinessService {
  check(): Promise<ReadinessResult>;
}

interface ReadinessDependencies {
  lifecycle: LifecycleController;
  checkDatabase?: (timeoutMs: number) => Promise<ReadinessCheck[]>;
}

function withTimeout<T>(operation: Promise<T>, timeoutMs: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error("readiness_timeout")), timeoutMs);
  });

  return Promise.race([operation, timeout]).finally(() => {
    if (timer) {
      clearTimeout(timer);
    }
  });
}

function configurationCheck(): { timeoutMs: number } | ReadinessCheck {
  try {
    const config = getRuntimeConfig();
    validateRuntimeConfiguration(config);
    validateWorkerConfiguration(config);
    resolveDatabaseUrl();
    resolveDbSchema();
    return { timeoutMs: resolveReadinessTimeoutMs(config) };
  } catch {
    return { name: "configuration", ready: false, reason: "configuration_invalid" };
  }
}

async function checkDatabase(timeoutMs: number): Promise<ReadinessCheck[]> {
  const client = db();
  let reserved: Awaited<ReturnType<typeof client.reserve>> | undefined;

  try {
    reserved = await client.reserve();
    await reserved`SELECT set_config('statement_timeout', ${String(timeoutMs)}, false)`;
    const schema = resolveDbSchema();
    await reserved`SELECT set_config('search_path', ${`${schema}, public`}, false)`;
    await reserved`SELECT 1`;

    const migration = await checkMigrationReadiness({ executor: reserved, schema });
    if (!migration.ready) {
      return [
        { name: "database", ready: true },
        { name: "migrations", ready: false, reason: migration.reason },
      ];
    }

    return [
      { name: "database", ready: true },
      { name: "migrations", ready: true },
    ];
  } catch (error) {
    const reason: ReadinessReason =
      error instanceof Error && error.message === "readiness_timeout"
        ? "check_timed_out"
        : "database_unavailable";
    return [
      { name: "database", ready: false, reason },
      { name: "migrations", ready: false, reason },
    ];
  } finally {
    reserved?.release();
  }
}

export function createReadinessService(
  dependencies: ReadinessDependencies,
): ReadinessService {
  return {
    async check(): Promise<ReadinessResult> {
      if (!dependencies.lifecycle.isReady()) {
        return {
          ready: false,
          checks: [{ name: "lifecycle", ready: false, reason: "draining" }],
        };
      }

      const configuration = configurationCheck();
      if ("name" in configuration) {
        return { ready: false, checks: [configuration] };
      }

      const databaseChecks = await withTimeout(
        (dependencies.checkDatabase ?? checkDatabase)(configuration.timeoutMs),
        configuration.timeoutMs,
      ).catch((): ReadinessCheck[] => [
        { name: "database", ready: false, reason: "check_timed_out" },
        { name: "migrations", ready: false, reason: "check_timed_out" },
      ]);
      const checks: ReadinessCheck[] = [
        { name: "lifecycle", ready: true },
        { name: "configuration", ready: true },
        ...databaseChecks,
      ];
      return { ready: checks.every((check) => check.ready), checks };
    },
  };
}
