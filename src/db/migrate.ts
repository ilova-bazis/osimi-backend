import { createHash } from "node:crypto";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { createSqlClient, resolveDatabaseUrl } from "./client.ts";
import { normalizeDbSchema } from "./runtime.ts";

const DEFAULT_MIGRATIONS_DIR = fileURLToPath(new URL("./migrations", import.meta.url));
const MIGRATION_LOCK_KEY_1 = 1_941_705_622;
const MIGRATION_LOCK_KEY_2 = 1_525_512_641;

interface MigrationRecord {
  name: string;
  checksum_sha256: string;
}

export interface MigrationFile {
  name: string;
  sql: string;
  checksum: string;
}

export interface RunMigrationsOptions {
  databaseUrl?: string;
  migrationsDir?: string;
  schema?: string;
  dryRun?: boolean;
}

export interface MigrationRunResult {
  schema: string;
  dryRun: boolean;
  applied: string[];
  pending: string[];
  skipped: string[];
  warnings: string[];
}

export interface ResolvedMigrationSchema {
  schema: string;
  warnings: string[];
}

function quoteIdentifier(identifier: string): string {
  return `"${identifier}"`;
}

function checksumOf(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

export function resolveMigrationSchema(params: {
  schema?: string;
  dbSchema?: string;
}): ResolvedMigrationSchema {
  const explicitSchema = params.schema;
  const environmentSchema = params.dbSchema;
  const schema = normalizeDbSchema(
    explicitSchema ?? environmentSchema ?? "public",
    explicitSchema !== undefined
      ? "explicit migration schema"
      : environmentSchema !== undefined
      ? "DB_SCHEMA"
      : "default migration schema",
  );
  const warnings: string[] = [];

  if (explicitSchema !== undefined && environmentSchema !== undefined) {
    const normalizedEnvironmentSchema = environmentSchema.trim().toLowerCase();

    if (schema !== normalizedEnvironmentSchema) {
      warnings.push(
        `Explicit migration schema '${schema}' overrides DB_SCHEMA '${normalizedEnvironmentSchema}'.`,
      );
    }
  }

  return { schema, warnings };
}

export async function readMigrationFiles(
  migrationsDir = DEFAULT_MIGRATIONS_DIR,
): Promise<MigrationFile[]> {
  try {
    const glob = new Bun.Glob("*.sql");
    const names = await Array.fromAsync(glob.scan({ cwd: migrationsDir }));
    names.sort((left, right) => left.localeCompare(right));

    if (names.length === 0) {
      throw new Error(`No migration files found in '${migrationsDir}'.`);
    }

    return await Promise.all(names.map(async (name) => {
      const sql = await Bun.file(join(migrationsDir, name)).text();

      if (sql.trim().length === 0) {
        throw new Error(`Migration '${name}' is empty.`);
      }

      return { name, sql, checksum: checksumOf(sql) };
    }));
  } catch (error) {
    throw new Error(`Failed to read migrations from '${migrationsDir}'.`, {
      cause: error,
    });
  }
}

async function migrationTrackingTableExists(
  client: Awaited<ReturnType<ReturnType<typeof createSqlClient>["reserve"]>>,
  schema: string,
): Promise<boolean> {
  const rows = await client<Array<{ exists: boolean }>>`
    SELECT EXISTS(
      SELECT 1
      FROM pg_catalog.pg_class class
      INNER JOIN pg_catalog.pg_namespace namespace ON namespace.oid = class.relnamespace
      WHERE namespace.nspname = ${schema}
        AND class.relname = ${"schema_migrations"}
        AND class.relkind = ${"r"}
    ) AS exists
  `;
  return rows[0]?.exists ?? false;
}

async function readAppliedMigrations(params: {
  client: Awaited<ReturnType<ReturnType<typeof createSqlClient>["reserve"]>>;
  schema: string;
}): Promise<Map<string, string>> {
  if (!(await migrationTrackingTableExists(params.client, params.schema))) {
    return new Map();
  }

  const migrationsTable = `${quoteIdentifier(params.schema)}.schema_migrations`;
  const rows = await params.client.unsafe(
    `SELECT name, checksum_sha256 FROM ${migrationsTable} ORDER BY name`,
  ) as MigrationRecord[];

  return new Map(rows.map(row => [row.name, row.checksum_sha256]));
}

function validateAppliedChecksums(params: {
  migrations: MigrationFile[];
  appliedChecksums: Map<string, string>;
}): void {
  for (const migration of params.migrations) {
    const existingChecksum = params.appliedChecksums.get(migration.name);

    if (existingChecksum && existingChecksum !== migration.checksum) {
      throw new Error(
        `Checksum mismatch for already applied migration '${migration.name}'. Expected '${existingChecksum}', got '${migration.checksum}'.`,
      );
    }
  }
}

async function acquireMigrationLock(
  client: Awaited<ReturnType<ReturnType<typeof createSqlClient>["reserve"]>>,
): Promise<void> {
  // Stable key pair for the "osimi-backend:migrations:v1" lock namespace.
  await client`
    SELECT pg_catalog.pg_advisory_lock(${MIGRATION_LOCK_KEY_1}, ${MIGRATION_LOCK_KEY_2})
  `;
}

async function releaseMigrationLock(
  client: Awaited<ReturnType<ReturnType<typeof createSqlClient>["reserve"]>>,
): Promise<void> {
  const rows = await client<Array<{ unlocked: boolean }>>`
    SELECT pg_catalog.pg_advisory_unlock(${MIGRATION_LOCK_KEY_1}, ${MIGRATION_LOCK_KEY_2}) AS unlocked
  `;

  if (!rows[0]?.unlocked) {
    throw new Error("Migration advisory lock was not held by the reserved connection.");
  }
}

function parseArguments(args: string[]): RunMigrationsOptions {
  const options: RunMigrationsOptions = {};

  for (const arg of args) {
    if (arg === "--dry-run") {
      options.dryRun = true;
      continue;
    }

    if (arg.startsWith("--database-url=")) {
      options.databaseUrl = arg.slice("--database-url=".length);
      continue;
    }

    if (arg.startsWith("--migrations-dir=")) {
      options.migrationsDir = arg.slice("--migrations-dir=".length);
      continue;
    }

    if (arg.startsWith("--schema=")) {
      options.schema = arg.slice("--schema=".length);
      continue;
    }

    throw new Error(`Unknown argument '${arg}'.`);
  }

  return options;
}

export async function runMigrations(options: RunMigrationsOptions = {}): Promise<MigrationRunResult> {
  const databaseUrl = resolveDatabaseUrl(options.databaseUrl);
  const migrationsDir = options.migrationsDir ?? DEFAULT_MIGRATIONS_DIR;
  const resolvedSchema = resolveMigrationSchema({
    schema: options.schema,
    dbSchema: process.env.DB_SCHEMA,
  });
  const migrations = await readMigrationFiles(migrationsDir);
  const pool = createSqlClient(databaseUrl);
  const client = await pool.reserve();
  const dryRun = options.dryRun ?? false;
  const qualifiedSchema = quoteIdentifier(resolvedSchema.schema);
  const qualifiedMigrationsTable = `${qualifiedSchema}.schema_migrations`;
  let lockAcquired = false;
  let operationError: unknown;

  try {
    await acquireMigrationLock(client);
    lockAcquired = true;

    if (!dryRun) {
      await client.unsafe(`CREATE SCHEMA IF NOT EXISTS ${qualifiedSchema}`);
      await client.unsafe(`
        CREATE TABLE IF NOT EXISTS ${qualifiedMigrationsTable} (
          name text PRIMARY KEY,
          checksum_sha256 char(64) NOT NULL,
          applied_at timestamptz NOT NULL DEFAULT now()
        )
      `);
    }

    const appliedChecksums = await readAppliedMigrations({
      client,
      schema: resolvedSchema.schema,
    });
    validateAppliedChecksums({ migrations, appliedChecksums });

    const skipped = migrations
      .filter(migration => appliedChecksums.has(migration.name))
      .map(migration => migration.name);
    const pendingMigrations = migrations.filter(
      migration => !appliedChecksums.has(migration.name),
    );

    if (dryRun) {
      return {
        schema: resolvedSchema.schema,
        dryRun: true,
        applied: [],
        pending: pendingMigrations.map(migration => migration.name),
        skipped,
        warnings: resolvedSchema.warnings,
      };
    }

    const applied: string[] = [];

    for (const migration of pendingMigrations) {
      await client.begin(async transaction => {
        await transaction`
          SELECT pg_catalog.set_config(
            ${"search_path"},
            ${`${resolvedSchema.schema}, public`},
            true
          )
        `;
        await transaction.unsafe(migration.sql);
        await transaction`
          INSERT INTO schema_migrations (name, checksum_sha256)
          VALUES (${migration.name}, ${migration.checksum})
        `;
      });
      applied.push(migration.name);
    }

    return {
      schema: resolvedSchema.schema,
      dryRun: false,
      applied,
      pending: [],
      skipped,
      warnings: resolvedSchema.warnings,
    };
  } catch (error) {
    operationError = error;
    throw error;
  } finally {
    let cleanupError: unknown;

    if (lockAcquired) {
      try {
        await releaseMigrationLock(client);
      } catch (error) {
        cleanupError = error;
      }
    }

    try {
      await client.release();
    } catch (error) {
      cleanupError ??= error;
    }

    try {
      await pool.close();
    } catch (error) {
      cleanupError ??= error;
    }

    if (cleanupError) {
      if (operationError) {
        throw new AggregateError([operationError, cleanupError], "Migration operation and cleanup both failed.");
      }

      throw cleanupError;
    }
  }
}

if (import.meta.main) {
  const args = process.argv.slice(2);

  try {
    const result = await runMigrations(parseArguments(args));

    for (const warning of result.warnings) {
      console.warn(`[migrations] warning: ${warning}`);
    }
    console.info(`[migrations] schema: ${result.schema}`);
    console.info(`[migrations] dry run: ${result.dryRun}`);
    console.info(`[migrations] applied: ${result.applied.length}`);
    console.info(`[migrations] pending: ${result.pending.length}`);
    console.info(`[migrations] skipped: ${result.skipped.length}`);

    if (result.pending.length > 0) {
      console.info(`[migrations] would apply: ${result.pending.join(", ")}`);
    }
  } catch (error) {
    console.error("[migrations] failed", error);
    process.exitCode = 1;
  }
}
