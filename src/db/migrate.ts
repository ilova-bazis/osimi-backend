import { createHash } from "node:crypto";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { createSqlClient, resolveDatabaseUrl } from "./client.ts";

const DEFAULT_MIGRATIONS_DIR = fileURLToPath(new URL("./migrations", import.meta.url));
const SCHEMA_NAME_PATTERN = /^[a-z_][a-z0-9_]*$/;

interface MigrationRecord {
  name: string;
  checksum_sha256: string;
}

export interface RunMigrationsOptions {
  databaseUrl?: string;
  migrationsDir?: string;
  schema?: string;
  dryRun?: boolean;
}

export interface MigrationRunResult {
  schema: string;
  applied: string[];
  skipped: string[];
}

function quoteIdentifier(identifier: string): string {
  return `"${identifier}"`;
}

function quoteLiteral(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function ensureValidSchemaName(schema: string): string {
  if (!SCHEMA_NAME_PATTERN.test(schema)) {
    throw new Error(
      `Invalid schema name '${schema}'. Schema must match ${SCHEMA_NAME_PATTERN.source} to remain safe for SQL identifiers.`,
    );
  }

  return schema;
}

async function readMigrationFiles(migrationsDir: string): Promise<string[]> {
  try {
    const glob = new Bun.Glob("*.sql");
    const files = await Array.fromAsync(glob.scan({ cwd: migrationsDir }));
    files.sort((left, right) => left.localeCompare(right));

    if (files.length === 0) {
      throw new Error(`No migration files found in '${migrationsDir}'.`);
    }

    return files;
  } catch (error) {
    throw new Error(`Failed to read migrations from '${migrationsDir}'.`, {
      cause: error,
    });
  }
}

function checksumOf(value: string): string {
  return createHash("sha256").update(value).digest("hex");
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
  const schema = ensureValidSchemaName(options.schema ?? "public");
  const migrationFiles = await readMigrationFiles(migrationsDir);
  const pool = createSqlClient(databaseUrl);
  const client = await pool.reserve();
  const qualifiedSchema = quoteIdentifier(schema);
  const qualifiedMigrationsTable = `${qualifiedSchema}.schema_migrations`;

  try {
    await client.unsafe(`CREATE SCHEMA IF NOT EXISTS ${qualifiedSchema}`);
    await client.unsafe(`SET search_path TO ${qualifiedSchema}, public`);

    await client.unsafe(`
      CREATE TABLE IF NOT EXISTS ${qualifiedMigrationsTable} (
        name text PRIMARY KEY,
        checksum_sha256 char(64) NOT NULL,
        applied_at timestamptz NOT NULL DEFAULT now()
      )
    `);

    const appliedRows = (await client.unsafe(
      `SELECT name, checksum_sha256 FROM ${qualifiedMigrationsTable} ORDER BY name`,
    )) as MigrationRecord[];

    const appliedChecksums = new Map<string, string>(
      appliedRows.map(row => [row.name, row.checksum_sha256]),
    );

    const applied: string[] = [];
    const skipped: string[] = [];

    for (const fileName of migrationFiles) {
      const filePath = join(migrationsDir, fileName);
      const sql = await Bun.file(filePath).text();

      if (sql.trim().length === 0) {
        throw new Error(`Migration '${fileName}' is empty.`);
      }

      const checksum = checksumOf(sql);
      const existingChecksum = appliedChecksums.get(fileName);

      if (existingChecksum) {
        if (existingChecksum !== checksum) {
          throw new Error(
            `Checksum mismatch for already applied migration '${fileName}'. Expected '${existingChecksum}', got '${checksum}'.`,
          );
        }

        skipped.push(fileName);
        continue;
      }

      if (!options.dryRun) {
        await client.begin(async transaction => {
          await transaction.unsafe(sql);
          await transaction.unsafe(
            `INSERT INTO ${qualifiedMigrationsTable} (name, checksum_sha256) VALUES (${quoteLiteral(fileName)}, ${quoteLiteral(checksum)})`,
          );
        });
      }

      applied.push(fileName);
    }

    return {
      schema,
      applied,
      skipped,
    };
  } finally {
    await client.release();
    await pool.close();
  }
}

if (import.meta.main) {
  const args = process.argv.slice(2);

  try {
    const options = parseArguments(args);
    const result = await runMigrations(options);

    console.info(`[migrations] schema: ${result.schema}`);
    console.info(`[migrations] applied: ${result.applied.length}`);
    console.info(`[migrations] skipped: ${result.skipped.length}`);

    if (result.applied.length > 0) {
      console.info(`[migrations] applied files: ${result.applied.join(", ")}`);
    }
  } catch (error) {
    console.error("[migrations] failed", error);
    process.exitCode = 1;
  }
}
