import { readMigrationFiles } from "./migrate.ts";
import type { SqlExecutor } from "./client.ts";

export type MigrationReadiness =
  | { ready: true }
  | { ready: false; reason: "migration_tracking_missing" | "migrations_pending" | "migration_checksum_mismatch" | "unknown_migration" | "migration_manifest_invalid" };

export async function checkMigrationReadiness(params: {
  executor: SqlExecutor;
  schema: string;
}): Promise<MigrationReadiness> {
  let migrations;
  try {
    migrations = await readMigrationFiles();
  } catch {
    return { ready: false, reason: "migration_manifest_invalid" };
  }

  const trackingRows = await params.executor<Array<{ exists: boolean }>>`
    SELECT EXISTS(
      SELECT 1
      FROM pg_catalog.pg_class class
      INNER JOIN pg_catalog.pg_namespace namespace ON namespace.oid = class.relnamespace
      WHERE namespace.nspname = ${params.schema}
        AND class.relname = 'schema_migrations'
        AND class.relkind = 'r'
    ) AS exists
  `;
  if (!trackingRows[0]?.exists) {
    return { ready: false, reason: "migration_tracking_missing" };
  }

  const appliedRows = await params.executor<Array<{ name: string; checksum_sha256: string }>>`
    SELECT name, checksum_sha256
    FROM schema_migrations
    ORDER BY name
  `;
  const expected = new Map(migrations.map((migration) => [migration.name, migration.checksum]));
  const applied = new Map(appliedRows.map((migration) => [migration.name, migration.checksum_sha256]));

  for (const [name, checksum] of expected) {
    const appliedChecksum = applied.get(name);
    if (!appliedChecksum) {
      return { ready: false, reason: "migrations_pending" };
    }
    if (appliedChecksum !== checksum) {
      return { ready: false, reason: "migration_checksum_mismatch" };
    }
  }

  for (const name of applied.keys()) {
    if (!expected.has(name)) {
      return { ready: false, reason: "unknown_migration" };
    }
  }

  return { ready: true };
}
