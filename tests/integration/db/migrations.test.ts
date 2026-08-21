import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, test } from "bun:test";
import { sql as sqlIdentifier } from "bun";

import { createSqlClient } from "../../../src/db/client.ts";
import { runMigrations } from "../../../src/db/migrate.ts";
import { TEST_DATABASE_URL } from "../test-database.ts";

function schemaName(prefix: string): string {
  return `${prefix}_${Date.now()}_${Math.floor(Math.random() * 100000)}`;
}

async function schemaExists(schema: string): Promise<boolean> {
  const sql = createSqlClient(TEST_DATABASE_URL);

  try {
    const rows = await sql<Array<{ exists: boolean }>>`
      SELECT EXISTS(
        SELECT 1
        FROM pg_catalog.pg_namespace
        WHERE nspname = ${schema}
      ) AS exists
    `;
    return rows[0]?.exists ?? false;
  } finally {
    await sql.close();
  }
}

async function dropSchema(schema: string): Promise<void> {
  const sql = createSqlClient(TEST_DATABASE_URL);

  try {
    await sql`DROP SCHEMA IF EXISTS ${sqlIdentifier(schema)} CASCADE`;
  } finally {
    await sql.close();
  }
}

describe("database migrations", () => {
  test(
    "applies migrations and tracks state",
    async () => {
      const schema = schemaName("phase1");

      const firstRun = await runMigrations({
        databaseUrl: TEST_DATABASE_URL,
        schema,
      });

      expect(firstRun.applied.length).toBeGreaterThan(0);

      const secondRun = await runMigrations({
        databaseUrl: TEST_DATABASE_URL,
        schema,
      });

      expect(secondRun.applied).toHaveLength(0);
      expect(secondRun.skipped.length).toBe(firstRun.applied.length);

      const sql = createSqlClient(TEST_DATABASE_URL);

      try {
        const tableRows = await sql<{ table_name: string }[]>`
          SELECT table_name
          FROM information_schema.tables
          WHERE table_schema = ${schema}
            AND table_name IN (
              'ingestions',
              'ingestion_files',
              'ingestion_leases',
              'objects',
              'tags',
              'object_tags',
              'object_access_assignments',
              'object_access_requests',
              'object_artifacts',
              'object_events',
              'schema_migrations'
            )
        `;

        const tableNames = new Set(tableRows.map((row) => row.table_name));
        expect(tableNames.has("ingestions")).toBe(true);
        expect(tableNames.has("ingestion_files")).toBe(true);
        expect(tableNames.has("ingestion_leases")).toBe(true);
        expect(tableNames.has("objects")).toBe(true);
        expect(tableNames.has("tags")).toBe(true);
        expect(tableNames.has("object_tags")).toBe(true);
        expect(tableNames.has("object_access_assignments")).toBe(true);
        expect(tableNames.has("object_access_requests")).toBe(true);
        expect(tableNames.has("object_artifacts")).toBe(true);
        expect(tableNames.has("object_events")).toBe(true);
        expect(tableNames.has("schema_migrations")).toBe(true);

        const accessRequestIndexRows = await sql<{ indexname: string }[]>`
          SELECT indexname
          FROM pg_indexes
          WHERE schemaname = ${schema}
            AND tablename = 'object_access_requests'
        `;

        const accessRequestIndexNames = accessRequestIndexRows.map((row) => row.indexname);
        expect(accessRequestIndexNames.includes("object_access_requests_one_pending_per_user_idx")).toBe(true);

        const archiveRequestIndexRows = await sql<{ indexname: string }[]>`
          SELECT indexname
          FROM pg_indexes
          WHERE schemaname = ${schema}
            AND tablename = 'archive_requests'
        `;
        const archiveRequestIndexNames = archiveRequestIndexRows.map((row) => row.indexname);
        expect(
          archiveRequestIndexNames.includes(
            "archive_requests_one_active_curation_apply_per_object_idx",
          ),
        ).toBe(true);

        await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
        const tenantId = crypto.randomUUID();
        const requestedBy = crypto.randomUUID();
        const targetId = "OBJ-20260820-MIGRATION";
        const firstRequestId = crypto.randomUUID();
        await sql`
          INSERT INTO archive_requests (
            id, tenant_id, target_type, target_id, action_type, requested_by, dedupe_key
          )
          VALUES (
            ${firstRequestId}, ${tenantId}, 'object', ${targetId}, 'curation_apply',
            ${requestedBy}, 'migration-active-1'
          )
        `;
        let uniquenessError: unknown;
        try {
          await sql`
            INSERT INTO archive_requests (
              id, tenant_id, target_type, target_id, action_type, requested_by, dedupe_key
            )
            VALUES (
              ${crypto.randomUUID()}, ${tenantId}, 'object', ${targetId}, 'curation_apply',
              ${requestedBy}, 'migration-active-2'
            )
          `;
        } catch (error) {
          uniquenessError = error;
        }
        const postgresError = uniquenessError as {
          code?: unknown;
          errno?: unknown;
          constraint?: unknown;
          message?: unknown;
        } | undefined;
        expect([postgresError?.code, postgresError?.errno]).toContain("23505");
        expect(
          postgresError?.constraint ===
            "archive_requests_one_active_curation_apply_per_object_idx" ||
            (typeof postgresError?.message === "string" &&
              postgresError.message.includes(
                "archive_requests_one_active_curation_apply_per_object_idx",
              )),
        ).toBe(true);
        await sql`
          UPDATE archive_requests SET status = 'COMPLETED' WHERE id = ${firstRequestId}
        `;
        await sql`
          INSERT INTO archive_requests (
            id, tenant_id, target_type, target_id, action_type, requested_by, dedupe_key
          )
          VALUES (
            ${crypto.randomUUID()}, ${tenantId}, 'object', ${targetId}, 'curation_apply',
            ${requestedBy}, 'migration-active-2'
          )
        `;

        const constraintRows = await sql<{ conname: string }[]>`
          SELECT conname
          FROM pg_constraint c
          INNER JOIN pg_class t ON t.oid = c.conrelid
          INNER JOIN pg_namespace n ON n.oid = t.relnamespace
          WHERE n.nspname = ${schema}
            AND t.relname = 'ingestion_leases'
        `;

        const constraintNames = constraintRows.map((row) => row.conname);
        expect(constraintNames.includes("ingestion_leases_no_overlap")).toBe(
          true,
        );

        const columnRows = await sql<{ column_name: string }[]>`
          SELECT column_name
          FROM information_schema.columns
          WHERE table_schema = ${schema}
            AND table_name = 'objects'
        `;

        const columnNames = new Set(columnRows.map((row) => row.column_name));
        expect(columnNames.has("ingest_manifest")).toBe(true);
        expect(columnNames.has("language_code")).toBe(true);
        expect(columnNames.has("updated_at")).toBe(true);
        expect(columnNames.has("embargo_kind")).toBe(true);
        expect(columnNames.has("embargo_curation_state")).toBe(true);
      } finally {
        await sql`DROP SCHEMA IF EXISTS ${sqlIdentifier(schema)} CASCADE`;
        await sql.close();
      }
    },
  );

  test("uses DB_SCHEMA when no explicit migration schema is provided", async () => {
    const schema = schemaName("environment");
    const previousSchema = process.env.DB_SCHEMA;
    process.env.DB_SCHEMA = schema;

    try {
      const result = await runMigrations({ databaseUrl: TEST_DATABASE_URL });
      expect(result.schema).toBe(schema);
      expect(result.warnings).toEqual([]);
    } finally {
      if (previousSchema === undefined) {
        delete process.env.DB_SCHEMA;
      } else {
        process.env.DB_SCHEMA = previousSchema;
      }
      await dropSchema(schema);
    }
  });

  test("lets an explicit schema override DB_SCHEMA with a warning", async () => {
    const environmentSchema = schemaName("environment");
    const explicitSchema = schemaName("explicit");
    const previousSchema = process.env.DB_SCHEMA;
    process.env.DB_SCHEMA = environmentSchema;

    try {
      const result = await runMigrations({
        databaseUrl: TEST_DATABASE_URL,
        schema: explicitSchema,
      });
      expect(result.schema).toBe(explicitSchema);
      expect(result.warnings).toEqual([
        `Explicit migration schema '${explicitSchema}' overrides DB_SCHEMA '${environmentSchema}'.`,
      ]);
      expect(await schemaExists(environmentSchema)).toBe(false);
      expect(await schemaExists(explicitSchema)).toBe(true);
    } finally {
      if (previousSchema === undefined) {
        delete process.env.DB_SCHEMA;
      } else {
        process.env.DB_SCHEMA = previousSchema;
      }
      await dropSchema(environmentSchema);
      await dropSchema(explicitSchema);
    }
  });

  test("dry run creates no schema or migration tracking table", async () => {
    const absentSchema = schemaName("dry_absent");
    const emptySchema = schemaName("dry_empty");

    try {
      const absentResult = await runMigrations({
        databaseUrl: TEST_DATABASE_URL,
        schema: absentSchema,
        dryRun: true,
      });
      expect(absentResult.dryRun).toBe(true);
      expect(absentResult.applied).toEqual([]);
      expect(absentResult.pending.length).toBeGreaterThan(0);
      expect(await schemaExists(absentSchema)).toBe(false);

      const sql = createSqlClient(TEST_DATABASE_URL);
      try {
        await sql`CREATE SCHEMA ${sqlIdentifier(emptySchema)}`;
      } finally {
        await sql.close();
      }

      await runMigrations({
        databaseUrl: TEST_DATABASE_URL,
        schema: emptySchema,
        dryRun: true,
      });

      const verifySql = createSqlClient(TEST_DATABASE_URL);
      try {
        const rows = await verifySql<Array<{ exists: boolean }>>`
          SELECT EXISTS(
            SELECT 1
            FROM pg_catalog.pg_class class
            INNER JOIN pg_catalog.pg_namespace namespace ON namespace.oid = class.relnamespace
            WHERE namespace.nspname = ${emptySchema}
              AND class.relname = ${"schema_migrations"}
          ) AS exists
        `;
        expect(rows[0]?.exists).toBe(false);
      } finally {
        await verifySql.close();
      }
    } finally {
      await dropSchema(absentSchema);
      await dropSchema(emptySchema);
    }
  });

  test("dry run preserves tracking rows for an already migrated schema", async () => {
    const schema = schemaName("dry_migrated");

    try {
      const applied = await runMigrations({
        databaseUrl: TEST_DATABASE_URL,
        schema,
      });
      const dryRun = await runMigrations({
        databaseUrl: TEST_DATABASE_URL,
        schema,
        dryRun: true,
      });
      expect(dryRun.applied).toEqual([]);
      expect(dryRun.pending).toEqual([]);
      expect(dryRun.skipped).toHaveLength(applied.applied.length);

      const sql = createSqlClient(TEST_DATABASE_URL);
      try {
        const rows = await sql<Array<{ count: number }>>`
          SELECT COUNT(*)::int AS count
          FROM ${sqlIdentifier(schema)}.schema_migrations
        `;
        expect(rows[0]?.count).toBe(applied.applied.length);
      } finally {
        await sql.close();
      }
    } finally {
      await dropSchema(schema);
    }
  });

  test("serializes concurrent migration runners and releases the lock", async () => {
    const schema = schemaName("concurrent");

    try {
      const [first, second] = await Promise.all([
        runMigrations({ databaseUrl: TEST_DATABASE_URL, schema }),
        runMigrations({ databaseUrl: TEST_DATABASE_URL, schema }),
      ]);
      const migrationCount = first.applied.length + second.applied.length;

      expect(migrationCount).toBeGreaterThan(0);
      expect(first.applied.length === 0 || second.applied.length === 0).toBe(true);
      expect(first.skipped.length === 0 || second.skipped.length === 0).toBe(true);

      const rerun = await runMigrations({ databaseUrl: TEST_DATABASE_URL, schema });
      expect(rerun.applied).toEqual([]);
      expect(rerun.skipped).toHaveLength(migrationCount);
    } finally {
      await dropSchema(schema);
    }
  });

  test("releases the advisory lock after a migration validation failure", async () => {
    const schema = schemaName("failure");
    const fixtureRoot = await mkdtemp(join(tmpdir(), "osimi-migration-fixtures-"));
    const baseMigration = join(fixtureRoot, "0001_base.sql");

    try {
      const baseSql = "CREATE TABLE migration_lock_recovery_test (id integer);";
      await Bun.write(baseMigration, baseSql);
      await runMigrations({
        databaseUrl: TEST_DATABASE_URL,
        schema,
        migrationsDir: fixtureRoot,
      });
      await Bun.write(baseMigration, "CREATE TABLE migration_lock_recovery_test (id text);");

      let failed = false;

      try {
        await runMigrations({
          databaseUrl: TEST_DATABASE_URL,
          schema,
          migrationsDir: fixtureRoot,
        });
      } catch {
        failed = true;
      }

      expect(failed).toBe(true);
      await Bun.write(baseMigration, baseSql);
      await Bun.write(join(fixtureRoot, "0002_success.sql"), "CREATE TABLE migration_lock_recovery_second_test (id integer);");

      const result = await runMigrations({
        databaseUrl: TEST_DATABASE_URL,
        schema,
        migrationsDir: fixtureRoot,
      });
      expect(result.applied).toEqual(["0002_success.sql"]);
    } finally {
      await rm(fixtureRoot, { recursive: true, force: true });
      await dropSchema(schema);
    }
  });
});
