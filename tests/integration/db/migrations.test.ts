import { describe, expect, test } from "bun:test";

import { createSqlClient } from "../../../src/db/client.ts";
import { runMigrations } from "../../../src/db/migrate.ts";

const TEST_DATABASE_URL =
  process.env.TEST_DATABASE_URL ?? process.env.DATABASE_URL;

function asSqlLiteral(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

describe("database migrations", () => {
  test.skipIf(!TEST_DATABASE_URL)(
    "applies migrations and tracks state",
    async () => {
      const schema = `phase1_${Date.now()}_${Math.floor(Math.random() * 100000)}`;

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
        const tableRows = (await sql.unsafe(`
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = ${asSqlLiteral(schema)}
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
      `)) as Array<{ table_name: string }>;

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

        const accessRequestIndexRows = (await sql.unsafe(`
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = ${asSqlLiteral(schema)}
          AND tablename = 'object_access_requests'
      `)) as Array<{ indexname: string }>;

        const accessRequestIndexNames = accessRequestIndexRows.map((row) => row.indexname);
        expect(accessRequestIndexNames.includes("object_access_requests_one_pending_per_user_idx")).toBe(true);

        const constraintRows = (await sql.unsafe(`
        SELECT conname
        FROM pg_constraint c
        INNER JOIN pg_class t ON t.oid = c.conrelid
        INNER JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = ${asSqlLiteral(schema)}
          AND t.relname = 'ingestion_leases'
      `)) as Array<{ conname: string }>;

        const constraintNames = constraintRows.map((row) => row.conname);
        expect(constraintNames.includes("ingestion_leases_no_overlap")).toBe(
          true,
        );

        const columnRows = (await sql.unsafe(`
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = ${asSqlLiteral(schema)}
          AND table_name = 'objects'
      `)) as Array<{ column_name: string }>;

        const columnNames = new Set(columnRows.map((row) => row.column_name));
        expect(columnNames.has("ingest_manifest")).toBe(true);
        expect(columnNames.has("language_code")).toBe(true);
        expect(columnNames.has("updated_at")).toBe(true);
        expect(columnNames.has("embargo_kind")).toBe(true);
        expect(columnNames.has("embargo_curation_state")).toBe(true);
      } finally {
        await sql.unsafe(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
        await sql.close();
      }
    },
  );
});
