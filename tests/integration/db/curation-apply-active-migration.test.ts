import { copyFile, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { describe, expect, test } from "bun:test";
import { sql as sqlIdentifier } from "bun";

import { createSqlClient } from "../../../src/db/client.ts";
import { runMigrations } from "../../../src/db/migrate.ts";
import { TEST_DATABASE_URL } from "../test-database.ts";

const migrationsSource = fileURLToPath(
  new URL("../../../src/db/migrations", import.meta.url),
);
const repositoryRoot = fileURLToPath(new URL("../../..", import.meta.url));
const migrationsThrough0014 = [
  "0001_init.sql",
  "0002_auth.sql",
  "0003_auth_audit.sql",
  "0004_archive_requests.sql",
  "0005_object_editing_foundation.sql",
  "0006_object_text_manifest.sql",
  "0007_object_edit_locks.sql",
  "0008_ingestion_file_previews.sql",
  "0009_immutable_staged_uploads.sql",
  "0010_ingestion_request_idempotency.sql",
  "0011_staging_purge_tracking.sql",
  "0012_object_event_envelope_identity.sql",
  "0013_object_artifact_search_documents.sql",
  "0014_archive_artifact_upload_attempts.sql",
];

interface MigratedRequestRow {
  id: string;
  status: "PENDING" | "PROCESSING" | "CANCELED";
  failure_reason: string | null;
  failure_details: {
    migration?: string;
    reason?: string;
    previous_status?: string;
    retained_request_id?: string;
    survivor_policy?: string;
  } | null;
  lease_id: string | null;
  lease_token_id: string | null;
  lease_expires_at: Date | null;
  leased_by: string | null;
  released_at: Date | null;
  completed_at: Date | null;
}

describe("migration 0015 active curation publication invariant", () => {
  test("retains one processing request or otherwise the newest pending request", async () => {
    const schema = `curation_apply_migration_${Date.now()}_${Math.floor(Math.random() * 100000)}`;
    const migrationsDir = await mkdtemp(
      join(tmpdir(), "osimi-curation-apply-migrations-"),
    );
    const pool = createSqlClient(TEST_DATABASE_URL);
    const sql = await pool.reserve();
    const tenantId = "10000000-0000-0000-0000-000000000001";
    const requestedBy = "10000000-0000-0000-0000-000000000002";
    const objectId = "OBJ-20260820-MIGRATION";
    const pendingOnlyObjectId = "OBJ-20260820-PENDING";
    const oldestPendingId = "20000000-0000-0000-0000-000000000001";
    const processingId = "20000000-0000-0000-0000-000000000002";
    const newestPendingId = "20000000-0000-0000-0000-000000000003";
    const processingLeaseId = "30000000-0000-0000-0000-000000000001";
    const processingLeaseTokenId = "40000000-0000-0000-0000-000000000001";
    const olderPendingOnlyId = "20000000-0000-0000-0000-000000000004";
    const newestPendingOnlyId = "20000000-0000-0000-0000-000000000005";

    try {
      for (const name of migrationsThrough0014) {
        await copyFile(join(migrationsSource, name), join(migrationsDir, name));
      }

      const legacyRun = await runMigrations({
        databaseUrl: TEST_DATABASE_URL,
        schema,
        migrationsDir,
      });
      expect(legacyRun.applied).toEqual(migrationsThrough0014);

      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`
        INSERT INTO archive_requests (
          id, tenant_id, target_type, target_id, action_type, requested_by,
          dedupe_key, status, lease_id, lease_token_id, lease_expires_at,
          leased_by, created_at, updated_at
        )
        VALUES
          (
            ${oldestPendingId}, ${tenantId}, 'object', ${objectId}, 'curation_apply',
            ${requestedBy}, 'legacy-pending-oldest', 'PENDING', NULL, NULL, NULL,
            NULL, '2026-08-20T10:00:00Z', '2026-08-20T10:00:00Z'
          ),
          (
            ${processingId}, ${tenantId}, 'object', ${objectId}, 'curation_apply',
            ${requestedBy}, 'legacy-processing', 'PROCESSING', ${processingLeaseId},
            ${processingLeaseTokenId}, '2026-08-21T12:00:00Z', 'legacy-worker',
            '2026-08-20T11:00:00Z', '2026-08-20T11:00:00Z'
          ),
          (
            ${newestPendingId}, ${tenantId}, 'object', ${objectId}, 'curation_apply',
            ${requestedBy}, 'legacy-pending-newest', 'PENDING', NULL, NULL, NULL,
            NULL, '2026-08-20T12:00:00Z', '2026-08-20T12:00:00Z'
          )
      `;
      await sql`
        INSERT INTO archive_requests (
          id, tenant_id, target_type, target_id, action_type, requested_by,
          dedupe_key, status, created_at, updated_at
        )
        VALUES
          (
            ${olderPendingOnlyId}, ${tenantId}, 'object', ${pendingOnlyObjectId},
            'curation_apply', ${requestedBy}, 'pending-only-older', 'PENDING',
            '2026-08-20T12:00:00Z', '2026-08-20T12:00:00Z'
          ),
          (
            ${newestPendingOnlyId}, ${tenantId}, 'object', ${pendingOnlyObjectId},
            'curation_apply', ${requestedBy}, 'pending-only-newest', 'PENDING',
            '2026-08-20T12:00:00Z', '2026-08-20T12:00:00Z'
          )
      `;

      await copyFile(
        join(migrationsSource, "0015_one_active_curation_apply_per_object.sql"),
        join(migrationsDir, "0015_one_active_curation_apply_per_object.sql"),
      );
      const upgradeRun = await runMigrations({
        databaseUrl: TEST_DATABASE_URL,
        schema,
        migrationsDir,
      });
      expect(upgradeRun.applied).toEqual([
        "0015_one_active_curation_apply_per_object.sql",
      ]);

      const rows = await sql<MigratedRequestRow[]>`
        SELECT id, status, failure_reason, failure_details, lease_id,
               lease_token_id, lease_expires_at, leased_by, released_at,
               completed_at
        FROM archive_requests
        WHERE tenant_id = ${tenantId} AND target_id = ${objectId}
        ORDER BY id
      `;
      expect(rows).toHaveLength(3);
      const byId = new Map(rows.map((row) => [row.id, row]));

      expect(byId.get(processingId)).toMatchObject({
        status: "PROCESSING",
        failure_reason: null,
        failure_details: null,
        lease_id: processingLeaseId,
        lease_token_id: processingLeaseTokenId,
        leased_by: "legacy-worker",
        released_at: null,
        completed_at: null,
      });
      expect(byId.get(processingId)?.lease_expires_at).toEqual(
        new Date("2026-08-21T12:00:00Z"),
      );

      for (const id of [oldestPendingId, newestPendingId]) {
        const row = byId.get(id);
        expect(row).toMatchObject({
          status: "CANCELED",
          failure_reason: "Canceled by migration 0015: safe active curation_apply request retained.",
          lease_expires_at: null,
        });
        expect(row?.failure_details).toEqual({
          migration: "0015_one_active_curation_apply_per_object",
          reason: "duplicate_active_curation_apply",
          previous_status: "PENDING",
          retained_request_id: processingId,
          survivor_policy: "processing_then_newest_pending",
        });
        expect(row?.released_at).toBeInstanceOf(Date);
        expect(row?.completed_at).toBeInstanceOf(Date);
      }

      const indexRows = await sql<Array<{ indexname: string }>>`
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = ${schema}
          AND indexname = 'archive_requests_one_active_curation_apply_per_object_idx'
      `;
      expect(indexRows).toHaveLength(1);

      const pendingOnlyRows = await sql<MigratedRequestRow[]>`
        SELECT id, status, failure_reason, failure_details, lease_id,
               lease_token_id, lease_expires_at, leased_by, released_at,
               completed_at
        FROM archive_requests
        WHERE tenant_id = ${tenantId} AND target_id = ${pendingOnlyObjectId}
        ORDER BY id
      `;
      expect(pendingOnlyRows).toHaveLength(2);
      expect(pendingOnlyRows[0]).toMatchObject({
        id: olderPendingOnlyId,
        status: "CANCELED",
        failure_details: {
          retained_request_id: newestPendingOnlyId,
          survivor_policy: "processing_then_newest_pending",
        },
      });
      expect(pendingOnlyRows[1]).toMatchObject({
        id: newestPendingOnlyId,
        status: "PENDING",
        failure_reason: null,
        failure_details: null,
      });

      let uniquenessError: unknown;
      try {
        await sql`
          INSERT INTO archive_requests (
            id, tenant_id, target_type, target_id, action_type, requested_by,
            dedupe_key, status
          )
          VALUES (
            ${crypto.randomUUID()}, ${tenantId}, 'object', ${objectId},
            'curation_apply', ${requestedBy}, 'post-migration-conflict', 'PENDING'
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
    } finally {
      await sql`DROP SCHEMA IF EXISTS ${sqlIdentifier(schema)} CASCADE`;
      await sql.release();
      await pool.close();
      await rm(migrationsDir, { recursive: true, force: true });
    }
  }, 30_000);

  test("waits for an in-flight worker write before reconciling requests", async () => {
    const schema = `curation_apply_fence_${Date.now()}_${Math.floor(Math.random() * 100000)}`;
    const migrationsDir = await mkdtemp(
      join(tmpdir(), "osimi-curation-apply-fence-migrations-"),
    );
    const pool = createSqlClient(TEST_DATABASE_URL);
    const sql = await pool.reserve();
    const workerPool = createSqlClient(TEST_DATABASE_URL);
    const workerSql = await workerPool.reserve();
    const tenantId = "90000000-0000-0000-0000-000000000001";
    const requestedBy = "90000000-0000-0000-0000-000000000002";
    const objectId = "OBJ-20260821-FENCE";
    const requestId = "90000000-0000-0000-0000-000000000003";
    let releaseWorker!: () => void;
    const holdWorker = new Promise<void>((resolve) => {
      releaseWorker = resolve;
    });
    let workerReady!: () => void;
    const workerStarted = new Promise<void>((resolve) => {
      workerReady = resolve;
    });

    try {
      for (const name of migrationsThrough0014) {
        await copyFile(join(migrationsSource, name), join(migrationsDir, name));
      }
      await runMigrations({ databaseUrl: TEST_DATABASE_URL, schema, migrationsDir });
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await workerSql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`
        INSERT INTO archive_requests (
          id, tenant_id, target_type, target_id, action_type, requested_by,
          dedupe_key, status
        ) VALUES (
          ${requestId}, ${tenantId}, 'object', ${objectId}, 'curation_apply',
          ${requestedBy}, 'worker-fence', 'PENDING'
        )
      `;
      await copyFile(
        join(migrationsSource, "0015_one_active_curation_apply_per_object.sql"),
        join(migrationsDir, "0015_one_active_curation_apply_per_object.sql"),
      );

      const workerTransaction = workerSql.begin(async (transaction) => {
        await transaction`
          UPDATE archive_requests
          SET status = 'PROCESSING',
              lease_id = ${"90000000-0000-0000-0000-000000000004"},
              lease_token_id = ${"90000000-0000-0000-0000-000000000005"},
              lease_expires_at = now() + interval '5 minutes',
              leased_by = 'worker-before-migration'
          WHERE id = ${requestId}
        `;
        workerReady();
        await holdWorker;
      });
      await workerStarted;

      let migrationSettled = false;
      const migration = runMigrations({
        databaseUrl: TEST_DATABASE_URL,
        schema,
        migrationsDir,
      }).finally(() => {
        migrationSettled = true;
      });
      await Bun.sleep(100);
      expect(migrationSettled).toBe(false);

      releaseWorker();
      await workerTransaction;
      const result = await migration;
      expect(result.applied).toEqual(["0015_one_active_curation_apply_per_object.sql"]);
      const rows = await sql<Array<{ status: string; leased_by: string | null }>>`
        SELECT status, leased_by FROM archive_requests WHERE id = ${requestId}
      `;
      expect(rows).toEqual([{
        status: "PROCESSING",
        leased_by: "worker-before-migration",
      }]);
    } finally {
      releaseWorker();
      await sql`DROP SCHEMA IF EXISTS ${sqlIdentifier(schema)} CASCADE`;
      await workerSql.release();
      await workerPool.close();
      await sql.release();
      await pool.close();
      await rm(migrationsDir, { recursive: true, force: true });
    }
  }, 30_000);

  test("fails closed for multiple processing requests and rolls back", async () => {
    const schema = `curation_apply_conflict_${Date.now()}_${Math.floor(Math.random() * 100000)}`;
    const migrationsDir = await mkdtemp(
      join(tmpdir(), "osimi-curation-apply-conflict-migrations-"),
    );
    const pool = createSqlClient(TEST_DATABASE_URL);
    const sql = await pool.reserve();
    const tenantId = "50000000-0000-0000-0000-000000000001";
    const requestedBy = "50000000-0000-0000-0000-000000000002";
    const objectId = "OBJ-20260820-CONFLICT";
    const firstProcessingId = "60000000-0000-0000-0000-000000000001";
    const secondProcessingId = "60000000-0000-0000-0000-000000000002";
    const pendingId = "60000000-0000-0000-0000-000000000003";

    try {
      for (const name of migrationsThrough0014) {
        await copyFile(join(migrationsSource, name), join(migrationsDir, name));
      }
      await runMigrations({
        databaseUrl: TEST_DATABASE_URL,
        schema,
        migrationsDir,
      });

      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`
        INSERT INTO archive_requests (
          id, tenant_id, target_type, target_id, action_type, requested_by,
          dedupe_key, status, lease_id, lease_token_id, lease_expires_at,
          leased_by, created_at, updated_at
        )
        VALUES
          (
            ${firstProcessingId}, ${tenantId}, 'object', ${objectId}, 'curation_apply',
            ${requestedBy}, 'processing-1', 'PROCESSING',
            '70000000-0000-0000-0000-000000000001',
            '80000000-0000-0000-0000-000000000001',
            '2026-08-21T12:00:00Z', 'worker-1',
            '2026-08-20T10:00:00Z', '2026-08-20T10:00:00Z'
          ),
          (
            ${secondProcessingId}, ${tenantId}, 'object', ${objectId}, 'curation_apply',
            ${requestedBy}, 'processing-2', 'PROCESSING',
            '70000000-0000-0000-0000-000000000002',
            '80000000-0000-0000-0000-000000000002',
            '2026-08-21T12:00:00Z', 'worker-2',
            '2026-08-20T11:00:00Z', '2026-08-20T11:00:00Z'
          ),
          (
            ${pendingId}, ${tenantId}, 'object', ${objectId}, 'curation_apply',
            ${requestedBy}, 'pending', 'PENDING', NULL, NULL, NULL, NULL,
            '2026-08-20T12:00:00Z', '2026-08-20T12:00:00Z'
          )
      `;
      await copyFile(
        join(migrationsSource, "0015_one_active_curation_apply_per_object.sql"),
        join(migrationsDir, "0015_one_active_curation_apply_per_object.sql"),
      );

      const migrationProcess = Bun.spawn({
        cmd: [
          process.execPath,
          "run",
          "src/db/migrate.ts",
          `--database-url=${TEST_DATABASE_URL}`,
          `--schema=${schema}`,
          `--migrations-dir=${migrationsDir}`,
        ],
        cwd: repositoryRoot,
        stdout: "pipe",
        stderr: "pipe",
      });
      const [migrationExitCode, migrationStderr] = await Promise.all([
        migrationProcess.exited,
        new Response(migrationProcess.stderr).text(),
      ]);
      expect(migrationExitCode).toBe(1);
      expect(migrationStderr).toContain(
        `Migration 0015 cannot reconcile multiple PROCESSING curation_apply requests for tenant_id=${tenantId} object_id=${objectId} processing_count=2. Quiesce workers and reconcile these requests before retrying the migration.`,
      );

      const unchangedRows = await sql<Array<{
        id: string;
        status: string;
        failure_reason: string | null;
        released_at: Date | null;
      }>>`
        SELECT id, status, failure_reason, released_at
        FROM archive_requests
        WHERE tenant_id = ${tenantId} AND target_id = ${objectId}
        ORDER BY id
      `;
      expect(unchangedRows).toEqual([
        { id: firstProcessingId, status: "PROCESSING", failure_reason: null, released_at: null },
        { id: secondProcessingId, status: "PROCESSING", failure_reason: null, released_at: null },
        { id: pendingId, status: "PENDING", failure_reason: null, released_at: null },
      ]);
      const failedMigrationRows = await sql<Array<{ count: number }>>`
        SELECT COUNT(*)::int AS count
        FROM schema_migrations
        WHERE name = '0015_one_active_curation_apply_per_object.sql'
      `;
      expect(failedMigrationRows[0]?.count).toBe(0);
      const absentIndexRows = await sql<Array<{ count: number }>>`
        SELECT COUNT(*)::int AS count
        FROM pg_indexes
        WHERE schemaname = ${schema}
          AND indexname = 'archive_requests_one_active_curation_apply_per_object_idx'
      `;
      expect(absentIndexRows[0]?.count).toBe(0);

      await sql`
        UPDATE archive_requests
        SET status = 'CANCELED', released_at = now(), completed_at = now(),
            lease_expires_at = NULL, updated_at = now()
        WHERE id = ${secondProcessingId}
      `;
      const processingCountRows = await sql<Array<{ count: number }>>`
        SELECT COUNT(*)::int AS count
        FROM archive_requests
        WHERE tenant_id = ${tenantId}
          AND target_id = ${objectId}
          AND status = 'PROCESSING'
      `;
      expect(processingCountRows[0]?.count).toBe(1);
      const retryRun = await runMigrations({
        databaseUrl: TEST_DATABASE_URL,
        schema,
        migrationsDir,
      });
      expect(retryRun.applied).toEqual([
        "0015_one_active_curation_apply_per_object.sql",
      ]);

      const reconciledRows = await sql<Array<{ id: string; status: string }>>`
        SELECT id, status
        FROM archive_requests
        WHERE tenant_id = ${tenantId} AND target_id = ${objectId}
        ORDER BY id
      `;
      expect(reconciledRows).toEqual([
        { id: firstProcessingId, status: "PROCESSING" },
        { id: secondProcessingId, status: "CANCELED" },
        { id: pendingId, status: "CANCELED" },
      ]);
      const indexRows = await sql<Array<{ count: number }>>`
        SELECT COUNT(*)::int AS count
        FROM pg_indexes
        WHERE schemaname = ${schema}
          AND indexname = 'archive_requests_one_active_curation_apply_per_object_idx'
      `;
      expect(indexRows[0]?.count).toBe(1);
    } finally {
      await sql`DROP SCHEMA IF EXISTS ${sqlIdentifier(schema)} CASCADE`;
      await sql.release();
      await pool.close();
      await rm(migrationsDir, { recursive: true, force: true });
    }
  }, 30_000);
});
