import { mkdir, mkdtemp, rm, utimes } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { sql as sqlIdentifier } from "bun";

import { createSqlClient } from "../../../src/db/client.ts";
import { runMigrations } from "../../../src/db/migrate.ts";
import { claimStagingPurgeBatch, completeStagingPurge } from "../../../src/repos/ingestion-repo.ts";
import {
  runCurationPublicationSourceCleanup,
  runStagingRetentionSweep,
  runStuckAttentionCheck,
} from "../../../src/jobs/operations.ts";
import { runWithRuntimeConfig } from "../../../src/runtime/config.ts";
import { resolveStagingPath } from "../../../src/storage/staging.ts";
import { TEST_DATABASE_URL } from "../test-database.ts";

describe("jobs operations", () => {
  let schema = "";
  let stagingRoot = "";

  beforeAll(async () => {
    schema = `jobs_${Date.now()}_${Math.floor(Math.random() * 100000)}`;
    stagingRoot = await mkdtemp(join(tmpdir(), "osimi-jobs-staging-"));

    await runMigrations({
      databaseUrl: TEST_DATABASE_URL,
      schema,
    });
  });

  afterAll(async () => {
    if (TEST_DATABASE_URL && schema) {
      const sql = createSqlClient(TEST_DATABASE_URL);
      try {
        await sql`DROP SCHEMA IF EXISTS ${sqlIdentifier(schema)} CASCADE`;
      } finally {
        await sql.close();
      }
    }

    if (stagingRoot) {
      await rm(stagingRoot, { recursive: true, force: true });
    }
  });

  test("applies staging retention windows by ingestion status", async () => {
    const sql = createSqlClient(TEST_DATABASE_URL!);
    const keepStorageKey = "tenants/00000000-0000-0000-0000-000000000001/ingestions/30000000-0000-0000-0000-000000000101/original/f-keep.txt";
    const cleanupCompletedStorageKey = "tenants/00000000-0000-0000-0000-000000000001/ingestions/30000000-0000-0000-0000-000000000102/original/f-completed.txt";
    const cleanupCompletedPreviewKey = "tenants/00000000-0000-0000-0000-000000000001/ingestions/30000000-0000-0000-0000-000000000102/preview/f-completed.jpg";
    const cleanupFailedStorageKey = "tenants/00000000-0000-0000-0000-000000000001/ingestions/30000000-0000-0000-0000-000000000103/original/f-failed.txt";
    const cleanupFailedPreviewKey = "tenants/00000000-0000-0000-0000-000000000001/ingestions/30000000-0000-0000-0000-000000000103/preview/f-failed.jpg";

    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;

      await sql`
        INSERT INTO ingestions (
          id,
          batch_label,
          tenant_id,
          status,
          created_by,
          schema_version,
          classification_type,
          item_kind,
          language_code,
          pipeline_preset,
          access_level,
          updated_at
        )
        VALUES
          (
            ${"30000000-0000-0000-0000-000000000101"},
            ${"b-keep"},
            ${"00000000-0000-0000-0000-000000000001"},
            ${"UPLOADING"}::ingestion_status,
            ${"10000000-0000-0000-0000-000000000001"},
            ${"1.0"},
            ${"document"}::ingestion_classification_type,
            ${"document"}::ingest_item_kind,
            ${"en"},
            ${"auto"}::ingestion_pipeline_preset,
            ${"private"}::object_access_level,
            now() - interval '1 day'
          ),
          (
            ${"30000000-0000-0000-0000-000000000102"},
            ${"b-completed"},
            ${"00000000-0000-0000-0000-000000000001"},
            ${"COMPLETED"}::ingestion_status,
            ${"10000000-0000-0000-0000-000000000001"},
            ${"1.0"},
            ${"document"}::ingestion_classification_type,
            ${"document"}::ingest_item_kind,
            ${"en"},
            ${"auto"}::ingestion_pipeline_preset,
            ${"private"}::object_access_level,
            now() - interval '8 day'
          ),
          (
            ${"30000000-0000-0000-0000-000000000103"},
            ${"b-failed"},
            ${"00000000-0000-0000-0000-000000000001"},
            ${"FAILED"}::ingestion_status,
            ${"10000000-0000-0000-0000-000000000001"},
            ${"1.0"},
            ${"document"}::ingestion_classification_type,
            ${"document"}::ingest_item_kind,
            ${"en"},
            ${"auto"}::ingestion_pipeline_preset,
            ${"private"}::object_access_level,
            now() - interval '15 day'
          ),
          (
            ${"30000000-0000-0000-0000-000000000104"},
            ${"b-canceled-fresh"},
            ${"00000000-0000-0000-0000-000000000001"},
            ${"CANCELED"}::ingestion_status,
            ${"10000000-0000-0000-0000-000000000001"},
            ${"1.0"},
            ${"document"}::ingestion_classification_type,
            ${"document"}::ingest_item_kind,
            ${"en"},
            ${"auto"}::ingestion_pipeline_preset,
            ${"private"}::object_access_level,
            now() - interval '10 day'
          )
      `;

      await sql`
        INSERT INTO ingestion_files (
          id,
          ingestion_id,
          filename,
          content_type,
          size_bytes,
          storage_key,
          preview_storage_key,
          status,
          checksum_sha256
        )
        VALUES
          (
            ${"40000000-0000-0000-0000-000000000101"},
            ${"30000000-0000-0000-0000-000000000101"},
            ${"keep.txt"},
            ${"text/plain"},
            ${4},
            ${keepStorageKey},
            ${null},
            ${"UPLOADED"}::ingestion_file_status,
            ${"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
          ),
          (
            ${"40000000-0000-0000-0000-000000000102"},
            ${"30000000-0000-0000-0000-000000000102"},
            ${"completed.txt"},
            ${"text/plain"},
            ${4},
            ${cleanupCompletedStorageKey},
            ${cleanupCompletedPreviewKey},
            ${"UPLOADED"}::ingestion_file_status,
            ${"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
          ),
          (
            ${"40000000-0000-0000-0000-000000000103"},
            ${"30000000-0000-0000-0000-000000000103"},
            ${"failed.txt"},
            ${"text/plain"},
            ${4},
            ${cleanupFailedStorageKey},
            ${cleanupFailedPreviewKey},
            ${"UPLOADED"}::ingestion_file_status,
            ${"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
          )
      `;

      const keepPath = runWithRuntimeConfig(
        { databaseUrl: TEST_DATABASE_URL, dbSchema: schema, stagingRoot },
        () => resolveStagingPath(keepStorageKey),
      );
      const completedPath = runWithRuntimeConfig(
        { databaseUrl: TEST_DATABASE_URL, dbSchema: schema, stagingRoot },
        () => resolveStagingPath(cleanupCompletedStorageKey),
      );
      const failedPath = runWithRuntimeConfig(
        { databaseUrl: TEST_DATABASE_URL, dbSchema: schema, stagingRoot },
        () => resolveStagingPath(cleanupFailedStorageKey),
      );
      const completedPreviewPath = runWithRuntimeConfig(
        { databaseUrl: TEST_DATABASE_URL, dbSchema: schema, stagingRoot },
        () => resolveStagingPath(cleanupCompletedPreviewKey),
      );
      const failedPreviewPath = runWithRuntimeConfig(
        { databaseUrl: TEST_DATABASE_URL, dbSchema: schema, stagingRoot },
        () => resolveStagingPath(cleanupFailedPreviewKey),
      );

      await mkdir(dirname(keepPath), { recursive: true });
      await mkdir(dirname(completedPath), { recursive: true });
      await mkdir(dirname(failedPath), { recursive: true });
      await mkdir(dirname(completedPreviewPath), { recursive: true });
      await mkdir(dirname(failedPreviewPath), { recursive: true });

      await Bun.write(keepPath, "keep");
      await Bun.write(completedPath, "done");
      await Bun.write(failedPath, "fail");
      await Bun.write(completedPreviewPath, "donep");
      await Bun.write(failedPreviewPath, "failp");

      const result = await runWithRuntimeConfig(
        { databaseUrl: TEST_DATABASE_URL, dbSchema: schema, stagingRoot },
        () =>
          runStagingRetentionSweep({
            completedRetentionDays: 7,
            failedCanceledRetentionDays: 14,
          }),
      );

      expect(result.claimed).toBe(2);
      expect(result.purged).toBe(2);
      expect(result.missing).toBe(0);
      expect(await Bun.file(keepPath).exists()).toBe(true);
      expect(await Bun.file(completedPath).exists()).toBe(false);
      expect(await Bun.file(failedPath).exists()).toBe(false);
      expect(await Bun.file(completedPreviewPath).exists()).toBe(false);
      expect(await Bun.file(failedPreviewPath).exists()).toBe(false);
    } finally {
      await sql.close();
    }
  });

  test("cleans terminal curation sources and old untracked files", async () => {
    const sql = createSqlClient(TEST_DATABASE_URL!);
    const tenantId = "00000000-0000-0000-0000-000000000901";
    const objectId = "OBJ-20260821-CLEANUP";
    const requestedBy = "10000000-0000-0000-0000-000000000901";
    const completedId = "20000000-0000-0000-0000-000000000901";
    const failedId = "20000000-0000-0000-0000-000000000902";
    const missingId = "20000000-0000-0000-0000-000000000903";
    const activeId = "20000000-0000-0000-0000-000000000904";
    const sourceKey = (requestId: string) =>
      `tenants/${tenantId}/archive-request-sources/${objectId}/${requestId}/source.txt`;
    const orphanKey = sourceKey("20000000-0000-0000-0000-000000000999");

    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`
        INSERT INTO tenants (id, slug, name)
        VALUES (${tenantId}, 'curation-source-cleanup', 'Curation source cleanup')
      `;
      await sql`
        INSERT INTO objects (object_id, tenant_id, title)
        VALUES (${objectId}, ${tenantId}, 'Cleanup object')
      `;
      await sql`
        INSERT INTO archive_requests (
          id, tenant_id, target_type, target_id, action_type, requested_by,
          dedupe_key, status, completed_at
        ) VALUES
          (${completedId}, ${tenantId}, 'object', ${objectId}, 'curation_apply', ${requestedBy}, 'cleanup-completed', 'COMPLETED', now() - interval '2 days'),
          (${failedId}, ${tenantId}, 'object', ${objectId}, 'curation_apply', ${requestedBy}, 'cleanup-failed', 'FAILED', NULL),
          (${missingId}, ${tenantId}, 'object', ${objectId}, 'curation_apply', ${requestedBy}, 'cleanup-missing', 'CANCELED', now() - interval '2 days'),
          (${activeId}, ${tenantId}, 'object', ${objectId}, 'curation_apply', ${requestedBy}, 'cleanup-active', 'PENDING', NULL)
      `;
      await sql`
        INSERT INTO curation_publications (
          request_id, tenant_id, object_id, curated_kind, publication_revision,
          target_version, storage_key, content_type, size_bytes, checksum_sha256,
          cleanup_eligible_at
        ) VALUES
          (${completedId}, ${tenantId}, ${objectId}, 'ocr_curated', 1, '2026-08-19', ${sourceKey(completedId)}, 'text/plain', 9, ${"a".repeat(64)}, now() - interval '1 hour'),
          (${failedId}, ${tenantId}, ${objectId}, 'ocr_curated', 2, '2026-08-19', ${sourceKey(failedId)}, 'text/plain', 6, ${"b".repeat(64)}, now() - interval '1 hour'),
          (${missingId}, ${tenantId}, ${objectId}, 'ocr_curated', 3, '2026-08-19', ${sourceKey(missingId)}, 'text/plain', 7, ${"c".repeat(64)}, now() - interval '1 hour'),
          (${activeId}, ${tenantId}, ${objectId}, 'ocr_curated', 4, '2026-08-21', ${sourceKey(activeId)}, 'text/plain', 6, ${"d".repeat(64)}, NULL)
      `;

      for (const [storageKey, contents] of [
        [sourceKey(completedId), "completed"],
        [sourceKey(failedId), "failed"],
        [sourceKey(activeId), "active"],
        [orphanKey, "orphan"],
      ] as const) {
        const path = runWithRuntimeConfig(
          { databaseUrl: TEST_DATABASE_URL, dbSchema: schema, stagingRoot },
          () => resolveStagingPath(storageKey),
        );
        await mkdir(dirname(path), { recursive: true });
        await Bun.write(path, contents);
      }
      const orphanPath = runWithRuntimeConfig(
        { databaseUrl: TEST_DATABASE_URL, dbSchema: schema, stagingRoot },
        () => resolveStagingPath(orphanKey),
      );
      const oldTime = new Date(Date.now() - 2 * 60 * 60 * 1_000);
      await utimes(orphanPath, oldTime, oldTime);

      const result = await runWithRuntimeConfig(
        { databaseUrl: TEST_DATABASE_URL, dbSchema: schema, stagingRoot },
        () => runCurationPublicationSourceCleanup({ orphanMinAgeSeconds: 3600 }),
      );

      expect(result).toEqual({ claimed: 3, purged: 3, missing: 1, failed: 0, orphaned: 1 });
      expect(await Bun.file(orphanPath).exists()).toBe(false);
      const activePath = runWithRuntimeConfig(
        { databaseUrl: TEST_DATABASE_URL, dbSchema: schema, stagingRoot },
        () => resolveStagingPath(sourceKey(activeId)),
      );
      expect(await Bun.file(activePath).exists()).toBe(true);
      const rows = await sql<Array<{ request_id: string; purged_at: Date | null }>>`
        SELECT request_id, purged_at
        FROM curation_publications
        WHERE object_id = ${objectId}
        ORDER BY publication_revision
      `;
      expect(rows.map((row) => [row.request_id, row.purged_at !== null])).toEqual([
        [completedId, true],
        [failedId, true],
        [missingId, true],
        [activeId, false],
      ]);
    } finally {
      await sql.close();
    }
  });

  test("detects stuck ingestions in UPLOADING and PROCESSING only", async () => {
    const sql = createSqlClient(TEST_DATABASE_URL!);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;

      await sql`
        INSERT INTO ingestions (
          id,
          batch_label,
          tenant_id,
          status,
          created_by,
          schema_version,
          classification_type,
          item_kind,
          language_code,
          pipeline_preset,
          access_level,
          updated_at
        )
        VALUES
          (
            ${"30000000-0000-0000-0000-000000000201"},
            ${"b-stuck-upload"},
            ${"00000000-0000-0000-0000-000000000001"},
            ${"UPLOADING"}::ingestion_status,
            ${"10000000-0000-0000-0000-000000000001"},
            ${"1.0"},
            ${"document"}::ingestion_classification_type,
            ${"document"}::ingest_item_kind,
            ${"en"},
            ${"auto"}::ingestion_pipeline_preset,
            ${"private"}::object_access_level,
            now() - interval '90 minute'
          ),
          (
            ${"30000000-0000-0000-0000-000000000202"},
            ${"b-stuck-process"},
            ${"00000000-0000-0000-0000-000000000001"},
            ${"PROCESSING"}::ingestion_status,
            ${"10000000-0000-0000-0000-000000000001"},
            ${"1.0"},
            ${"document"}::ingestion_classification_type,
            ${"document"}::ingest_item_kind,
            ${"en"},
            ${"auto"}::ingestion_pipeline_preset,
            ${"private"}::object_access_level,
            now() - interval '120 minute'
          ),
          (
            ${"30000000-0000-0000-0000-000000000203"},
            ${"b-fresh-process"},
            ${"00000000-0000-0000-0000-000000000001"},
            ${"PROCESSING"}::ingestion_status,
            ${"10000000-0000-0000-0000-000000000001"},
            ${"1.0"},
            ${"document"}::ingestion_classification_type,
            ${"document"}::ingest_item_kind,
            ${"en"},
            ${"auto"}::ingestion_pipeline_preset,
            ${"private"}::object_access_level,
            now() - interval '10 minute'
          )
      `;

      const result = await runWithRuntimeConfig(
        { databaseUrl: TEST_DATABASE_URL, dbSchema: schema, stagingRoot },
        () => runStuckAttentionCheck({ thresholdMinutes: 60 }),
      );

      expect(result.thresholdMinutes).toBe(60);
      const stuckIds = result.ingestions.map((item) => item.ingestion_id);
      expect(stuckIds).toEqual(
        expect.arrayContaining([
          "30000000-0000-0000-0000-000000000201",
          "30000000-0000-0000-0000-000000000202",
        ]),
      );
      expect(stuckIds.includes("30000000-0000-0000-0000-000000000203")).toBe(false);
    } finally {
      await sql.close();
    }
  });

  test("claims bounded purge batches exclusively and converges after completion or lease expiry", async () => {
    const sql = createSqlClient(TEST_DATABASE_URL!);
    const config = {
      completedRetentionDays: 7,
      failedCanceledRetentionDays: 14,
      batchSize: 2,
      claimTimeoutSeconds: 900,
    };
    const runtimeConfig = { databaseUrl: TEST_DATABASE_URL, dbSchema: schema, stagingRoot };

    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`
        INSERT INTO ingestions (
          id, batch_label, tenant_id, status, created_by, schema_version,
          classification_type, item_kind, language_code, pipeline_preset,
          access_level, updated_at
        )
        VALUES
          (${"30000000-0000-0000-0000-000000000301"}, ${"batch-301"}, ${"00000000-0000-0000-0000-000000000001"}, ${"COMPLETED"}::ingestion_status, ${"10000000-0000-0000-0000-000000000001"}, ${"1.0"}, ${"document"}::ingestion_classification_type, ${"document"}::ingest_item_kind, ${"en"}, ${"auto"}::ingestion_pipeline_preset, ${"private"}::object_access_level, now() - interval '8 day'),
          (${"30000000-0000-0000-0000-000000000302"}, ${"batch-302"}, ${"00000000-0000-0000-0000-000000000001"}, ${"COMPLETED_WITH_ERRORS"}::ingestion_status, ${"10000000-0000-0000-0000-000000000001"}, ${"1.0"}, ${"document"}::ingestion_classification_type, ${"document"}::ingest_item_kind, ${"en"}, ${"auto"}::ingestion_pipeline_preset, ${"private"}::object_access_level, now() - interval '8 day'),
          (${"30000000-0000-0000-0000-000000000303"}, ${"batch-303"}, ${"00000000-0000-0000-0000-000000000001"}, ${"FAILED"}::ingestion_status, ${"10000000-0000-0000-0000-000000000001"}, ${"1.0"}, ${"document"}::ingestion_classification_type, ${"document"}::ingest_item_kind, ${"en"}, ${"auto"}::ingestion_pipeline_preset, ${"private"}::object_access_level, now() - interval '15 day'),
          (${"30000000-0000-0000-0000-000000000304"}, ${"batch-304"}, ${"00000000-0000-0000-0000-000000000001"}, ${"CANCELED"}::ingestion_status, ${"10000000-0000-0000-0000-000000000001"}, ${"1.0"}, ${"document"}::ingestion_classification_type, ${"document"}::ingest_item_kind, ${"en"}, ${"auto"}::ingestion_pipeline_preset, ${"private"}::object_access_level, now() - interval '15 day'),
          (${"30000000-0000-0000-0000-000000000305"}, ${"batch-305"}, ${"00000000-0000-0000-0000-000000000001"}, ${"COMPLETED"}::ingestion_status, ${"10000000-0000-0000-0000-000000000001"}, ${"1.0"}, ${"document"}::ingestion_classification_type, ${"document"}::ingest_item_kind, ${"en"}, ${"auto"}::ingestion_pipeline_preset, ${"private"}::object_access_level, now() - interval '8 day')
      `;

      const [first, second] = await Promise.all([
        runWithRuntimeConfig(runtimeConfig, () => claimStagingPurgeBatch(config)),
        runWithRuntimeConfig(runtimeConfig, () => claimStagingPurgeBatch(config)),
      ]);
      const claims = [...first, ...second];

      expect(claims).toHaveLength(4);
      expect(new Set(claims.map((claim) => claim.ingestionId)).size).toBe(4);

      await Promise.all(claims.map((claim) =>
        runWithRuntimeConfig(runtimeConfig, () => completeStagingPurge(claim)),
      ));

      const completedRows = await sql<Array<{ count: string }>>`
        SELECT count(*)::text AS count
        FROM ingestions
        WHERE id IN (
          ${"30000000-0000-0000-0000-000000000301"},
          ${"30000000-0000-0000-0000-000000000302"},
          ${"30000000-0000-0000-0000-000000000303"},
          ${"30000000-0000-0000-0000-000000000304"}
        )
          AND staging_purged_at IS NOT NULL
      `;
      expect(completedRows[0]?.count).toBe("4");

      await sql`
        UPDATE ingestions
        SET staging_purge_started_at = now() - interval '16 minute',
            staging_purge_claim_token = ${"00000000-0000-0000-0000-000000000305"},
            staging_purge_claimed_at = now() - interval '16 minute',
            staging_purge_attempt_count = 1
        WHERE id = ${"30000000-0000-0000-0000-000000000305"}
      `;

      const reclaimed = await runWithRuntimeConfig(runtimeConfig, () => claimStagingPurgeBatch(config));
      expect(reclaimed).toHaveLength(1);
      expect(reclaimed[0]?.ingestionId).toBe("30000000-0000-0000-0000-000000000305");
      await runWithRuntimeConfig(runtimeConfig, () => completeStagingPurge(reclaimed[0]!));

      const remaining = await runWithRuntimeConfig(runtimeConfig, () => claimStagingPurgeBatch(config));
      expect(remaining).toEqual([]);
    } finally {
      await sql.close();
    }
  });
});
