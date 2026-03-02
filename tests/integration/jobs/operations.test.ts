import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { sql as sqlIdentifier } from "bun";

import { createSqlClient } from "../../../src/db/client.ts";
import { runMigrations } from "../../../src/db/migrate.ts";
import { runStagingRetentionSweep, runStuckAttentionCheck } from "../../../src/jobs/operations.ts";
import { runWithRuntimeConfig } from "../../../src/runtime/config.ts";
import { resolveStagingPath } from "../../../src/storage/staging.ts";

const TEST_DATABASE_URL = process.env.TEST_DATABASE_URL ?? process.env.DATABASE_URL;

describe.skipIf(!TEST_DATABASE_URL)("jobs operations", () => {
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
    const keepStorageKey = "tenants/t1/ingestions/i-keep/original/f-keep.txt";
    const cleanupCompletedStorageKey = "tenants/t1/ingestions/i-completed/original/f-completed.txt";
    const cleanupFailedStorageKey = "tenants/t1/ingestions/i-failed/original/f-failed.txt";

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

      await mkdir(dirname(keepPath), { recursive: true });
      await mkdir(dirname(completedPath), { recursive: true });
      await mkdir(dirname(failedPath), { recursive: true });

      await Bun.write(keepPath, "keep");
      await Bun.write(completedPath, "done");
      await Bun.write(failedPath, "fail");

      const result = await runWithRuntimeConfig(
        { databaseUrl: TEST_DATABASE_URL, dbSchema: schema, stagingRoot },
        () =>
          runStagingRetentionSweep({
            completedRetentionDays: 7,
            failedCanceledRetentionDays: 14,
          }),
      );

      expect(result.scanned).toBe(2);
      expect(result.deleted).toBe(2);
      expect(result.missing).toBe(0);
      expect(await Bun.file(keepPath).exists()).toBe(true);
      expect(await Bun.file(completedPath).exists()).toBe(false);
      expect(await Bun.file(failedPath).exists()).toBe(false);
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
});
