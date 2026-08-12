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
const legacyMigrationNames = [
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
];

interface AttemptRow {
  request_id: string;
  state: "AUTHORIZED" | "VERIFIED" | "MATERIALIZED";
  storage_key: string;
  content_type: string;
  size_bytes: number;
  computed_sha256: string | null;
  verified_at: Date | null;
  artifact_id: string | null;
  materialized_at: Date | null;
  invalidated_at: Date | null;
}

interface RequestRow {
  id: string;
  status: string;
  lease_expires_at: Date | null;
  released_at: Date | null;
}

describe("migration 0014 populated legacy upgrade", () => {
  test(
    "backfills upload attempts without linking unsafe legacy requests",
    async () => {
      const schema = `artifact_upload_migration_${Date.now()}_${Math.floor(Math.random() * 100000)}`;
      const migrationsDir = await mkdtemp(
        join(tmpdir(), "osimi-artifact-upload-migrations-"),
      );
      const sql = createSqlClient(TEST_DATABASE_URL);
      const tenantId = "10000000-0000-0000-0000-000000000001";
      const requestedBy = "10000000-0000-0000-0000-000000000002";
      const objectId = "OBJ-20260805-MIGRATION1";
      const otherObjectId = "OBJ-20260805-MIGRATION2";
      const checksum = "a".repeat(64);
      const otherChecksum = "b".repeat(64);
      const ids = {
        active: "20000000-0000-0000-0000-000000000001",
        expiredPending: "20000000-0000-0000-0000-000000000002",
        processingVerified: "20000000-0000-0000-0000-000000000003",
        pendingVerified: "20000000-0000-0000-0000-000000000004",
        materialized: "20000000-0000-0000-0000-000000000005",
        completedNullChecksum: "20000000-0000-0000-0000-000000000006",
        crossObject: "20000000-0000-0000-0000-000000000007",
        failedChecksum: "20000000-0000-0000-0000-000000000008",
        canceledNullChecksum: "20000000-0000-0000-0000-000000000009",
      };

      try {
        for (const name of legacyMigrationNames) {
          await copyFile(join(migrationsSource, name), join(migrationsDir, name));
        }

        const legacyRun = await runMigrations({
          databaseUrl: TEST_DATABASE_URL,
          schema,
          migrationsDir,
        });
        expect(legacyRun.applied).toEqual(legacyMigrationNames);

        await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
        await sql`
          INSERT INTO tenants (id, slug, name)
          VALUES (${tenantId}, ${"migration-0014"}, ${"Migration 0014"})
        `;
        await sql`
          INSERT INTO objects (object_id, tenant_id, title)
          VALUES
            (${objectId}, ${tenantId}, ${"Migration target"}),
            (${otherObjectId}, ${tenantId}, ${"Other migration target"})
        `;

        const materializedArtifactId = "30000000-0000-0000-0000-000000000001";
        const nullChecksumArtifactId = "30000000-0000-0000-0000-000000000002";
        const crossObjectArtifactId = "30000000-0000-0000-0000-000000000003";
        await sql`
          INSERT INTO object_artifacts (
            id, object_id, kind, storage_key, content_type, size_bytes
          )
          VALUES
            (${materializedArtifactId}, ${objectId}, ${"original"}::artifact_kind,
             ${"legacy/materialized"}, ${"application/pdf"}, 105),
            (${nullChecksumArtifactId}, ${objectId}, ${"original"}::artifact_kind,
             ${"legacy/completed-null"}, ${"image/tiff"}, 106),
            (${crossObjectArtifactId}, ${otherObjectId}, ${"original"}::artifact_kind,
             ${"legacy/cross-object"}, ${"audio/wav"}, 107)
        `;

        const future = new Date(Date.now() + 60 * 60 * 1000);
        const past = new Date(Date.now() - 60 * 60 * 1000);
        const completedAt = new Date(Date.now() - 10 * 60 * 1000);
        const cases = [
          { id: ids.active, status: "PROCESSING", checksum: null, expiresAt: future, key: "legacy/active", contentType: "video/mp4", size: 101 },
          { id: ids.expiredPending, status: "PENDING", checksum: null, expiresAt: past, key: "legacy/expired", contentType: "text/plain", size: 102 },
          { id: ids.processingVerified, status: "PROCESSING", checksum, expiresAt: future, key: "legacy/processing-verified", contentType: "application/zip", size: 103 },
          { id: ids.pendingVerified, status: "PENDING", checksum: otherChecksum, expiresAt: future, key: "legacy/pending-verified", contentType: "application/json", size: 104 },
          { id: ids.materialized, status: "COMPLETED", checksum, expiresAt: null, key: "legacy/materialized", contentType: "application/pdf", size: 105 },
          { id: ids.completedNullChecksum, status: "COMPLETED", checksum: null, expiresAt: null, key: "legacy/completed-null", contentType: "image/tiff", size: 106 },
          { id: ids.crossObject, status: "COMPLETED", checksum, expiresAt: null, key: "legacy/cross-object", contentType: "audio/wav", size: 107 },
          { id: ids.failedChecksum, status: "FAILED", checksum, expiresAt: null, key: "legacy/failed", contentType: "application/octet-stream", size: 108 },
          { id: ids.canceledNullChecksum, status: "CANCELED", checksum: null, expiresAt: null, key: "legacy/canceled", contentType: "application/octet-stream", size: 109 },
        ] as const;

        for (const [index, legacyCase] of cases.entries()) {
          await sql`
            INSERT INTO archive_requests (
              id, tenant_id, target_type, target_id, action_type, requested_by,
              status, lease_id, lease_token_id, lease_expires_at, leased_by,
              completed_at, artifact_upload_token_id, artifact_upload_storage_key,
              artifact_upload_content_type, artifact_upload_size_bytes,
              artifact_upload_checksum_sha256
            )
            VALUES (
              ${legacyCase.id}, ${tenantId}, ${"object"}::archive_request_target_type,
              ${objectId}, ${"artifact_fetch"}::archive_request_action_type, ${requestedBy},
              ${legacyCase.status}::archive_request_status,
              ${`40000000-0000-0000-0000-${String(index + 1).padStart(12, "0")}`},
              ${`50000000-0000-0000-0000-${String(index + 1).padStart(12, "0")}`},
              ${legacyCase.expiresAt}, ${"legacy-worker"},
              ${legacyCase.status === "COMPLETED" ? completedAt : null},
              ${`60000000-0000-0000-0000-${String(index + 1).padStart(12, "0")}`},
              ${legacyCase.key}, ${legacyCase.contentType}, ${legacyCase.size},
              ${legacyCase.checksum}
            )
          `;
        }

        await copyFile(
          join(migrationsSource, "0014_archive_artifact_upload_attempts.sql"),
          join(migrationsDir, "0014_archive_artifact_upload_attempts.sql"),
        );
        const upgradeRun = await runMigrations({
          databaseUrl: TEST_DATABASE_URL,
          schema,
          migrationsDir,
        });
        expect(upgradeRun.applied).toEqual([
          "0014_archive_artifact_upload_attempts.sql",
        ]);

        const attempts = await sql<AttemptRow[]>`
          SELECT request_id, state, storage_key, content_type,
                 size_bytes::int AS size_bytes, computed_sha256, verified_at,
                 artifact_id, materialized_at, invalidated_at
          FROM archive_artifact_upload_attempts
          ORDER BY request_id
        `;
        const attemptsByRequest = new Map(
          attempts.map((attempt) => [attempt.request_id, attempt]),
        );

        expect(attempts).toHaveLength(7);
        expect(attemptsByRequest.get(ids.active)).toMatchObject({
          state: "AUTHORIZED",
          computed_sha256: null,
          invalidated_at: null,
        });
        expect(attemptsByRequest.get(ids.expiredPending)).toMatchObject({
          state: "AUTHORIZED",
          computed_sha256: null,
        });
        expect(attemptsByRequest.get(ids.expiredPending)?.invalidated_at).toBeInstanceOf(Date);

        expect(attemptsByRequest.get(ids.processingVerified)).toMatchObject({
          state: "VERIFIED",
          storage_key: "legacy/processing-verified",
          content_type: "application/zip",
          size_bytes: 103,
          computed_sha256: checksum,
          artifact_id: null,
          invalidated_at: null,
        });
        expect(attemptsByRequest.get(ids.processingVerified)?.verified_at).toBeInstanceOf(Date);
        expect(attemptsByRequest.get(ids.pendingVerified)).toMatchObject({
          state: "VERIFIED",
          storage_key: "legacy/pending-verified",
          content_type: "application/json",
          size_bytes: 104,
          computed_sha256: otherChecksum,
          artifact_id: null,
          invalidated_at: null,
        });

        expect(attemptsByRequest.get(ids.materialized)).toMatchObject({
          state: "MATERIALIZED",
          storage_key: "legacy/materialized",
          content_type: "application/pdf",
          size_bytes: 105,
          computed_sha256: checksum,
          artifact_id: materializedArtifactId,
          invalidated_at: null,
        });
        expect(attemptsByRequest.get(ids.materialized)?.materialized_at).toEqual(completedAt);

        expect(attemptsByRequest.get(ids.completedNullChecksum)).toMatchObject({
          state: "AUTHORIZED",
          computed_sha256: null,
          artifact_id: null,
          materialized_at: null,
        });
        expect(attemptsByRequest.get(ids.completedNullChecksum)?.invalidated_at).toBeInstanceOf(Date);

        expect(attemptsByRequest.has(ids.crossObject)).toBe(false);
        expect(attemptsByRequest.has(ids.failedChecksum)).toBe(false);
        expect(attemptsByRequest.get(ids.canceledNullChecksum)).toMatchObject({
          state: "AUTHORIZED",
          computed_sha256: null,
          artifact_id: null,
        });
        expect(attemptsByRequest.get(ids.canceledNullChecksum)?.invalidated_at).toBeInstanceOf(Date);

        const requestRows = await sql<RequestRow[]>`
          SELECT id, status::text AS status, lease_expires_at, released_at
          FROM archive_requests
          WHERE id IN (${ids.processingVerified}, ${ids.pendingVerified})
          ORDER BY id
        `;
        expect(requestRows).toHaveLength(2);
        for (const request of requestRows) {
          expect(request.status).toBe("PROCESSING");
          expect(request.lease_expires_at).toBeNull();
          expect(request.released_at).toBeInstanceOf(Date);
        }

        const rerun = await runMigrations({
          databaseUrl: TEST_DATABASE_URL,
          schema,
          migrationsDir,
        });
        expect(rerun.applied).toEqual([]);
        expect(rerun.skipped).toHaveLength(legacyMigrationNames.length + 1);
        const countRows = await sql<Array<{ count: number }>>`
          SELECT COUNT(*)::int AS count FROM archive_artifact_upload_attempts
        `;
        expect(countRows[0]?.count).toBe(7);
      } finally {
        await sql`DROP SCHEMA IF EXISTS ${sqlIdentifier(schema)} CASCADE`;
        await sql.close();
        await rm(migrationsDir, { recursive: true, force: true });
      }
    },
    30_000,
  );
});
