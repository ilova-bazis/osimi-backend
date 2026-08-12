import { mkdir, mkdtemp, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import { afterAll, beforeAll, beforeEach, describe, expect, test } from "bun:test";
import { sql as sqlIdentifier } from "bun";

import { createAppWithOptions as createApp } from "../../../src/app.ts";
import { createArchiveRequestLeaseToken } from "../../../src/auth/worker-archive-request.ts";
import { createSqlClient } from "../../../src/db/client.ts";
import { runMigrations } from "../../../src/db/migrate.ts";
import { closeDatabaseClients } from "../../../src/db/runtime.ts";
import { runArtifactFinalizationSweep } from "../../../src/jobs/operations.ts";
import { runWithRuntimeConfig } from "../../../src/runtime/config.ts";
import { TEST_DATABASE_URL } from "../test-database.ts";

const tenantId = "00000000-0000-4000-8000-000000000901";
const objectId = "OBJ-20260806-CHECKSUM1";
const availableFileId = "00000000-0000-4000-8000-000000000902";
const requestId = "10000000-0000-4000-8000-000000000901";
const requestedBy = "00000000-0000-4000-8000-000000000903";
const leaseId = "20000000-0000-4000-8000-000000000901";
const leaseTokenId = "30000000-0000-4000-8000-000000000901";
const uploadSigningSecret = "artifact-checksum-upload-signing-secret-000";
const leaseSigningSecret = "artifact-checksum-lease-signing-secret-0000";
const correctBytes = "hello world";
const rejectedBytes = "goodbye all";
const finalizationBarrierKey = 530053;

function sha256Hex(value: string): string {
  return new Bun.CryptoHasher("sha256").update(value).digest("hex");
}

describe("archive artifact checksum upload routes", () => {
  let schema = "";
  let stagingRoot = "";

  function createTestApp() {
    return createApp({
      runtimeConfig: {
        databaseUrl: TEST_DATABASE_URL,
        dbSchema: schema,
        stagingRoot,
        workerAuthToken: "worker-secret",
        uploadSigningSecret,
        leaseSigningSecret,
      },
    });
  }

  function createLeaseToken(): string {
    return runWithRuntimeConfig(
      { leaseSigningSecret },
      () => createArchiveRequestLeaseToken({
        request_id: requestId,
        lease_id: leaseId,
        lease_token_id: leaseTokenId,
        tenant_id: tenantId,
        target_type: "object",
        target_id: objectId,
        action_type: "artifact_fetch",
        worker_id: "checksum-worker",
        exp: new Date(Date.now() + 60_000).toISOString(),
      }),
    );
  }

  function presignRequest(
    app: ReturnType<typeof createTestApp>,
    contentType = "text/plain",
  ): Promise<Response> {
    return app.fetch(
      new Request(`http://localhost/api/archive-requests/${requestId}/artifacts/presign`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-worker-auth-token": "worker-secret",
        },
        body: JSON.stringify({
          lease_token: createLeaseToken(),
          content_type: contentType,
          size_bytes: Buffer.byteLength(correctBytes),
          extension: "txt",
        }),
      }),
    );
  }

  async function presignUpload(
    app: ReturnType<typeof createTestApp>,
    contentType = "text/plain",
  ): Promise<{
    upload_token: string;
    upload_url: string;
    storage_key: string;
    headers: { "content-type": string; "content-length": number };
  }> {
    const response = await presignRequest(app, contentType);
    expect(response.status).toBe(200);
    return await response.json() as {
      upload_token: string;
      upload_url: string;
      storage_key: string;
      headers: { "content-type": string; "content-length": number };
    };
  }

  function uploadRequest(
    uploadUrl: string,
    bytes: string,
    contentType = "text/plain",
  ): Request {
    return new Request(`http://localhost${uploadUrl}`, {
      method: "PUT",
      headers: {
        "content-type": contentType,
        "content-length": String(Buffer.byteLength(bytes)),
      },
      body: bytes,
    });
  }

  async function temporaryUploads(storageKey: string): Promise<string[]> {
    try {
      const entries = await readdir(dirname(join(stagingRoot, storageKey)));
      return entries.filter((entry) => entry.includes(".upload-") && entry.endsWith(".tmp"));
    } catch (error) {
      if (error instanceof Error && "code" in error && error.code === "ENOENT") return [];
      throw error;
    }
  }

  async function waitForTemporaryUploads(storageKey: string, count: number): Promise<void> {
    for (let attempt = 0; attempt < 200; attempt += 1) {
      if ((await temporaryUploads(storageKey)).length === count) return;
      await Bun.sleep(5);
    }
    throw new Error(`Timed out waiting for ${count} staged artifact uploads.`);
  }

  async function dropFixtureTriggers(): Promise<void> {
    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`DROP TRIGGER IF EXISTS reject_fixture_projection ON object_artifact_search_documents`;
      await sql`DROP FUNCTION IF EXISTS reject_fixture_projection()`;
      await sql`DROP TRIGGER IF EXISTS pause_fixture_finalization_claim ON archive_artifact_upload_attempts`;
      await sql`DROP FUNCTION IF EXISTS pause_fixture_finalization_claim()`;
    } finally {
      await sql.close();
    }
  }

  async function installProjectionFailure(): Promise<void> {
    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`
        CREATE FUNCTION reject_fixture_projection()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
          RAISE EXCEPTION 'forced fixture projection failure';
        END;
        $$
      `;
      await sql`
        CREATE TRIGGER reject_fixture_projection
        BEFORE INSERT ON object_artifact_search_documents
        FOR EACH ROW EXECUTE FUNCTION reject_fixture_projection()
      `;
    } finally {
      await sql.close();
    }
  }

  async function runPutWithExpectedDeferredFinalization(
    uploadUrl: string,
  ): Promise<void> {
    const config = JSON.stringify({
      databaseUrl: TEST_DATABASE_URL,
      dbSchema: schema,
      stagingRoot,
      workerAuthToken: "worker-secret",
      uploadSigningSecret,
      leaseSigningSecret,
    });
    const script = `
      import { createAppWithOptions } from "./src/app.ts";

      const app = createAppWithOptions({ runtimeConfig: ${config} });
      const response = await app.fetch(new Request(${JSON.stringify(`http://localhost${uploadUrl}`)}, {
        method: "PUT",
        headers: {
          "content-type": "text/plain",
          "content-length": "${Buffer.byteLength(correctBytes)}",
        },
        body: ${JSON.stringify(correctBytes)},
      }));
      await response.json();
      process.exit(response.status === 200 ? 0 : 2);
    `;
    const child = Bun.spawn([process.execPath, "-e", script], {
      cwd: process.cwd(),
      stdout: "pipe",
      stderr: "pipe",
    });
    const [exitCode, stderr] = await Promise.all([
      child.exited,
      new Response(child.stderr).text(),
    ]);
    expect(exitCode).toBe(0);
    expect(stderr).toContain("forced fixture projection failure");
  }

  async function installFinalizationClaimBarrier(): Promise<void> {
    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`
        CREATE FUNCTION pause_fixture_finalization_claim()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
          PERFORM pg_advisory_xact_lock(530053);
          RETURN NEW;
        END;
        $$
      `;
      await sql`
        CREATE TRIGGER pause_fixture_finalization_claim
        AFTER UPDATE OF finalization_claim_token ON archive_artifact_upload_attempts
        FOR EACH ROW
        WHEN (NEW.finalization_claim_token IS NOT NULL AND
              OLD.finalization_claim_token IS DISTINCT FROM NEW.finalization_claim_token)
        EXECUTE FUNCTION pause_fixture_finalization_claim()
      `;
    } finally {
      await sql.close();
    }
  }

  async function waitForBlockedFinalizationQueries(count: number): Promise<void> {
    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      for (let attempt = 0; attempt < 200; attempt += 1) {
        const rows = await sql<Array<{ blocked_count: number }>>`
          SELECT COUNT(*)::int AS blocked_count
          FROM pg_stat_activity
          WHERE cardinality(pg_blocking_pids(pid)) > 0
            AND query LIKE '%archive_artifact_upload_attempts%'
        `;
        if ((rows[0]?.blocked_count ?? 0) >= count) return;
        await Bun.sleep(5);
      }
      throw new Error(`Timed out waiting for ${count} blocked finalization queries.`);
    } finally {
      await sql.close();
    }
  }

  async function readPersistedOutcome() {
    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      const rows = await sql<Array<{
        request_status: string;
        attempt_state: string;
        attempt_count: number;
        checkpoint_count: number;
        artifact_count: number;
        projection_count: number;
        completed_count: number;
      }>>`
        SELECT req.status AS request_status, attempt.state AS attempt_state,
               attempt.finalization_attempt_count AS attempt_count,
               (SELECT COUNT(*)::int FROM archive_artifact_upload_attempts
                WHERE request_id = ${requestId} AND state IN ('VERIFIED', 'MATERIALIZED')) AS checkpoint_count,
               (SELECT COUNT(*)::int FROM object_artifacts) AS artifact_count,
               (SELECT COUNT(*)::int FROM object_artifact_search_documents) AS projection_count,
               (SELECT COUNT(*)::int FROM archive_requests
                WHERE id = ${requestId} AND status = 'COMPLETED' AND completed_at IS NOT NULL) AS completed_count
        FROM archive_requests req
        INNER JOIN archive_artifact_upload_attempts attempt ON attempt.request_id = req.id
        WHERE req.id = ${requestId}
      `;
      return rows[0]!;
    } finally {
      await sql.close();
    }
  }

  function expectExactlyOnceCompleted(outcome: Awaited<ReturnType<typeof readPersistedOutcome>>): void {
    expect(outcome).toMatchObject({
      request_status: "COMPLETED",
      attempt_state: "MATERIALIZED",
      checkpoint_count: 1,
      artifact_count: 1,
      projection_count: 1,
      completed_count: 1,
    });
  }

  beforeAll(async () => {
    schema = `artifact_checksum_${Date.now()}_${Math.floor(Math.random() * 100000)}`;
    stagingRoot = await mkdtemp(join(tmpdir(), "osimi-artifact-checksum-"));
    await runMigrations({ databaseUrl: TEST_DATABASE_URL, schema });
  });

  beforeEach(async () => {
    await dropFixtureTriggers();
    await rm(stagingRoot, { recursive: true, force: true });
    await mkdir(stagingRoot, { recursive: true });
    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`
        TRUNCATE archive_artifact_upload_attempts, archive_requests,
                 object_artifact_search_documents, object_available_files,
                 object_artifacts, objects, tenants CASCADE
      `;
      await sql`
        INSERT INTO tenants (id, slug, name)
        VALUES (${tenantId}, ${"artifact-checksum"}, ${"Artifact Checksum"})
      `;
      await sql`
        INSERT INTO objects (object_id, tenant_id, title)
        VALUES (${objectId}, ${tenantId}, ${"Checksum upload object"})
      `;
      await sql`
        INSERT INTO object_available_files (
          id, object_id, tenant_id, archive_file_key, artifact_kind,
          variant, display_name, content_type, size_bytes, checksum_sha256,
          is_available
        )
        VALUES (
          ${availableFileId}, ${objectId}, ${tenantId}, ${"archive/checksum.txt"},
          ${"ocr_text"}::artifact_kind, ${"checksum_v1"}, ${"Checksum OCR"},
          ${"text/plain"}, ${Buffer.byteLength(correctBytes)},
          ${sha256Hex(correctBytes)}, true
        )
      `;
      await sql`
        INSERT INTO archive_requests (
          id, tenant_id, target_type, target_id, action_type, action_payload,
          requested_by, status, lease_id, lease_token_id, lease_expires_at,
          leased_by
        )
        VALUES (
          ${requestId}, ${tenantId}, ${"object"}::archive_request_target_type,
          ${objectId}, ${"artifact_fetch"}::archive_request_action_type,
          ${{
            available_file_id: availableFileId,
            artifact_kind: "ocr_text",
            variant: "checksum_v1",
          }},
          ${requestedBy}, ${"PROCESSING"}::archive_request_status, ${leaseId},
          ${leaseTokenId}, ${new Date(Date.now() + 60_000)}, ${"checksum-worker"}
        )
      `;
    } finally {
      await sql.close();
    }
  });

  afterAll(async () => {
    await dropFixtureTriggers();
    await closeDatabaseClients({ timeoutMs: 1_000 });
    if (schema) {
      const sql = createSqlClient(TEST_DATABASE_URL);
      try {
        await sql`DROP SCHEMA IF EXISTS ${sqlIdentifier(schema)} CASCADE`;
      } finally {
        await sql.close();
      }
    }
    if (stagingRoot) await rm(stagingRoot, { recursive: true, force: true });
  });

  test("preserves an inactive source checksum and accepts a corrected same-URL retry", async () => {
    const app = createTestApp();
    const inactiveSql = createSqlClient(TEST_DATABASE_URL);
    try {
      await inactiveSql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await inactiveSql`
        UPDATE object_available_files
        SET is_available = false
        WHERE id = ${availableFileId}
      `;
    } finally {
      await inactiveSql.close();
    }
    const presigned = await presignUpload(app);
    const destinationPath = join(stagingRoot, presigned.storage_key);

    const rejected = await app.fetch(uploadRequest(presigned.upload_url, rejectedBytes));
    expect(rejected.status).toBe(409);
    expect(await rejected.json()).toMatchObject({
      error: {
        code: "CONFLICT",
        message: "Uploaded artifact checksum does not match the expected source checksum.",
        details: {
          reason: "expected_checksum_mismatch",
          expected_checksum_sha256: sha256Hex(correctBytes),
          actual_checksum_sha256: sha256Hex(rejectedBytes),
          retry_action: "retry_same_upload_url",
        },
      },
    });
    expect(await Bun.file(destinationPath).exists()).toBe(false);
    expect(await temporaryUploads(presigned.storage_key)).toEqual([]);

    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      const state = await sql<Array<{
        request_status: string;
        released_at: Date | null;
        state: string;
        expected_sha256: string | null;
        computed_sha256: string | null;
        verified_at: Date | null;
        artifact_id: string | null;
        invalidated_at: Date | null;
      }>>`
        SELECT req.status AS request_status, req.released_at, attempt.state,
               attempt.expected_sha256, attempt.computed_sha256,
               attempt.verified_at, attempt.artifact_id, attempt.invalidated_at
        FROM archive_requests req
        INNER JOIN archive_artifact_upload_attempts attempt
          ON attempt.request_id = req.id
        WHERE req.id = ${requestId}
      `;
      expect(state).toEqual([{
        request_status: "PROCESSING",
        released_at: null,
        state: "AUTHORIZED",
        expected_sha256: sha256Hex(correctBytes),
        computed_sha256: null,
        verified_at: null,
        artifact_id: null,
        invalidated_at: null,
      }]);
      expect(await sql<Array<{ id: string }>>`SELECT id FROM object_artifacts`).toEqual([]);
      expect(
        await sql<Array<{ artifact_id: string }>>`
          SELECT artifact_id FROM object_artifact_search_documents
        `,
      ).toEqual([]);
    } finally {
      await sql.close();
    }

    const prematureCompletion = await app.fetch(
      new Request(`http://localhost/api/archive-requests/${requestId}/complete`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-worker-auth-token": "worker-secret",
        },
        body: JSON.stringify({
          lease_token: createLeaseToken(),
          upload_token: presigned.upload_token,
        }),
      }),
    );
    expect(prematureCompletion.status).toBe(409);

    const corrected = await app.fetch(uploadRequest(presigned.upload_url, correctBytes));
    expect(corrected.status).toBe(200);
    expect(await Bun.file(destinationPath).text()).toBe(correctBytes);
    expect(await temporaryUploads(presigned.storage_key)).toEqual([]);

    const rejectedAfterAcceptance = await app.fetch(
      uploadRequest(presigned.upload_url, rejectedBytes),
    );
    expect(rejectedAfterAcceptance.status).toBe(409);
    expect(await rejectedAfterAcceptance.json()).toMatchObject({
      error: {
        details: {
          reason: "accepted_checkpoint_mismatch",
          retry_action: "retry_exact_accepted_bytes_only",
        },
      },
    });
    expect(await Bun.file(destinationPath).text()).toBe(correctBytes);
    expect(await temporaryUploads(presigned.storage_key)).toEqual([]);

    await Bun.write(destinationPath, rejectedBytes);
    const storageConflict = await app.fetch(
      uploadRequest(presigned.upload_url, correctBytes),
    );
    expect(storageConflict.status).toBe(409);
    expect(await storageConflict.json()).toMatchObject({
      error: {
        message: "Immutable artifact storage does not match the accepted checkpoint.",
        details: {
          reason: "accepted_checkpoint_storage_conflict",
          retry_action: "operator_repair_required",
        },
      },
    });
    await Bun.write(destinationPath, correctBytes);

    const verifiedSql = createSqlClient(TEST_DATABASE_URL);
    try {
      await verifiedSql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      const completed = await verifiedSql<Array<{
        request_status: string;
        attempt_state: string;
        computed_sha256: string;
        text_content: string;
        artifact_count: number;
        source_is_available: boolean;
      }>>`
        SELECT req.status AS request_status, attempt.state AS attempt_state,
               attempt.computed_sha256, doc.text_content,
               (SELECT COUNT(*)::int FROM object_artifacts) AS artifact_count,
               source.is_available AS source_is_available
        FROM archive_requests req
        INNER JOIN archive_artifact_upload_attempts attempt
          ON attempt.request_id = req.id
        INNER JOIN object_artifact_search_documents doc
          ON doc.artifact_id = attempt.artifact_id
        INNER JOIN object_available_files source
          ON source.id = doc.available_file_id
        WHERE req.id = ${requestId}
      `;
      expect(completed).toEqual([{
        request_status: "COMPLETED",
        attempt_state: "MATERIALIZED",
        computed_sha256: sha256Hex(correctBytes),
        text_content: correctBytes,
        artifact_count: 1,
        source_is_available: false,
      }]);
    } finally {
      await verifiedSql.close();
    }
  });

  test("rejects changed source identity before authorizing an upload", async () => {
    const app = createTestApp();
    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`
        UPDATE object_available_files
        SET variant = ${"changed_v2"}
        WHERE id = ${availableFileId}
      `;
    } finally {
      await sql.close();
    }

    const changed = await presignRequest(app);
    expect(changed.status).toBe(409);
    expect(await changed.json()).toMatchObject({
      error: {
        code: "CONFLICT",
        details: { reason: "artifact_source_identity_changed" },
      },
    });

    const stateSql = createSqlClient(TEST_DATABASE_URL);
    try {
      await stateSql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      expect(
        await stateSql<Array<{ upload_token_id: string }>>`
          SELECT upload_token_id FROM archive_artifact_upload_attempts
        `,
      ).toEqual([]);
    } finally {
      await stateSql.close();
    }
  });

  test("rejects a missing queued source before authorizing an upload", async () => {
    const app = createTestApp();
    const missingAvailableFileId = "00000000-0000-4000-8000-000000000999";
    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`
        UPDATE archive_requests
        SET action_payload = ${{
          available_file_id: missingAvailableFileId,
          artifact_kind: "ocr_text",
          variant: "checksum_v1",
        }}
        WHERE id = ${requestId}
      `;
    } finally {
      await sql.close();
    }

    const missing = await presignRequest(app);
    expect(missing.status).toBe(409);
    expect(await missing.json()).toMatchObject({
      error: {
        code: "CONFLICT",
        details: { reason: "artifact_source_missing" },
      },
    });

    const stateSql = createSqlClient(TEST_DATABASE_URL);
    try {
      await stateSql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      expect(
        await stateSql<Array<{ upload_token_id: string }>>`
          SELECT upload_token_id FROM archive_artifact_upload_attempts
        `,
      ).toEqual([]);
    } finally {
      await stateSql.close();
    }
  });

  test("keeps the presigned source checksum immutable when the source changes", async () => {
    const app = createTestApp();
    const presigned = await presignUpload(app);
    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`
        UPDATE object_available_files
        SET checksum_sha256 = ${sha256Hex(rejectedBytes)},
            is_available = false
        WHERE id = ${availableFileId}
      `;
    } finally {
      await sql.close();
    }

    const accepted = await app.fetch(uploadRequest(presigned.upload_url, correctBytes));
    expect(accepted.status).toBe(200);

    const stateSql = createSqlClient(TEST_DATABASE_URL);
    try {
      await stateSql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      const attempts = await stateSql<Array<{
        expected_sha256: string;
        computed_sha256: string;
        state: string;
      }>>`
        SELECT expected_sha256, computed_sha256, state
        FROM archive_artifact_upload_attempts
        WHERE request_id = ${requestId}
      `;
      expect(attempts).toEqual([{
        expected_sha256: sha256Hex(correctBytes),
        computed_sha256: sha256Hex(correctBytes),
        state: "MATERIALIZED",
      }]);
    } finally {
      await stateSql.close();
    }
  });

  test("rejects a malformed historical checksum without invalidating a prior attempt", async () => {
    const app = createTestApp();
    await presignUpload(app);
    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`
        UPDATE object_available_files
        SET checksum_sha256 = ${"legacy-invalid-checksum"}
        WHERE id = ${availableFileId}
      `;
    } finally {
      await sql.close();
    }

    const rejected = await presignRequest(app);
    expect(rejected.status).toBe(409);
    expect(await rejected.json()).toMatchObject({
      error: {
        code: "CONFLICT",
        details: { reason: "artifact_source_checksum_invalid" },
      },
    });

    const stateSql = createSqlClient(TEST_DATABASE_URL);
    try {
      await stateSql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      const attempts = await stateSql<Array<{
        expected_sha256: string;
        state: string;
        invalidated_at: Date | null;
      }>>`
        SELECT expected_sha256, state, invalidated_at
        FROM archive_artifact_upload_attempts
        WHERE request_id = ${requestId}
      `;
      expect(attempts).toEqual([{
        expected_sha256: sha256Hex(correctBytes),
        state: "AUTHORIZED",
        invalidated_at: null,
      }]);
    } finally {
      await stateSql.close();
    }
  });

  test("accepts parameterized signed content types by case-insensitive base type", async () => {
    const app = createTestApp();
    const malformed = await presignRequest(app, "text/plain; charset");
    expect(malformed.status).toBe(400);

    const declaredContentType = 'Text/Plain; charset="utf-8"';
    const presigned = await presignUpload(app, declaredContentType);
    expect(presigned.headers).toEqual({
      "content-type": declaredContentType,
      "content-length": Buffer.byteLength(correctBytes),
    });

    const mismatched = await app.fetch(
      uploadRequest(presigned.upload_url, correctBytes, "application/json; charset=utf-8"),
    );
    expect(mismatched.status).toBe(400);
    expect(await temporaryUploads(presigned.storage_key)).toEqual([]);

    const accepted = await app.fetch(
      uploadRequest(presigned.upload_url, correctBytes, "text/plain; charset=us-ascii"),
    );
    expect(accepted.status).toBe(200);

    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      const persisted = await sql<Array<{
        attempt_content_type: string;
        artifact_content_type: string;
        text_content: string;
      }>>`
        SELECT attempt.content_type AS attempt_content_type,
               artifact.content_type AS artifact_content_type,
               document.text_content
        FROM archive_artifact_upload_attempts attempt
        INNER JOIN object_artifacts artifact ON artifact.id = attempt.artifact_id
        INNER JOIN object_artifact_search_documents document
          ON document.artifact_id = artifact.id
        WHERE attempt.request_id = ${requestId}
      `;
      expect(persisted).toEqual([{
        attempt_content_type: declaredContentType,
        artifact_content_type: declaredContentType,
        text_content: correctBytes,
      }]);
    } finally {
      await sql.close();
    }
  });

  test("does not advertise same-URL correction after the lease expires", async () => {
    const app = createTestApp();
    const presigned = await presignUpload(app);
    const destinationPath = join(stagingRoot, presigned.storage_key);
    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`
        UPDATE archive_requests
        SET lease_expires_at = now() - interval '1 second'
        WHERE id = ${requestId}
      `;
    } finally {
      await sql.close();
    }

    const request = uploadRequest(presigned.upload_url, rejectedBytes);
    const response = await app.fetch(request);
    expect(response.status).toBe(409);
    expect(await response.json()).toMatchObject({
      error: {
        code: "CONFLICT",
        message: "Upload token is no longer active.",
      },
    });
    expect(request.bodyUsed).toBe(false);
    expect(await Bun.file(destinationPath).exists()).toBe(false);
    expect(await temporaryUploads(presigned.storage_key)).toEqual([]);
  });

  test("rejects a mismatched persisted lease without staging upload bytes", async () => {
    const app = createTestApp();
    const presigned = await presignUpload(app);
    const destinationPath = join(stagingRoot, presigned.storage_key);
    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`
        UPDATE archive_requests
        SET lease_id = ${"20000000-0000-4000-8000-000000000999"},
            lease_token_id = ${"30000000-0000-4000-8000-000000000999"}
        WHERE id = ${requestId}
      `;
    } finally {
      await sql.close();
    }

    const request = uploadRequest(presigned.upload_url, rejectedBytes);
    const response = await app.fetch(request);
    expect(response.status).toBe(409);
    expect(await response.json()).toMatchObject({
      error: {
        code: "CONFLICT",
        message: "Upload token is no longer active.",
      },
    });
    expect(request.bodyUsed).toBe(false);
    expect(await Bun.file(destinationPath).exists()).toBe(false);
    expect(await temporaryUploads(presigned.storage_key)).toEqual([]);
  });

  test("preserves the correct winner during concurrent bad and corrected uploads", async () => {
    const app = createTestApp();
    const presigned = await presignUpload(app);
    const destinationPath = join(stagingRoot, presigned.storage_key);
    const locker = createSqlClient(TEST_DATABASE_URL);
    await locker`SET search_path TO ${sqlIdentifier(schema)}, public`;

    let lockAcquired!: () => void;
    let releaseLock!: () => void;
    const acquired = new Promise<void>((resolve) => {
      lockAcquired = resolve;
    });
    const release = new Promise<void>((resolve) => {
      releaseLock = resolve;
    });
    const lockTransaction = locker.begin(async (transaction) => {
      await transaction`SELECT id FROM archive_requests WHERE id = ${requestId} FOR UPDATE`;
      lockAcquired();
      await release;
    });

    await acquired;
    const rejectedPromise = app.fetch(uploadRequest(presigned.upload_url, rejectedBytes));
    const correctedPromise = app.fetch(uploadRequest(presigned.upload_url, correctBytes));
    try {
      await waitForTemporaryUploads(presigned.storage_key, 2);
    } finally {
      releaseLock();
      await lockTransaction;
      await locker.close();
    }

    const [rejected, corrected] = await Promise.all([rejectedPromise, correctedPromise]);
    expect(rejected.status).toBe(409);
    expect(corrected.status).toBe(200);
    const rejectedBody = await rejected.json() as {
      error: { details?: { reason?: string; retry_action?: string } };
    };
    expect([
      "expected_checksum_mismatch",
      "accepted_checkpoint_mismatch",
    ].includes(rejectedBody.error.details?.reason ?? "")).toBe(true);
    expect([
      "retry_same_upload_url",
      "retry_exact_accepted_bytes_only",
    ].includes(rejectedBody.error.details?.retry_action ?? "")).toBe(true);
    expect(await Bun.file(destinationPath).text()).toBe(correctBytes);
    expect(await temporaryUploads(presigned.storage_key)).toEqual([]);

    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      const state = await sql<Array<{
        request_status: string;
        attempt_state: string;
        computed_sha256: string;
        artifact_count: number;
        projection_count: number;
        text_content: string;
      }>>`
        SELECT req.status AS request_status, attempt.state AS attempt_state,
               attempt.computed_sha256,
               (SELECT COUNT(*)::int FROM object_artifacts) AS artifact_count,
               (SELECT COUNT(*)::int FROM object_artifact_search_documents) AS projection_count,
               doc.text_content
        FROM archive_requests req
        INNER JOIN archive_artifact_upload_attempts attempt
          ON attempt.request_id = req.id
        INNER JOIN object_artifact_search_documents doc
          ON doc.artifact_id = attempt.artifact_id
        WHERE req.id = ${requestId}
      `;
      expect(state).toEqual([{
        request_status: "COMPLETED",
        attempt_state: "MATERIALIZED",
        computed_sha256: sha256Hex(correctBytes),
        artifact_count: 1,
        projection_count: 1,
        text_content: correctBytes,
      }]);
    } finally {
      await sql.close();
    }
  });

  test("returns idempotent success for two simultaneous identical PUTs", async () => {
    const firstApp = createTestApp();
    const secondApp = createTestApp();
    const presigned = await presignUpload(firstApp);
    const locker = createSqlClient(TEST_DATABASE_URL);
    await locker`SET search_path TO ${sqlIdentifier(schema)}, public`;
    let releaseLock!: () => void;
    let lockAcquired!: () => void;
    const release = new Promise<void>((resolve) => {
      releaseLock = resolve;
    });
    const acquired = new Promise<void>((resolve) => {
      lockAcquired = resolve;
    });
    const lockTransaction = locker.begin(async (transaction) => {
      await transaction`SELECT id FROM archive_requests WHERE id = ${requestId} FOR UPDATE`;
      lockAcquired();
      await release;
    });

    await acquired;
    const firstPut = firstApp.fetch(uploadRequest(presigned.upload_url, correctBytes));
    const secondPut = secondApp.fetch(uploadRequest(presigned.upload_url, correctBytes));
    try {
      await waitForTemporaryUploads(presigned.storage_key, 2);
    } finally {
      releaseLock();
      await lockTransaction;
      await locker.close();
    }

    const [first, second] = await Promise.all([firstPut, secondPut]);
    expect(first.status).toBe(200);
    expect(second.status).toBe(200);
    expect(await first.json()).toEqual({
      status: "ok",
      request_id: requestId,
      size_bytes: Buffer.byteLength(correctBytes),
    });
    expect(await second.json()).toEqual({
      status: "ok",
      request_id: requestId,
      size_bytes: Buffer.byteLength(correctBytes),
    });
    expect(await temporaryUploads(presigned.storage_key)).toEqual([]);
    expectExactlyOnceCompleted(await readPersistedOutcome());
  });

  test("races synchronous PUT finalization with the background sweep", async () => {
    const app = createTestApp();
    const presigned = await presignUpload(app);
    await installFinalizationClaimBarrier();
    const barrier = createSqlClient(TEST_DATABASE_URL);
    await barrier`SELECT pg_advisory_lock(${finalizationBarrierKey})`;

    const put = app.fetch(uploadRequest(presigned.upload_url, correctBytes));
    await waitForBlockedFinalizationQueries(1);
    const sweep = runWithRuntimeConfig(
      { databaseUrl: TEST_DATABASE_URL, dbSchema: schema, stagingRoot },
      () => runArtifactFinalizationSweep({ batchSize: 1, claimTimeoutSeconds: 30 }),
    );
    let sweepResult;
    try {
      sweepResult = await sweep;
    } finally {
      await barrier`SELECT pg_advisory_unlock(${finalizationBarrierKey})`;
      await barrier.close();
    }

    expect((await put).status).toBe(200);
    expect(sweepResult).toEqual({ claimed: 0, completed: 0, failed: 0 });
    expectExactlyOnceCompleted(await readPersistedOutcome());
  });

  test("recovers a route-level synchronous finalization failure through a later sweep", async () => {
    const app = createTestApp();
    const presigned = await presignUpload(app);
    await installProjectionFailure();

    await runPutWithExpectedDeferredFinalization(presigned.upload_url);
    const failed = await readPersistedOutcome();
    expect(failed).toMatchObject({
      request_status: "PROCESSING",
      attempt_state: "VERIFIED",
      attempt_count: 1,
      checkpoint_count: 1,
      artifact_count: 0,
      projection_count: 0,
      completed_count: 0,
    });

    await dropFixtureTriggers();
    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`
        UPDATE archive_artifact_upload_attempts
        SET finalization_next_retry_at = now()
        WHERE request_id = ${requestId}
      `;
    } finally {
      await sql.close();
    }
    const sweep = await runWithRuntimeConfig(
      { databaseUrl: TEST_DATABASE_URL, dbSchema: schema, stagingRoot },
      () => runArtifactFinalizationSweep({ batchSize: 1, claimTimeoutSeconds: 30 }),
    );
    expect(sweep).toEqual({ claimed: 1, completed: 1, failed: 0 });
    expectExactlyOnceCompleted(await readPersistedOutcome());
  });

  test("races completion acknowledgement with background reconciliation", async () => {
    const app = createTestApp();
    const presigned = await presignUpload(app);
    await installProjectionFailure();
    await runPutWithExpectedDeferredFinalization(presigned.upload_url);
    await dropFixtureTriggers();
    const retrySql = createSqlClient(TEST_DATABASE_URL);
    try {
      await retrySql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await retrySql`
        UPDATE archive_artifact_upload_attempts
        SET finalization_next_retry_at = NULL
        WHERE request_id = ${requestId}
      `;
    } finally {
      await retrySql.close();
    }

    await installFinalizationClaimBarrier();
    const barrier = createSqlClient(TEST_DATABASE_URL);
    await barrier`SELECT pg_advisory_lock(${finalizationBarrierKey})`;
    const completion = app.fetch(
      new Request(`http://localhost/api/archive-requests/${requestId}/complete`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-worker-auth-token": "worker-secret",
        },
        body: JSON.stringify({
          lease_token: createLeaseToken(),
          upload_token: presigned.upload_token,
        }),
      }),
    );
    await waitForBlockedFinalizationQueries(1);
    const reconciliation = runWithRuntimeConfig(
      { databaseUrl: TEST_DATABASE_URL, dbSchema: schema, stagingRoot },
      () => runArtifactFinalizationSweep({ batchSize: 1, claimTimeoutSeconds: 30 }),
    );
    let reconciliationResult;
    try {
      reconciliationResult = await reconciliation;
    } finally {
      await barrier`SELECT pg_advisory_unlock(${finalizationBarrierKey})`;
      await barrier.close();
    }

    const completionResponse = await completion;
    expect(completionResponse.status).toBe(200);
    expect(await completionResponse.json()).toMatchObject({ status: "completed" });
    expect(reconciliationResult).toEqual({ claimed: 0, completed: 0, failed: 0 });
    expectExactlyOnceCompleted(await readPersistedOutcome());
  });
});
