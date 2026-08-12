import { createHash } from "node:crypto";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import { afterAll, beforeAll, beforeEach, describe, expect, test } from "bun:test";
import { sql as sqlIdentifier } from "bun";

import { createSqlClient } from "../../../src/db/client.ts";
import { runMigrations } from "../../../src/db/migrate.ts";
import { closeDatabaseClients } from "../../../src/db/runtime.ts";
import { runArtifactFinalizationSweep } from "../../../src/jobs/operations.ts";
import {
  claimVerifiedArchiveArtifactUploadAttempt,
  findArchiveArtifactUploadAttemptById,
} from "../../../src/repos/archive-artifact-upload-attempt-repo.ts";
import { runWithRuntimeConfig } from "../../../src/runtime/config.ts";
import {
  finalizeClaimedArchiveArtifactUpload,
  finalizeVerifiedArchiveArtifactUpload,
} from "../../../src/services/archive-artifact-finalization-service.ts";
import { resolveStagingPath } from "../../../src/storage/staging.ts";
import { TEST_DATABASE_URL } from "../test-database.ts";

const tenantId = "00000000-0000-0000-0000-000000000801";
const userId = "00000000-0000-0000-0000-000000000802";
const objectId = "OBJ-20260805-FINALIZE1";
const availableFileId = "00000000-0000-0000-0000-000000000803";
const requestId = "10000000-0000-0000-0000-000000000801";
const leaseId = "20000000-0000-0000-0000-000000000801";
const leaseTokenId = "30000000-0000-0000-0000-000000000801";
const uploadTokenId = "40000000-0000-0000-0000-000000000801";
const storageKey = `tenants/${tenantId}/objects/${objectId}/artifacts/${requestId}-${uploadTokenId}-ocr_text-page_0001.txt`;
const textContent = "Atomic artifact finalization text\n";
const textChecksum = createHash("sha256").update(textContent).digest("hex");

describe("archive artifact finalization service", () => {
  let schema = "";
  let stagingRoot = "";

  function inTestRuntime<T>(callback: () => T): T {
    return runWithRuntimeConfig(
      { databaseUrl: TEST_DATABASE_URL, dbSchema: schema, stagingRoot },
      callback,
    );
  }

  async function dropFailureTrigger(): Promise<void> {
    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`DROP TRIGGER IF EXISTS reject_fixture_metadata_update ON objects`;
      await sql`DROP FUNCTION IF EXISTS reject_fixture_metadata_update()`;
    } finally {
      await sql.close();
    }
  }

  async function waitForBlockedAttemptQueries(blockerPid: number, count: number): Promise<void> {
    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      for (let attempt = 0; attempt < 200; attempt += 1) {
        const rows = await sql<Array<{ blocked_count: number }>>`
          WITH RECURSIVE blocked(pid) AS (
            SELECT pid
            FROM pg_stat_activity
            WHERE ${blockerPid} = ANY(pg_blocking_pids(pid))
            UNION
            SELECT activity.pid
            FROM pg_stat_activity activity
            INNER JOIN blocked blocker
              ON blocker.pid = ANY(pg_blocking_pids(activity.pid))
          )
          SELECT COUNT(*)::int AS blocked_count FROM blocked
        `;
        if ((rows[0]?.blocked_count ?? 0) >= count) return;
        await Bun.sleep(5);
      }
      throw new Error(`Timed out waiting for ${count} blocked finalization claims.`);
    } finally {
      await sql.close();
    }
  }

  async function readFinalizationState() {
    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      const requests = await sql<Array<{ status: string; completed_at: Date | string | null }>>`
        SELECT status, completed_at
        FROM archive_requests
        WHERE id = ${requestId}
      `;
      const attempts = await sql<Array<{
        state: string;
        artifact_id: string | null;
        finalization_claim_token: string | null;
        finalization_attempt_count: number;
        finalization_next_retry_at: Date | string | null;
        finalization_last_error: string | null;
        retry_scheduled: boolean;
      }>>`
        SELECT state, artifact_id, finalization_claim_token,
               finalization_attempt_count, finalization_next_retry_at,
               finalization_last_error,
               finalization_next_retry_at > now() AS retry_scheduled
        FROM archive_artifact_upload_attempts
        WHERE upload_token_id = ${uploadTokenId}
      `;
      const artifacts = await sql<Array<{
        id: string;
        storage_key: string;
        kind: string;
        variant: string | null;
      }>>`
        SELECT id, storage_key, kind, variant
        FROM object_artifacts
        WHERE object_id = ${objectId}
      `;
      const searchDocuments = await sql<Array<{
        artifact_id: string;
        available_file_id: string | null;
        text_content: string | null;
        indexed_at: Date | string | null;
      }>>`
        SELECT artifact_id, available_file_id, text_content, indexed_at
        FROM object_artifact_search_documents
      `;
      const objects = await sql<Array<{ metadata: { pages?: Array<Record<string, unknown>> } }>>`
        SELECT metadata
        FROM objects
        WHERE object_id = ${objectId}
      `;
      return {
        request: requests[0]!,
        attempt: attempts[0]!,
        artifacts,
        searchDocuments,
        metadata: objects[0]!.metadata,
      };
    } finally {
      await sql.close();
    }
  }

  async function runExpectedFailingFinalization(): Promise<void> {
    const config = JSON.stringify({
      databaseUrl: TEST_DATABASE_URL,
      dbSchema: schema,
      stagingRoot,
    });
    const script = `
      import { runWithRuntimeConfig } from "./src/runtime/config.ts";
      import { finalizeVerifiedArchiveArtifactUpload } from "./src/services/archive-artifact-finalization-service.ts";

      try {
        await runWithRuntimeConfig(${config}, () =>
          finalizeVerifiedArchiveArtifactUpload({
            uploadTokenId: "${uploadTokenId}",
            ignoreRetrySchedule: true,
          }),
        );
        console.error("finalization unexpectedly succeeded");
        process.exit(2);
      } catch (error) {
        if (!(error instanceof Error) || !error.message.includes("forced fixture metadata update failure")) {
          console.error(error);
          process.exit(3);
        }
        console.log(error.message);
        process.exit(0);
      }
    `;
    const child = Bun.spawn([process.execPath, "-e", script], {
      cwd: process.cwd(),
      stdout: "pipe",
      stderr: "pipe",
    });
    const [exitCode, stdout, stderr] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ]);
    expect(exitCode).toBe(0);
    expect(stderr).toContain("forced fixture metadata update failure");
    expect(stdout).toContain("forced fixture metadata update failure");
  }

  async function runExpectedStaleFinalization(
    stale: NonNullable<Awaited<ReturnType<typeof claimVerifiedArchiveArtifactUploadAttempt>>>,
  ): Promise<void> {
    const config = JSON.stringify({
      databaseUrl: TEST_DATABASE_URL,
      dbSchema: schema,
      stagingRoot,
    });
    const script = `
      import { runWithRuntimeConfig } from "./src/runtime/config.ts";
      import { finalizeClaimedArchiveArtifactUpload } from "./src/services/archive-artifact-finalization-service.ts";

      try {
        await runWithRuntimeConfig(${config}, () =>
          finalizeClaimedArchiveArtifactUpload(${JSON.stringify(stale)}),
        );
        process.exit(2);
      } catch (error) {
        if (!(error instanceof Error) || error.message !== "Artifact upload finalization claim is no longer active.") {
          console.error(error);
          process.exit(3);
        }
        process.exit(0);
      }
    `;
    const child = Bun.spawn([process.execPath, "-e", script], {
      cwd: process.cwd(),
      stdout: "pipe",
      stderr: "pipe",
    });
    const [exitCode, stdout, stderr] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ]);
    expect(exitCode).toBe(0);
    expect(stdout).toBe("");
    expect(stderr).toContain("Artifact upload finalization claim is no longer active.");
  }

  function expectCommitted(state: Awaited<ReturnType<typeof readFinalizationState>>): void {
    const artifactId = state.attempt.artifact_id;
    expect(artifactId).toBeString();
    if (!artifactId) throw new Error("Materialized attempt is missing its artifact ID.");

    expect(state.request).toMatchObject({
      status: "COMPLETED",
      completed_at: expect.anything(),
    });
    expect(state.attempt).toMatchObject({
      state: "MATERIALIZED",
      artifact_id: expect.any(String),
      finalization_claim_token: null,
      finalization_next_retry_at: null,
      finalization_last_error: null,
    });
    expect(state.artifacts).toHaveLength(1);
    expect(state.artifacts[0]).toMatchObject({
      id: artifactId,
      storage_key: storageKey,
      kind: "ocr_text",
      variant: "page_0001",
    });
    expect(state.searchDocuments).toEqual([
      {
        artifact_id: artifactId,
        available_file_id: availableFileId,
        text_content: textContent,
        indexed_at: expect.anything(),
      },
    ]);
    expect(state.metadata.pages).toEqual([
      {
        page_number: 1,
        label: "1",
        image_artifact_id: null,
        ocr_text_artifact_id: artifactId,
      },
    ]);
  }

  async function expectIntegrityFailure(reason: string): Promise<void> {
    await expect(
      inTestRuntime(() =>
        finalizeVerifiedArchiveArtifactUpload({
          uploadTokenId,
          ignoreRetrySchedule: true,
        }),
      ),
    ).rejects.toThrow(reason);

    const state = await readFinalizationState();
    expect(state.request).toEqual({ status: "PROCESSING", completed_at: null });
    expect(state.attempt).toMatchObject({
      state: "VERIFIED",
      artifact_id: null,
      finalization_claim_token: null,
      finalization_next_retry_at: expect.anything(),
      finalization_last_error: expect.stringContaining(reason),
      retry_scheduled: true,
    });
    expect(state.artifacts).toEqual([]);
    expect(state.searchDocuments).toEqual([]);
    expect(state.metadata.pages?.[0]?.ocr_text_artifact_id).toBeNull();
  }

  beforeAll(async () => {
    schema = `artifact_finalize_${Date.now()}_${Math.floor(Math.random() * 100000)}`;
    stagingRoot = await mkdtemp(join(tmpdir(), "osimi-artifact-finalization-"));
    await runMigrations({ databaseUrl: TEST_DATABASE_URL, schema });
  });

  beforeEach(async () => {
    await dropFailureTrigger();
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
        VALUES (${tenantId}, ${"artifact-finalization"}, ${"Artifact Finalization"})
      `;
      await sql`
        INSERT INTO objects (
          object_id, tenant_id, type, title, availability_state, metadata
        )
        VALUES (
          ${objectId}, ${tenantId}, ${"DOCUMENT"}::object_type,
          ${"Atomic finalization fixture"}, ${"AVAILABLE"}::object_availability_state,
          ${{
            page_count: 1,
            pages: [{
              page_number: 1,
              label: "1",
              image_artifact_id: null,
              ocr_text_artifact_id: null,
            }],
          }}
        )
      `;
      await sql`
        INSERT INTO object_available_files (
          id, object_id, tenant_id, archive_file_key, artifact_kind,
          variant, display_name, content_type, size_bytes, is_available
        )
        VALUES (
          ${availableFileId}, ${objectId}, ${tenantId}, ${"archive/page-0001.txt"},
          ${"ocr_text"}::artifact_kind, ${"page_0001"}, ${"Page 1 OCR"},
          ${"text/plain"}, ${Buffer.byteLength(textContent)}, true
        )
      `;
      await sql`
        INSERT INTO archive_requests (
          id, tenant_id, target_type, target_id, action_type, action_payload,
          requested_by, status, lease_id, lease_token_id, leased_by, released_at
        )
        VALUES (
          ${requestId}, ${tenantId}, ${"object"}::archive_request_target_type,
          ${objectId}, ${"artifact_fetch"}::archive_request_action_type,
          ${{
            available_file_id: availableFileId,
            artifact_kind: "ocr_text",
            variant: "page_0001",
          }},
          ${userId}, ${"PROCESSING"}::archive_request_status, ${leaseId},
          ${leaseTokenId}, ${"artifact-upload"}, now()
        )
      `;
      await sql`
        INSERT INTO archive_artifact_upload_attempts (
          upload_token_id, request_id, authorized_lease_id,
          authorized_lease_token_id, storage_key, content_type, size_bytes,
          computed_sha256, state, verified_at
        )
        VALUES (
          ${uploadTokenId}, ${requestId}, ${leaseId}, ${leaseTokenId},
          ${storageKey}, ${"text/plain"}, ${Buffer.byteLength(textContent)},
          ${textChecksum}, ${"VERIFIED"}, now()
        )
      `;
    } finally {
      await sql.close();
    }

    const stagedPath = inTestRuntime(() => resolveStagingPath(storageKey));
    await rm(stagedPath, { recursive: true, force: true });
    await mkdir(dirname(stagedPath), { recursive: true });
    await Bun.write(stagedPath, textContent);
  });

  afterAll(async () => {
    await closeDatabaseClients({ timeoutMs: 1_000 });
    if (schema) {
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

  test("rolls back every projection on failure and atomically commits a retry", async () => {
    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`
        CREATE FUNCTION reject_fixture_metadata_update()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
          RAISE EXCEPTION 'forced fixture metadata update failure';
        END;
        $$
      `;
      await sql`
        CREATE TRIGGER reject_fixture_metadata_update
        BEFORE UPDATE OF metadata ON objects
        FOR EACH ROW
        WHEN (OLD.object_id = 'OBJ-20260805-FINALIZE1')
        EXECUTE FUNCTION reject_fixture_metadata_update()
      `;
    } finally {
      await sql.close();
    }

    await runExpectedFailingFinalization();

    const failed = await readFinalizationState();
    expect(failed.request).toEqual({ status: "PROCESSING", completed_at: null });
    expect(failed.attempt).toMatchObject({
      state: "VERIFIED",
      artifact_id: null,
      finalization_claim_token: null,
      finalization_attempt_count: 1,
      finalization_next_retry_at: expect.anything(),
      finalization_last_error: expect.stringContaining("forced fixture metadata update failure"),
      retry_scheduled: true,
    });
    expect(failed.artifacts).toEqual([]);
    expect(failed.searchDocuments).toEqual([]);
    expect(failed.metadata.pages?.[0]?.ocr_text_artifact_id).toBeNull();

    await dropFailureTrigger();
    await inTestRuntime(() =>
      finalizeVerifiedArchiveArtifactUpload({
        uploadTokenId,
        ignoreRetrySchedule: true,
      }),
    );

    const committed = await readFinalizationState();
    expect(committed.attempt.finalization_attempt_count).toBe(2);
    expectCommitted(committed);
  });

  test("leaves a missing verified upload retryable without side effects", async () => {
    await rm(inTestRuntime(() => resolveStagingPath(storageKey)));
    await expectIntegrityFailure("verified_storage_missing");
  });

  test("leaves non-regular verified storage retryable without side effects", async () => {
    const path = inTestRuntime(() => resolveStagingPath(storageKey));
    await rm(path);
    await mkdir(path);
    await expectIntegrityFailure("verified_storage_not_regular");
  });

  test("leaves a truncated verified upload retryable without side effects", async () => {
    await Bun.write(inTestRuntime(() => resolveStagingPath(storageKey)), "truncated");
    await expectIntegrityFailure("verified_storage_size_mismatch");
  });

  test("leaves same-size corrupt verified bytes retryable without side effects", async () => {
    await Bun.write(
      inTestRuntime(() => resolveStagingPath(storageKey)),
      textContent.replace("Atomic", "Mutant"),
    );
    await expectIntegrityFailure("verified_storage_checksum_mismatch");
  });

  test("leaves an oversized verified upload retryable without side effects", async () => {
    await Bun.write(inTestRuntime(() => resolveStagingPath(storageKey)), `${textContent}extra`);
    await expectIntegrityFailure("verified_storage_size_mismatch");
  });

  test("completes a verified attempt through the background sweep", async () => {
    const result = await inTestRuntime(() =>
      runArtifactFinalizationSweep({ batchSize: 1, claimTimeoutSeconds: 30 }),
    );

    expect(result).toEqual({ claimed: 1, completed: 1, failed: 0 });
    expectCommitted(await readFinalizationState());
  });

  test("allows exactly one simultaneous claim from independent service contexts", async () => {
    const locker = createSqlClient(TEST_DATABASE_URL);
    await locker`SET search_path TO ${sqlIdentifier(schema)}, public`;
    const lockerPid = await locker<Array<{ pid: number }>>`SELECT pg_backend_pid() AS pid`;
    let releaseLock!: () => void;
    let lockAcquired!: () => void;
    const release = new Promise<void>((resolve) => {
      releaseLock = resolve;
    });
    const acquired = new Promise<void>((resolve) => {
      lockAcquired = resolve;
    });
    const lockTransaction = locker.begin(async (transaction) => {
      await transaction`
        SELECT upload_token_id
        FROM archive_artifact_upload_attempts
        WHERE upload_token_id = ${uploadTokenId}
        FOR UPDATE
      `;
      lockAcquired();
      await release;
    });

    await acquired;
    const specificSql = createSqlClient(TEST_DATABASE_URL);
    const secondSql = createSqlClient(TEST_DATABASE_URL);
    await Promise.all([
      specificSql`SET search_path TO ${sqlIdentifier(schema)}, public`,
      secondSql`SET search_path TO ${sqlIdentifier(schema)}, public`,
    ]);
    const specificClaim = claimVerifiedArchiveArtifactUploadAttempt({
      uploadTokenId,
      claimTimeoutSeconds: 30,
      executor: specificSql,
    });
    const secondClaim = claimVerifiedArchiveArtifactUploadAttempt({
      uploadTokenId,
      claimTimeoutSeconds: 30,
      executor: secondSql,
    });
    try {
      await waitForBlockedAttemptQueries(lockerPid[0]!.pid, 2);
    } finally {
      releaseLock();
      await lockTransaction;
      await locker.close();
    }

    const [specific, second] = await Promise.all([specificClaim, secondClaim]);
    await Promise.all([specificSql.close(), secondSql.close()]);
    const winners = [specific, second].filter((claim) => claim !== undefined);
    expect(winners).toHaveLength(1);
    const winningToken = winners[0]!.finalizationClaimToken;
    expect(winningToken).toBeString();
    expect(winners[0]).toMatchObject({
      uploadTokenId,
      finalizationAttemptCount: 1,
    });
    const attempt = await inTestRuntime(() =>
      findArchiveArtifactUploadAttemptById({ uploadTokenId }),
    );
    expect(attempt?.finalizationClaimToken).toBe(winningToken);
    expect(attempt?.finalizationAttemptCount).toBe(1);
  });

  test("fences a timed-out claimant from materialization and replacement retry state", async () => {
    const stale = await inTestRuntime(() =>
      claimVerifiedArchiveArtifactUploadAttempt({
        uploadTokenId,
        claimTimeoutSeconds: 30,
      }),
    );
    expect(stale?.finalizationClaimToken).toBeString();

    const locker = createSqlClient(TEST_DATABASE_URL);
    await locker`SET search_path TO ${sqlIdentifier(schema)}, public`;
    const lockerPid = await locker<Array<{ pid: number }>>`SELECT pg_backend_pid() AS pid`;
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
    const staleFinalization = runExpectedStaleFinalization(stale!);
    await waitForBlockedAttemptQueries(lockerPid[0]!.pid, 1);

    const sql = createSqlClient(TEST_DATABASE_URL);
    let replacement: Awaited<ReturnType<typeof claimVerifiedArchiveArtifactUploadAttempt>>;
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`
        UPDATE archive_artifact_upload_attempts
        SET finalization_claimed_at = now() - interval '31 seconds'
        WHERE upload_token_id = ${uploadTokenId}
      `;
      replacement = await claimVerifiedArchiveArtifactUploadAttempt({
        uploadTokenId,
        claimTimeoutSeconds: 30,
        executor: sql,
      });
    } finally {
      await sql.close();
      releaseLock();
      await lockTransaction;
      await locker.close();
    }
    await staleFinalization;
    expect(replacement).toMatchObject({
      finalizationAttemptCount: 2,
      finalizationNextRetryAt: null,
      finalizationLastError: null,
    });
    expect(replacement?.finalizationClaimToken).not.toBe(stale?.finalizationClaimToken);

    const fenced = await inTestRuntime(() =>
      findArchiveArtifactUploadAttemptById({ uploadTokenId }),
    );
    expect(fenced).toMatchObject({
      state: "VERIFIED",
      artifactId: null,
      finalizationClaimToken: replacement!.finalizationClaimToken,
      finalizationAttemptCount: 2,
      finalizationNextRetryAt: null,
      finalizationLastError: null,
    });
    const beforeReplacement = await readFinalizationState();
    expect(beforeReplacement.artifacts).toEqual([]);
    expect(beforeReplacement.searchDocuments).toEqual([]);
    expect(beforeReplacement.request).toEqual({ status: "PROCESSING", completed_at: null });

    await inTestRuntime(() => finalizeClaimedArchiveArtifactUpload(replacement!));
    expectCommitted(await readFinalizationState());
  });
});
