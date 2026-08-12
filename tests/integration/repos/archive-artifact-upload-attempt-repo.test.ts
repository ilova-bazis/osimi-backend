import { afterAll, beforeAll, beforeEach, describe, expect, test } from "bun:test";
import { sql as sqlIdentifier } from "bun";

import { createSqlClient, withSchemaClient } from "../../../src/db/client.ts";
import { runMigrations } from "../../../src/db/migrate.ts";
import { closeDatabaseClients } from "../../../src/db/runtime.ts";
import {
  claimVerifiedArchiveArtifactUploadAttempt,
  claimVerifiedArchiveArtifactUploadAttemptBatch,
  createAuthorizedArchiveArtifactUploadAttempt,
  findArchiveArtifactUploadAttemptById,
  markArchiveArtifactUploadAttemptMaterialized,
  recordArchiveArtifactUploadFinalizationFailure,
  verifyArchiveArtifactUploadAttempt,
} from "../../../src/repos/archive-artifact-upload-attempt-repo.ts";
import {
  failArchiveRequest,
  leaseNextPendingArchiveRequest,
  releaseArchiveRequestLease,
  sweepExpiredArchiveRequestLeases,
  transferArchiveRequestUploadToBackend,
} from "../../../src/repos/archive-request-repo.ts";
import { runWithRuntimeConfig } from "../../../src/runtime/config.ts";
import { TEST_DATABASE_URL } from "../test-database.ts";

const tenantId = "00000000-0000-0000-0000-000000000101";
const userId = "00000000-0000-0000-0000-000000000102";
const objectId = "OBJ-20260805-UPLOAD1";
const artifactId = "00000000-0000-0000-0000-000000000103";
const requestId = "10000000-0000-0000-0000-000000000001";
const leaseId = "20000000-0000-0000-0000-000000000001";
const leaseTokenId = "30000000-0000-0000-0000-000000000001";
const checksum = "a".repeat(64);
const otherChecksum = "b".repeat(64);

describe("archive artifact upload attempt repository", () => {
  let schema = "";

  function inTestSchema<T>(callback: () => T): T {
    return runWithRuntimeConfig(
      { databaseUrl: TEST_DATABASE_URL, dbSchema: schema },
      callback,
    );
  }

  async function seedRequest(params: {
    id?: string;
    leaseId?: string;
    leaseTokenId?: string;
    expiresAt?: Date;
  } = {}): Promise<void> {
    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`
        INSERT INTO archive_requests (
          id, tenant_id, target_type, target_id, action_type, requested_by,
          status, lease_id, lease_token_id, lease_expires_at, leased_by
        )
        VALUES (
          ${params.id ?? requestId}, ${tenantId}, ${"object"}::archive_request_target_type,
          ${objectId}, ${"artifact_fetch"}::archive_request_action_type, ${userId},
          ${"PROCESSING"}::archive_request_status, ${params.leaseId ?? leaseId},
          ${params.leaseTokenId ?? leaseTokenId},
          ${params.expiresAt ?? new Date(Date.now() + 60_000)}, ${"integration-test"}
        )
      `;
    } finally {
      await sql.close();
    }
  }

  async function authorize(params: {
    requestId?: string;
    leaseId?: string;
    leaseTokenId?: string;
    uploadTokenId: string;
    expectedSha256?: string | null;
  }) {
    return inTestSchema(() =>
      withSchemaClient((sql) =>
        createAuthorizedArchiveArtifactUploadAttempt(sql, {
          requestId: params.requestId ?? requestId,
          leaseId: params.leaseId ?? leaseId,
          leaseTokenId: params.leaseTokenId ?? leaseTokenId,
          uploadTokenId: params.uploadTokenId,
          storageKey: `attempts/${params.uploadTokenId}`,
          contentType: "application/octet-stream",
          sizeBytes: 42,
          expectedSha256: params.expectedSha256,
          expiresAt: new Date(Date.now() + 60_000),
        }),
      ),
    );
  }

  async function verify(params: {
    requestId?: string;
    leaseId?: string;
    leaseTokenId?: string;
    uploadTokenId: string;
    computedSizeBytes?: number;
    computedSha256?: string;
  }) {
    return inTestSchema(() =>
      verifyArchiveArtifactUploadAttempt({
        uploadTokenId: params.uploadTokenId,
        requestId: params.requestId ?? requestId,
        leaseId: params.leaseId ?? leaseId,
        leaseTokenId: params.leaseTokenId ?? leaseTokenId,
        computedSizeBytes: params.computedSizeBytes ?? 42,
        computedSha256: params.computedSha256 ?? checksum,
      }),
    );
  }

  async function setAttemptClaimedAt(uploadTokenId: string, claimedAt: Date): Promise<void> {
    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`
        UPDATE archive_artifact_upload_attempts
        SET finalization_claimed_at = ${claimedAt}
        WHERE upload_token_id = ${uploadTokenId}
      `;
    } finally {
      await sql.close();
    }
  }

  beforeAll(async () => {
    schema = `upload_attempt_${Date.now()}_${Math.floor(Math.random() * 100000)}`;
    await runMigrations({ databaseUrl: TEST_DATABASE_URL, schema });
  });

  beforeEach(async () => {
    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`TRUNCATE archive_artifact_upload_attempts, archive_requests, object_artifacts, objects, tenants CASCADE`;
      await sql`
        INSERT INTO tenants (id, slug, name)
        VALUES (${tenantId}, ${"upload-attempts"}, ${"Upload Attempts"})
      `;
      await sql`
        INSERT INTO objects (object_id, tenant_id, title)
        VALUES (${objectId}, ${tenantId}, ${"Upload attempt object"})
      `;
      await sql`
        INSERT INTO object_artifacts (id, object_id, kind, storage_key, content_type, size_bytes)
        VALUES (${artifactId}, ${objectId}, ${"original"}::artifact_kind, ${"artifacts/materialized"}, ${"application/octet-stream"}, 42)
      `;
    } finally {
      await sql.close();
    }
    await seedRequest();
  });

  afterAll(async () => {
    if (!schema) return;

    await closeDatabaseClients({ timeoutMs: 1_000 });
    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`DROP SCHEMA IF EXISTS ${sqlIdentifier(schema)} CASCADE`;
    } finally {
      await sql.close();
    }
  });

  test("creates an authorization only for an active lease and supersedes the prior authorization", async () => {
    const firstToken = "40000000-0000-0000-0000-000000000001";
    const secondToken = "40000000-0000-0000-0000-000000000002";

    const first = await authorize({ uploadTokenId: firstToken });
    expect(first?.state).toBe("AUTHORIZED");
    expect(first?.authorizedLeaseId).toBe(leaseId);

    const second = await authorize({ uploadTokenId: secondToken });
    expect(second?.state).toBe("AUTHORIZED");
    expect(
      await inTestSchema(() =>
        findArchiveArtifactUploadAttemptById({ uploadTokenId: firstToken }),
      ),
    ).toMatchObject({ invalidatedAt: expect.any(Date) });

    expect(
      await authorize({
        uploadTokenId: "40000000-0000-0000-0000-000000000003",
        leaseTokenId: "30000000-0000-0000-0000-000000000099",
      }),
    ).toBeUndefined();
  });

  test("rejects stale or released leases and verifies an active matching lease with its expected checksum", async () => {
    const staleToken = "41000000-0000-0000-0000-000000000001";
    await authorize({ uploadTokenId: staleToken });

    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`UPDATE archive_requests SET lease_expires_at = now() - interval '1 second' WHERE id = ${requestId}`;
    } finally {
      await sql.close();
    }
    expect(await verify({ uploadTokenId: staleToken })).toBeUndefined();

    const releasedRequestId = "10000000-0000-0000-0000-000000000002";
    const releasedToken = "41000000-0000-0000-0000-000000000002";
    await seedRequest({ id: releasedRequestId });
    await authorize({ requestId: releasedRequestId, uploadTokenId: releasedToken });
    const releaseSql = createSqlClient(TEST_DATABASE_URL);
    try {
      await releaseSql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await releaseSql`UPDATE archive_requests SET released_at = now() WHERE id = ${releasedRequestId}`;
    } finally {
      await releaseSql.close();
    }
    expect(await verify({ requestId: releasedRequestId, uploadTokenId: releasedToken })).toBeUndefined();

    const activeRequestId = "10000000-0000-0000-0000-000000000003";
    const activeToken = "41000000-0000-0000-0000-000000000003";
    await seedRequest({ id: activeRequestId });
    await authorize({
      requestId: activeRequestId,
      uploadTokenId: activeToken,
      expectedSha256: checksum,
    });
    expect(
      await verify({
        requestId: activeRequestId,
        uploadTokenId: activeToken,
        computedSha256: otherChecksum,
      }),
    ).toBeUndefined();
    expect(
      await verify({
        requestId: activeRequestId,
        uploadTokenId: activeToken,
        computedSizeBytes: 41,
      }),
    ).toBeUndefined();
    expect(
      await verify({
        requestId: activeRequestId,
        uploadTokenId: activeToken,
        leaseTokenId: "30000000-0000-0000-0000-000000000099",
      }),
    ).toBeUndefined();
    expect(await verify({ requestId: activeRequestId, uploadTokenId: activeToken })).toMatchObject({
      state: "VERIFIED",
      computedSha256: checksum,
    });
  });

  test("accepts only one upload attempt per request", async () => {
    const firstToken = "42000000-0000-0000-0000-000000000001";
    const secondToken = "42000000-0000-0000-0000-000000000002";
    await authorize({ uploadTokenId: firstToken });
    expect(await verify({ uploadTokenId: firstToken })).toBeDefined();
    await authorize({ uploadTokenId: secondToken });

    expect(await verify({ uploadTokenId: secondToken })).toBeUndefined();
  });

  test("fences specific claims until their timeout expires", async () => {
    const uploadTokenId = "43000000-0000-0000-0000-000000000001";
    await authorize({ uploadTokenId });
    await verify({ uploadTokenId });

    const first = await inTestSchema(() =>
      claimVerifiedArchiveArtifactUploadAttempt({ uploadTokenId, claimTimeoutSeconds: 30 }),
    );
    expect(first?.finalizationClaimToken).toBeString();
    expect(first?.finalizationAttemptCount).toBe(1);
    expect(
      await inTestSchema(() =>
        claimVerifiedArchiveArtifactUploadAttempt({ uploadTokenId, claimTimeoutSeconds: 30 }),
      ),
    ).toBeUndefined();

    await setAttemptClaimedAt(uploadTokenId, new Date(Date.now() - 31_000));
    const recovered = await inTestSchema(() =>
      claimVerifiedArchiveArtifactUploadAttempt({ uploadTokenId, claimTimeoutSeconds: 30 }),
    );
    expect(recovered?.finalizationClaimToken).not.toBe(first?.finalizationClaimToken);
    expect(recovered?.finalizationAttemptCount).toBe(2);
  });

  test("batch claims are fenced and recover timed-out claims", async () => {
    const secondRequestId = "10000000-0000-0000-0000-000000000002";
    const firstToken = "44000000-0000-0000-0000-000000000001";
    const secondToken = "44000000-0000-0000-0000-000000000002";
    await seedRequest({ id: secondRequestId });
    await authorize({ uploadTokenId: firstToken });
    await authorize({ requestId: secondRequestId, uploadTokenId: secondToken });
    await verify({ uploadTokenId: firstToken });
    await verify({ requestId: secondRequestId, uploadTokenId: secondToken });

    const claimed = await inTestSchema(() =>
      claimVerifiedArchiveArtifactUploadAttemptBatch({ batchSize: 10, claimTimeoutSeconds: 30 }),
    );
    expect(claimed).toHaveLength(2);
    expect(new Set(claimed.map((attempt) => attempt.finalizationClaimToken)).size).toBe(1);
    expect(
      await inTestSchema(() =>
        claimVerifiedArchiveArtifactUploadAttemptBatch({ batchSize: 10, claimTimeoutSeconds: 30 }),
      ),
    ).toEqual([]);

    await setAttemptClaimedAt(firstToken, new Date(Date.now() - 31_000));
    const recovered = await inTestSchema(() =>
      claimVerifiedArchiveArtifactUploadAttemptBatch({ batchSize: 10, claimTimeoutSeconds: 30 }),
    );
    expect(recovered.map((attempt) => attempt.uploadTokenId)).toEqual([firstToken]);
    expect(recovered[0]?.finalizationAttemptCount).toBe(2);
  });

  test("records failure against the matching claim, clears it, and schedules retry", async () => {
    const uploadTokenId = "45000000-0000-0000-0000-000000000001";
    await authorize({ uploadTokenId });
    await verify({ uploadTokenId });
    const claimed = await inTestSchema(() =>
      claimVerifiedArchiveArtifactUploadAttempt({ uploadTokenId, claimTimeoutSeconds: 30 }),
    );

    expect(
      await inTestSchema(() =>
        recordArchiveArtifactUploadFinalizationFailure({
          uploadTokenId,
          claimToken: "50000000-0000-0000-0000-000000000099",
          retryDelaySeconds: 60,
          lastError: "wrong claim",
        }),
      ),
    ).toBeUndefined();
    const failed = await inTestSchema(() =>
      recordArchiveArtifactUploadFinalizationFailure({
        uploadTokenId,
        claimToken: claimed!.finalizationClaimToken!,
        retryDelaySeconds: 60,
        lastError: "storage unavailable",
      }),
    );
    expect(failed).toMatchObject({
      finalizationClaimToken: null,
      finalizationClaimedAt: null,
      finalizationLastError: "storage unavailable",
    });
    expect(failed?.finalizationNextRetryAt?.getTime()).toBeGreaterThan(Date.now());
    expect(
      await inTestSchema(() =>
        claimVerifiedArchiveArtifactUploadAttempt({ uploadTokenId, claimTimeoutSeconds: 30 }),
      ),
    ).toBeUndefined();
  });

  test("materializes only with the matching claim and links the artifact", async () => {
    const uploadTokenId = "46000000-0000-0000-0000-000000000001";
    await authorize({ uploadTokenId });
    await verify({ uploadTokenId });
    const claimed = await inTestSchema(() =>
      claimVerifiedArchiveArtifactUploadAttempt({ uploadTokenId, claimTimeoutSeconds: 30 }),
    );

    expect(
      await inTestSchema(() =>
        withSchemaClient((sql) =>
          markArchiveArtifactUploadAttemptMaterialized(sql, {
            uploadTokenId,
            claimToken: "50000000-0000-0000-0000-000000000099",
            artifactId,
          }),
        ),
      ),
    ).toBeUndefined();
    const materialized = await inTestSchema(() =>
      withSchemaClient((sql) =>
        markArchiveArtifactUploadAttemptMaterialized(sql, {
          uploadTokenId,
          claimToken: claimed!.finalizationClaimToken!,
          artifactId,
        }),
      ),
    );
    expect(materialized).toMatchObject({
      state: "MATERIALIZED",
      artifactId,
      finalizationClaimToken: null,
    });
    expect(materialized?.materializedAt).toBeInstanceOf(Date);
  });

  test("lease release and expiry sweep invalidate authorized attempts", async () => {
    const releasedToken = "47000000-0000-0000-0000-000000000001";
    const expiredRequestId = "10000000-0000-0000-0000-000000000002";
    const expiredToken = "47000000-0000-0000-0000-000000000002";
    await authorize({ uploadTokenId: releasedToken });
    await seedRequest({ id: expiredRequestId });
    await authorize({ requestId: expiredRequestId, uploadTokenId: expiredToken });
    const expirySql = createSqlClient(TEST_DATABASE_URL);
    try {
      await expirySql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await expirySql`
        UPDATE archive_requests
        SET lease_expires_at = now() - interval '1 second'
        WHERE id = ${expiredRequestId}
      `;
    } finally {
      await expirySql.close();
    }

    expect(
      await inTestSchema(() =>
        releaseArchiveRequestLease({ requestId, leaseId, leaseTokenId }),
      ),
    ).toBeDefined();
    expect(await inTestSchema(() => sweepExpiredArchiveRequestLeases())).toBe(1);

    for (const uploadTokenId of [releasedToken, expiredToken]) {
      expect(
        await inTestSchema(() =>
          findArchiveArtifactUploadAttemptById({ uploadTokenId }),
        ),
      ).toMatchObject({ state: "AUTHORIZED", invalidatedAt: expect.any(Date) });
    }
  });

  test("a released and reassigned lease cannot verify the previous worker upload", async () => {
    const uploadTokenId = "47500000-0000-0000-0000-000000000001";
    await authorize({ uploadTokenId });
    expect(await inTestSchema(() => releaseArchiveRequestLease({
      requestId,
      leaseId,
      leaseTokenId,
    }))).toBeDefined();

    const reassigned = await inTestSchema(() => leaseNextPendingArchiveRequest({
      workerId: "replacement-worker",
      leaseDurationSeconds: 60,
      actionType: "artifact_fetch",
    }));
    expect(reassigned?.request.id).toBe(requestId);
    expect(reassigned?.leaseId).not.toBe(leaseId);
    expect(await verify({ uploadTokenId })).toBeUndefined();
    expect(await inTestSchema(() => findArchiveArtifactUploadAttemptById({ uploadTokenId })))
      .toMatchObject({ state: "AUTHORIZED", invalidatedAt: expect.any(Date) });
  });

  test("a failed request invalidates its unfinished upload", async () => {
    const uploadTokenId = "47600000-0000-0000-0000-000000000001";
    await authorize({ uploadTokenId });
    expect(await inTestSchema(() => failArchiveRequest({
      requestId,
      leaseId,
      leaseTokenId,
      failureReason: "worker failed",
      failureDetails: {},
    }))).toMatchObject({ status: "FAILED" });
    expect(await verify({ uploadTokenId })).toBeUndefined();
    expect(await inTestSchema(() => findArchiveArtifactUploadAttemptById({ uploadTokenId })))
      .toMatchObject({ invalidatedAt: expect.any(Date) });
  });

  test("does not requeue a verified request after its lease is released and cleared", async () => {
    const uploadTokenId = "48000000-0000-0000-0000-000000000001";
    await authorize({ uploadTokenId });
    await verify({ uploadTokenId });
    expect(
      await inTestSchema(() =>
        withSchemaClient((sql) =>
          transferArchiveRequestUploadToBackend({ requestId, leaseId, leaseTokenId, executor: sql }),
        ),
      ),
    ).toBeDefined();

    expect(await inTestSchema(() => sweepExpiredArchiveRequestLeases())).toBe(0);
    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      const rows = await sql<Array<{
        status: string;
        released_at: Date | null;
        lease_expires_at: Date | null;
      }>>`
        SELECT status, released_at, lease_expires_at
        FROM archive_requests
        WHERE id = ${requestId}
      `;
      expect(rows[0]).toMatchObject({
        status: "PROCESSING",
        released_at: expect.any(Date),
        lease_expires_at: null,
      });
    } finally {
      await sql.close();
    }
  });
});
