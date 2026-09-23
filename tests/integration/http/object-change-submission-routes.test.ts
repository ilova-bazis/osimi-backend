import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { sql as sqlIdentifier } from "bun";

import { createAppWithOptions as createApp } from "../../../src/app.ts";
import { createSqlClient } from "../../../src/db/client.ts";
import { runMigrations } from "../../../src/db/migrate.ts";
import { TEST_DATABASE_URL } from "../test-database.ts";

describe("object change submission routes", () => {
    let schema = "";
    let stagingRoot = "";

    let operatorToken = "";
    let viewerToken = "";
    let adminToken = "";

    const tenantOneId = "00000000-0000-0000-0000-000000000001";
    const documentObjectId = "OBJ-20260922-DOC001";
    const imageObjectId = "OBJ-20260922-IMG001";
    const pageOneArtifactId = "60000000-0000-4000-8000-000000001001";
    const pageOneStorageKey = `tenants/${tenantOneId}/objects/${documentObjectId}/artifacts/page-1.txt`;

    function createTestApp() {
        return createApp({
            runtimeConfig: {
                databaseUrl: TEST_DATABASE_URL,
                dbSchema: schema,
                stagingRoot,
                workerAuthToken: "worker-secret",
                uploadSigningSecret: "object-change-submission-upload-signing-secret-000",
                leaseSigningSecret: "object-change-submission-lease-signing-secret-0000",
            },
        });
    }

    const workerHeaders = {
        "x-worker-auth-token": "worker-secret",
        "x-worker-id": "worker-changes",
        "content-type": "application/json",
    };

    async function resetObjectEditState(params: {
        objectId: string;
        metadata: Record<string, unknown>;
    }) {
        const sql = createSqlClient(TEST_DATABASE_URL!);
        try {
            await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            await sql`
                DELETE FROM object_curated_document_pages WHERE object_id = ${params.objectId}
            `;
            await sql`
                DELETE FROM object_edit_events WHERE object_id = ${params.objectId}
            `;
            await sql`
                DELETE FROM archive_request_sources
                USING archive_requests
                WHERE archive_request_sources.request_id = archive_requests.id
                  AND archive_requests.target_id = ${params.objectId}
            `;
            await sql`
                DELETE FROM archive_requests WHERE target_id = ${params.objectId}
            `;
            await sql`
                INSERT INTO object_edits (object_id, revision, updated_at, updated_by)
                VALUES (${params.objectId}, 0, now(), NULL)
                ON CONFLICT (object_id)
                DO UPDATE SET revision = 0, updated_at = now(), updated_by = NULL,
                              locked_by = NULL, locked_until = NULL
            `;
            await sql`
                UPDATE objects
                SET metadata = ${params.metadata},
                    curation_state = ${"needs_review"}::object_curation_state,
                    updated_at = now()
                WHERE object_id = ${params.objectId}
            `;
        } finally {
            await sql.close();
        }
    }

    async function login(app: ReturnType<typeof createApp>, username: string, password: string): Promise<string> {
        const response = await app.fetch(
            new Request("http://localhost/api/auth/login", {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({ username, password }),
            }),
        );
        expect(response.status).toBe(200);
        const body = (await response.json()) as { token: string };
        return body.token;
    }

    beforeAll(async () => {
        process.env.OBJECT_REVISION_APPLY_ENABLED = "true";
        schema = `changes_${Date.now()}_${Math.floor(Math.random() * 100000)}`;
        stagingRoot = await mkdtemp(join(tmpdir(), "osimi-changes-staging-"));

        await runMigrations({
            databaseUrl: TEST_DATABASE_URL,
            schema,
        });

        const sql = createSqlClient(TEST_DATABASE_URL!);
        try {
            const operatorHash = await Bun.password.hash("operator123");
            const viewerHash = await Bun.password.hash("viewer123");
            const adminHash = await Bun.password.hash("admin123");

            await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;

            await sql`
        INSERT INTO tenants (id, slug, name)
        VALUES (${tenantOneId}, ${"tenant-one"}, ${"Tenant One"})
      `;

            await sql`
        INSERT INTO users (id, username, username_normalized, password_hash)
        VALUES
          (${"10000000-0000-0000-0000-000000000001"}, ${"archiver@osimi.local"}, ${"archiver@osimi.local"}, ${operatorHash}),
          (${"10000000-0000-0000-0000-000000000002"}, ${"viewer@osimi.local"}, ${"viewer@osimi.local"}, ${viewerHash}),
          (${"10000000-0000-0000-0000-000000000003"}, ${"admin@osimi.local"}, ${"admin@osimi.local"}, ${adminHash})
      `;

            await sql`
        INSERT INTO tenant_memberships (id, tenant_id, user_id, role)
        VALUES
          (${"20000000-0000-0000-0000-000000000001"}, ${tenantOneId}, ${"10000000-0000-0000-0000-000000000001"}, ${"archiver"}),
          (${"20000000-0000-0000-0000-000000000002"}, ${tenantOneId}, ${"10000000-0000-0000-0000-000000000002"}, ${"viewer"}),
          (${"20000000-0000-0000-0000-000000000003"}, ${tenantOneId}, ${"10000000-0000-0000-0000-000000000003"}, ${"admin"})
      `;

            await sql`
        INSERT INTO objects (
          object_id,
          tenant_id,
          type,
          title,
          metadata,
          ingest_manifest,
          source_ingestion_id,
          availability_state
        )
        VALUES
          (
            ${documentObjectId},
            ${tenantOneId},
            ${"DOCUMENT"}::object_type,
            ${"Change Submission Document"},
            ${{ source: "scanner-changes", page_count: 1, pages: [
                { page_number: 1, label: "1", ocr_text_artifact_id: pageOneArtifactId },
            ] }},
            NULL,
            NULL,
            ${"AVAILABLE"}::object_availability_state
          ),
          (
            ${imageObjectId},
            ${tenantOneId},
            ${"IMAGE"}::object_type,
            ${"Change Submission Image"},
            ${{ source: "camera-changes" }},
            NULL,
            NULL,
            ${"AVAILABLE"}::object_availability_state
          )
      `;

            await sql`
        INSERT INTO object_access_assignments (object_id, tenant_id, user_id, granted_level, created_by)
        VALUES
          (${documentObjectId}, ${tenantOneId}, ${"10000000-0000-0000-0000-000000000001"}, ${"private"}::object_access_granted_level, ${"10000000-0000-0000-0000-000000000003"}),
          (${imageObjectId}, ${tenantOneId}, ${"10000000-0000-0000-0000-000000000001"}, ${"private"}::object_access_granted_level, ${"10000000-0000-0000-0000-000000000003"})
      `;

            await sql`
        INSERT INTO object_artifacts (id, object_id, kind, variant, storage_key, content_type, size_bytes)
        VALUES (
          ${pageOneArtifactId},
          ${documentObjectId},
          ${"ocr_text"}::artifact_kind,
          ${"page-1"},
          ${pageOneStorageKey},
          ${"text/plain"},
          ${21}
        )
      `;
        } finally {
            await sql.close();
        }

        const machineText = "machine change page 1\n";
        const artifactPath = join(stagingRoot, pageOneStorageKey);
        await mkdir(dirname(artifactPath), { recursive: true });
        await Bun.write(artifactPath, machineText);

        const app = createTestApp();
        operatorToken = await login(app, "archiver@osimi.local", "operator123");
        viewerToken = await login(app, "viewer@osimi.local", "viewer123");
        adminToken = await login(app, "admin@osimi.local", "admin123");
    });

    afterAll(() => {
        delete process.env.OBJECT_REVISION_APPLY_ENABLED;
        return rm(stagingRoot, { recursive: true, force: true });
    });

    interface LeaseResult {
        request: {
            request_id: string;
            lease_id: string;
            lease_token: string;
            lease_expires_at: string;
            action_type: string;
            action_payload: Record<string, unknown>;
        } | null;
    }

    async function leaseNextChangeRequest(app: ReturnType<typeof createApp>): Promise<NonNullable<LeaseResult["request"]>> {
        const leaseResponse = await app.fetch(
            new Request("http://localhost/api/archive-requests/lease", {
                method: "POST",
                headers: workerHeaders,
                body: JSON.stringify({ action_type: "object_revision_apply" }),
            }),
        );
        expect(leaseResponse.status).toBe(200);
        const leaseBody = (await leaseResponse.json()) as LeaseResult;
        const lease = leaseBody.request;
        if (!lease) {
            throw new Error("expected a leaseable object_revision_apply request");
        }
        return lease;
    }

    async function downloadSource(app: ReturnType<typeof createApp>, requestId: string, leaseToken: string): Promise<{ body: Record<string, unknown>; checksum: string }> {
        const sourceResponse = await app.fetch(
            new Request(`http://localhost/api/archive-requests/${requestId}/source`, {
                headers: {
                    "x-worker-auth-token": "worker-secret",
                    "x-worker-id": "worker-changes",
                    "x-archive-request-lease-token": leaseToken,
                },
            }),
        );
        expect(sourceResponse.status).toBe(200);
        const checksum = sourceResponse.headers.get("x-content-sha256") ?? "";
        return {
            body: (await sourceResponse.json()) as Record<string, unknown>,
            checksum,
        };
    }

    async function completeRequest(app: ReturnType<typeof createApp>, requestId: string, leaseToken: string, result: Record<string, unknown>): Promise<number> {
        const response = await app.fetch(
            new Request(`http://localhost/api/archive-requests/${requestId}/complete`, {
                method: "POST",
                headers: workerHeaders,
                body: JSON.stringify({ lease_token: leaseToken, result }),
            }),
        );
        return response.status;
    }

    async function failRequest(app: ReturnType<typeof createApp>, requestId: string, leaseToken: string, failure: Record<string, unknown>): Promise<number> {
        const response = await app.fetch(
            new Request(`http://localhost/api/archive-requests/${requestId}/fail`, {
                method: "POST",
                headers: workerHeaders,
                body: JSON.stringify({ lease_token: leaseToken, failure }),
            }),
        );
        return response.status;
    }

    async function submitChanges(app: ReturnType<typeof createApp>, objectId: string, revision: number): Promise<{ status: number; body: Record<string, unknown> }> {
        const response = await app.fetch(
            new Request(`http://localhost/api/objects/${objectId}/changes/submit`, {
                method: "POST",
                headers: {
                    authorization: `Bearer ${operatorToken}`,
                    "content-type": "application/json",
                },
                body: JSON.stringify({ revision, submission_note: "Sync for tests." }),
            }),
        );
        return { status: response.status, body: (await response.json()) as Record<string, unknown> };
    }

    async function saveMetadata(app: ReturnType<typeof createApp>, objectId: string, revision: number, title: string): Promise<number> {
        const response = await app.fetch(
            new Request(`http://localhost/api/objects/${objectId}/metadata`, {
                method: "PATCH",
                headers: {
                    authorization: `Bearer ${operatorToken}`,
                    "content-type": "application/json",
                },
                body: JSON.stringify({
                    revision,
                    metadata: {
                        title,
                        publication_date: "",
                        date_precision: "none",
                        date_approximate: false,
                        language: "en",
                        tags: [],
                        people: [],
                        description: null,
                    },
                    rights: {
                        rights_note: null,
                        sensitivity_note: null,
                    },
                }),
            }),
        );
        return response.status;
    }

    async function saveCuratedPage(app: ReturnType<typeof createApp>, objectId: string, revision: number, curatedText: string): Promise<number> {
        const response = await app.fetch(
            new Request(`http://localhost/api/objects/${objectId}/curation/document`, {
                method: "PUT",
                headers: {
                    authorization: `Bearer ${operatorToken}`,
                    "content-type": "application/json",
                },
                body: JSON.stringify({
                    revision,
                    pages: [{ page_number: 1, curated_text: curatedText }],
                }),
            }),
        );
        return response.status;
    }

    test("submits a saved revision as an object_revision_apply request with a complete package", async () => {
        const app = createTestApp();
        await resetObjectEditState({
            objectId: documentObjectId,
            metadata: {
                source: "scanner-changes",
                page_count: 1,
                pages: [
                    { page_number: 1, label: "1", ocr_text_artifact_id: pageOneArtifactId },
                ],
            },
        });

        expect(await saveCuratedPage(app, documentObjectId, 0, "curated change page 1")).toBe(200);
        expect(await saveMetadata(app, documentObjectId, 1, "Synchronized Document")).toBe(200);

        const submit = await submitChanges(app, documentObjectId, 2);
        expect(submit.status).toBe(202);
        const submission = submit.body.submission as Record<string, unknown>;
        expect(submit.body.current_revision).toBe(2);
        expect(submit.body.submitted_revision).toBe(2);
        expect(submission.action_type).toBe("object_revision_apply");
        expect(submission.status).toBe("PENDING");

        const lease = await leaseNextChangeRequest(app);
        expect(lease.request_id).toBe(submission.request_id as string);
        const actionPayload = lease.action_payload as Record<string, unknown>;
        expect(actionPayload.object_revision).toBe(2);
        expect(actionPayload.checksum_sha256).toMatch(/^[0-9a-f]{64}$/);
        const sourceRef = actionPayload.source_ref as Record<string, unknown>;
        expect(sourceRef.type).toBe("request_source");
        expect(sourceRef.url).toBe(`/api/archive-requests/${submission.request_id}/source`);

        const source = await downloadSource(app, lease.request_id, lease.lease_token);
        const packageBody = source.body;
        expect(packageBody.schema_version).toBe("1.0");
        expect(packageBody.object_id).toBe(documentObjectId);
        expect(packageBody.object_revision).toBe(2);
        expect(packageBody.submission_id).toBe(submission.id);
        const metadata = packageBody.metadata as Record<string, unknown>;
        expect(metadata.title).toBe("Synchronized Document");
        const rights = packageBody.rights as Record<string, unknown>;
        expect(rights.rights_note).toBeNull();
        const accessPolicy = packageBody.access_policy as Record<string, unknown>;
        expect(accessPolicy.access_level).toBe("private");
        const documentOcr = packageBody.document_ocr as Record<string, unknown>;
        expect(documentOcr.mode).toBe("full_replacement");
        const pages = documentOcr.pages as Array<Record<string, unknown>>;
        expect(pages).toHaveLength(1);
        expect(pages[0]?.text).toBe("curated change page 1");
        expect(pages[0]?.text_origin).toBe("curated");
        expect(pages[0]?.machine_ocr_checksum_sha256).toMatch(/^[0-9a-f]{64}$/);
        expect(source.checksum).toBe(actionPayload.checksum_sha256 as string);

        const completeStatus = await completeRequest(app, lease.request_id, lease.lease_token, {
            schema_version: "1.0",
            submission_id: submission.id,
            object_id: documentObjectId,
            object_revision: 2,
            package_sha256: source.checksum,
            disposition: "applied",
            archive_revision_id: "archive-rev-2",
            applied_at: "2026-09-22T12:00:00.000Z",
        });
        expect(completeStatus).toBe(200);

        const statusResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${documentObjectId}/changes/status`, {
                headers: { authorization: `Bearer ${operatorToken}` },
            }),
        );
        expect(statusResponse.status).toBe(200);
        const statusBody = (await statusResponse.json()) as Record<string, unknown>;
        expect(statusBody.current_revision).toBe(2);
        expect(statusBody.latest_submitted_revision).toBe(2);
        expect(statusBody.latest_applied_revision).toBe(2);
        expect(statusBody.archive_out_of_sync).toBe(false);
        expect(statusBody.active_submission).toBeNull();
        const latest = statusBody.latest_submission as Record<string, unknown>;
        expect(latest.status).toBe("COMPLETED");
        expect(latest.failure_reason).toBeNull();

        const editResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${documentObjectId}/edit`, {
                headers: { authorization: `Bearer ${operatorToken}` },
            }),
        );
        const editBody = (await editResponse.json()) as { revision: number };
        expect(editBody.revision).toBe(2);

        await resetObjectEditState({
            objectId: documentObjectId,
            metadata: { source: "scanner-changes" },
        });
    });

    test("replays an exact submission idempotently", async () => {
        const app = createTestApp();
        await resetObjectEditState({
            objectId: documentObjectId,
            metadata: {
                source: "scanner-changes",
                page_count: 1,
                pages: [
                    { page_number: 1, label: "1", ocr_text_artifact_id: pageOneArtifactId },
                ],
            },
        });

        const first = await submitChanges(app, documentObjectId, 0);
        expect(first.status).toBe(202);
        const firstSubmission = first.body.submission as Record<string, unknown>;

        const second = await submitChanges(app, documentObjectId, 0);
        expect(second.status).toBe(200);
        const secondSubmission = second.body.submission as Record<string, unknown>;
        expect(secondSubmission.id as string).toBe(firstSubmission.id as string);
        expect(secondSubmission.request_id as string).toBe(firstSubmission.request_id as string);

        const sql = createSqlClient(TEST_DATABASE_URL!);
        try {
            await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            const rows = await sql<Array<{ count: number }>>`
                SELECT COUNT(*)::int AS count
                FROM object_revision_submissions
                WHERE object_id = ${documentObjectId}
            `;
            expect(rows[0]?.count).toBe(1);
        } finally {
            await sql.close();
        }

        await resetObjectEditState({
            objectId: documentObjectId,
            metadata: { source: "scanner-changes" },
        });
    });

    test("rejects stale revisions and blocks a second active submission", async () => {
        const app = createTestApp();
        await resetObjectEditState({
            objectId: documentObjectId,
            metadata: { source: "scanner-changes" },
        });

        const stale = await submitChanges(app, documentObjectId, 3);
        expect(stale.status).toBe(409);

        expect(await saveMetadata(app, documentObjectId, 0, "First Draft")).toBe(200);

        const first = await submitChanges(app, documentObjectId, 1);
        expect(first.status).toBe(202);

        expect(await saveMetadata(app, documentObjectId, 1, "Second Draft")).toBe(200);

        const second = await submitChanges(app, documentObjectId, 2);
        expect(second.status).toBe(409);
        const secondBody = second.body;
        const secondError = secondBody.error as Record<string, unknown>;
        expect(secondError.code).toBe("CHANGES_ALREADY_ACTIVE");

        await resetObjectEditState({
            objectId: documentObjectId,
            metadata: { source: "scanner-changes" },
        });
    });

    test("supports metadata-only submission for non-document objects", async () => {
        const app = createTestApp();
        await resetObjectEditState({
            objectId: imageObjectId,
            metadata: { source: "camera-changes" },
        });

        expect(await saveMetadata(app, imageObjectId, 0, "Synchronized Image")).toBe(200);

        const submit = await submitChanges(app, imageObjectId, 1);
        expect(submit.status).toBe(202);
        const submission = submit.body.submission as Record<string, unknown>;

        const editResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${imageObjectId}/edit`, {
                headers: { authorization: `Bearer ${operatorToken}` },
            }),
        );
        expect(editResponse.status).toBe(200);
        const editBody = (await editResponse.json()) as {
            capabilities: { can_submit_changes: boolean };
        };
        expect(editBody.capabilities.can_submit_changes).toBe(true);

        const lease = await leaseNextChangeRequest(app);
        const source = await downloadSource(app, lease.request_id, lease.lease_token);
        expect(source.body.document_ocr).toBeNull();
        const metadata = source.body.metadata as Record<string, unknown>;
        expect(metadata.title).toBe("Synchronized Image");

        expect(await completeRequest(app, lease.request_id, lease.lease_token, {
            schema_version: "1.0",
            submission_id: submission.id,
            object_id: imageObjectId,
            object_revision: 1,
            package_sha256: source.checksum,
            disposition: "applied",
            archive_revision_id: "archive-rev-img-1",
            applied_at: "2026-09-22T12:00:00.000Z",
        })).toBe(200);

        await resetObjectEditState({
            objectId: imageObjectId,
            metadata: { source: "camera-changes" },
        });
    });

    test("reports newer saved changes as archive out of sync", async () => {
        const app = createTestApp();
        await resetObjectEditState({
            objectId: documentObjectId,
            metadata: { source: "scanner-changes" },
        });

        const submit = await submitChanges(app, documentObjectId, 0);
        expect(submit.status).toBe(202);
        const submission = submit.body.submission as Record<string, unknown>;
        const lease = await leaseNextChangeRequest(app);
        const source = await downloadSource(app, lease.request_id, lease.lease_token);
        expect(await completeRequest(app, lease.request_id, lease.lease_token, {
            schema_version: "1.0",
            submission_id: submission.id,
            object_id: documentObjectId,
            object_revision: 0,
            package_sha256: source.checksum,
            disposition: "applied",
            archive_revision_id: "archive-rev-0",
            applied_at: "2026-09-22T12:00:00.000Z",
        })).toBe(200);

        expect(await saveMetadata(app, documentObjectId, 0, "Newer Draft")).toBe(200);

        const statusResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${documentObjectId}/changes/status`, {
                headers: { authorization: `Bearer ${viewerToken}` },
            }),
        );
        expect(statusResponse.status).toBe(200);
        const statusBody = (await statusResponse.json()) as Record<string, unknown>;
        expect(statusBody.current_revision).toBe(1);
        expect(statusBody.latest_applied_revision).toBe(0);
        expect(statusBody.archive_out_of_sync).toBe(true);

        await resetObjectEditState({
            objectId: documentObjectId,
            metadata: { source: "scanner-changes" },
        });
    });

    test("requeues retryable failures and rejects non-retryable retries", async () => {
        const app = createTestApp();
        await resetObjectEditState({
            objectId: documentObjectId,
            metadata: { source: "scanner-changes" },
        });

        const submit = await submitChanges(app, documentObjectId, 0);
        expect(submit.status).toBe(202);
        const submission = submit.body.submission as Record<string, unknown>;
        const requestId = submission.request_id as string;

        const lease = await leaseNextChangeRequest(app);
        expect(await failRequest(app, requestId, lease.lease_token, {
            code: "OBJECT_UPDATE_METADATA_WRITE_FAILED",
            message: "Archive metadata write failed.",
            retryable: true,
        })).toBe(200);

        const retryResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${documentObjectId}/change-submissions/${requestId}/retry`, {
                method: "POST",
                headers: {
                    authorization: `Bearer ${operatorToken}`,
                    "content-type": "application/json",
                },
                body: JSON.stringify({ retry_reason: "Manual retry." }),
            }),
        );
        expect(retryResponse.status).toBe(202);
        const retryBody = (await retryResponse.json()) as Record<string, unknown>;
        const retriedSubmission = retryBody.submission as Record<string, unknown>;
        expect(retriedSubmission.request_id).toBe(requestId);
        expect(retriedSubmission.status).toBe("PENDING");

        const secondLease = await leaseNextChangeRequest(app);
        expect(secondLease.request_id).toBe(requestId);
        expect(await failRequest(app, requestId, secondLease.lease_token, {
            code: "OBJECT_UPDATE_INVALID_SOURCE",
            message: "Source package is invalid.",
            retryable: false,
        })).toBe(200);

        const nonRetryableResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${documentObjectId}/change-submissions/${requestId}/retry`, {
                method: "POST",
                headers: {
                    authorization: `Bearer ${operatorToken}`,
                    "content-type": "application/json",
                },
                body: JSON.stringify({ retry_reason: null }),
            }),
        );
        expect(nonRetryableResponse.status).toBe(409);
        const nonRetryableBody = (await nonRetryableResponse.json()) as {
            error: { code: string };
        };
        expect(nonRetryableBody.error.code).toBe("NOT_RETRYABLE");

        await resetObjectEditState({
            objectId: documentObjectId,
            metadata: { source: "scanner-changes" },
        });
    });

    test("rejects a superseded retry after a newer revision was applied", async () => {
        const app = createTestApp();
        await resetObjectEditState({
            objectId: documentObjectId,
            metadata: { source: "scanner-changes" },
        });

        const firstSubmit = await submitChanges(app, documentObjectId, 0);
        expect(firstSubmit.status).toBe(202);
        const firstSubmission = firstSubmit.body.submission as Record<string, unknown>;
        const firstRequestId = firstSubmission.request_id as string;

        const firstLease = await leaseNextChangeRequest(app);
        expect(await failRequest(app, firstRequestId, firstLease.lease_token, {
            code: "OBJECT_UPDATE_COMMIT_FAILED",
            message: "Archive commit failed.",
            retryable: true,
        })).toBe(200);

        expect(await saveMetadata(app, documentObjectId, 0, "Superseding Draft")).toBe(200);

        const secondSubmit = await submitChanges(app, documentObjectId, 1);
        expect(secondSubmit.status).toBe(202);
        const secondSubmission = secondSubmit.body.submission as Record<string, unknown>;
        const secondLease = await leaseNextChangeRequest(app);
        const secondSource = await downloadSource(app, secondLease.request_id, secondLease.lease_token);
        expect(await completeRequest(app, secondLease.request_id, secondLease.lease_token, {
            schema_version: "1.0",
            submission_id: secondSubmission.id,
            object_id: documentObjectId,
            object_revision: 1,
            package_sha256: secondSource.checksum,
            disposition: "applied",
            archive_revision_id: "archive-rev-1",
            applied_at: "2026-09-22T12:00:00.000Z",
        })).toBe(200);

        const supersededResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${documentObjectId}/change-submissions/${firstRequestId}/retry`, {
                method: "POST",
                headers: {
                    authorization: `Bearer ${operatorToken}`,
                    "content-type": "application/json",
                },
                body: JSON.stringify({ retry_reason: null }),
            }),
        );
        expect(supersededResponse.status).toBe(409);
        const supersededBody = (await supersededResponse.json()) as {
            error: { code: string };
        };
        expect(supersededBody.error.code).toBe("RETRY_SUPERSEDED");

        await resetObjectEditState({
            objectId: documentObjectId,
            metadata: { source: "scanner-changes" },
        });
    });

    test("rejects completion with a mismatched worker result", async () => {
        const app = createTestApp();
        await resetObjectEditState({
            objectId: documentObjectId,
            metadata: { source: "scanner-changes" },
        });

        const submit = await submitChanges(app, documentObjectId, 0);
        expect(submit.status).toBe(202);
        const submission = submit.body.submission as Record<string, unknown>;
        const requestId = submission.request_id as string;

        const lease = await leaseNextChangeRequest(app);
        const mismatchedStatus = await completeRequest(app, requestId, lease.lease_token, {
            schema_version: "1.0",
            submission_id: "00000000-0000-4000-8000-00000000ffff",
            object_id: documentObjectId,
            object_revision: 0,
            package_sha256: "a".repeat(64),
            disposition: "applied",
            archive_revision_id: "archive-rev-wrong",
            applied_at: "2026-09-22T12:00:00.000Z",
        });
        expect(mismatchedStatus).toBe(409);

        const sql = createSqlClient(TEST_DATABASE_URL!);
        try {
            await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            const rows = await sql<Array<{ status: string }>>`
                SELECT status FROM archive_requests WHERE id = ${requestId}
            `;
            expect(rows[0]?.status).toBe("PROCESSING");
        } finally {
            await sql.close();
        }

        await resetObjectEditState({
            objectId: documentObjectId,
            metadata: { source: "scanner-changes" },
        });
    });

    test("records submission and synchronization events in edit history", async () => {
        const app = createTestApp();
        await resetObjectEditState({
            objectId: documentObjectId,
            metadata: { source: "scanner-changes" },
        });

        const submit = await submitChanges(app, documentObjectId, 0);
        expect(submit.status).toBe(202);
        const submission = submit.body.submission as Record<string, unknown>;
        const lease = await leaseNextChangeRequest(app);
        const source = await downloadSource(app, lease.request_id, lease.lease_token);
        expect(await completeRequest(app, lease.request_id, lease.lease_token, {
            schema_version: "1.0",
            submission_id: submission.id,
            object_id: documentObjectId,
            object_revision: 0,
            package_sha256: source.checksum,
            disposition: "applied",
            archive_revision_id: "archive-rev-0",
            applied_at: "2026-09-22T12:00:00.000Z",
        })).toBe(200);

        const historyResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${documentObjectId}/curation/history`, {
                headers: { authorization: `Bearer ${operatorToken}` },
            }),
        );
        expect(historyResponse.status).toBe(200);
        const historyBody = (await historyResponse.json()) as {
            events: Array<{ type: string; revision_before: number | null; revision_after: number | null }>;
        };
        const types = historyBody.events.map((event) => event.type);
        expect(types).toContain("CHANGES_SUBMITTED");
        expect(types).toContain("CHANGES_SYNCHRONIZED");
        const submittedEvent = historyBody.events.find((event) => event.type === "CHANGES_SUBMITTED");
        expect(submittedEvent?.revision_before).toBe(0);
        expect(submittedEvent?.revision_after).toBe(0);

        await resetObjectEditState({
            objectId: documentObjectId,
            metadata: { source: "scanner-changes" },
        });
    });

    test("revision-guards access-policy updates and includes the policy in the package", async () => {
        const app = createTestApp();
        await resetObjectEditState({
            objectId: documentObjectId,
            metadata: { source: "scanner-changes" },
        });

        const wrongRevisionResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${documentObjectId}/access-policy`, {
                method: "PATCH",
                headers: {
                    authorization: `Bearer ${adminToken}`,
                    "content-type": "application/json",
                },
                body: JSON.stringify({
                    revision: 99,
                    access_level: "family",
                    embargo_kind: "none",
                }),
            }),
        );
        expect(wrongRevisionResponse.status).toBe(409);

        const policyResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${documentObjectId}/access-policy`, {
                method: "PATCH",
                headers: {
                    authorization: `Bearer ${adminToken}`,
                    "content-type": "application/json",
                },
                body: JSON.stringify({
                    revision: 0,
                    access_level: "family",
                    embargo_kind: "none",
                }),
            }),
        );
        expect(policyResponse.status).toBe(200);
        const policyBody = (await policyResponse.json()) as { revision: number };
        expect(policyBody.revision).toBe(1);

        const submit = await submitChanges(app, documentObjectId, 1);
        expect(submit.status).toBe(202);
        const submission = submit.body.submission as Record<string, unknown>;
        const lease = await leaseNextChangeRequest(app);
        const source = await downloadSource(app, lease.request_id, lease.lease_token);
        const accessPolicy = source.body.access_policy as Record<string, unknown>;
        expect(accessPolicy.access_level).toBe("family");
        expect(accessPolicy.embargo_kind).toBe("none");

        expect(await completeRequest(app, lease.request_id, lease.lease_token, {
            schema_version: "1.0",
            submission_id: submission.id,
            object_id: documentObjectId,
            object_revision: 1,
            package_sha256: source.checksum,
            disposition: "applied",
            archive_revision_id: "archive-rev-1",
            applied_at: "2026-09-22T12:00:00.000Z",
        })).toBe(200);

        const historyResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${documentObjectId}/curation/history`, {
                headers: { authorization: `Bearer ${operatorToken}` },
            }),
        );
        const historyBody = (await historyResponse.json()) as {
            events: Array<{ type: string }>;
        };
        expect(historyBody.events.map((event) => event.type)).toContain("ACCESS_POLICY_UPDATED");

        await resetObjectEditState({
            objectId: documentObjectId,
            metadata: { source: "scanner-changes" },
        });
    });

    test("returns 503 when object revision apply is disabled", async () => {
        delete process.env.OBJECT_REVISION_APPLY_ENABLED;
        try {
            const app = createTestApp();
            const response = await app.fetch(
                new Request(`http://localhost/api/objects/${documentObjectId}/changes/submit`, {
                    method: "POST",
                    headers: {
                        authorization: `Bearer ${operatorToken}`,
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({ revision: 0, submission_note: null }),
                }),
            );
            expect(response.status).toBe(503);
        } finally {
            process.env.OBJECT_REVISION_APPLY_ENABLED = "true";
        }
    });
});
