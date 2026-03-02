import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { sql as sqlIdentifier } from "bun";

import { createAppWithOptions as createApp } from "../../../src/app.ts";
import { createSqlClient } from "../../../src/db/client.ts";
import { runMigrations } from "../../../src/db/migrate.ts";

const TEST_DATABASE_URL =
    process.env.TEST_DATABASE_URL ?? process.env.DATABASE_URL;

describe.skipIf(!TEST_DATABASE_URL)("object routes", () => {
    let schema = "";
    let stagingRoot = "";

    let operatorToken = "";
    let viewerToken = "";
    let adminToken = "";

    const tenantOneId = "00000000-0000-0000-0000-000000000001";
    const tenantTwoId = "00000000-0000-0000-0000-000000000002";
    const tenantOneObjectId = "OBJ-20260209-ABC123";
    const tenantOneObjectIdTwo = "OBJ-20260209-DEF456";
    const tenantOneObjectIdThree = "OBJ-20260209-GHI789";
    const tenantTwoObjectId = "OBJ-20260209-XYZ789";
    const sourceIngestionId = "30000000-0000-4000-8000-000000000001";
    const artifactId = "60000000-0000-4000-8000-000000000001";
    const artifactStorageKey = `tenants/${tenantOneId}/objects/${tenantOneObjectId}/artifacts/ingest.json`;

    function createTestApp() {
        return createApp({
            runtimeConfig: {
                databaseUrl: TEST_DATABASE_URL,
                dbSchema: schema,
                stagingRoot,
            },
        });
    }

    beforeAll(async () => {
        schema = `objects_${Date.now()}_${Math.floor(Math.random() * 100000)}`;
        stagingRoot = await mkdtemp(join(tmpdir(), "osimi-objects-staging-"));

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
        VALUES
          (${tenantOneId}, ${"tenant-one"}, ${"Tenant One"}),
          (${tenantTwoId}, ${"tenant-two"}, ${"Tenant Two"})
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
          access_level
        )
        VALUES (
          ${sourceIngestionId},
          ${"batch-alpha-2026"},
          ${tenantOneId},
          ${"COMPLETED"}::ingestion_status,
          ${"10000000-0000-0000-0000-000000000001"},
          ${"1.0"},
          ${"document"}::ingestion_classification_type,
          ${"document"}::ingest_item_kind,
          ${"en"},
          ${"auto"}::ingestion_pipeline_preset,
          ${"private"}::object_access_level
        )
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
            ${tenantOneObjectId},
            ${tenantOneId},
            ${"DOCUMENT"}::object_type,
            ${"Tenant One Object"},
            ${{ source: "scanner-a" }},
            ${{ schema_version: "1.0", ingest: { ingest_id: "ING-object-routes" } }},
            NULL,
            ${"AVAILABLE"}::object_availability_state
          ),
          (
            ${tenantOneObjectIdTwo},
            ${tenantOneId},
            ${"DOCUMENT"}::object_type,
            ${"Project Ledger"},
            ${{ source: "scanner-b" }},
            NULL,
            ${sourceIngestionId},
            ${"ARCHIVED"}::object_availability_state
          ),
          (
            ${tenantOneObjectIdThree},
            ${tenantOneId},
            ${"IMAGE"}::object_type,
            ${"Summer Photo"},
            ${{ source: "camera-1" }},
            NULL,
            NULL,
            ${"AVAILABLE"}::object_availability_state
          ),
          (
            ${tenantTwoObjectId},
            ${tenantTwoId},
            ${"IMAGE"}::object_type,
            ${"Tenant Two Object"},
            ${{ source: "private-upload" }},
            NULL,
            NULL,
            ${"AVAILABLE"}::object_availability_state
          )
      `;

            await sql`
        UPDATE objects
        SET
          created_at = ${"2026-02-09T10:00:00.000Z"}::timestamptz,
          updated_at = ${"2026-02-09T10:00:00.000Z"}::timestamptz,
          language_code = ${null}
        WHERE object_id = ${tenantOneObjectId}
      `;

            await sql`
        UPDATE objects
        SET
          created_at = ${"2026-02-10T10:00:00.000Z"}::timestamptz,
          updated_at = ${"2026-02-12T12:00:00.000Z"}::timestamptz,
          language_code = ${"en"}
        WHERE object_id = ${tenantOneObjectIdTwo}
      `;

            await sql`
        UPDATE objects
        SET
          created_at = ${"2026-02-11T10:00:00.000Z"}::timestamptz,
          updated_at = ${"2026-02-11T11:00:00.000Z"}::timestamptz,
          language_code = ${null},
          access_level = ${"public"}::object_access_level,
          embargo_kind = ${"curation_state"}::object_embargo_kind,
          embargo_curation_state = ${"reviewed"}::object_curation_state
        WHERE object_id = ${tenantOneObjectIdThree}
      `;

            await sql`
        INSERT INTO tags (id, name_normalized, display_name)
        VALUES
          (${"70000000-0000-0000-0000-000000000001"}, ${"history"}, ${"History"}),
          (${"70000000-0000-0000-0000-000000000002"}, ${"finance"}, ${"Finance"}),
          (${"70000000-0000-0000-0000-000000000003"}, ${"photo"}, ${"Photo"})
      `;

            await sql`
        INSERT INTO object_tags (object_id, tag_id)
        VALUES
          (${tenantOneObjectId}, ${"70000000-0000-0000-0000-000000000001"}),
          (${tenantOneObjectIdTwo}, ${"70000000-0000-0000-0000-000000000002"}),
          (${tenantOneObjectIdThree}, ${"70000000-0000-0000-0000-000000000003"})
      `;

            await sql`
        INSERT INTO object_artifacts (id, object_id, kind, storage_key, content_type, size_bytes)
        VALUES (
          ${artifactId},
          ${tenantOneObjectId},
          ${"metadata"}::artifact_kind,
          ${artifactStorageKey},
          ${"application/json"},
          ${18}
        )
      `;
        } finally {
            await sql.close();
        }

        const artifactPath = join(stagingRoot, artifactStorageKey);
        await mkdir(dirname(artifactPath), { recursive: true });
        await Bun.write(artifactPath, '{"status":"ready"}');

        const app = createTestApp();

        const operatorLogin = await app.fetch(
            new Request("http://localhost/api/auth/login", {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    username: "archiver@osimi.local",
                    password: "operator123",
                }),
            }),
        );

        const operatorBody = (await operatorLogin.json()) as { token: string };
        operatorToken = operatorBody.token;

        const viewerLogin = await app.fetch(
            new Request("http://localhost/api/auth/login", {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    username: "viewer@osimi.local",
                    password: "viewer123",
                }),
            }),
        );

        const viewerBody = (await viewerLogin.json()) as { token: string };
        viewerToken = viewerBody.token;

        const adminLogin = await app.fetch(
            new Request("http://localhost/api/auth/login", {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    username: "admin@osimi.local",
                    password: "admin123",
                }),
            }),
        );

        const adminBody = (await adminLogin.json()) as { token: string };
        adminToken = adminBody.token;
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

    test("lists tenant-scoped objects", async () => {
        const app = createTestApp();
        const response = await app.fetch(
            new Request(
                "http://localhost/api/objects?type=DOCUMENT&q=Tenant%20One%20Object",
                {
                    method: "GET",
                    headers: {
                        authorization: `Bearer ${operatorToken}`,
                    },
                },
            ),
        );

        expect(response.status).toBe(200);
        const body = (await response.json()) as {
            objects: Array<{
                id: string;
                object_id: string;
                title: string;
                processing_state: string;
                curation_state: string;
                availability_state: string;
                access_level: string;
                type: string;
                language: string | null;
                source_ingestion_id: string | null;
                source_batch_label: string | null;
                tags: string[];
                created_at: string;
                updated_at: string;
                ingest_manifest?: unknown;
            }>;
            total_count: number;
            filtered_count: number;
        };

        expect(body.objects.length).toBe(1);
        expect(body.total_count).toBe(3);
        expect(body.filtered_count).toBe(1);
        expect(body.objects[0]?.id).toBe(tenantOneObjectId);
        expect(body.objects[0]?.object_id).toBe(tenantOneObjectId);
        expect(body.objects[0]?.title).toBe("Tenant One Object");
        expect(body.objects[0]?.processing_state).toBe("queued");
        expect(body.objects[0]?.curation_state).toBe("needs_review");
        expect(body.objects[0]?.availability_state).toBe("AVAILABLE");
        expect(body.objects[0]?.access_level).toBe("private");
        expect(body.objects[0]?.type).toBe("DOCUMENT");
        expect(body.objects[0]?.language).toBeNull();
        expect(body.objects[0]?.source_ingestion_id).toBeNull();
        expect(body.objects[0]?.source_batch_label).toBeNull();
        expect(body.objects[0]?.tags).toEqual(["history"]);
        expect(typeof body.objects[0]?.created_at).toBe("string");
        expect(typeof body.objects[0]?.updated_at).toBe("string");
        expect(
            Object.prototype.hasOwnProperty.call(
                body.objects[0] ?? {},
                "ingest_manifest",
            ),
        ).toBe(false);
    });

    test("supports object list filters, sorting, and counts", async () => {
        const app = createTestApp();

        const filtered = await app.fetch(
            new Request(
                "http://localhost/api/objects?availability_state=ARCHIVED&language=en&batch_label=batch-alpha&tag=finance&sort=updated_at_desc",
                {
                    method: "GET",
                    headers: {
                        authorization: `Bearer ${operatorToken}`,
                    },
                },
            ),
        );

        expect(filtered.status).toBe(200);
        const filteredBody = (await filtered.json()) as {
            objects: Array<{
                object_id: string;
                source_batch_label: string | null;
                tags: string[];
                language: string | null;
            }>;
            total_count: number;
            filtered_count: number;
            next_cursor: string | null;
        };

        expect(filteredBody.total_count).toBe(3);
        expect(filteredBody.filtered_count).toBe(1);
        expect(filteredBody.objects.length).toBe(1);
        expect(filteredBody.objects[0]?.object_id).toBe(tenantOneObjectIdTwo);
        expect(filteredBody.objects[0]?.source_batch_label).toBe(
            "batch-alpha-2026",
        );
        expect(filteredBody.objects[0]?.tags).toEqual(["finance"]);
        expect(filteredBody.objects[0]?.language).toBe("en");
        expect(filteredBody.next_cursor).toBeNull();

        const firstPage = await app.fetch(
            new Request(
                "http://localhost/api/objects?limit=1&sort=created_at_desc",
                {
                    method: "GET",
                    headers: {
                        authorization: `Bearer ${operatorToken}`,
                    },
                },
            ),
        );

        expect(firstPage.status).toBe(200);
        const firstPageBody = (await firstPage.json()) as {
            objects: Array<{ object_id: string }>;
            total_count: number;
            filtered_count: number;
            next_cursor: string | null;
        };
        expect(firstPageBody.total_count).toBe(3);
        expect(firstPageBody.filtered_count).toBe(3);
        expect(firstPageBody.objects.length).toBe(1);
        expect(firstPageBody.objects[0]?.object_id).toBe(
            tenantOneObjectIdThree,
        );
        expect(typeof firstPageBody.next_cursor).toBe("string");

        const secondPage = await app.fetch(
            new Request(
                `http://localhost/api/objects?limit=1&sort=created_at_desc&cursor=${firstPageBody.next_cursor}`,
                {
                    method: "GET",
                    headers: {
                        authorization: `Bearer ${operatorToken}`,
                    },
                },
            ),
        );

        expect(secondPage.status).toBe(200);
        const secondPageBody = (await secondPage.json()) as {
            objects: Array<{ object_id: string }>;
            next_cursor: string | null;
        };
        expect(secondPageBody.objects.length).toBe(1);
        expect(secondPageBody.objects[0]?.object_id).toBe(tenantOneObjectIdTwo);
    });

    test("returns embargo curation fields and access decisions in list responses", async () => {
        const app = createTestApp();
        const sorts = [
            "created_at_desc",
            "created_at_asc",
            "updated_at_desc",
            "updated_at_asc",
            "title_asc",
            "title_desc",
        ];

        for (const sort of sorts) {
            const response = await app.fetch(
                new Request(
                    `http://localhost/api/objects?sort=${sort}&q=Summer%20Photo`,
                    {
                        method: "GET",
                        headers: {
                            authorization: `Bearer ${viewerToken}`,
                        },
                    },
                ),
            );

            expect(response.status).toBe(200);

            const body = (await response.json()) as {
                objects: Array<{
                    object_id: string;
                    embargo_kind: string;
                    embargo_curation_state: string | null;
                    can_download: boolean;
                    access_reason_code: string;
                }>;
            };

            expect(body.objects.length).toBe(1);
            expect(body.objects[0]?.object_id).toBe(tenantOneObjectIdThree);
            expect(body.objects[0]?.embargo_kind).toBe("curation_state");
            expect(body.objects[0]?.embargo_curation_state).toBe("reviewed");
            expect(body.objects[0]?.can_download).toBe(false);
            expect(body.objects[0]?.access_reason_code).toBe("EMBARGO_ACTIVE");
        }
    });

    test("returns object detail and blocks cross-tenant object", async () => {
        const app = createTestApp();

        const okResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${tenantOneObjectId}`, {
                method: "GET",
                headers: {
                    authorization: `Bearer ${operatorToken}`,
                },
            }),
        );

        expect(okResponse.status).toBe(200);
        const okBody = (await okResponse.json()) as {
            object: {
                ingest_manifest: {
                    schema_version: string;
                } | null;
            };
        };
        expect(okBody.object.ingest_manifest?.schema_version).toBe("1.0");

        const notFoundResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${tenantTwoObjectId}`, {
                method: "GET",
                headers: {
                    authorization: `Bearer ${operatorToken}`,
                },
            }),
        );

        expect(notFoundResponse.status).toBe(404);
    });

    test("patches title for archiver and blocks viewer", async () => {
        const app = createTestApp();

        const patchResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${tenantOneObjectId}`, {
                method: "PATCH",
                headers: {
                    authorization: `Bearer ${operatorToken}`,
                    "content-type": "application/json",
                },
                body: JSON.stringify({
                    title: "Retitled Object",
                }),
            }),
        );

        expect(patchResponse.status).toBe(200);
        const patchBody = (await patchResponse.json()) as {
            object: { title: string };
        };
        expect(patchBody.object.title).toBe("Retitled Object");

        const viewerPatchResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${tenantOneObjectId}`, {
                method: "PATCH",
                headers: {
                    authorization: `Bearer ${viewerToken}`,
                    "content-type": "application/json",
                },
                body: JSON.stringify({
                    title: "Viewer cannot patch",
                }),
            }),
        );

        expect(viewerPatchResponse.status).toBe(403);
    });

    test("lists and downloads object artifacts", async () => {
        const app = createTestApp();

        const listResponse = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectId}/artifacts`,
                {
                    method: "GET",
                    headers: {
                        authorization: `Bearer ${operatorToken}`,
                    },
                },
            ),
        );

        expect(listResponse.status).toBe(200);
        const listBody = (await listResponse.json()) as {
            artifacts: Array<{ id: string }>;
        };
        expect(listBody.artifacts.length).toBe(1);
        expect(listBody.artifacts[0]?.id).toBe(artifactId);

        console.log("ARTIFACT ID IS", artifactId);
        const downloadResponse = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectId}/artifacts/${artifactId}/download`,
                {
                    method: "GET",
                    headers: {
                        authorization: `Bearer ${adminToken}`,
                    },
                },
            ),
        );

        expect(downloadResponse.status).toBe(200);
        expect(downloadResponse.headers.get("content-type")).toBe(
            "application/json",
        );
        expect(await downloadResponse.text()).toBe('{"status":"ready"}');
    });

    test("queues object download request when artifact is missing and dedupes active request", async () => {
        const app = createTestApp();

        const firstResponse = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectId}/download-requests`,
                {
                    method: "POST",
                    headers: {
                        authorization: `Bearer ${viewerToken}`,
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        artifact_kind: "pdf",
                    }),
                },
            ),
        );

        expect(firstResponse.status).toBe(201);
        const firstBody = (await firstResponse.json()) as {
            status: "queued";
            request: { id: string; status: string; artifact_kind: string };
        };
        expect(firstBody.status).toBe("queued");
        expect(firstBody.request.status).toBe("PENDING");
        expect(firstBody.request.artifact_kind).toBe("pdf");

        const secondResponse = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectId}/download-requests`,
                {
                    method: "POST",
                    headers: {
                        authorization: `Bearer ${viewerToken}`,
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        artifact_kind: "pdf",
                    }),
                },
            ),
        );

        expect(secondResponse.status).toBe(200);
        const secondBody = (await secondResponse.json()) as {
            status: "queued";
            request: { id: string };
        };
        expect(secondBody.request.id).toBe(firstBody.request.id);

        const listResponse = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectId}/download-requests`,
                {
                    method: "GET",
                    headers: {
                        authorization: `Bearer ${viewerToken}`,
                    },
                },
            ),
        );

        expect(listResponse.status).toBe(200);
        const listBody = (await listResponse.json()) as {
            requests: Array<{ id: string; artifact_kind: string }>;
        };
        expect(listBody.requests.length).toBeGreaterThanOrEqual(1);
        expect(listBody.requests[0]?.id).toBe(firstBody.request.id);
        expect(listBody.requests[0]?.artifact_kind).toBe("pdf");
    });

    test("returns available when requested artifact already exists", async () => {
        const sql = createSqlClient(TEST_DATABASE_URL!);
        const existingPdfArtifactId = "60000000-0000-4000-8000-000000000099";
        const existingPdfStorageKey = `tenants/${tenantOneId}/objects/${tenantOneObjectId}/artifacts/source.pdf`;

        try {
            await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            await sql`
        INSERT INTO object_artifacts (id, object_id, kind, storage_key, content_type, size_bytes)
        VALUES (
          ${existingPdfArtifactId},
          ${tenantOneObjectId},
          ${"pdf"}::artifact_kind,
          ${existingPdfStorageKey},
          ${"application/pdf"},
          ${12345}
        )
      `;
        } finally {
            await sql.close();
        }

        const app = createTestApp();
        const response = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectId}/download-requests`,
                {
                    method: "POST",
                    headers: {
                        authorization: `Bearer ${viewerToken}`,
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        artifact_kind: "pdf",
                    }),
                },
            ),
        );

        expect(response.status).toBe(200);
        const body = (await response.json()) as {
            status: "available";
            artifact: { id: string; kind: string };
        };

        expect(body.status).toBe("available");
        expect(body.artifact.id).toBe(existingPdfArtifactId);
        expect(body.artifact.kind).toBe("pdf");
    });

    test("handles concurrent download request creation without duplicate queue rows", async () => {
        const app = createTestApp();

        const [first, second] = await Promise.all([
            app.fetch(
                new Request(
                    `http://localhost/api/objects/${tenantOneObjectIdTwo}/download-requests`,
                    {
                        method: "POST",
                        headers: {
                            authorization: `Bearer ${viewerToken}`,
                            "content-type": "application/json",
                        },
                        body: JSON.stringify({
                            artifact_kind: "web_version",
                        }),
                    },
                ),
            ),
            app.fetch(
                new Request(
                    `http://localhost/api/objects/${tenantOneObjectIdTwo}/download-requests`,
                    {
                        method: "POST",
                        headers: {
                            authorization: `Bearer ${viewerToken}`,
                            "content-type": "application/json",
                        },
                        body: JSON.stringify({
                            artifact_kind: "web_version",
                        }),
                    },
                ),
            ),
        ]);

        const statuses = [first.status, second.status].sort();
        expect(statuses).toEqual([200, 201]);

        const firstBody = (await first.json()) as {
            status: "queued";
            request: { id: string };
        };
        const secondBody = (await second.json()) as {
            status: "queued";
            request: { id: string };
        };

        expect(firstBody.status).toBe("queued");
        expect(secondBody.status).toBe("queued");
        expect(firstBody.request.id).toBe(secondBody.request.id);
    });

    test("supports admin-only access approvals and explicit assignment downloads", async () => {
        const app = createTestApp();

        const deniedBeforeApproval = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectId}/artifacts/${artifactId}/download`,
                {
                    method: "GET",
                    headers: {
                        authorization: `Bearer ${viewerToken}`,
                    },
                },
            ),
        );

        expect(deniedBeforeApproval.status).toBe(400);

        const createRequestResponse = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectId}/access-requests`,
                {
                    method: "POST",
                    headers: {
                        authorization: `Bearer ${viewerToken}`,
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        requested_level: "private",
                        reason: "Research usage",
                    }),
                },
            ),
        );

        expect(createRequestResponse.status).toBe(201);
        const createRequestBody = (await createRequestResponse.json()) as {
            request: { id: string; status: string };
        };
        expect(createRequestBody.request.status).toBe("PENDING");

        const archiverListResponse = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectId}/access-requests`,
                {
                    method: "GET",
                    headers: {
                        authorization: `Bearer ${operatorToken}`,
                    },
                },
            ),
        );

        expect(archiverListResponse.status).toBe(403);

        const approveResponse = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectId}/access-requests/${createRequestBody.request.id}/approve`,
                {
                    method: "POST",
                    headers: {
                        authorization: `Bearer ${adminToken}`,
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        decision_note: "Approved for this object",
                    }),
                },
            ),
        );

        expect(approveResponse.status).toBe(200);

        const listAssignmentsResponse = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectId}/access-assignments`,
                {
                    method: "GET",
                    headers: {
                        authorization: `Bearer ${adminToken}`,
                    },
                },
            ),
        );

        expect(listAssignmentsResponse.status).toBe(200);
        const listAssignmentsBody = (await listAssignmentsResponse.json()) as {
            assignments: Array<{ user_id: string; granted_level: string }>;
        };
        expect(listAssignmentsBody.assignments).toEqual(
            expect.arrayContaining([
                expect.objectContaining({
                    user_id: "10000000-0000-0000-0000-000000000002",
                    granted_level: "private",
                }),
            ]),
        );

        const allowedAfterApproval = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectId}/artifacts/${artifactId}/download`,
                {
                    method: "GET",
                    headers: {
                        authorization: `Bearer ${viewerToken}`,
                    },
                },
            ),
        );

        expect(allowedAfterApproval.status).toBe(200);
        expect(await allowedAfterApproval.text()).toBe('{"status":"ready"}');
    });

    test("rejects duplicate pending requests and re-approval of resolved request", async () => {
        const app = createTestApp();
        const targetObjectId = tenantOneObjectIdTwo;

        const firstCreate = await app.fetch(
            new Request(
                `http://localhost/api/objects/${targetObjectId}/access-requests`,
                {
                    method: "POST",
                    headers: {
                        authorization: `Bearer ${viewerToken}`,
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        requested_level: "family",
                        reason: "Need read access",
                    }),
                },
            ),
        );

        expect(firstCreate.status).toBe(201);
        const firstCreateBody = (await firstCreate.json()) as {
            request: { id: string; status: string };
        };
        expect(firstCreateBody.request.status).toBe("PENDING");

        const secondCreate = await app.fetch(
            new Request(
                `http://localhost/api/objects/${targetObjectId}/access-requests`,
                {
                    method: "POST",
                    headers: {
                        authorization: `Bearer ${viewerToken}`,
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        requested_level: "private",
                        reason: "Escalation",
                    }),
                },
            ),
        );

        expect(secondCreate.status).toBe(409);

        const approve = await app.fetch(
            new Request(
                `http://localhost/api/objects/${targetObjectId}/access-requests/${firstCreateBody.request.id}/approve`,
                {
                    method: "POST",
                    headers: {
                        authorization: `Bearer ${adminToken}`,
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({ decision_note: "Approved" }),
                },
            ),
        );

        expect(approve.status).toBe(200);

        const reapprove = await app.fetch(
            new Request(
                `http://localhost/api/objects/${targetObjectId}/access-requests/${firstCreateBody.request.id}/approve`,
                {
                    method: "POST",
                    headers: {
                        authorization: `Bearer ${adminToken}`,
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({ decision_note: "Again" }),
                },
            ),
        );

        expect(reapprove.status).toBe(409);
    });

    test("allows approve/reject with empty request body", async () => {
        const app = createTestApp();

        const createRequestResponse = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectId}/access-requests`,
                {
                    method: "POST",
                    headers: {
                        authorization: `Bearer ${viewerToken}`,
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        requested_level: "family",
                        reason: "Need access",
                    }),
                },
            ),
        );

        expect(createRequestResponse.status).toBe(201);
        const createRequestBody = (await createRequestResponse.json()) as {
            request: { id: string };
        };

        const approveResponse = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectId}/access-requests/${createRequestBody.request.id}/approve`,
                {
                    method: "POST",
                    headers: {
                        authorization: `Bearer ${adminToken}`,
                    },
                },
            ),
        );

        expect(approveResponse.status).toBe(200);

        const secondRequestResponse = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectId}/access-requests`,
                {
                    method: "POST",
                    headers: {
                        authorization: `Bearer ${viewerToken}`,
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        requested_level: "family",
                        reason: "Need access again",
                    }),
                },
            ),
        );

        expect(secondRequestResponse.status).toBe(201);
        const secondRequestBody = (await secondRequestResponse.json()) as {
            request: { id: string };
        };

        const rejectResponse = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectId}/access-requests/${secondRequestBody.request.id}/reject`,
                {
                    method: "POST",
                    headers: {
                        authorization: `Bearer ${adminToken}`,
                    },
                },
            ),
        );

        expect(rejectResponse.status).toBe(200);
    });
});
