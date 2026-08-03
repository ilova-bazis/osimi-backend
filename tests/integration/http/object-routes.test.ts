import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { sql as sqlIdentifier } from "bun";

import { createAppWithOptions as createApp } from "../../../src/app.ts";
import { createSqlClient } from "../../../src/db/client.ts";
import { runMigrations } from "../../../src/db/migrate.ts";
import { TEST_DATABASE_URL } from "../test-database.ts";

describe("object routes", () => {
    let schema = "";
    let stagingRoot = "";

    let operatorToken = "";
    let unassignedArchiverToken = "";
    let viewerToken = "";
    let adminToken = "";

    const tenantOneId = "00000000-0000-0000-0000-000000000001";
    const tenantTwoId = "00000000-0000-0000-0000-000000000002";
    const tenantOneObjectId = "OBJ-20260209-ABC123";
    const tenantOneObjectIdTwo = "OBJ-20260209-DEF456";
    const tenantOneObjectIdThree = "OBJ-20260209-GHI789";
    const editPolicyObjectId = "OBJ-20260209-POLICY1";
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
                workerAuthToken: "worker-secret",
                uploadSigningSecret: "object-routes-upload-signing-secret-000",
                leaseSigningSecret: "object-routes-lease-signing-secret-0000",
            },
        });
    }

    async function resetObjectEditState(params: {
        objectId: string;
        metadata: Record<string, unknown>;
        curationState?: string;
        updatedAtIso: string;
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
                INSERT INTO object_edits (object_id, revision, updated_at, updated_by)
                VALUES (${params.objectId}, 0, now(), NULL)
                ON CONFLICT (object_id)
                DO UPDATE SET revision = 0, updated_at = now(), updated_by = NULL
            `;
            await sql`
                UPDATE objects
                SET metadata = ${params.metadata},
                    curation_state = ${
                        params.curationState ?? "needs_review"
                    }::object_curation_state,
                    updated_at = ${params.updatedAtIso}::timestamptz
                WHERE object_id = ${params.objectId}
            `;
        } finally {
            await sql.close();
        }
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
            const unassignedArchiverHash = await Bun.password.hash("unassigned123");
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
          (${"10000000-0000-0000-0000-000000000004"}, ${"unassigned@osimi.local"}, ${"unassigned@osimi.local"}, ${unassignedArchiverHash}),
          (${"10000000-0000-0000-0000-000000000002"}, ${"viewer@osimi.local"}, ${"viewer@osimi.local"}, ${viewerHash}),
          (${"10000000-0000-0000-0000-000000000003"}, ${"admin@osimi.local"}, ${"admin@osimi.local"}, ${adminHash})
      `;

            await sql`
        INSERT INTO tenant_memberships (id, tenant_id, user_id, role)
        VALUES
          (${"20000000-0000-0000-0000-000000000001"}, ${tenantOneId}, ${"10000000-0000-0000-0000-000000000001"}, ${"archiver"}),
          (${"20000000-0000-0000-0000-000000000004"}, ${tenantOneId}, ${"10000000-0000-0000-0000-000000000004"}, ${"archiver"}),
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
        INSERT INTO object_access_assignments (object_id, tenant_id, user_id, granted_level, created_by)
        VALUES
          (${tenantOneObjectId}, ${tenantOneId}, ${"10000000-0000-0000-0000-000000000001"}, ${"private"}::object_access_granted_level, ${"10000000-0000-0000-0000-000000000003"}),
          (${tenantOneObjectIdTwo}, ${tenantOneId}, ${"10000000-0000-0000-0000-000000000001"}, ${"private"}::object_access_granted_level, ${"10000000-0000-0000-0000-000000000003"})
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

        const unassignedArchiverLogin = await app.fetch(
            new Request("http://localhost/api/auth/login", {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    username: "unassigned@osimi.local",
                    password: "unassigned123",
                }),
            }),
        );

        const unassignedArchiverBody = (await unassignedArchiverLogin.json()) as { token: string };
        unassignedArchiverToken = unassignedArchiverBody.token;

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
                thumbnail_artifact_id: string | null;
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
        expect(body.objects[0]?.thumbnail_artifact_id).toBeNull();
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
                thumbnail_artifact_id: string | null;
                ingest_manifest: {
                    schema_version: string;
                } | null;
            };
        };
        expect(okBody.object.thumbnail_artifact_id).toBeNull();
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

    test("returns viewer contract and serves inline view artifacts", async () => {
        const app = createTestApp();
        const sql = createSqlClient(TEST_DATABASE_URL!);
        const objectId = "OBJ-20260210-VIEW01";
        const pdfArtifactId = "60000000-0000-4000-8000-000000000410";
        const thumbnailArtifactId = "60000000-0000-4000-8000-000000000411";
        const ocrArtifactId = "60000000-0000-4000-8000-000000000412";
        const availableFileId = "70000000-0000-4000-8000-000000000410";
        const pdfStorageKey = `tenants/${tenantOneId}/objects/${objectId}/artifacts/viewer.pdf`;
        const thumbStorageKey = `tenants/${tenantOneId}/objects/${objectId}/artifacts/thumb.jpg`;
        const ocrStorageKey = `tenants/${tenantOneId}/objects/${objectId}/artifacts/ocr.txt`;

        try {
            await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            await sql`
        INSERT INTO objects (
          object_id,
          tenant_id,
          type,
          title,
          metadata,
          ingest_manifest,
          availability_state,
          access_level,
          language_code
        )
        VALUES (
          ${objectId},
          ${tenantOneId},
          ${"DOCUMENT"}::object_type,
          ${"Viewer Ready Document"},
          ${{ source: "archive-system" }},
          NULL,
          ${"AVAILABLE"}::object_availability_state,
          ${"public"}::object_access_level,
          ${"en"}
        )
      `;

            await sql`
        INSERT INTO object_artifacts (id, object_id, kind, variant, storage_key, content_type, size_bytes)
        VALUES
          (
            ${pdfArtifactId},
            ${objectId},
            ${"pdf"}::artifact_kind,
            NULL,
            ${pdfStorageKey},
            ${"application/pdf"},
            ${12}
          ),
          (
            ${thumbnailArtifactId},
            ${objectId},
            ${"thumbnail"}::artifact_kind,
            NULL,
            ${thumbStorageKey},
            ${"image/jpeg"},
            ${9}
          ),
          (
            ${ocrArtifactId},
            ${objectId},
            ${"ocr_text"}::artifact_kind,
            NULL,
            ${ocrStorageKey},
            ${"text/plain"},
            ${14}
          )
      `;

            await sql`
        INSERT INTO object_available_files (
          id,
          object_id,
          tenant_id,
          archive_file_key,
          artifact_kind,
          variant,
          display_name,
          content_type,
          size_bytes,
          metadata,
          is_available
        )
        VALUES (
          ${availableFileId},
          ${objectId},
          ${tenantOneId},
          ${"archive-pdf-viewer"},
          ${"pdf"}::artifact_kind,
          NULL,
          ${"Reading PDF"},
          ${"application/pdf"},
          ${12},
          ${{
              page_count: 2,
              pages: [
                  {
                      page_number: 1,
                      label: "1",
                      image_artifact_id: null,
                      ocr_text_artifact_id: null,
                  },
                  {
                      page_number: 2,
                      label: "2",
                      image_artifact_id: null,
                      ocr_text_artifact_id: null,
                  },
              ],
          }},
          true
        )
      `;
        } finally {
            await sql.close();
        }

        await mkdir(dirname(join(stagingRoot, pdfStorageKey)), { recursive: true });
        await Bun.write(join(stagingRoot, pdfStorageKey), "pdf-content\n");
        await Bun.write(join(stagingRoot, thumbStorageKey), "thumb-jpg\n");
        await Bun.write(join(stagingRoot, ocrStorageKey), "ocr-content\n");

        const detailResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${objectId}`, {
                method: "GET",
                headers: {
                    authorization: `Bearer ${viewerToken}`,
                },
            }),
        );

        expect(detailResponse.status).toBe(200);
        const detailBody = (await detailResponse.json()) as {
            viewer: {
                media_type: string;
                primary_source: {
                    source_type: string;
                    artifact_kind: string;
                    status: string;
                    artifact_id: string | null;
                    available_file_id: string | null;
                };
                preview_artifacts: {
                    thumbnail: { artifact_id: string } | null;
                    ocr_text: { artifact_id: string } | null;
                };
                viewer_payload: {
                    kind: string;
                    artifact_id: string | null;
                    page_count: number | null;
                    pages?: Array<{ page_number: number; label: string | null }>;
                };
            } | null;
        };

        expect(detailBody.viewer?.media_type).toBe("document");
        expect(detailBody.viewer?.primary_source.source_type).toBe("access_copy");
        expect(detailBody.viewer?.primary_source.artifact_kind).toBe("pdf");
        expect(detailBody.viewer?.primary_source.status).toBe("available");
        expect(detailBody.viewer?.primary_source.artifact_id).toBe(pdfArtifactId);
        expect(detailBody.viewer?.primary_source.available_file_id).toBe(
            availableFileId,
        );
        expect(detailBody.viewer?.preview_artifacts.thumbnail?.artifact_id).toBe(
            thumbnailArtifactId,
        );
        expect(detailBody.viewer?.preview_artifacts.ocr_text?.artifact_id).toBe(
            ocrArtifactId,
        );
        expect(detailBody.viewer?.viewer_payload.kind).toBe("document");
        expect(detailBody.viewer?.viewer_payload.artifact_id).toBe(pdfArtifactId);
        expect(detailBody.viewer?.viewer_payload.page_count).toBe(2);
        expect(detailBody.viewer?.viewer_payload.pages?.map((page) => page.page_number)).toEqual([
            1,
            2,
        ]);

        const viewResponse = await app.fetch(
            new Request(
                `http://localhost/api/objects/${objectId}/artifacts/${pdfArtifactId}/view`,
                {
                    method: "GET",
                    headers: {
                        authorization: `Bearer ${viewerToken}`,
                    },
                },
            ),
        );

        expect(viewResponse.status).toBe(200);
        expect(viewResponse.headers.get("content-type")).toBe("application/pdf");
        expect(viewResponse.headers.get("content-disposition")).toContain("inline");
        expect(viewResponse.headers.get("content-length")).toBe("12");
        expect(viewResponse.headers.get("accept-ranges")).toBe("bytes");
        expect(viewResponse.headers.get("content-range")).toBeNull();
        const etag = viewResponse.headers.get("etag");
        const lastModified = viewResponse.headers.get("last-modified");
        expect(etag).toBe(`"artifact-${pdfArtifactId}"`);
        expect(lastModified).not.toBeNull();
        expect(await viewResponse.text()).toBe("pdf-content\n");

        const viewUrl = `http://localhost/api/objects/${objectId}/artifacts/${pdfArtifactId}/view`;
        const rangeResponse = await app.fetch(new Request(viewUrl, {
            headers: { authorization: `Bearer ${viewerToken}`, range: "bytes=0-2" },
        }));
        expect(rangeResponse.status).toBe(206);
        expect(rangeResponse.headers.get("content-range")).toBe("bytes 0-2/12");
        expect(rangeResponse.headers.get("content-length")).toBe("3");
        expect(await rangeResponse.text()).toBe("pdf");

        const openEndedResponse = await app.fetch(new Request(viewUrl, {
            headers: { authorization: `Bearer ${viewerToken}`, range: "bytes=3-" },
        }));
        expect(openEndedResponse.status).toBe(206);
        expect(openEndedResponse.headers.get("content-range")).toBe("bytes 3-11/12");
        expect(await openEndedResponse.text()).toBe("-content\n");

        const suffixResponse = await app.fetch(new Request(viewUrl, {
            headers: { authorization: `Bearer ${viewerToken}`, range: "bytes=-3" },
        }));
        expect(suffixResponse.status).toBe(206);
        expect(suffixResponse.headers.get("content-range")).toBe("bytes 9-11/12");
        expect(await suffixResponse.text()).toBe("nt\n");

        const unsatisfiableResponse = await app.fetch(new Request(viewUrl, {
            headers: { authorization: `Bearer ${viewerToken}`, range: "bytes=12-" },
        }));
        expect(unsatisfiableResponse.status).toBe(416);
        expect(unsatisfiableResponse.headers.get("content-range")).toBe("bytes */12");
        expect(await unsatisfiableResponse.text()).toBe("");

        const matchingIfRangeResponse = await app.fetch(new Request(viewUrl, {
            headers: {
                authorization: `Bearer ${viewerToken}`,
                range: "bytes=0-2",
                "if-range": etag!,
            },
        }));
        expect(matchingIfRangeResponse.status).toBe(206);

        const staleIfRangeResponse = await app.fetch(new Request(viewUrl, {
            headers: {
                authorization: `Bearer ${viewerToken}`,
                range: "bytes=0-2",
                "if-range": '"stale"',
            },
        }));
        expect(staleIfRangeResponse.status).toBe(200);
        expect(staleIfRangeResponse.headers.get("content-range")).toBeNull();
        expect(await staleIfRangeResponse.text()).toBe("pdf-content\n");
    });

    test("returns request-required viewer state and rejects non-viewable artifacts", async () => {
        const app = createTestApp();
        const sql = createSqlClient(TEST_DATABASE_URL!);
        const objectId = "OBJ-20260210-VIEW02";
        const availableFileId = "70000000-0000-4000-8000-000000000420";

        try {
            await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            await sql`
        INSERT INTO objects (
          object_id,
          tenant_id,
          type,
          title,
          metadata,
          ingest_manifest,
          availability_state,
          access_level
        )
        VALUES (
          ${objectId},
          ${tenantOneId},
          ${"DOCUMENT"}::object_type,
          ${"Archived Viewer Document"},
          ${{ source: "archive-system" }},
          NULL,
          ${"ARCHIVED"}::object_availability_state,
          ${"public"}::object_access_level
        )
      `;

            await sql`
        INSERT INTO object_available_files (
          id,
          object_id,
          tenant_id,
          archive_file_key,
          artifact_kind,
          variant,
          display_name,
          content_type,
          size_bytes,
          metadata,
          is_available
        )
        VALUES (
          ${availableFileId},
          ${objectId},
          ${tenantOneId},
          ${"archive-pdf-requestable"},
          ${"pdf"}::artifact_kind,
          NULL,
          ${"Requestable PDF"},
          ${"application/pdf"},
          ${64},
          ${{ page_count: 4 }},
          true
        )
      `;
        } finally {
            await sql.close();
        }

        const detailResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${objectId}`, {
                method: "GET",
                headers: {
                    authorization: `Bearer ${viewerToken}`,
                },
            }),
        );

        expect(detailResponse.status).toBe(200);
        const detailBody = (await detailResponse.json()) as {
            viewer: {
                primary_source: {
                    status: string;
                    artifact_id: string | null;
                    available_file_id: string | null;
                };
                active_request: unknown;
            } | null;
        };

        expect(detailBody.viewer?.primary_source.status).toBe("request_required");
        expect(detailBody.viewer?.primary_source.artifact_id).toBeNull();
        expect(detailBody.viewer?.primary_source.available_file_id).toBe(
            availableFileId,
        );
        expect(detailBody.viewer?.active_request).toBeNull();

        const rejectedViewResponse = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectId}/artifacts/${artifactId}/view`,
                {
                    method: "GET",
                    headers: {
                        authorization: `Bearer ${adminToken}`,
                    },
                },
            ),
        );

        expect(rejectedViewResponse.status).toBe(409);
    });

    test("includes preferred thumbnail_artifact_id in list and detail", async () => {
        const app = createTestApp();
        const sql = createSqlClient(TEST_DATABASE_URL!);
        const variantThumbnailId = "60000000-0000-4000-8000-000000000310";
        const nullVariantThumbnailId = "60000000-0000-4000-8000-000000000311";

        try {
            await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            await sql`
        INSERT INTO object_artifacts (id, object_id, kind, variant, storage_key, content_type, size_bytes)
        VALUES
          (
            ${variantThumbnailId},
            ${tenantOneObjectIdThree},
            ${"thumbnail"}::artifact_kind,
            ${"small"},
            ${`tenants/${tenantOneId}/objects/${tenantOneObjectIdThree}/artifacts/thumb-small.jpg`},
            ${"image/jpeg"},
            ${1024}
          ),
          (
            ${nullVariantThumbnailId},
            ${tenantOneObjectIdThree},
            ${"thumbnail"}::artifact_kind,
            ${null},
            ${`tenants/${tenantOneId}/objects/${tenantOneObjectIdThree}/artifacts/thumb-primary.jpg`},
            ${"image/jpeg"},
            ${2048}
          )
      `;

            await sql`
        UPDATE object_artifacts
        SET created_at = ${"2026-02-12T10:00:00.000Z"}::timestamptz
        WHERE id = ${variantThumbnailId}
      `;

            await sql`
        UPDATE object_artifacts
        SET created_at = ${"2026-02-10T10:00:00.000Z"}::timestamptz
        WHERE id = ${nullVariantThumbnailId}
      `;
        } finally {
            await sql.close();
        }

        const listResponse = await app.fetch(
            new Request("http://localhost/api/objects?q=Summer%20Photo", {
                method: "GET",
                headers: {
                    authorization: `Bearer ${viewerToken}`,
                },
            }),
        );

        expect(listResponse.status).toBe(200);
        const listBody = (await listResponse.json()) as {
            objects: Array<{
                object_id: string;
                thumbnail_artifact_id: string | null;
            }>;
        };

        expect(listBody.objects[0]?.object_id).toBe(tenantOneObjectIdThree);
        expect(listBody.objects[0]?.thumbnail_artifact_id).toBe(
            nullVariantThumbnailId,
        );

        const detailResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${tenantOneObjectIdThree}`, {
                method: "GET",
                headers: {
                    authorization: `Bearer ${viewerToken}`,
                },
            }),
        );

        expect(detailResponse.status).toBe(200);
        const detailBody = (await detailResponse.json()) as {
            object: { thumbnail_artifact_id: string | null };
        };

        expect(detailBody.object.thumbnail_artifact_id).toBe(
            nullVariantThumbnailId,
        );
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

    test("enforces object assignment policy across edit operations", async () => {
        const app = createTestApp();
        const sql = createSqlClient(TEST_DATABASE_URL!);
        const metadataBody = {
            revision: 0,
            metadata: {
                title: "Denied edit",
                publication_date: "",
                date_precision: "none",
                date_approximate: false,
                language: null,
                tags: [],
                people: [],
                description: null,
            },
            rights: {
                rights_note: null,
                sensitivity_note: null,
            },
        };
        const protectedRequests = [
            new Request(`http://localhost/api/objects/${editPolicyObjectId}`, {
                method: "PATCH",
                headers: { authorization: `Bearer ${unassignedArchiverToken}`, "content-type": "application/json" },
                body: JSON.stringify({ title: "Denied title" }),
            }),
            new Request(`http://localhost/api/objects/${editPolicyObjectId}/edit`, {
                headers: { authorization: `Bearer ${unassignedArchiverToken}` },
            }),
            new Request(`http://localhost/api/objects/${editPolicyObjectId}/metadata`, {
                method: "PATCH",
                headers: { authorization: `Bearer ${unassignedArchiverToken}`, "content-type": "application/json" },
                body: JSON.stringify(metadataBody),
            }),
            new Request(`http://localhost/api/objects/${editPolicyObjectId}/curation/document`, {
                method: "PUT",
                headers: { authorization: `Bearer ${unassignedArchiverToken}`, "content-type": "application/json" },
                body: JSON.stringify({ revision: 0, pages: [{ page_number: 1, curated_text: "Denied" }] }),
            }),
            new Request(`http://localhost/api/objects/${editPolicyObjectId}/curation/submit`, {
                method: "POST",
                headers: { authorization: `Bearer ${unassignedArchiverToken}`, "content-type": "application/json" },
                body: JSON.stringify({ revision: 0, review_note: null }),
            }),
            new Request(`http://localhost/api/objects/${editPolicyObjectId}/edit-lock`, {
                method: "DELETE",
                headers: { authorization: `Bearer ${unassignedArchiverToken}` },
            }),
            new Request(`http://localhost/api/objects/${editPolicyObjectId}/curation/history`, {
                headers: { authorization: `Bearer ${unassignedArchiverToken}` },
            }),
        ];

        try {
            await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            await sql`
                INSERT INTO objects (object_id, tenant_id, type, title, metadata, availability_state)
                VALUES (
                    ${editPolicyObjectId},
                    ${tenantOneId},
                    ${"DOCUMENT"}::object_type,
                    ${"Edit Policy Object"},
                    ${{}},
                    ${"AVAILABLE"}::object_availability_state
                )
            `;

            const protectedResponses = await Promise.all(protectedRequests.map((request) => app.fetch(request)));
            expect(protectedResponses.map((response) => response.status)).toEqual([403, 403, 403, 403, 403, 403, 403]);

            const untouched = await sql<Array<{ title: string; event_count: string; page_count: string }>>`
                SELECT obj.title,
                  (SELECT count(*)::text FROM object_edit_events WHERE object_id = obj.object_id) AS event_count,
                  (SELECT count(*)::text FROM object_curated_document_pages WHERE object_id = obj.object_id) AS page_count
                FROM objects obj
                WHERE obj.object_id = ${editPolicyObjectId}
            `;
            expect(untouched[0]).toEqual({ title: "Edit Policy Object", event_count: "0", page_count: "0" });

            await sql`
                INSERT INTO object_access_assignments (object_id, tenant_id, user_id, granted_level, created_by)
                VALUES (${editPolicyObjectId}, ${tenantOneId}, ${"10000000-0000-0000-0000-000000000004"}, ${"family"}::object_access_granted_level, ${"10000000-0000-0000-0000-000000000003"})
            `;
            await sql`
                UPDATE objects
                SET access_level = ${"family"}::object_access_level
                WHERE object_id = ${editPolicyObjectId}
            `;

            const familyEdit = await app.fetch(
                new Request(`http://localhost/api/objects/${editPolicyObjectId}`, {
                    method: "PATCH",
                    headers: { authorization: `Bearer ${unassignedArchiverToken}`, "content-type": "application/json" },
                    body: JSON.stringify({ title: "Family editor title" }),
                }),
            );
            expect(familyEdit.status).toBe(200);

            await sql`
                UPDATE objects
                SET access_level = ${"private"}::object_access_level
                WHERE object_id = ${editPolicyObjectId}
            `;
            const familyPrivateEdit = await app.fetch(
                new Request(`http://localhost/api/objects/${editPolicyObjectId}/edit`, {
                    headers: { authorization: `Bearer ${unassignedArchiverToken}` },
                }),
            );
            expect(familyPrivateEdit.status).toBe(403);

            await sql`
                UPDATE object_access_assignments
                SET granted_level = ${"private"}::object_access_granted_level
                WHERE object_id = ${editPolicyObjectId}
                  AND user_id = ${"10000000-0000-0000-0000-000000000004"}
            `;
            const privateEdit = await app.fetch(
                new Request(`http://localhost/api/objects/${editPolicyObjectId}/edit`, {
                    headers: { authorization: `Bearer ${unassignedArchiverToken}` },
                }),
            );
            expect(privateEdit.status).toBe(200);

            const adminPrivateEdit = await app.fetch(
                new Request(`http://localhost/api/objects/${editPolicyObjectId}/edit`, {
                    headers: { authorization: `Bearer ${adminToken}` },
                }),
            );
            expect(adminPrivateEdit.status).toBe(200);

            const releaseLock = await app.fetch(
                new Request(`http://localhost/api/objects/${editPolicyObjectId}/edit-lock`, {
                    method: "DELETE",
                    headers: { authorization: `Bearer ${unassignedArchiverToken}` },
                }),
            );
            expect(releaseLock.status).toBe(200);

            await sql`
                DELETE FROM object_access_assignments
                WHERE object_id = ${editPolicyObjectId}
                  AND user_id = ${"10000000-0000-0000-0000-000000000004"}
            `;
            await sql`
                UPDATE objects
                SET access_level = ${"public"}::object_access_level
                WHERE object_id = ${editPolicyObjectId}
            `;
            const publicEdit = await app.fetch(
                new Request(`http://localhost/api/objects/${editPolicyObjectId}/edit`, {
                    headers: { authorization: `Bearer ${unassignedArchiverToken}` },
                }),
            );
            expect(publicEdit.status).toBe(200);

            const publicHistory = await app.fetch(
                new Request(`http://localhost/api/objects/${editPolicyObjectId}/curation/history`, {
                    headers: { authorization: `Bearer ${viewerToken}` },
                }),
            );
            expect(publicHistory.status).toBe(200);

            await sql`
                UPDATE objects
                SET access_level = ${"private"}::object_access_level
                WHERE object_id = ${editPolicyObjectId}
            `;
            await sql`
                INSERT INTO object_access_assignments (object_id, tenant_id, user_id, granted_level, created_by)
                VALUES (${editPolicyObjectId}, ${tenantOneId}, ${"10000000-0000-0000-0000-000000000002"}, ${"private"}::object_access_granted_level, ${"10000000-0000-0000-0000-000000000003"})
            `;
            const assignedViewerHistory = await app.fetch(
                new Request(`http://localhost/api/objects/${editPolicyObjectId}/curation/history`, {
                    headers: { authorization: `Bearer ${viewerToken}` },
                }),
            );
            expect(assignedViewerHistory.status).toBe(200);

            const crossTenantEdit = await app.fetch(
                new Request(`http://localhost/api/objects/${tenantTwoObjectId}/edit`, {
                    headers: { authorization: `Bearer ${adminToken}` },
                }),
            );
            expect(crossTenantEdit.status).toBe(404);
        } finally {
            await sql`
                DELETE FROM objects
                WHERE object_id = ${editPolicyObjectId}
            `;
            await sql.close();
        }
    });

    test("returns object edit payload with metadata foundation", async () => {
        const app = createTestApp();

        const response = await app.fetch(
            new Request(`http://localhost/api/objects/${tenantOneObjectIdTwo}/edit`, {
                headers: {
                    authorization: `Bearer ${operatorToken}`,
                },
            }),
        );

        expect(response.status).toBe(200);
        const body = (await response.json()) as {
            object_id: string;
            media_type: string;
            revision: number;
            draft: { updated_at: string; updated_by: string | null } | null;
            metadata: {
                title: string;
                publication_date: string;
                date_precision: string;
                date_approximate: boolean;
                language: string | null;
                tags: string[];
                people: string[];
                description: string | null;
            };
            rights: {
                access_level: string;
                rights_note: string | null;
                sensitivity_note: string | null;
            };
            capabilities: {
                can_edit_metadata: boolean;
                can_curate_text: boolean;
                can_submit_review: boolean;
            };
            curation_payload: {
                kind: "document";
                machine_ocr_artifact_id: string | null;
                page_count: number | null;
                pages: Array<{
                    page_number: number;
                    label: string | null;
                    machine_text: string;
                    curated_text: string | null;
                    status?: string;
                }>;
            };
        };

        expect(body.object_id).toBe(tenantOneObjectIdTwo);
        expect(body.media_type).toBe("document");
        expect(body.revision).toBe(0);
        expect(body.draft).toBeNull();
        expect(body.metadata).toEqual({
            title: "Project Ledger",
            publication_date: "",
            date_precision: "none",
            date_approximate: false,
            language: "en",
            tags: ["finance"],
            people: [],
            description: null,
        });
        expect(body.rights).toEqual({
            access_level: "private",
            rights_note: null,
            sensitivity_note: null,
        });
        expect(body.capabilities).toEqual({
            can_edit_metadata: true,
            can_curate_text: true,
            can_submit_review: true,
        });
        expect(body.curation_payload.kind).toBe("document");
        expect(body.curation_payload.machine_ocr_artifact_id).toBeNull();
        expect(body.curation_payload.page_count).toBeNull();
        expect(body.curation_payload.pages).toEqual([]);
    });

    test("supports document OCR page editing through edit payload and curation write", async () => {
        const app = createTestApp();
        const sql = createSqlClient(TEST_DATABASE_URL!);
        const pageOneArtifactId = "60000000-0000-4000-8000-000000000881";
        const pageTwoArtifactId = "60000000-0000-4000-8000-000000000882";
        const pageOneStorageKey = `tenants/${tenantOneId}/objects/${tenantOneObjectIdTwo}/artifacts/ocr-page-1.txt`;
        const pageTwoStorageKey = `tenants/${tenantOneId}/objects/${tenantOneObjectIdTwo}/artifacts/ocr-page-2.txt`;

        try {
            await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            await sql`
                INSERT INTO object_artifacts (id, object_id, kind, variant, storage_key, content_type, size_bytes)
                VALUES
                    (
                        ${pageOneArtifactId},
                        ${tenantOneObjectIdTwo},
                        ${"ocr_text"}::artifact_kind,
                        ${"page-1"},
                        ${pageOneStorageKey},
                        ${"text/plain"},
                        ${20}
                    ),
                    (
                        ${pageTwoArtifactId},
                        ${tenantOneObjectIdTwo},
                        ${"ocr_text"}::artifact_kind,
                        ${"page-2"},
                        ${pageTwoStorageKey},
                        ${"text/plain"},
                        ${22}
                    )
                ON CONFLICT (storage_key)
                DO NOTHING
            `;
            await sql`
                UPDATE objects
                SET metadata = COALESCE(metadata, '{}'::jsonb) || ${{page_count: 2, pages: [
                        {
                            page_number: 1,
                            label: "1",
                            ocr_text_artifact_id: pageOneArtifactId,
                        },
                        {
                            page_number: 2,
                            label: "2",
                            ocr_text_artifact_id: pageTwoArtifactId,
                        },
                    ]}}
                WHERE object_id = ${tenantOneObjectIdTwo}
            `;
        } finally {
            await sql.close();
        }

        await mkdir(dirname(join(stagingRoot, pageOneStorageKey)), { recursive: true });
        await Bun.write(join(stagingRoot, pageOneStorageKey), "machine page 1\n");
        await Bun.write(join(stagingRoot, pageTwoStorageKey), "machine page 2\n");

        const initialEditResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${tenantOneObjectIdTwo}/edit`, {
                headers: {
                    authorization: `Bearer ${operatorToken}`,
                },
            }),
        );

        expect(initialEditResponse.status).toBe(200);
        const initialEditBody = (await initialEditResponse.json()) as {
            revision: number;
            curation_payload: {
                kind: string;
                machine_ocr_artifact_id: string | null;
                page_count: number | null;
                pages: Array<{
                    page_number: number;
                    label: string | null;
                    machine_text: string;
                    curated_text: string | null;
                    status?: string;
                }>;
            };
        };

        expect(initialEditBody.revision).toBe(0);
        expect(initialEditBody.curation_payload.kind).toBe("document");
        if (initialEditBody.curation_payload.kind !== "document") {
            throw new Error("Expected document curation payload");
        }
        expect(initialEditBody.curation_payload.machine_ocr_artifact_id).toBe(
            pageOneArtifactId,
        );
        expect(initialEditBody.curation_payload.page_count).toBe(2);
        expect(initialEditBody.curation_payload.pages).toEqual([
            {
                page_number: 1,
                label: "1",
                machine_text: "machine page 1\n",
                curated_text: null,
                status: "machine",
            },
            {
                page_number: 2,
                label: "2",
                machine_text: "machine page 2\n",
                curated_text: null,
                status: "machine",
            },
        ]);

        const updateResponse = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectIdTwo}/curation/document`,
                {
                    method: "PUT",
                    headers: {
                        authorization: `Bearer ${operatorToken}`,
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        revision: 0,
                        pages: [
                            {
                                page_number: 1,
                                curated_text: "curated page 1",
                            },
                            {
                                page_number: 2,
                                curated_text: "curated page 2",
                            },
                        ],
                    }),
                },
            ),
        );

        expect(updateResponse.status).toBe(200);
        const updateBody = (await updateResponse.json()) as {
            object_id: string;
            revision: number;
            updated_count: number;
            updated_at: string;
        };
        expect(updateBody.object_id).toBe(tenantOneObjectIdTwo);
        expect(updateBody.revision).toBe(1);
        expect(updateBody.updated_count).toBe(2);
        expect(Date.parse(updateBody.updated_at)).not.toBeNaN();

        const updatedEditResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${tenantOneObjectIdTwo}/edit`, {
                headers: {
                    authorization: `Bearer ${operatorToken}`,
                },
            }),
        );

        expect(updatedEditResponse.status).toBe(200);
        const updatedEditBody = (await updatedEditResponse.json()) as {
            revision: number;
            curation_payload: {
                kind: string;
                pages: Array<{
                    page_number: number;
                    label: string | null;
                    machine_text: string;
                    curated_text: string | null;
                    status?: string;
                }>;
            };
        };
        expect(updatedEditBody.revision).toBe(1);
        expect(updatedEditBody.curation_payload.kind).toBe("document");
        expect(updatedEditBody.curation_payload.pages).toEqual([
            {
                page_number: 1,
                label: "1",
                machine_text: "machine page 1\n",
                curated_text: "curated page 1",
                status: "edited",
            },
            {
                page_number: 2,
                label: "2",
                machine_text: "machine page 2\n",
                curated_text: "curated page 2",
                status: "edited",
            },
        ]);

        const historyResponse = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectIdTwo}/curation/history`,
                {
                    headers: {
                        authorization: `Bearer ${operatorToken}`,
                    },
                },
            ),
        );

        expect(historyResponse.status).toBe(200);
        const historyBody = (await historyResponse.json()) as {
            events: Array<{
                type: string;
                revision_before: number;
                revision_after: number;
                payload: { page_numbers?: number[] };
            }>;
        };
        expect(historyBody.events[0]?.type).toBe("DOCUMENT_PAGE_UPDATED");
        expect(historyBody.events[0]?.revision_before).toBe(0);
        expect(historyBody.events[0]?.revision_after).toBe(1);
        expect(historyBody.events[0]?.payload.page_numbers).toEqual([1, 2]);

        const cleanupSql = createSqlClient(TEST_DATABASE_URL!);
        try {
            await cleanupSql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            await cleanupSql`
                DELETE FROM object_curated_document_pages WHERE object_id = ${tenantOneObjectIdTwo}
            `;
            await cleanupSql`
                DELETE FROM object_edit_events WHERE object_id = ${tenantOneObjectIdTwo}
            `;
            await cleanupSql`
                INSERT INTO object_edits (object_id, revision, updated_at, updated_by)
                VALUES (${tenantOneObjectIdTwo}, 0, now(), NULL)
                ON CONFLICT (object_id)
                DO UPDATE SET revision = 0, updated_at = now(), updated_by = NULL
            `;
            await cleanupSql`
                UPDATE objects
                SET metadata = ${{source: "scanner-b"}},
                    curation_state = ${"needs_review"}::object_curation_state,
                    updated_at = ${"2026-02-12T12:00:00.000Z"}::timestamptz
                WHERE object_id = ${tenantOneObjectIdTwo}
            `;
            await cleanupSql`
                DELETE FROM object_artifacts WHERE object_id = ${tenantOneObjectIdTwo}
            `;
        } finally {
            await cleanupSql.close();
        }
    });

    test("submits document OCR curation as curation_apply request", async () => {
        const app = createTestApp();
        const sql = createSqlClient(TEST_DATABASE_URL!);
        const pageOneArtifactId = "60000000-0000-4000-8000-000000000883";
        const pageOneStorageKey = `tenants/${tenantOneId}/objects/${tenantOneObjectId}/artifacts/ocr-submit-page-1.txt`;

        try {
            await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            await sql`
                INSERT INTO object_artifacts (id, object_id, kind, variant, storage_key, content_type, size_bytes)
                VALUES (
                    ${pageOneArtifactId},
                    ${tenantOneObjectId},
                    ${"ocr_text"}::artifact_kind,
                    ${"page-1"},
                    ${pageOneStorageKey},
                    ${"text/plain"},
                    ${20}
                )
                ON CONFLICT (storage_key)
                DO NOTHING
            `;
            await sql`
                UPDATE objects
                SET metadata = COALESCE(metadata, '{}'::jsonb) || ${{page_count: 1, pages: [
                        {
                            page_number: 1,
                            label: "1",
                            ocr_text_artifact_id: pageOneArtifactId,
                        },
                    ]}}
                WHERE object_id = ${tenantOneObjectId}
            `;
        } finally {
            await sql.close();
        }

        await mkdir(dirname(join(stagingRoot, pageOneStorageKey)), { recursive: true });
        await Bun.write(join(stagingRoot, pageOneStorageKey), "machine submit page 1\n");

        const saveResponse = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectId}/curation/document`,
                {
                    method: "PUT",
                    headers: {
                        authorization: `Bearer ${operatorToken}`,
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        revision: 0,
                        pages: [
                            {
                                page_number: 1,
                                curated_text: "curated submit page 1",
                            },
                        ],
                    }),
                },
            ),
        );
        expect(saveResponse.status).toBe(200);

        const submitResponse = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectId}/curation/submit`,
                {
                    method: "POST",
                    headers: {
                        authorization: `Bearer ${operatorToken}`,
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        revision: 1,
                        review_note: "Ready for archive apply.",
                    }),
                },
            ),
        );

        expect(submitResponse.status).toBe(200);
        const submitBody = (await submitResponse.json()) as {
            object_id: string;
            revision: number;
            curation_state: string;
            request: {
                id: string;
                action_type: string;
                status: string;
            };
            submitted_at: string;
            submitted_by: string;
        };

        expect(submitBody.object_id).toBe(tenantOneObjectId);
        expect(submitBody.revision).toBe(2);
        expect(submitBody.curation_state).toBe("review_in_progress");
        expect(submitBody.request.action_type).toBe("curation_apply");
        expect(submitBody.request.status).toBe("PENDING");
        expect(Date.parse(submitBody.submitted_at)).not.toBeNaN();
        expect(submitBody.submitted_by).toBe(
            "10000000-0000-0000-0000-000000000001",
        );

        const historyResponse = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectId}/curation/history`,
                {
                    headers: {
                        authorization: `Bearer ${operatorToken}`,
                    },
                },
            ),
        );
        expect(historyResponse.status).toBe(200);
        const historyBody = (await historyResponse.json()) as {
            events: Array<{
                type: string;
                revision_before: number;
                revision_after: number;
                payload: { request_id?: string; review_note?: string | null };
            }>;
        };
        expect(historyBody.events[0]?.type).toBe("CURATION_SUBMITTED");
        expect(historyBody.events[0]?.revision_before).toBe(1);
        expect(historyBody.events[0]?.revision_after).toBe(2);
        expect(historyBody.events[0]?.payload.request_id).toBe(
            submitBody.request.id,
        );
        expect(historyBody.events[0]?.payload.review_note).toBe(
            "Ready for archive apply.",
        );

        const verifySql = createSqlClient(TEST_DATABASE_URL!);
        try {
            await verifySql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            const archiveRows = await verifySql<
                Array<{
                    id: string;
                    target_id: string;
                    action_type: "curation_apply";
                    requested_by: string;
                    dedupe_key: string | null;
                    status: "PENDING" | "PROCESSING" | "COMPLETED" | "FAILED" | "CANCELED";
                }>
            >`
                SELECT id, target_id, action_type, requested_by, dedupe_key, status
                FROM archive_requests
                WHERE id = ${submitBody.request.id}
                LIMIT 1
            `;

            expect(archiveRows).toHaveLength(1);
            expect(archiveRows[0]?.target_id).toBe(tenantOneObjectId);
            expect(archiveRows[0]?.action_type).toBe("curation_apply");
            expect(archiveRows[0]?.requested_by).toBe(submitBody.submitted_by);
            expect(archiveRows[0]?.dedupe_key).toContain(`${tenantOneObjectId}:ocr_curated:`);
            expect(archiveRows[0]?.status).toBe("PENDING");
        } finally {
            await verifySql.close();
        }

        const cleanupSql = createSqlClient(TEST_DATABASE_URL!);
        try {
            await cleanupSql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            await cleanupSql`
                DELETE FROM archive_requests WHERE tenant_id = ${tenantOneId}
                    AND action_type = ${"curation_apply"}::archive_request_action_type
            `;
            await cleanupSql`
                DELETE FROM object_curated_document_pages WHERE object_id = ${tenantOneObjectId}
            `;
            await cleanupSql`
                DELETE FROM object_edit_events WHERE object_id = ${tenantOneObjectId}
            `;
            await cleanupSql`
                INSERT INTO object_edits (object_id, revision, updated_at, updated_by)
                VALUES (${tenantOneObjectId}, 0, now(), NULL)
                ON CONFLICT (object_id)
                DO UPDATE SET revision = 0, updated_at = now(), updated_by = NULL
            `;
            await cleanupSql`
                UPDATE objects
                SET metadata = ${{source: "scanner-a"}},
                    curation_state = ${"needs_review"}::object_curation_state,
                    updated_at = ${"2026-02-09T10:00:00.000Z"}::timestamptz
                WHERE object_id = ${tenantOneObjectId}
            `;
            await cleanupSql`
                DELETE FROM object_artifacts WHERE object_id = ${tenantOneObjectId}
                    AND storage_key LIKE '%ocr-submit-page%'
            `;
        } finally {
            await cleanupSql.close();
        }
    });

    test("updates object metadata with revisioning and records history", async () => {
        const app = createTestApp();

        await resetObjectEditState({
            objectId: tenantOneObjectId,
            metadata: { source: "scanner-a" },
            updatedAtIso: "2026-02-09T10:00:00.000Z",
        });

        const patchResponse = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectId}/metadata`,
                {
                    method: "PATCH",
                    headers: {
                        authorization: `Bearer ${operatorToken}`,
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        revision: 0,
                        metadata: {
                            title: "Edited Metadata Title",
                            publication_date: "1987-06-14",
                            date_precision: "day",
                            date_approximate: true,
                            language: "Tajik",
                            tags: ["Oral History", "Migration"],
                            people: ["Zarina T.", "M. Davlatov"],
                            description: "Updated description",
                        },
                        rights: {
                            rights_note: "Updated rights note",
                            sensitivity_note: "Updated sensitivity note",
                        },
                    }),
                },
            ),
        );

        expect(patchResponse.status).toBe(200);
        const patchBody = (await patchResponse.json()) as {
            object_id: string;
            revision: number;
            curation_state: string;
            updated_at: string;
        };
        expect(patchBody.object_id).toBe(tenantOneObjectId);
        expect(patchBody.revision).toBe(1);
        expect(patchBody.curation_state).toBe("needs_review");
        expect(Date.parse(patchBody.updated_at)).not.toBeNaN();

        const editResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${tenantOneObjectId}/edit`, {
                headers: {
                    authorization: `Bearer ${operatorToken}`,
                },
            }),
        );

        expect(editResponse.status).toBe(200);
        const editBody = (await editResponse.json()) as {
            revision: number;
            draft: { updated_by: string | null } | null;
            metadata: {
                title: string;
                publication_date: string;
                date_precision: string;
                date_approximate: boolean;
                language: string | null;
                tags: string[];
                people: string[];
                description: string | null;
            };
            rights: {
                access_level: string;
                rights_note: string | null;
                sensitivity_note: string | null;
            };
        };

        expect(editBody.revision).toBe(1);
        expect(editBody.draft?.updated_by).toBe(
            "10000000-0000-0000-0000-000000000001",
        );
        expect(editBody.metadata).toEqual({
            title: "Edited Metadata Title",
            publication_date: "1987-06-14",
            date_precision: "day",
            date_approximate: true,
            language: "Tajik",
            tags: ["migration", "oral history"],
            people: ["Zarina T.", "M. Davlatov"],
            description: "Updated description",
        });
        expect(editBody.rights).toEqual({
            access_level: "private",
            rights_note: "Updated rights note",
            sensitivity_note: "Updated sensitivity note",
        });

        const historyResponse = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectId}/curation/history`,
                {
                    headers: {
                        authorization: `Bearer ${operatorToken}`,
                    },
                },
            ),
        );

        expect(historyResponse.status).toBe(200);
        const historyBody = (await historyResponse.json()) as {
            object_id: string;
            events: Array<{
                type: string;
                revision_before: number;
                revision_after: number;
            }>;
            next_cursor: string | null;
        };

        expect(historyBody.object_id).toBe(tenantOneObjectId);
        expect(historyBody.events).toHaveLength(2);
        expect(historyBody.events.map((event) => event.type).sort()).toEqual([
            "METADATA_UPDATED",
            "RIGHTS_UPDATED",
        ]);
        expect(historyBody.events[0]?.revision_before).toBe(0);
        expect(historyBody.events[0]?.revision_after).toBe(1);
        expect(historyBody.next_cursor).toBeNull();
    });

    test("rejects stale object metadata revision", async () => {
        const app = createTestApp();

        await resetObjectEditState({
            objectId: tenantOneObjectId,
            metadata: { source: "scanner-a" },
            updatedAtIso: "2026-02-09T10:00:00.000Z",
        });

        await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectId}/metadata`,
                {
                    method: "PATCH",
                    headers: {
                        authorization: `Bearer ${operatorToken}`,
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        revision: 0,
                        metadata: {
                            title: "Prime metadata revision",
                            publication_date: "",
                            date_precision: "none",
                            date_approximate: false,
                            language: null,
                            tags: [],
                            people: [],
                            description: null,
                        },
                        rights: {
                            rights_note: null,
                            sensitivity_note: null,
                        },
                    }),
                },
            ),
        );

        const response = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectId}/metadata`,
                {
                    method: "PATCH",
                    headers: {
                        authorization: `Bearer ${operatorToken}`,
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        revision: 0,
                        metadata: {
                            title: "Stale edit",
                            publication_date: "",
                            date_precision: "none",
                            date_approximate: false,
                            language: null,
                            tags: [],
                            people: [],
                            description: null,
                        },
                        rights: {
                            rights_note: null,
                            sensitivity_note: null,
                        },
                    }),
                },
            ),
        );

        expect(response.status).toBe(409);
        const body = (await response.json()) as {
            error: {
                code: string;
                details: { latest_revision: number };
            };
        };

        expect(body.error.code).toBe("REVISION_CONFLICT");
        expect(body.error.details.latest_revision).toBe(1);
    });

    test("serializes concurrent metadata edits at the object revision", async () => {
        const app = createTestApp();

        await resetObjectEditState({
            objectId: tenantOneObjectId,
            metadata: { source: "scanner-a" },
            updatedAtIso: "2026-02-09T10:00:00.000Z",
        });

        const request = (title: string) => app.fetch(
            new Request(`http://localhost/api/objects/${tenantOneObjectId}/metadata`, {
                method: "PATCH",
                headers: {
                    authorization: `Bearer ${operatorToken}`,
                    "content-type": "application/json",
                },
                body: JSON.stringify({
                    revision: 0,
                    metadata: {
                        title,
                        publication_date: "",
                        date_precision: "none",
                        date_approximate: false,
                        language: null,
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

        const responses = await Promise.all([
            request("Concurrent metadata edit one"),
            request("Concurrent metadata edit two"),
        ]);

        expect(responses.map((response) => response.status).sort()).toEqual([200, 409]);

        const sql = createSqlClient(TEST_DATABASE_URL!);
        try {
            await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            const rows = await sql<Array<{ revision: number; event_count: string }>>`
                SELECT edit.revision,
                       (SELECT count(*)::text FROM object_edit_events WHERE object_id = edit.object_id) AS event_count
                FROM object_edits edit
                WHERE edit.object_id = ${tenantOneObjectId}
            `;
            expect(rows).toEqual([{ revision: 1, event_count: "1" }]);
        } finally {
            await sql.close();
        }
    });

    test("rejects invalid document OCR page updates", async () => {
        const app = createTestApp();
        const sql = createSqlClient(TEST_DATABASE_URL!);

        try {
            await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            await sql`
                UPDATE objects
                SET metadata = COALESCE(metadata, '{}'::jsonb) || ${{page_count: 1, pages: [
                        {
                            page_number: 1,
                            label: "1",
                            ocr_text_artifact_id: null,
                        },
                    ]}}
                WHERE object_id = ${tenantOneObjectId}
            `;
        } finally {
            await sql.close();
        }

        const editResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${tenantOneObjectId}/edit`, {
                headers: {
                    authorization: `Bearer ${operatorToken}`,
                },
            }),
        );
        const editBody = (await editResponse.json()) as { revision: number };

        const response = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectId}/curation/document`,
                {
                    method: "PUT",
                    headers: {
                        authorization: `Bearer ${operatorToken}`,
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        revision: editBody.revision,
                        pages: [
                            {
                                page_number: 999,
                                curated_text: "invalid page",
                            },
                        ],
                    }),
                },
            ),
        );

        expect(response.status).toBe(422);
        const body = (await response.json()) as {
            error: {
                code: string;
                details: Array<{ path: string; code: string; page_number: number }>;
            };
        };
        expect(body.error.code).toBe("VALIDATION_FAILED");
        expect(body.error.details).toEqual([
            {
                path: "pages",
                code: "INVALID_PAGE_NUMBER",
                page_number: 999,
            },
        ]);
    });

    test("returns resolved thumbnail_artifact_id in patch responses", async () => {
        const app = createTestApp();
        const sql = createSqlClient(TEST_DATABASE_URL!);
        const thumbnailArtifactId = "60000000-0000-4000-8000-000000000312";

        try {
            await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            await sql`
        INSERT INTO object_artifacts (id, object_id, kind, variant, storage_key, content_type, size_bytes)
        VALUES (
          ${thumbnailArtifactId},
          ${tenantOneObjectId},
          ${"thumbnail"}::artifact_kind,
          NULL,
          ${`tenants/${tenantOneId}/objects/${tenantOneObjectId}/artifacts/thumb-route.jpg`},
          ${"image/jpeg"},
          ${1024}
        )
      `;
        } finally {
            await sql.close();
        }

        const patchTitleResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${tenantOneObjectId}`, {
                method: "PATCH",
                headers: {
                    authorization: `Bearer ${operatorToken}`,
                    "content-type": "application/json",
                },
                body: JSON.stringify({
                    title: "Retitled With Thumbnail",
                }),
            }),
        );

        expect(patchTitleResponse.status).toBe(200);
        const patchTitleBody = (await patchTitleResponse.json()) as {
            object: { thumbnail_artifact_id: string | null };
        };
        expect(patchTitleBody.object.thumbnail_artifact_id).toBe(
            thumbnailArtifactId,
        );

        const patchPolicyResponse = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectId}/access-policy`,
                {
                    method: "PATCH",
                    headers: {
                        authorization: `Bearer ${adminToken}`,
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        access_level: "family",
                        embargo_kind: "none",
                    }),
                },
            ),
        );

        expect(patchPolicyResponse.status).toBe(200);
        const patchPolicyBody = (await patchPolicyResponse.json()) as {
            object: { thumbnail_artifact_id: string | null };
        };
        expect(patchPolicyBody.object.thumbnail_artifact_id).toBe(
            thumbnailArtifactId,
        );
    });

    test("validates conditional access-policy embargo fields", async () => {
        const app = createTestApp();

        const missingTimedEmbargoUntil = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectId}/access-policy`,
                {
                    method: "PATCH",
                    headers: {
                        authorization: `Bearer ${adminToken}`,
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        access_level: "private",
                        embargo_kind: "timed",
                    }),
                },
            ),
        );

        expect(missingTimedEmbargoUntil.status).toBe(400);

        const missingCurationState = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectId}/access-policy`,
                {
                    method: "PATCH",
                    headers: {
                        authorization: `Bearer ${adminToken}`,
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        access_level: "private",
                        embargo_kind: "curation_state",
                    }),
                },
            ),
        );

        expect(missingCurationState.status).toBe(400);
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
        expect(listBody.artifacts.some((artifact) => artifact.id === artifactId)).toBe(
            true,
        );

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

    test("syncs and lists object available files", async () => {
        const app = createTestApp();

        const syncResponse = await app.fetch(
            new Request(
                `http://localhost/api/internal/objects/${tenantOneObjectId}/available-files`,
                {
                    method: "PUT",
                    headers: {
                        "x-worker-auth-token": "worker-secret",
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        files: [
                            {
                                archive_file_key: "archive-original-bundle",
                                artifact_kind: "original",
                                variant: null,
                                display_name: "Original Bundle",
                                content_type: "application/pdf",
                                size_bytes: 1024,
                                checksum_sha256: null,
                                metadata: {},
                            },
                        ],
                    }),
                },
            ),
        );

        expect(syncResponse.status).toBe(200);

        const listResponse = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectId}/available-files`,
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
            available_files: Array<{ artifact_kind: string; archive_file_key: string }>;
        };

        expect(listBody.available_files.length).toBe(1);
        expect(listBody.available_files[0]?.artifact_kind).toBe("original");
        expect(listBody.available_files[0]?.archive_file_key).toBe(
            "archive-original-bundle",
        );
    });

    test("sync snapshot marks omitted available files as unavailable", async () => {
        const app = createTestApp();
        const sql = createSqlClient(TEST_DATABASE_URL!);

        try {
            await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            await sql`
        UPDATE archive_requests
        SET
          status = ${"CANCELED"}::archive_request_status,
          completed_at = now(),
          lease_id = NULL,
          lease_token_id = NULL,
          lease_expires_at = NULL,
          leased_by = NULL,
          released_at = now(),
          updated_at = now()
        WHERE tenant_id = ${tenantOneId}
          AND action_type = ${"artifact_fetch"}::archive_request_action_type
          AND status IN (${"PENDING"}::archive_request_status, ${"PROCESSING"}::archive_request_status)
      `;

            await sql`
        INSERT INTO object_available_files (
          id,
          object_id,
          tenant_id,
          archive_file_key,
          artifact_kind,
          variant,
          display_name,
          is_available
        )
        VALUES
          (
            ${"70000000-0000-4000-8000-000000000021"},
            ${tenantOneObjectId},
            ${tenantOneId},
            ${"archive-k1"},
            ${"original"}::artifact_kind,
            NULL,
            ${"K1"},
            true
          ),
          (
            ${"70000000-0000-4000-8000-000000000022"},
            ${tenantOneObjectId},
            ${tenantOneId},
            ${"archive-k2"},
            ${"pdf"}::artifact_kind,
            NULL,
            ${"K2"},
            true
          ),
          (
            ${"70000000-0000-4000-8000-000000000023"},
            ${tenantOneObjectId},
            ${tenantOneId},
            ${"archive-k3"},
            ${"thumbnail"}::artifact_kind,
            NULL,
            ${"K3"},
            true
          )
      `;
        } finally {
            await sql.close();
        }

        const syncResponse = await app.fetch(
            new Request(
                `http://localhost/api/internal/objects/${tenantOneObjectId}/available-files`,
                {
                    method: "PUT",
                    headers: {
                        "x-worker-auth-token": "worker-secret",
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        files: [
                            {
                                archive_file_key: "archive-k1",
                                artifact_kind: "original",
                                variant: null,
                                display_name: "K1",
                            },
                            {
                                archive_file_key: "archive-k2",
                                artifact_kind: "pdf",
                                variant: null,
                                display_name: "K2",
                            },
                        ],
                    }),
                },
            ),
        );

        expect(syncResponse.status).toBe(200);

        const verifySql = createSqlClient(TEST_DATABASE_URL!);
        try {
            await verifySql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            const rows = await verifySql<
                Array<{ archive_file_key: string; is_available: boolean }>
            >`
        SELECT archive_file_key, is_available
        FROM object_available_files
        WHERE object_id = ${tenantOneObjectId}
          AND archive_file_key IN (${"archive-k1"}, ${"archive-k2"}, ${"archive-k3"})
        ORDER BY archive_file_key ASC
      `;

            expect(rows).toEqual([
                { archive_file_key: "archive-k1", is_available: true },
                { archive_file_key: "archive-k2", is_available: true },
                { archive_file_key: "archive-k3", is_available: false },
            ]);
        } finally {
            await verifySql.close();
        }

        const listResponse = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectId}/available-files`,
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
            available_files: Array<{ archive_file_key: string }>;
        };

        expect(listBody.available_files.map((file) => file.archive_file_key).sort()).toEqual([
            "archive-k1",
            "archive-k2",
        ]);
    });

    test("sync with empty files marks all entries unavailable", async () => {
        const app = createTestApp();
        const sql = createSqlClient(TEST_DATABASE_URL!);

        try {
            await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            await sql`
        INSERT INTO object_available_files (
          id,
          object_id,
          tenant_id,
          archive_file_key,
          artifact_kind,
          variant,
          display_name,
          is_available
        )
        VALUES
          (
            ${"70000000-0000-4000-8000-000000000031"},
            ${tenantOneObjectIdTwo},
            ${tenantOneId},
            ${"archive-clear-1"},
            ${"web_version"}::artifact_kind,
            NULL,
            ${"Clear 1"},
            true
          ),
          (
            ${"70000000-0000-4000-8000-000000000032"},
            ${tenantOneObjectIdTwo},
            ${tenantOneId},
            ${"archive-clear-2"},
            ${"thumbnail"}::artifact_kind,
            NULL,
            ${"Clear 2"},
            true
          )
      `;
        } finally {
            await sql.close();
        }

        const syncResponse = await app.fetch(
            new Request(
                `http://localhost/api/internal/objects/${tenantOneObjectIdTwo}/available-files`,
                {
                    method: "PUT",
                    headers: {
                        "x-worker-auth-token": "worker-secret",
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        files: [],
                    }),
                },
            ),
        );

        expect(syncResponse.status).toBe(200);

        const verifySql = createSqlClient(TEST_DATABASE_URL!);
        try {
            await verifySql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            const rows = await verifySql<Array<{ is_available: boolean }>>`
        SELECT is_available
        FROM object_available_files
        WHERE object_id = ${tenantOneObjectIdTwo}
      `;

            expect(rows.every((row) => row.is_available === false)).toBe(true);
        } finally {
            await verifySql.close();
        }

        const listResponse = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectIdTwo}/available-files`,
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
            available_files: Array<unknown>;
        };
        expect(listBody.available_files).toEqual([]);
    });

    test("auto-queues one request per configured artifact kind and prefers null variant", async () => {
        const app = createTestApp();
        const sql = createSqlClient(TEST_DATABASE_URL!);
        const objectId = "OBJ-20260305-THMBN1";

        try {
            await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            await sql`
        INSERT INTO objects (
          object_id,
          tenant_id,
          type,
          title,
          metadata,
          availability_state
        )
        VALUES (
          ${objectId},
          ${tenantOneId},
          ${"VIDEO"}::object_type,
          ${"Thumbnail Candidate One"},
          ${{ source: "thumbnail-sync" }},
          ${"AVAILABLE"}::object_availability_state
        )
      `;
        } finally {
            await sql.close();
        }

        const syncResponse = await app.fetch(
            new Request(
                `http://localhost/api/internal/objects/${objectId}/available-files`,
                {
                    method: "PUT",
                    headers: {
                        "x-worker-auth-token": "worker-secret",
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        files: [
                            {
                                archive_file_key: "archive-thumb-small",
                                artifact_kind: "thumbnail",
                                variant: "small",
                                display_name: "Thumb Small",
                                content_type: "image/jpeg",
                                size_bytes: 1200,
                            },
                            {
                                archive_file_key: "archive-thumb-primary",
                                artifact_kind: "thumbnail",
                                variant: null,
                                display_name: "Thumb Primary",
                                content_type: "image/jpeg",
                                size_bytes: 2400,
                            },
                            {
                                archive_file_key: "archive-ocr-v1",
                                artifact_kind: "ocr_text",
                                variant: "v1",
                                display_name: "OCR Text V1",
                                content_type: "text/plain",
                                size_bytes: 900,
                            },
                            {
                                archive_file_key: "archive-ocr-primary",
                                artifact_kind: "ocr_text",
                                variant: null,
                                display_name: "OCR Text Primary",
                                content_type: "text/plain",
                                size_bytes: 1000,
                            },
                        ],
                    }),
                },
            ),
        );

        expect(syncResponse.status).toBe(200);

        const availableResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${objectId}/available-files`, {
                method: "GET",
                headers: {
                    authorization: `Bearer ${viewerToken}`,
                },
            }),
        );

        expect(availableResponse.status).toBe(200);
        const availableBody = (await availableResponse.json()) as {
            available_files: Array<{ id: string; archive_file_key: string }>;
        };

        const fileKeyById = new Map(
            availableBody.available_files.map((file) => [
                file.id,
                file.archive_file_key,
            ]),
        );

        const requestsResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${objectId}/download-requests`, {
                method: "GET",
                headers: {
                    authorization: `Bearer ${viewerToken}`,
                },
            }),
        );

        expect(requestsResponse.status).toBe(200);
        const requestsBody = (await requestsResponse.json()) as {
            requests: Array<{
                available_file_id: string | null;
                artifact_kind: string;
                variant: string | null;
            }>;
        };

        const thumbnailRequests = requestsBody.requests.filter(
            (request) => request.artifact_kind === "thumbnail",
        );
        const ocrRequests = requestsBody.requests.filter(
            (request) => request.artifact_kind === "ocr_text",
        );

        expect(thumbnailRequests.length).toBe(1);
        expect(thumbnailRequests[0]?.variant).toBeNull();
        expect(
            fileKeyById.get(thumbnailRequests[0]?.available_file_id ?? ""),
        ).toBe("archive-thumb-primary");
        expect(ocrRequests.length).toBe(1);
        expect(ocrRequests[0]?.variant).toBeNull();
        expect(fileKeyById.get(ocrRequests[0]?.available_file_id ?? "")).toBe(
            "archive-ocr-primary",
        );
    });

    test("auto-queues web_version for image objects", async () => {
        const app = createTestApp();
        const sql = createSqlClient(TEST_DATABASE_URL!);
        const objectId = "OBJ-20260305-WEBIM1";

        try {
            await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            await sql`
        INSERT INTO objects (
          object_id,
          tenant_id,
          type,
          title,
          metadata,
          availability_state
        )
        VALUES (
          ${objectId},
          ${tenantOneId},
          ${"IMAGE"}::object_type,
          ${"Web Candidate Image"},
          ${{ source: "web-sync" }},
          ${"AVAILABLE"}::object_availability_state
        )
      `;
        } finally {
            await sql.close();
        }

        const syncResponse = await app.fetch(
            new Request(
                `http://localhost/api/internal/objects/${objectId}/available-files`,
                {
                    method: "PUT",
                    headers: {
                        "x-worker-auth-token": "worker-secret",
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        files: [
                            {
                                archive_file_key: "archive-web-primary",
                                artifact_kind: "web_version",
                                variant: null,
                                display_name: "Web Version Primary",
                            },
                            {
                                archive_file_key: "archive-web-alt",
                                artifact_kind: "web_version",
                                variant: "mobile",
                                display_name: "Web Version Mobile",
                            },
                        ],
                    }),
                },
            ),
        );

        expect(syncResponse.status).toBe(200);

        const availableResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${objectId}/available-files`, {
                method: "GET",
                headers: {
                    authorization: `Bearer ${viewerToken}`,
                },
            }),
        );

        expect(availableResponse.status).toBe(200);
        const availableBody = (await availableResponse.json()) as {
            available_files: Array<{ id: string; archive_file_key: string }>;
        };

        const fileKeyById = new Map(
            availableBody.available_files.map((file) => [
                file.id,
                file.archive_file_key,
            ]),
        );

        const requestsResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${objectId}/download-requests`, {
                method: "GET",
                headers: {
                    authorization: `Bearer ${viewerToken}`,
                },
            }),
        );

        expect(requestsResponse.status).toBe(200);
        const requestsBody = (await requestsResponse.json()) as {
            requests: Array<{
                available_file_id: string | null;
                artifact_kind: string;
                variant: string | null;
            }>;
        };

        const webRequests = requestsBody.requests.filter(
            (request) => request.artifact_kind === "web_version",
        );

        expect(webRequests.length).toBe(1);
        expect(webRequests[0]?.variant).toBeNull();
        expect(fileKeyById.get(webRequests[0]?.available_file_id ?? "")).toBe(
            "archive-web-primary",
        );
    });

    test("does not auto-queue web_version for non-image objects", async () => {
        const app = createTestApp();
        const sql = createSqlClient(TEST_DATABASE_URL!);
        const objectId = "OBJ-20260305-WEBDOC";

        try {
            await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            await sql`
        INSERT INTO objects (
          object_id,
          tenant_id,
          type,
          title,
          metadata,
          availability_state
        )
        VALUES (
          ${objectId},
          ${tenantOneId},
          ${"DOCUMENT"}::object_type,
          ${"Web Candidate Document"},
          ${{ source: "web-sync" }},
          ${"AVAILABLE"}::object_availability_state
        )
      `;
        } finally {
            await sql.close();
        }

        const syncResponse = await app.fetch(
            new Request(
                `http://localhost/api/internal/objects/${objectId}/available-files`,
                {
                    method: "PUT",
                    headers: {
                        "x-worker-auth-token": "worker-secret",
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        files: [
                            {
                                archive_file_key: "archive-web-document",
                                artifact_kind: "web_version",
                                variant: null,
                                display_name: "Web Version Document",
                            },
                        ],
                    }),
                },
            ),
        );

        expect(syncResponse.status).toBe(200);

        const requestsResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${objectId}/download-requests`, {
                method: "GET",
                headers: {
                    authorization: `Bearer ${viewerToken}`,
                },
            }),
        );

        expect(requestsResponse.status).toBe(200);
        const requestsBody = (await requestsResponse.json()) as {
            requests: Array<{ artifact_kind: string }>;
        };

        const webRequests = requestsBody.requests.filter(
            (request) => request.artifact_kind === "web_version",
        );

        expect(webRequests).toEqual([]);
    });

    test("auto-queue thumbnail picks lowest archive key when no null variant exists", async () => {
        const app = createTestApp();
        const sql = createSqlClient(TEST_DATABASE_URL!);
        const objectId = "OBJ-20260305-THMBN2";

        try {
            await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            await sql`
        INSERT INTO objects (
          object_id,
          tenant_id,
          type,
          title,
          metadata,
          availability_state
        )
        VALUES (
          ${objectId},
          ${tenantOneId},
          ${"IMAGE"}::object_type,
          ${"Thumbnail Candidate Two"},
          ${{ source: "thumbnail-sync" }},
          ${"AVAILABLE"}::object_availability_state
        )
      `;
        } finally {
            await sql.close();
        }

        const syncResponse = await app.fetch(
            new Request(
                `http://localhost/api/internal/objects/${objectId}/available-files`,
                {
                    method: "PUT",
                    headers: {
                        "x-worker-auth-token": "worker-secret",
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        files: [
                            {
                                archive_file_key: "archive-thumb-z",
                                artifact_kind: "thumbnail",
                                variant: "large",
                                display_name: "Thumb Z",
                            },
                            {
                                archive_file_key: "archive-thumb-a",
                                artifact_kind: "thumbnail",
                                variant: "small",
                                display_name: "Thumb A",
                            },
                        ],
                    }),
                },
            ),
        );

        expect(syncResponse.status).toBe(200);

        const availableResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${objectId}/available-files`, {
                method: "GET",
                headers: {
                    authorization: `Bearer ${viewerToken}`,
                },
            }),
        );

        expect(availableResponse.status).toBe(200);
        const availableBody = (await availableResponse.json()) as {
            available_files: Array<{ id: string; archive_file_key: string }>;
        };

        const fileKeyById = new Map(
            availableBody.available_files.map((file) => [
                file.id,
                file.archive_file_key,
            ]),
        );

        const requestsResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${objectId}/download-requests`, {
                method: "GET",
                headers: {
                    authorization: `Bearer ${viewerToken}`,
                },
            }),
        );

        expect(requestsResponse.status).toBe(200);
        const requestsBody = (await requestsResponse.json()) as {
            requests: Array<{
                available_file_id: string | null;
                artifact_kind: string;
            }>;
        };

        const thumbnailRequests = requestsBody.requests.filter(
            (request) => request.artifact_kind === "thumbnail",
        );

        expect(thumbnailRequests.length).toBe(1);
        expect(
            fileKeyById.get(thumbnailRequests[0]?.available_file_id ?? ""),
        ).toBe("archive-thumb-a");
    });

    test("does not auto-queue thumbnail when thumbnail artifact already exists", async () => {
        const app = createTestApp();
        const sql = createSqlClient(TEST_DATABASE_URL!);
        const objectId = "OBJ-20260305-THMBN3";

        try {
            await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            await sql`
        INSERT INTO objects (
          object_id,
          tenant_id,
          type,
          title,
          metadata,
          availability_state
        )
        VALUES (
          ${objectId},
          ${tenantOneId},
          ${"IMAGE"}::object_type,
          ${"Thumbnail Candidate Three"},
          ${{ source: "thumbnail-sync" }},
          ${"AVAILABLE"}::object_availability_state
        )
      `;

            await sql`
        INSERT INTO object_artifacts (id, object_id, kind, storage_key, content_type, size_bytes)
        VALUES (
          ${"60000000-0000-4000-8000-000000000211"},
          ${objectId},
          ${"thumbnail"}::artifact_kind,
          ${`tenants/${tenantOneId}/objects/${objectId}/artifacts/existing-thumb.jpg`},
          ${"image/jpeg"},
          ${4096}
        )
      `;
        } finally {
            await sql.close();
        }

        const syncResponse = await app.fetch(
            new Request(
                `http://localhost/api/internal/objects/${objectId}/available-files`,
                {
                    method: "PUT",
                    headers: {
                        "x-worker-auth-token": "worker-secret",
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        files: [
                            {
                                archive_file_key: "archive-thumb-existing",
                                artifact_kind: "thumbnail",
                                variant: null,
                                display_name: "Thumb Existing",
                            },
                        ],
                    }),
                },
            ),
        );

        expect(syncResponse.status).toBe(200);

        const requestsResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${objectId}/download-requests`, {
                method: "GET",
                headers: {
                    authorization: `Bearer ${viewerToken}`,
                },
            }),
        );

        expect(requestsResponse.status).toBe(200);
        const requestsBody = (await requestsResponse.json()) as {
            requests: Array<{ artifact_kind: string }>;
        };

        const thumbnailRequests = requestsBody.requests.filter(
            (request) => request.artifact_kind === "thumbnail",
        );

        expect(thumbnailRequests).toEqual([]);
    });

    test("does not auto-queue ocr_text when ocr_text artifact already exists", async () => {
        const app = createTestApp();
        const sql = createSqlClient(TEST_DATABASE_URL!);
        const objectId = "OBJ-20260305-THMBN5";

        try {
            await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            await sql`
        INSERT INTO objects (
          object_id,
          tenant_id,
          type,
          title,
          metadata,
          availability_state
        )
        VALUES (
          ${objectId},
          ${tenantOneId},
          ${"DOCUMENT"}::object_type,
          ${"OCR Candidate One"},
          ${{ source: "ocr-sync" }},
          ${"AVAILABLE"}::object_availability_state
        )
      `;

            await sql`
        INSERT INTO object_artifacts (id, object_id, kind, storage_key, content_type, size_bytes)
        VALUES (
          ${"60000000-0000-4000-8000-000000000212"},
          ${objectId},
          ${"ocr_text"}::artifact_kind,
          ${`tenants/${tenantOneId}/objects/${objectId}/artifacts/existing-ocr.txt`},
          ${"text/plain"},
          ${2048}
        )
      `;
        } finally {
            await sql.close();
        }

        const syncResponse = await app.fetch(
            new Request(
                `http://localhost/api/internal/objects/${objectId}/available-files`,
                {
                    method: "PUT",
                    headers: {
                        "x-worker-auth-token": "worker-secret",
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        files: [
                            {
                                archive_file_key: "archive-ocr-existing",
                                artifact_kind: "ocr_text",
                                variant: null,
                                display_name: "OCR Existing",
                            },
                        ],
                    }),
                },
            ),
        );

        expect(syncResponse.status).toBe(200);

        const requestsResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${objectId}/download-requests`, {
                method: "GET",
                headers: {
                    authorization: `Bearer ${viewerToken}`,
                },
            }),
        );

        expect(requestsResponse.status).toBe(200);
        const requestsBody = (await requestsResponse.json()) as {
            requests: Array<{ artifact_kind: string }>;
        };

        const ocrRequests = requestsBody.requests.filter(
            (request) => request.artifact_kind === "ocr_text",
        );

        expect(ocrRequests).toEqual([]);
    });

    test("does not create duplicate auto-requests across repeated syncs", async () => {
        const app = createTestApp();
        const sql = createSqlClient(TEST_DATABASE_URL!);
        const objectId = "OBJ-20260305-THMBN4";

        try {
            await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            await sql`
        INSERT INTO objects (
          object_id,
          tenant_id,
          type,
          title,
          metadata,
          availability_state
        )
        VALUES (
          ${objectId},
          ${tenantOneId},
          ${"IMAGE"}::object_type,
          ${"Thumbnail Candidate Four"},
          ${{ source: "thumbnail-sync" }},
          ${"AVAILABLE"}::object_availability_state
        )
      `;
        } finally {
            await sql.close();
        }

        const payload = {
            files: [
                {
                    archive_file_key: "archive-thumb-repeat",
                    artifact_kind: "thumbnail",
                    variant: null,
                    display_name: "Thumb Repeat",
                },
                {
                    archive_file_key: "archive-ocr-repeat",
                    artifact_kind: "ocr_text",
                    variant: null,
                    display_name: "OCR Repeat",
                },
            ],
        };

        const firstSync = await app.fetch(
            new Request(
                `http://localhost/api/internal/objects/${objectId}/available-files`,
                {
                    method: "PUT",
                    headers: {
                        "x-worker-auth-token": "worker-secret",
                        "content-type": "application/json",
                    },
                    body: JSON.stringify(payload),
                },
            ),
        );

        expect(firstSync.status).toBe(200);

        const secondSync = await app.fetch(
            new Request(
                `http://localhost/api/internal/objects/${objectId}/available-files`,
                {
                    method: "PUT",
                    headers: {
                        "x-worker-auth-token": "worker-secret",
                        "content-type": "application/json",
                    },
                    body: JSON.stringify(payload),
                },
            ),
        );

        expect(secondSync.status).toBe(200);

        const requestsResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${objectId}/download-requests`, {
                method: "GET",
                headers: {
                    authorization: `Bearer ${viewerToken}`,
                },
            }),
        );

        expect(requestsResponse.status).toBe(200);
        const requestsBody = (await requestsResponse.json()) as {
            requests: Array<{ artifact_kind: string }>;
        };

        const thumbnailRequests = requestsBody.requests.filter(
            (request) => request.artifact_kind === "thumbnail",
        );
        const ocrRequests = requestsBody.requests.filter(
            (request) => request.artifact_kind === "ocr_text",
        );

        expect(thumbnailRequests.length).toBe(1);
        expect(ocrRequests.length).toBe(1);
    });

    test("queues object download request when artifact is missing and dedupes active request", async () => {
        const app = createTestApp();
        const sql = createSqlClient(TEST_DATABASE_URL!);
        const availableFileId = "70000000-0000-4000-8000-000000000001";

        try {
            await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            await sql`
        INSERT INTO object_available_files (
          id,
          object_id,
          tenant_id,
          archive_file_key,
          artifact_kind,
          variant,
          display_name,
          is_available
        )
        VALUES (
          ${availableFileId},
          ${tenantOneObjectId},
          ${tenantOneId},
          ${"archive-pdf-primary"},
          ${"pdf"}::artifact_kind,
          NULL,
          ${"Primary PDF"},
          true
        )
      `;
        } finally {
            await sql.close();
        }

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
                        available_file_id: availableFileId,
                    }),
                },
            ),
        );

        expect(firstResponse.status).toBe(201);
        const firstBody = (await firstResponse.json()) as {
            status: "queued";
            request: {
                id: string;
                status: string;
                artifact_kind: string;
                available_file_id: string | null;
            };
        };
        expect(firstBody.status).toBe("queued");
        expect(firstBody.request.status).toBe("PENDING");
        expect(firstBody.request.artifact_kind).toBe("pdf");
        expect(firstBody.request.available_file_id).toBe(availableFileId);

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
                        available_file_id: availableFileId,
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
        const availableFileId = "70000000-0000-4000-8000-000000000002";

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
            await sql`
        INSERT INTO object_available_files (
          id,
          object_id,
          tenant_id,
          archive_file_key,
          artifact_kind,
          variant,
          display_name,
          is_available
        )
        VALUES (
          ${availableFileId},
          ${tenantOneObjectId},
          ${tenantOneId},
          ${"archive-pdf-existing"},
          ${"pdf"}::artifact_kind,
          NULL,
          ${"Existing PDF"},
          true
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
                        available_file_id: availableFileId,
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
        const sql = createSqlClient(TEST_DATABASE_URL!);
        const availableFileId = "70000000-0000-4000-8000-000000000003";

        try {
            await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            await sql`
        INSERT INTO object_available_files (
          id,
          object_id,
          tenant_id,
          archive_file_key,
          artifact_kind,
          variant,
          display_name,
          is_available
        )
        VALUES (
          ${availableFileId},
          ${tenantOneObjectIdTwo},
          ${tenantOneId},
          ${"archive-web-primary"},
          ${"web_version"}::artifact_kind,
          NULL,
          ${"Web Version"},
          true
        )
      `;
        } finally {
            await sql.close();
        }

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
                            available_file_id: availableFileId,
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
                            available_file_id: availableFileId,
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

    test("worker leases request, uploads artifact, and completes request", async () => {
        const app = createTestApp();
        const sql = createSqlClient(TEST_DATABASE_URL!);
        const availableFileId = "70000000-0000-4000-8000-000000000041";
        const artifactVariant = "worker-flow";

        try {
            await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            await sql`
        INSERT INTO object_available_files (
          id,
          object_id,
          tenant_id,
          archive_file_key,
          artifact_kind,
          variant,
          display_name,
          is_available
        )
        VALUES (
          ${availableFileId},
          ${tenantOneObjectIdTwo},
          ${tenantOneId},
          ${"archive-worker-web-v1"},
          ${"web_version"}::artifact_kind,
          ${artifactVariant},
          ${"Web Version"},
          true
        )
      `;
        } finally {
            await sql.close();
        }

        const createResponse = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectIdTwo}/download-requests`,
                {
                    method: "POST",
                    headers: {
                        authorization: `Bearer ${viewerToken}`,
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        available_file_id: availableFileId,
                    }),
                },
            ),
        );

        expect(createResponse.status).toBe(201);
        const createBody = (await createResponse.json()) as {
            request: { id: string };
        };

        let leasedRequest:
            | {
                  request_id: string;
                  lease_token: string;
                  action_type: string;
                  target_id: string;
              }
            | null = null;

        for (let attempt = 0; attempt < 20; attempt += 1) {
            const leaseResponse = await app.fetch(
                new Request("http://localhost/api/archive-requests/lease", {
                    method: "POST",
                    headers: {
                        "x-worker-auth-token": "worker-secret",
                        "x-worker-id": "worker-artifact-flow",
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({ action_type: "artifact_fetch" }),
                }),
            );

            expect(leaseResponse.status).toBe(200);
            const leaseBody = (await leaseResponse.json()) as {
                request: {
                    request_id: string;
                    lease_token: string;
                    action_type: string;
                    target_id: string;
                } | null;
            };

            expect(leaseBody.request).not.toBeNull();

            if (!leaseBody.request) {
                continue;
            }

            if (leaseBody.request.request_id === createBody.request.id) {
                leasedRequest = leaseBody.request;
                break;
            }

            const releaseResponse = await app.fetch(
                new Request(
                    `http://localhost/api/archive-requests/${leaseBody.request.request_id}/lease/release`,
                    {
                        method: "POST",
                        headers: {
                            "x-worker-auth-token": "worker-secret",
                            "content-type": "application/json",
                        },
                        body: JSON.stringify({
                            lease_token: leaseBody.request.lease_token,
                        }),
                    },
                ),
            );

            expect(releaseResponse.status).toBe(200);
        }

        expect(leasedRequest).not.toBeNull();
        expect(leasedRequest?.request_id).toBe(createBody.request.id);
        expect(leasedRequest?.action_type).toBe("artifact_fetch");
        expect(leasedRequest?.target_id).toBe(tenantOneObjectIdTwo);

        const presignResponse = await app.fetch(
            new Request(
                `http://localhost/api/archive-requests/${leasedRequest!.request_id}/artifacts/presign`,
                {
                    method: "POST",
                    headers: {
                        "x-worker-auth-token": "worker-secret",
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        lease_token: leasedRequest!.lease_token,
                        content_type: "text/plain",
                        size_bytes: 11,
                        extension: "txt",
                    }),
                },
            ),
        );

        expect(presignResponse.status).toBe(200);
        const presignBody = (await presignResponse.json()) as {
            upload_token: string;
            upload_url: string;
            storage_key: string;
        };

        const represignResponse = await app.fetch(
            new Request(
                `http://localhost/api/archive-requests/${leasedRequest!.request_id}/artifacts/presign`,
                {
                    method: "POST",
                    headers: {
                        "x-worker-auth-token": "worker-secret",
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        lease_token: leasedRequest!.lease_token,
                        content_type: "text/plain",
                        size_bytes: 11,
                        extension: "txt",
                    }),
                },
            ),
        );

        expect(represignResponse.status).toBe(200);
        const represignBody = (await represignResponse.json()) as {
            upload_token: string;
            upload_url: string;
            storage_key: string;
        };
        expect(represignBody.storage_key).not.toBe(presignBody.storage_key);

        const staleUploadResponse = await app.fetch(
            new Request(`http://localhost${presignBody.upload_url}`, {
                method: "PUT",
                headers: {
                    "content-type": "text/plain",
                    "content-length": "11",
                },
                body: "hello world",
            }),
        );

        expect(staleUploadResponse.status).toBe(409);

        const uploadResponse = await app.fetch(
            new Request(`http://localhost${represignBody.upload_url}`, {
                method: "PUT",
                headers: {
                    "content-type": "text/plain",
                    "content-length": "11",
                },
                body: "hello world",
            }),
        );

        expect(uploadResponse.status).toBe(200);

        const missingUploadTokenResponse = await app.fetch(
            new Request(
                `http://localhost/api/archive-requests/${leasedRequest!.request_id}/complete`,
                {
                    method: "POST",
                    headers: {
                        "x-worker-auth-token": "worker-secret",
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        lease_token: leasedRequest!.lease_token,
                    }),
                },
            ),
        );

        expect(missingUploadTokenResponse.status).toBe(400);

        const completeResponse = await app.fetch(
            new Request(
                `http://localhost/api/archive-requests/${leasedRequest!.request_id}/complete`,
                {
                    method: "POST",
                    headers: {
                        "x-worker-auth-token": "worker-secret",
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        lease_token: leasedRequest!.lease_token,
                        upload_token: represignBody.upload_token,
                    }),
                },
            ),
        );

        expect(completeResponse.status).toBe(200);
        const completeBody = (await completeResponse.json()) as {
            status: string;
            request: { status: string; action_type: string };
        };
        expect(completeBody.status).toBe("completed");
        expect(completeBody.request.status).toBe("COMPLETED");
        expect(completeBody.request.action_type).toBe("artifact_fetch");

        const replayResponse = await app.fetch(
            new Request(`http://localhost${represignBody.upload_url}`, {
                method: "PUT",
                headers: {
                    "content-type": "text/plain",
                    "content-length": "11",
                },
                body: "goodbye all",
            }),
        );

        expect(replayResponse.status).toBe(409);
        expect(await Bun.file(join(stagingRoot, represignBody.storage_key)).text()).toBe(
            "hello world",
        );

        const listRequestsResponse = await app.fetch(
            new Request(
                `http://localhost/api/objects/${tenantOneObjectIdTwo}/download-requests`,
                {
                    method: "GET",
                    headers: {
                        authorization: `Bearer ${viewerToken}`,
                    },
                },
            ),
        );

        expect(listRequestsResponse.status).toBe(200);
        const listRequestsBody = (await listRequestsResponse.json()) as {
            requests: Array<{ status: string }>;
        };
        expect(listRequestsBody.requests[0]?.status).toBe("COMPLETED");
    });

    test("creates and dedupes object resync requests, then worker completes lifecycle", async () => {
        const app = createTestApp();
        const sql = createSqlClient(TEST_DATABASE_URL!);
        const objectId = "OBJ-20260306-RSYNC1";

        try {
            await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            await sql`
        INSERT INTO objects (
          object_id,
          tenant_id,
          type,
          title,
          metadata,
          availability_state
        )
        VALUES (
          ${objectId},
          ${tenantOneId},
          ${"DOCUMENT"}::object_type,
          ${"Resync Target One"},
          ${{ source: "resync-test" }},
          ${"ARCHIVED"}::object_availability_state
        )
      `;
        } finally {
            await sql.close();
        }

        const createOne = await app.fetch(
            new Request(`http://localhost/api/objects/${objectId}/resync`, {
                method: "POST",
                headers: {
                    authorization: `Bearer ${operatorToken}`,
                    "content-type": "application/json",
                },
                body: JSON.stringify({
                    action_payload: { source: "manual" },
                }),
            }),
        );

        expect(createOne.status).toBe(201);
        const createOneBody = (await createOne.json()) as {
            status: "queued";
            object_id: string;
            request: {
                id: string;
                action_type: string;
                target_type: string;
                target_id: string;
                dedupe_key: string | null;
                status: string;
            };
        };
        expect(createOneBody.status).toBe("queued");
        expect(createOneBody.object_id).toBe(objectId);
        expect(createOneBody.request.action_type).toBe("object_resync");
        expect(createOneBody.request.target_type).toBe("object");
        expect(createOneBody.request.target_id).toBe(objectId);
        expect(createOneBody.request.status).toBe("PENDING");

        const createTwo = await app.fetch(
            new Request(`http://localhost/api/objects/${objectId}/resync`, {
                method: "POST",
                headers: {
                    authorization: `Bearer ${operatorToken}`,
                    "content-type": "application/json",
                },
                body: JSON.stringify({}),
            }),
        );

        expect(createTwo.status).toBe(200);
        const createTwoBody = (await createTwo.json()) as {
            request: { id: string };
        };
        expect(createTwoBody.request.id).toBe(createOneBody.request.id);

        const leaseResponse = await app.fetch(
            new Request("http://localhost/api/archive-requests/lease", {
                method: "POST",
                headers: {
                    "x-worker-auth-token": "worker-secret",
                    "x-worker-id": "worker-resync-one",
                    "content-type": "application/json",
                },
                body: JSON.stringify({
                    action_type: "object_resync",
                }),
            }),
        );

        expect(leaseResponse.status).toBe(200);
        const leaseBody = (await leaseResponse.json()) as {
            request: {
                request_id: string;
                lease_token: string;
                action_type: string;
                target_type: string;
                target_id: string;
            } | null;
        };

        expect(leaseBody.request).not.toBeNull();
        expect(leaseBody.request?.request_id).toBe(createOneBody.request.id);
        expect(leaseBody.request?.action_type).toBe("object_resync");
        expect(leaseBody.request?.target_type).toBe("object");
        expect(leaseBody.request?.target_id).toBe(objectId);

        const heartbeatResponse = await app.fetch(
            new Request(
                `http://localhost/api/archive-requests/${leaseBody.request!.request_id}/lease/heartbeat`,
                {
                    method: "POST",
                    headers: {
                        "x-worker-auth-token": "worker-secret",
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        lease_token: leaseBody.request!.lease_token,
                    }),
                },
            ),
        );

        expect(heartbeatResponse.status).toBe(200);
        const heartbeatBody = (await heartbeatResponse.json()) as {
            request: { lease_token: string };
        };

        const completeResponse = await app.fetch(
            new Request(
                `http://localhost/api/archive-requests/${leaseBody.request!.request_id}/complete`,
                {
                    method: "POST",
                    headers: {
                        "x-worker-auth-token": "worker-secret",
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        lease_token: heartbeatBody.request.lease_token,
                        upload_token: "ignored-for-object-resync",
                    }),
                },
            ),
        );

        expect(completeResponse.status).toBe(200);
        const completeBody = (await completeResponse.json()) as {
            status: string;
            request: { status: string };
        };
        expect(completeBody.status).toBe("completed");
        expect(completeBody.request.status).toBe("COMPLETED");

        const listResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${objectId}/resync-requests`, {
                method: "GET",
                headers: {
                    authorization: `Bearer ${viewerToken}`,
                },
            }),
        );

        expect(listResponse.status).toBe(200);
        const listBody = (await listResponse.json()) as {
            requests: Array<{ id: string; status: string }>;
        };
        expect(listBody.requests[0]?.id).toBe(createOneBody.request.id);
        expect(listBody.requests[0]?.status).toBe("COMPLETED");
    });

    test("fails object resync request via worker fail endpoint", async () => {
        const app = createTestApp();
        const sql = createSqlClient(TEST_DATABASE_URL!);
        const objectId = "OBJ-20260306-RSYNC2";

        try {
            await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
            await sql`
        INSERT INTO objects (
          object_id,
          tenant_id,
          type,
          title,
          metadata,
          availability_state
        )
        VALUES (
          ${objectId},
          ${tenantOneId},
          ${"DOCUMENT"}::object_type,
          ${"Resync Target Two"},
          ${{ source: "resync-test" }},
          ${"UNAVAILABLE"}::object_availability_state
        )
      `;
        } finally {
            await sql.close();
        }

        const createResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${objectId}/resync`, {
                method: "POST",
                headers: {
                    authorization: `Bearer ${operatorToken}`,
                    "content-type": "application/json",
                },
                body: JSON.stringify({}),
            }),
        );

        expect(createResponse.status).toBe(201);
        const createBody = (await createResponse.json()) as {
            request: { id: string };
        };

        const leaseResponse = await app.fetch(
            new Request("http://localhost/api/archive-requests/lease", {
                method: "POST",
                headers: {
                    "x-worker-auth-token": "worker-secret",
                    "content-type": "application/json",
                },
                body: JSON.stringify({
                    action_type: "object_resync",
                }),
            }),
        );

        expect(leaseResponse.status).toBe(200);
        const leaseBody = (await leaseResponse.json()) as {
            request: { request_id: string; lease_token: string } | null;
        };
        expect(leaseBody.request).not.toBeNull();
        expect(leaseBody.request?.request_id).toBe(createBody.request.id);

        const failResponse = await app.fetch(
            new Request(
                `http://localhost/api/archive-requests/${leaseBody.request!.request_id}/fail`,
                {
                    method: "POST",
                    headers: {
                        "x-worker-auth-token": "worker-secret",
                        "content-type": "application/json",
                    },
                    body: JSON.stringify({
                        lease_token: leaseBody.request!.lease_token,
                        failure: {
                            code: "SYNC_FAILED",
                            message: "Archive source unavailable",
                            retryable: true,
                            details: {
                                upstream_status: 503,
                            },
                        },
                    }),
                },
            ),
        );

        expect(failResponse.status).toBe(200);
        const failBody = (await failResponse.json()) as {
            status: string;
            request_id: string;
            retryable: boolean;
        };
        expect(failBody.status).toBe("failed");
        expect(failBody.request_id).toBe(createBody.request.id);
        expect(failBody.retryable).toBe(true);

        const listResponse = await app.fetch(
            new Request(`http://localhost/api/objects/${objectId}/resync-requests`, {
                method: "GET",
                headers: {
                    authorization: `Bearer ${viewerToken}`,
                },
            }),
        );

        expect(listResponse.status).toBe(200);
        const listBody = (await listResponse.json()) as {
            requests: Array<{ status: string; failure_reason: string | null }>;
        };
        expect(listBody.requests[0]?.status).toBe("FAILED");
        expect(listBody.requests[0]?.failure_reason).toBe(
            "Archive source unavailable",
        );
    });

    test("lists archive requests with filters and hides payload by default", async () => {
        const app = createTestApp();
        const sql = createSqlClient(TEST_DATABASE_URL!);
        const requestOneId = "91000000-0000-4000-8000-000000000001";
        const requestTwoId = "91000000-0000-4000-8000-000000000002";
        const requestThreeId = "91000000-0000-4000-8000-000000000003";
        const requestTenantTwoId = "91000000-0000-4000-8000-000000000004";

        try {
            await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;

            await sql`
        INSERT INTO archive_requests (
          id,
          tenant_id,
          target_type,
          target_id,
          action_type,
          action_payload,
          requested_by,
          dedupe_key,
          status,
          created_at,
          updated_at
        )
        VALUES
          (
            ${requestOneId},
            ${tenantOneId},
            ${"object"}::archive_request_target_type,
            ${tenantOneObjectId},
            ${"object_resync"}::archive_request_action_type,
            ${{ marker: "first" }},
            ${"10000000-0000-0000-0000-000000000001"},
            ${"resync:first"},
            ${"PENDING"}::archive_request_status,
            ${"2026-03-06T10:00:00.000Z"}::timestamptz,
            ${"2026-03-06T10:00:00.000Z"}::timestamptz
          ),
          (
            ${requestTwoId},
            ${tenantOneId},
            ${"object"}::archive_request_target_type,
            ${tenantOneObjectId},
            ${"object_resync"}::archive_request_action_type,
            ${{ marker: "second" }},
            ${"10000000-0000-0000-0000-000000000001"},
            ${"resync:second"},
            ${"COMPLETED"}::archive_request_status,
            ${"2026-03-06T11:00:00.000Z"}::timestamptz,
            ${"2026-03-06T11:00:00.000Z"}::timestamptz
          ),
          (
            ${requestThreeId},
            ${tenantOneId},
            ${"object"}::archive_request_target_type,
            ${tenantOneObjectIdTwo},
            ${"artifact_fetch"}::archive_request_action_type,
            ${{ marker: "third" }},
            ${"10000000-0000-0000-0000-000000000001"},
            ${"fetch:third"},
            ${"PROCESSING"}::archive_request_status,
            ${"2026-03-06T12:00:00.000Z"}::timestamptz,
            ${"2026-03-06T12:00:00.000Z"}::timestamptz
          ),
          (
            ${requestTenantTwoId},
            ${tenantTwoId},
            ${"object"}::archive_request_target_type,
            ${tenantTwoObjectId},
            ${"object_resync"}::archive_request_action_type,
            ${{ marker: "tenant-two" }},
            ${"10000000-0000-0000-0000-000000000001"},
            ${"resync:tenant-two"},
            ${"PENDING"}::archive_request_status,
            ${"2026-03-06T13:00:00.000Z"}::timestamptz,
            ${"2026-03-06T13:00:00.000Z"}::timestamptz
          )
      `;
        } finally {
            await sql.close();
        }

        const filteredResponse = await app.fetch(
            new Request(
                `http://localhost/api/archive-requests?target_type=object&target_id=${tenantOneObjectId}&action_type=object_resync&active_only=true`,
                {
                    method: "GET",
                    headers: {
                        authorization: `Bearer ${viewerToken}`,
                    },
                },
            ),
        );

        expect(filteredResponse.status).toBe(200);
        const filteredBody = (await filteredResponse.json()) as {
            requests: Array<Record<string, unknown>>;
            filtered_count: number;
        };

        expect(filteredBody.filtered_count).toBe(1);
        expect(filteredBody.requests.length).toBe(1);
        expect(filteredBody.requests[0]?.id).toBe(requestOneId);
        expect(filteredBody.requests[0]?.action_type).toBe("object_resync");
        expect(Object.hasOwn(filteredBody.requests[0] ?? {}, "action_payload")).toBe(
            false,
        );

        const includePayloadResponse = await app.fetch(
            new Request(
                `http://localhost/api/archive-requests?target_type=object&target_id=${tenantOneObjectId}&action_type=object_resync&status=COMPLETED&include_payload=true`,
                {
                    method: "GET",
                    headers: {
                        authorization: `Bearer ${viewerToken}`,
                    },
                },
            ),
        );

        expect(includePayloadResponse.status).toBe(200);
        const includePayloadBody = (await includePayloadResponse.json()) as {
            requests: Array<{ id: string; action_payload?: { marker?: string } }>;
            filtered_count: number;
        };

        expect(includePayloadBody.filtered_count).toBe(1);
        expect(includePayloadBody.requests[0]?.id).toBe(requestTwoId);
        expect(includePayloadBody.requests[0]?.action_payload?.marker).toBe(
            "second",
        );
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
