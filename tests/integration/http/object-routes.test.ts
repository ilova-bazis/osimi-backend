import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { createAppWithOptions as createApp } from "../../../src/app.ts";
import { createSqlClient } from "../../../src/db/client.ts";
import { runMigrations } from "../../../src/db/migrate.ts";

const TEST_DATABASE_URL = process.env.TEST_DATABASE_URL ?? process.env.DATABASE_URL;

function quoteIdentifier(identifier: string): string {
  return `"${identifier}"`;
}

function qualifiedTable(schema: string, table: string): string {
  return `${quoteIdentifier(schema)}.${quoteIdentifier(table)}`;
}

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
  const sourceIngestionId = "30000000-0000-0000-0000-000000000001";
  const artifactId = "60000000-0000-0000-0000-000000000001";
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
      const tenantsTable = qualifiedTable(schema, "tenants");
      const usersTable = qualifiedTable(schema, "users");
      const membershipsTable = qualifiedTable(schema, "tenant_memberships");
      const ingestionsTable = qualifiedTable(schema, "ingestions");
      const objectsTable = qualifiedTable(schema, "objects");
      const tagsTable = qualifiedTable(schema, "tags");
      const objectTagsTable = qualifiedTable(schema, "object_tags");
      const artifactsTable = qualifiedTable(schema, "object_artifacts");

      const operatorHash = await Bun.password.hash("operator123");
      const viewerHash = await Bun.password.hash("viewer123");
      const adminHash = await Bun.password.hash("admin123");

      await sql.unsafe(
        `
          INSERT INTO ${tenantsTable} (id, slug, name)
          VALUES
            ($1, $2, $3),
            ($4, $5, $6)
        `,
        [tenantOneId, "tenant-one", "Tenant One", tenantTwoId, "tenant-two", "Tenant Two"],
      );

      await sql.unsafe(
        `
          INSERT INTO ${usersTable} (id, username, username_normalized, password_hash)
          VALUES
            ($1, $2, $3, $4),
            ($5, $6, $7, $8),
            ($9, $10, $11, $12)
        `,
        [
          "10000000-0000-0000-0000-000000000001",
          "archiver@osimi.local",
          "archiver@osimi.local",
          operatorHash,
          "10000000-0000-0000-0000-000000000002",
          "viewer@osimi.local",
          "viewer@osimi.local",
          viewerHash,
          "10000000-0000-0000-0000-000000000003",
          "admin@osimi.local",
          "admin@osimi.local",
          adminHash,
        ],
      );

      await sql.unsafe(
        `
          INSERT INTO ${membershipsTable} (id, tenant_id, user_id, role)
          VALUES
            ($1, $2, $3, $4),
            ($5, $6, $7, $8),
            ($9, $10, $11, $12)
        `,
        [
          "20000000-0000-0000-0000-000000000001",
          tenantOneId,
          "10000000-0000-0000-0000-000000000001",
          "archiver",
          "20000000-0000-0000-0000-000000000002",
          tenantOneId,
          "10000000-0000-0000-0000-000000000002",
          "viewer",
          "20000000-0000-0000-0000-000000000003",
          tenantOneId,
          "10000000-0000-0000-0000-000000000003",
          "admin",
        ],
      );

      await sql.unsafe(
        `
          INSERT INTO ${ingestionsTable} (
            id,
            batch_label,
            tenant_id,
            status,
            created_by,
            schema_version,
            document_type,
            language_code,
            pipeline_preset,
            access_level
          )
          VALUES ($1, $2, $3, 'COMPLETED', $4, '1.0', 'document', 'en', 'auto', 'private')
        `,
        [
          sourceIngestionId,
          "batch-alpha-2026",
          tenantOneId,
          "10000000-0000-0000-0000-000000000001",
        ],
      );

      await sql.unsafe(
        `
          INSERT INTO ${objectsTable} (object_id, tenant_id, type, title, metadata, ingest_manifest, source_ingestion_id, availability_state)
          VALUES
            ($1, $2, 'DOCUMENT', $3, $4::jsonb, '{"schema_version":"1.0","ingest":{"ingest_id":"ING-object-routes"}}'::jsonb, NULL, 'AVAILABLE'),
            ($5, $6, 'DOCUMENT', $7, $8::jsonb, NULL, $9, 'ARCHIVED'),
            ($10, $11, 'IMAGE', $12, $13::jsonb, NULL, NULL, 'AVAILABLE'),
            ($14, $15, 'IMAGE', $16, $17::jsonb, NULL, NULL, 'AVAILABLE')
        `,
        [
          tenantOneObjectId,
          tenantOneId,
          "Tenant One Object",
          JSON.stringify({ source: "scanner-a" }),
          tenantOneObjectIdTwo,
          tenantOneId,
          "Project Ledger",
          JSON.stringify({ source: "scanner-b" }),
          sourceIngestionId,
          tenantOneObjectIdThree,
          tenantOneId,
          "Summer Photo",
          JSON.stringify({ source: "camera-1" }),
          tenantTwoObjectId,
          tenantTwoId,
          "Tenant Two Object",
          JSON.stringify({ source: "private-upload" }),
        ],
      );

      await sql.unsafe(
        `
          UPDATE ${objectsTable}
          SET
            created_at = $2::timestamptz,
            updated_at = $3::timestamptz,
            language_code = $4
          WHERE object_id = $1
        `,
        [
          tenantOneObjectId,
          "2026-02-09T10:00:00.000Z",
          "2026-02-09T10:00:00.000Z",
          null,
        ],
      );

      await sql.unsafe(
        `
          UPDATE ${objectsTable}
          SET
            created_at = $2::timestamptz,
            updated_at = $3::timestamptz,
            language_code = $4
          WHERE object_id = $1
        `,
        [
          tenantOneObjectIdTwo,
          "2026-02-10T10:00:00.000Z",
          "2026-02-12T12:00:00.000Z",
          "en",
        ],
      );

      await sql.unsafe(
        `
          UPDATE ${objectsTable}
          SET
            created_at = $2::timestamptz,
            updated_at = $3::timestamptz,
            language_code = $4,
            access_level = 'public',
            embargo_kind = 'curation_state',
            embargo_curation_state = 'reviewed'
          WHERE object_id = $1
        `,
        [
          tenantOneObjectIdThree,
          "2026-02-11T10:00:00.000Z",
          "2026-02-11T11:00:00.000Z",
          null,
        ],
      );

      await sql.unsafe(
        `
          INSERT INTO ${tagsTable} (id, name_normalized, display_name)
          VALUES
            ($1, 'history', 'History'),
            ($2, 'finance', 'Finance'),
            ($3, 'photo', 'Photo')
        `,
        [
          "70000000-0000-0000-0000-000000000001",
          "70000000-0000-0000-0000-000000000002",
          "70000000-0000-0000-0000-000000000003",
        ],
      );

      await sql.unsafe(
        `
          INSERT INTO ${objectTagsTable} (object_id, tag_id)
          VALUES
            ($1, $2),
            ($3, $4),
            ($5, $6)
        `,
        [
          tenantOneObjectId,
          "70000000-0000-0000-0000-000000000001",
          tenantOneObjectIdTwo,
          "70000000-0000-0000-0000-000000000002",
          tenantOneObjectIdThree,
          "70000000-0000-0000-0000-000000000003",
        ],
      );

      await sql.unsafe(
        `
          INSERT INTO ${artifactsTable} (id, object_id, kind, storage_key, content_type, size_bytes)
          VALUES ($1, $2, 'metadata', $3, 'application/json', $4)
        `,
        [artifactId, tenantOneObjectId, artifactStorageKey, 18],
      );
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
        await sql.unsafe(`DROP SCHEMA IF EXISTS ${quoteIdentifier(schema)} CASCADE`);
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
      new Request("http://localhost/api/objects?type=DOCUMENT&q=Tenant%20One%20Object", {
        method: "GET",
        headers: {
          authorization: `Bearer ${operatorToken}`,
        },
      }),
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
    expect(Object.prototype.hasOwnProperty.call(body.objects[0] ?? {}, "ingest_manifest")).toBe(false);
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
      objects: Array<{ object_id: string; source_batch_label: string | null; tags: string[]; language: string | null }>;
      total_count: number;
      filtered_count: number;
      next_cursor: string | null;
    };

    expect(filteredBody.total_count).toBe(3);
    expect(filteredBody.filtered_count).toBe(1);
    expect(filteredBody.objects.length).toBe(1);
    expect(filteredBody.objects[0]?.object_id).toBe(tenantOneObjectIdTwo);
    expect(filteredBody.objects[0]?.source_batch_label).toBe("batch-alpha-2026");
    expect(filteredBody.objects[0]?.tags).toEqual(["finance"]);
    expect(filteredBody.objects[0]?.language).toBe("en");
    expect(filteredBody.next_cursor).toBeNull();

    const firstPage = await app.fetch(
      new Request("http://localhost/api/objects?limit=1&sort=created_at_desc", {
        method: "GET",
        headers: {
          authorization: `Bearer ${operatorToken}`,
        },
      }),
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
    expect(firstPageBody.objects[0]?.object_id).toBe(tenantOneObjectIdThree);
    expect(typeof firstPageBody.next_cursor).toBe("string");

    const secondPage = await app.fetch(
      new Request(`http://localhost/api/objects?limit=1&sort=created_at_desc&cursor=${firstPageBody.next_cursor}`, {
        method: "GET",
        headers: {
          authorization: `Bearer ${operatorToken}`,
        },
      }),
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
        new Request(`http://localhost/api/objects?sort=${sort}&q=Summer%20Photo`, {
          method: "GET",
          headers: {
            authorization: `Bearer ${viewerToken}`,
          },
        }),
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
      new Request(`http://localhost/api/objects/${tenantOneObjectId}/artifacts`, {
        method: "GET",
        headers: {
          authorization: `Bearer ${operatorToken}`,
        },
      }),
    );

    expect(listResponse.status).toBe(200);
    const listBody = (await listResponse.json()) as {
      artifacts: Array<{ id: string }>;
    };
    expect(listBody.artifacts.length).toBe(1);
    expect(listBody.artifacts[0]?.id).toBe(artifactId);

    const downloadResponse = await app.fetch(
      new Request(`http://localhost/api/objects/${tenantOneObjectId}/artifacts/${artifactId}/download`, {
        method: "GET",
        headers: {
          authorization: `Bearer ${adminToken}`,
        },
      }),
    );

    expect(downloadResponse.status).toBe(200);
    expect(downloadResponse.headers.get("content-type")).toBe("application/json");
    expect(await downloadResponse.text()).toBe('{"status":"ready"}');
  });

  test("supports admin-only access approvals and explicit assignment downloads", async () => {
    const app = createTestApp();

    const deniedBeforeApproval = await app.fetch(
      new Request(`http://localhost/api/objects/${tenantOneObjectId}/artifacts/${artifactId}/download`, {
        method: "GET",
        headers: {
          authorization: `Bearer ${viewerToken}`,
        },
      }),
    );

    expect(deniedBeforeApproval.status).toBe(400);

    const createRequestResponse = await app.fetch(
      new Request(`http://localhost/api/objects/${tenantOneObjectId}/access-requests`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${viewerToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          requested_level: "private",
          reason: "Research usage",
        }),
      }),
    );

    expect(createRequestResponse.status).toBe(201);
    const createRequestBody = (await createRequestResponse.json()) as {
      request: { id: string; status: string };
    };
    expect(createRequestBody.request.status).toBe("PENDING");

    const archiverListResponse = await app.fetch(
      new Request(`http://localhost/api/objects/${tenantOneObjectId}/access-requests`, {
        method: "GET",
        headers: {
          authorization: `Bearer ${operatorToken}`,
        },
      }),
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
      new Request(`http://localhost/api/objects/${tenantOneObjectId}/access-assignments`, {
        method: "GET",
        headers: {
          authorization: `Bearer ${adminToken}`,
        },
      }),
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
      new Request(`http://localhost/api/objects/${tenantOneObjectId}/artifacts/${artifactId}/download`, {
        method: "GET",
        headers: {
          authorization: `Bearer ${viewerToken}`,
        },
      }),
    );

    expect(allowedAfterApproval.status).toBe(200);
    expect(await allowedAfterApproval.text()).toBe('{"status":"ready"}');
  });

  test("rejects duplicate pending requests and re-approval of resolved request", async () => {
    const app = createTestApp();
    const targetObjectId = tenantOneObjectIdTwo;

    const firstCreate = await app.fetch(
      new Request(`http://localhost/api/objects/${targetObjectId}/access-requests`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${viewerToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          requested_level: "family",
          reason: "Need read access",
        }),
      }),
    );

    expect(firstCreate.status).toBe(201);
    const firstCreateBody = (await firstCreate.json()) as {
      request: { id: string; status: string };
    };
    expect(firstCreateBody.request.status).toBe("PENDING");

    const secondCreate = await app.fetch(
      new Request(`http://localhost/api/objects/${targetObjectId}/access-requests`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${viewerToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          requested_level: "private",
          reason: "Escalation",
        }),
      }),
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
      new Request(`http://localhost/api/objects/${tenantOneObjectId}/access-requests`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${viewerToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          requested_level: "family",
          reason: "Need access",
        }),
      }),
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
      new Request(`http://localhost/api/objects/${tenantOneObjectId}/access-requests`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${viewerToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          requested_level: "family",
          reason: "Need access again",
        }),
      }),
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
