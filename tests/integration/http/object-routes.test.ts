import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { createApp } from "../../../src/app.ts";
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

  const tenantOneId = "00000000-0000-0000-0000-000000000001";
  const tenantTwoId = "00000000-0000-0000-0000-000000000002";
  const tenantOneObjectId = "OBJ-20260209-ABC123";
  const tenantTwoObjectId = "OBJ-20260209-XYZ789";
  const artifactId = "60000000-0000-0000-0000-000000000001";
  const artifactStorageKey = `tenants/${tenantOneId}/objects/${tenantOneObjectId}/artifacts/ingest.json`;

  let previousDatabaseUrl: string | undefined;
  let previousSchema: string | undefined;
  let previousStagingRoot: string | undefined;

  beforeAll(async () => {
    schema = `objects_${Date.now()}_${Math.floor(Math.random() * 100000)}`;
    stagingRoot = await mkdtemp(join(tmpdir(), "osimi-objects-staging-"));

    await runMigrations({
      databaseUrl: TEST_DATABASE_URL,
      schema,
    });

    previousDatabaseUrl = process.env.DATABASE_URL;
    previousSchema = process.env.DB_SCHEMA;
    previousStagingRoot = process.env.STAGING_ROOT;

    process.env.DATABASE_URL = TEST_DATABASE_URL;
    process.env.DB_SCHEMA = schema;
    process.env.STAGING_ROOT = stagingRoot;

    const sql = createSqlClient(TEST_DATABASE_URL!);

    try {
      const tenantsTable = qualifiedTable(schema, "tenants");
      const usersTable = qualifiedTable(schema, "users");
      const membershipsTable = qualifiedTable(schema, "tenant_memberships");
      const objectsTable = qualifiedTable(schema, "objects");
      const artifactsTable = qualifiedTable(schema, "object_artifacts");

      const operatorHash = await Bun.password.hash("operator123");
      const viewerHash = await Bun.password.hash("viewer123");

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
            ($5, $6, $7, $8)
        `,
        [
          "10000000-0000-0000-0000-000000000001",
          "operator@osimi.local",
          "operator@osimi.local",
          operatorHash,
          "10000000-0000-0000-0000-000000000002",
          "viewer@osimi.local",
          "viewer@osimi.local",
          viewerHash,
        ],
      );

      await sql.unsafe(
        `
          INSERT INTO ${membershipsTable} (id, tenant_id, user_id, role)
          VALUES
            ($1, $2, $3, $4),
            ($5, $6, $7, $8)
        `,
        [
          "20000000-0000-0000-0000-000000000001",
          tenantOneId,
          "10000000-0000-0000-0000-000000000001",
          "operator",
          "20000000-0000-0000-0000-000000000002",
          tenantOneId,
          "10000000-0000-0000-0000-000000000002",
          "viewer",
        ],
      );

      await sql.unsafe(
        `
          INSERT INTO ${objectsTable} (object_id, tenant_id, type, title, metadata, source_ingestion_id, status)
          VALUES
            ($1, $2, 'DOCUMENT', $3, $4::jsonb, NULL, 'ACTIVE'),
            ($5, $6, 'IMAGE', $7, $8::jsonb, NULL, 'ACTIVE')
        `,
        [
          tenantOneObjectId,
          tenantOneId,
          "Tenant One Object",
          JSON.stringify({ tags: ["history", "archive"] }),
          tenantTwoObjectId,
          tenantTwoId,
          "Tenant Two Object",
          JSON.stringify({ tags: ["private"] }),
        ],
      );

      await sql.unsafe(
        `
          INSERT INTO ${artifactsTable} (id, object_id, kind, storage_key, content_type, size_bytes)
          VALUES ($1, $2, 'ingest_json', $3, 'application/json', $4)
        `,
        [artifactId, tenantOneObjectId, artifactStorageKey, 18],
      );
    } finally {
      await sql.close();
    }

    const artifactPath = join(stagingRoot, artifactStorageKey);
    await mkdir(dirname(artifactPath), { recursive: true });
    await Bun.write(artifactPath, '{"status":"ready"}');

    const app = createApp();

    const operatorLogin = await app.fetch(
      new Request("http://localhost/api/auth/login", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          username: "operator@osimi.local",
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

    if (previousDatabaseUrl === undefined) {
      delete process.env.DATABASE_URL;
    } else {
      process.env.DATABASE_URL = previousDatabaseUrl;
    }

    if (previousSchema === undefined) {
      delete process.env.DB_SCHEMA;
    } else {
      process.env.DB_SCHEMA = previousSchema;
    }

    if (previousStagingRoot === undefined) {
      delete process.env.STAGING_ROOT;
    } else {
      process.env.STAGING_ROOT = previousStagingRoot;
    }
  });

  test("lists tenant-scoped objects", async () => {
    const app = createApp();
    const response = await app.fetch(
      new Request("http://localhost/api/objects?type=DOCUMENT", {
        method: "GET",
        headers: {
          authorization: `Bearer ${operatorToken}`,
        },
      }),
    );

    expect(response.status).toBe(200);
    const body = (await response.json()) as {
      objects: Array<{ object_id: string }>;
    };

    expect(body.objects.length).toBe(1);
    expect(body.objects[0]?.object_id).toBe(tenantOneObjectId);
  });

  test("returns object detail and blocks cross-tenant object", async () => {
    const app = createApp();

    const okResponse = await app.fetch(
      new Request(`http://localhost/api/objects/${tenantOneObjectId}`, {
        method: "GET",
        headers: {
          authorization: `Bearer ${operatorToken}`,
        },
      }),
    );

    expect(okResponse.status).toBe(200);

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

  test("patches title for operator and blocks viewer", async () => {
    const app = createApp();

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
    const app = createApp();

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
          authorization: `Bearer ${operatorToken}`,
        },
      }),
    );

    expect(downloadResponse.status).toBe(200);
    expect(downloadResponse.headers.get("content-type")).toBe("application/json");
    expect(await downloadResponse.text()).toBe('{"status":"ready"}');
  });
});
