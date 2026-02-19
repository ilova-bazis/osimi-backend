import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

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

function sha256Hex(value: string): string {
  return new Bun.CryptoHasher("sha256").update(value).digest("hex");
}

describe.skipIf(!TEST_DATABASE_URL)("ingestion routes", () => {
  let schema = "";
  let stagingRoot = "";
  let authToken = "";

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
    schema = `ingest_${Date.now()}_${Math.floor(Math.random() * 100000)}`;
    stagingRoot = await mkdtemp(join(tmpdir(), "osimi-staging-"));

    await runMigrations({
      databaseUrl: TEST_DATABASE_URL,
      schema,
    });

    const sql = createSqlClient(TEST_DATABASE_URL);

    try {
      const tenantsTable = qualifiedTable(schema, "tenants");
      const usersTable = qualifiedTable(schema, "users");
      const membershipsTable = qualifiedTable(schema, "tenant_memberships");

      const operatorHash = await Bun.password.hash("operator123");

      await sql.unsafe(
        `
          INSERT INTO ${tenantsTable} (id, slug, name)
          VALUES ($1, $2, $3)
        `,
        ["00000000-0000-0000-0000-000000000001", "tenant-one", "Tenant One"],
      );

      await sql.unsafe(
        `
          INSERT INTO ${usersTable} (id, username, username_normalized, password_hash)
          VALUES ($1, $2, $3, $4)
        `,
        [
          "10000000-0000-0000-0000-000000000002",
          "archiver@osimi.local",
          "archiver@osimi.local",
          operatorHash,
        ],
      );

      await sql.unsafe(
        `
          INSERT INTO ${membershipsTable} (id, tenant_id, user_id, role)
          VALUES ($1, $2, $3, $4)
        `,
        [
          "20000000-0000-0000-0000-000000000002",
          "00000000-0000-0000-0000-000000000001",
          "10000000-0000-0000-0000-000000000002",
          "archiver",
        ],
      );
    } finally {
      await sql.close();
    }

    const app = createTestApp();
    const loginResponse = await app.fetch(
      new Request("http://localhost/api/auth/login", {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({
          username: "archiver@osimi.local",
          password: "operator123",
        }),
      }),
    );

    const body = (await loginResponse.json()) as { token: string };
    authToken = body.token;
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

  test("creates ingestion, uploads via signed url, commits, and submits", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          batch_label: "batch-001",
        }),
      }),
    );

    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as {
      ingestion: { id: string; status: string };
    };
    expect(createBody.ingestion.status).toBe("DRAFT");

    const ingestionId = createBody.ingestion.id;

    const presignResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/presign`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          filename: "sample.txt",
          content_type: "text/plain",
          size_bytes: 11,
        }),
      }),
    );

    expect(presignResponse.status).toBe(201);
    const presignBody = (await presignResponse.json()) as {
      file_id: string;
      upload_url: string;
    };

    const filePayload = "hello world";
    const uploadResponse = await app.fetch(
      new Request(`http://localhost${presignBody.upload_url}`, {
        method: "PUT",
        headers: {
          "content-type": "text/plain",
          "content-length": String(filePayload.length),
        },
        body: filePayload,
      }),
    );

    expect(uploadResponse.status).toBe(200);

    const commitResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/commit`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          file_id: presignBody.file_id,
          checksum_sha256: sha256Hex(filePayload),
        }),
      }),
    );

    expect(commitResponse.status).toBe(200);
    const commitBody = (await commitResponse.json()) as {
      file: { status: string };
    };
    expect(commitBody.file.status).toBe("UPLOADED");

    const submitResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/submit`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
        },
      }),
    );

    expect(submitResponse.status).toBe(200);
    const submitBody = (await submitResponse.json()) as {
      ingestion: { status: string };
    };
    expect(submitBody.ingestion.status).toBe("QUEUED");

    const detailResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}`, {
        method: "GET",
        headers: {
          authorization: `Bearer ${authToken}`,
        },
      }),
    );

    expect(detailResponse.status).toBe(200);
    const detailBody = (await detailResponse.json()) as {
      ingestion: { status: string };
      files: Array<{ status: string }>;
    };

    expect(detailBody.ingestion.status).toBe("QUEUED");
    expect(detailBody.files.length).toBe(1);
    expect(detailBody.files[0]?.status).toBe("UPLOADED");
  });

  test("re-presigns the same file without creating duplicate file rows", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          batch_label: "batch-represign-001",
        }),
      }),
    );

    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as {
      ingestion: { id: string };
    };
    const ingestionId = createBody.ingestion.id;

    const firstPresignResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/presign`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          filename: "retryable.txt",
          content_type: "text/plain",
          size_bytes: 13,
        }),
      }),
    );

    expect(firstPresignResponse.status).toBe(201);
    const firstPresignBody = (await firstPresignResponse.json()) as {
      file_id: string;
      upload_url: string;
      storage_key: string;
    };

    const secondPresignResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/presign`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          file_id: firstPresignBody.file_id,
        }),
      }),
    );

    expect(secondPresignResponse.status).toBe(201);
    const secondPresignBody = (await secondPresignResponse.json()) as {
      file_id: string;
      upload_url: string;
      storage_key: string;
    };

    expect(secondPresignBody.file_id).toBe(firstPresignBody.file_id);
    expect(secondPresignBody.storage_key).toBe(firstPresignBody.storage_key);
    expect(secondPresignBody.upload_url).not.toBe(firstPresignBody.upload_url);

    const payload = "hello repres!";
    const uploadResponse = await app.fetch(
      new Request(`http://localhost${secondPresignBody.upload_url}`, {
        method: "PUT",
        headers: {
          "content-type": "text/plain",
          "content-length": String(payload.length),
        },
        body: payload,
      }),
    );

    expect(uploadResponse.status).toBe(200);

    const commitResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/commit`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          file_id: firstPresignBody.file_id,
          checksum_sha256: sha256Hex(payload),
        }),
      }),
    );

    expect(commitResponse.status).toBe(200);

    const detailResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}`, {
        method: "GET",
        headers: {
          authorization: `Bearer ${authToken}`,
        },
      }),
    );

    expect(detailResponse.status).toBe(200);
    const detailBody = (await detailResponse.json()) as {
      files: Array<{ id: string; status: string }>;
    };

    expect(detailBody.files.length).toBe(1);
    expect(detailBody.files[0]?.id).toBe(firstPresignBody.file_id);
    expect(detailBody.files[0]?.status).toBe("UPLOADED");
  });

  test("rejects upload content-type/content-length mismatches and checksum mismatches", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          batch_label: "batch-mismatch-001",
        }),
      }),
    );

    const createBody = (await createResponse.json()) as { ingestion: { id: string } };
    const ingestionId = createBody.ingestion.id;

    const presignResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/presign`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          filename: "constraints.txt",
          content_type: "text/plain",
          size_bytes: 5,
        }),
      }),
    );

    expect(presignResponse.status).toBe(201);
    const presignBody = (await presignResponse.json()) as { file_id: string; upload_url: string };

    const wrongType = await app.fetch(
      new Request(`http://localhost${presignBody.upload_url}`, {
        method: "PUT",
        headers: {
          "content-type": "application/json",
          "content-length": "5",
        },
        body: "hello",
      }),
    );

    expect(wrongType.status).toBe(400);

    const wrongLength = await app.fetch(
      new Request(`http://localhost${presignBody.upload_url}`, {
        method: "PUT",
        headers: {
          "content-type": "text/plain",
          "content-length": "4",
        },
        body: "hello",
      }),
    );

    expect(wrongLength.status).toBe(400);

    const validUpload = await app.fetch(
      new Request(`http://localhost${presignBody.upload_url}`, {
        method: "PUT",
        headers: {
          "content-type": "text/plain",
          "content-length": "5",
        },
        body: "hello",
      }),
    );

    expect(validUpload.status).toBe(200);

    const badCommit = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/commit`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          file_id: presignBody.file_id,
          checksum_sha256: sha256Hex("wrong"),
        }),
      }),
    );

    expect(badCommit.status).toBe(409);
  });

  test("rejects re-presign after file is already committed", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          batch_label: "batch-represign-after-commit-001",
        }),
      }),
    );

    const createBody = (await createResponse.json()) as { ingestion: { id: string } };
    const ingestionId = createBody.ingestion.id;

    const presignResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/presign`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          filename: "once.txt",
          content_type: "text/plain",
          size_bytes: 4,
        }),
      }),
    );

    const presignBody = (await presignResponse.json()) as { file_id: string; upload_url: string };

    await app.fetch(
      new Request(`http://localhost${presignBody.upload_url}`, {
        method: "PUT",
        headers: {
          "content-type": "text/plain",
          "content-length": "4",
        },
        body: "once",
      }),
    );

    const commitResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/commit`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          file_id: presignBody.file_id,
          checksum_sha256: sha256Hex("once"),
        }),
      }),
    );

    expect(commitResponse.status).toBe(200);

    const reprsignResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/presign`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          file_id: presignBody.file_id,
        }),
      }),
    );

    expect(reprsignResponse.status).toBe(409);
  });

  test("rejects adding new files after ingestion is submitted", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          batch_label: "batch-submitted-file-guard-001",
        }),
      }),
    );

    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as {
      ingestion: { id: string };
    };
    const ingestionId = createBody.ingestion.id;

    const firstPresign = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/presign`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          filename: "before-submit.txt",
          content_type: "text/plain",
          size_bytes: 5,
        }),
      }),
    );

    expect(firstPresign.status).toBe(201);
    const firstPresignBody = (await firstPresign.json()) as {
      file_id: string;
      upload_url: string;
    };

    const uploadResponse = await app.fetch(
      new Request(`http://localhost${firstPresignBody.upload_url}`, {
        method: "PUT",
        headers: {
          "content-type": "text/plain",
          "content-length": "5",
        },
        body: "hello",
      }),
    );
    expect(uploadResponse.status).toBe(200);

    const commitResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/commit`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          file_id: firstPresignBody.file_id,
          checksum_sha256: sha256Hex("hello"),
        }),
      }),
    );
    expect(commitResponse.status).toBe(200);

    const submitResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/submit`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
        },
      }),
    );
    expect(submitResponse.status).toBe(200);

    const blockedPresign = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/presign`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          filename: "after-submit.txt",
          content_type: "text/plain",
          size_bytes: 3,
        }),
      }),
    );

    expect(blockedPresign.status).toBe(409);
  });

  test("rejects file commit after ingestion is submitted", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          batch_label: "batch-submitted-commit-guard-001",
        }),
      }),
    );

    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as {
      ingestion: { id: string };
    };
    const ingestionId = createBody.ingestion.id;

    const presignResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/presign`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          filename: "late-commit.txt",
          content_type: "text/plain",
          size_bytes: 4,
        }),
      }),
    );

    expect(presignResponse.status).toBe(201);
    const presignBody = (await presignResponse.json()) as {
      file_id: string;
      upload_url: string;
    };

    const uploadResponse = await app.fetch(
      new Request(`http://localhost${presignBody.upload_url}`, {
        method: "PUT",
        headers: {
          "content-type": "text/plain",
          "content-length": "4",
        },
        body: "late",
      }),
    );
    expect(uploadResponse.status).toBe(200);

    const submitResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/submit`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
        },
      }),
    );
    expect(submitResponse.status).toBe(409);

    const commitResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/commit`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          file_id: presignBody.file_id,
          checksum_sha256: sha256Hex("late"),
        }),
      }),
    );

    expect(commitResponse.status).toBe(200);

    const submittedAfterCommit = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/submit`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
        },
      }),
    );
    expect(submittedAfterCommit.status).toBe(200);

    const blockedCommit = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/commit`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          file_id: presignBody.file_id,
          checksum_sha256: sha256Hex("late"),
        }),
      }),
    );

    expect(blockedCommit.status).toBe(409);
  });
});
