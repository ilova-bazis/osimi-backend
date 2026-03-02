import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { sql as sqlIdentifier } from "bun";

import { createAppWithOptions as createApp } from "../../../src/app.ts";
import { createSqlClient } from "../../../src/db/client.ts";
import { runMigrations } from "../../../src/db/migrate.ts";

const TEST_DATABASE_URL = process.env.TEST_DATABASE_URL ?? process.env.DATABASE_URL;

function sha256Hex(value: string): string {
  return new Bun.CryptoHasher("sha256").update(value).digest("hex");
}

function buildSummary(overrides?: Record<string, unknown>): Record<string, unknown> {
  return {
    title: {
      primary: "Sample title",
      original_script: null,
      translations: [],
    },
    classification: {
      tags: ["source:test"],
      summary: null,
    },
    dates: {
      published: {
        value: null,
        approximate: true,
        confidence: "low",
        note: null,
      },
      created: {
        value: null,
        approximate: true,
        confidence: "low",
        note: null,
      },
    },
    ...(overrides ?? {}),
  };
}

function buildIngestionBody(overrides?: Record<string, unknown>): Record<string, unknown> {
  return {
    batch_label: `batch-${Date.now()}-${Math.floor(Math.random() * 100000)}`,
    schema_version: "1.0",
    classification_type: "document",
    item_kind: "document",
    language_code: "en",
    pipeline_preset: "auto",
    access_level: "private",
    summary: buildSummary(),
    ...(overrides ?? {}),
  };
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
      const operatorHash = await Bun.password.hash("operator123");
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;

      await sql`
        INSERT INTO tenants (id, slug, name)
        VALUES (${"00000000-0000-0000-0000-000000000001"}, ${"tenant-one"}, ${"Tenant One"})
      `;

      await sql`
        INSERT INTO users (id, username, username_normalized, password_hash)
        VALUES (${"10000000-0000-0000-0000-000000000002"}, ${"archiver@osimi.local"}, ${"archiver@osimi.local"}, ${operatorHash})
      `;

      await sql`
        INSERT INTO tenant_memberships (id, tenant_id, user_id, role)
        VALUES (${"20000000-0000-0000-0000-000000000002"}, ${"00000000-0000-0000-0000-000000000001"}, ${"10000000-0000-0000-0000-000000000002"}, ${"archiver"})
      `;
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
        await sql`DROP SCHEMA IF EXISTS ${sqlIdentifier(schema)} CASCADE`;
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
        body: JSON.stringify(buildIngestionBody({ batch_label: "batch-001" })),
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

  test("updates ingestion metadata while draft", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(buildIngestionBody({ batch_label: "batch-update-001" })),
      }),
    );

    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as {
      ingestion: { id: string };
    };
    const ingestionId = createBody.ingestion.id;

    const patchResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}`, {
        method: "PATCH",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          batch_label: "batch-update-002",
          rights_note: "Updated rights",
          sensitivity_note: null,
          embargo_until: "2026-02-21T10:00:00.000Z",
          summary: buildSummary({
            classification: {
              tags: ["updated"],
              summary: null,
            },
          }),
        }),
      }),
    );

    expect(patchResponse.status).toBe(200);
    const patchBody = (await patchResponse.json()) as {
      ingestion: {
        batch_label: string;
        rights_note: string | null;
        sensitivity_note: string | null;
        embargo_until: string | null;
        summary: { classification: { tags: string[] } };
      };
    };
    expect(patchBody.ingestion.batch_label).toBe("batch-update-002");
    expect(patchBody.ingestion.rights_note).toBe("Updated rights");
    expect(patchBody.ingestion.sensitivity_note).toBeNull();
    expect(patchBody.ingestion.embargo_until).toBe("2026-02-21T10:00:00.000Z");
    expect(patchBody.ingestion.summary.classification.tags).toEqual(["updated"]);

    const getResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}`, {
        method: "GET",
        headers: {
          authorization: `Bearer ${authToken}`,
        },
      }),
    );

    expect(getResponse.status).toBe(200);
    const getBody = (await getResponse.json()) as {
      ingestion: {
        batch_label: string;
        rights_note: string | null;
        sensitivity_note: string | null;
        embargo_until: string | null;
      };
    };
    expect(getBody.ingestion.batch_label).toBe("batch-update-002");
    expect(getBody.ingestion.rights_note).toBe("Updated rights");
    expect(getBody.ingestion.sensitivity_note).toBeNull();
    expect(getBody.ingestion.embargo_until).toBe("2026-02-21T10:00:00.000Z");
  });

  test("rejects empty ingestion update payload", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(buildIngestionBody({ batch_label: "batch-update-empty-001" })),
      }),
    );

    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as {
      ingestion: { id: string };
    };

    const patchResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${createBody.ingestion.id}`, {
        method: "PATCH",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({}),
      }),
    );

    expect(patchResponse.status).toBe(400);
  });

  test("returns ingestion capabilities", async () => {
    const app = createTestApp();

    const response = await app.fetch(
      new Request("http://localhost/api/ingestions/capabilities", {
        method: "GET",
        headers: {
          authorization: `Bearer ${authToken}`,
        },
      }),
    );

    expect(response.status).toBe(200);
    const body = (await response.json()) as {
      media_kinds: string[];
      extensions_by_kind: Record<string, string[]>;
      mime_by_kind: Record<string, string[]>;
      mime_aliases: Record<string, string>;
    };

    expect(body.media_kinds).toContain("image");
    expect(body.extensions_by_kind.image).toContain("jpg");
    expect(body.mime_by_kind.image).toContain("image/png");
    expect(body.mime_aliases["image/jpg"]).toBe("image/jpeg");
  });

  test("cancels a queued ingestion back to uploading", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(buildIngestionBody({ batch_label: "batch-cancel-queued-001" })),
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
          filename: "queued.txt",
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

    const payload = "data";
    const uploadResponse = await app.fetch(
      new Request(`http://localhost${presignBody.upload_url}`, {
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
          file_id: presignBody.file_id,
          checksum_sha256: sha256Hex(payload),
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

    const cancelResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/cancel`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
        },
      }),
    );

    expect(cancelResponse.status).toBe(200);
    const cancelBody = (await cancelResponse.json()) as {
      ingestion: { status: string };
    };
    expect(cancelBody.ingestion.status).toBe("UPLOADING");
  });

  test("restores a canceled ingestion based on files", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(buildIngestionBody({ batch_label: "batch-restore-001" })),
      }),
    );

    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as {
      ingestion: { id: string };
    };
    const ingestionId = createBody.ingestion.id;

    const cancelDraftResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/cancel`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
        },
      }),
    );

    expect(cancelDraftResponse.status).toBe(200);

    const restoreDraftResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/restore`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
        },
      }),
    );

    expect(restoreDraftResponse.status).toBe(200);
    const restoreDraftBody = (await restoreDraftResponse.json()) as {
      ingestion: { status: string };
    };
    expect(restoreDraftBody.ingestion.status).toBe("DRAFT");

    const presignResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/presign`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          filename: "restore.txt",
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

    const payload = "data";
    const uploadResponse = await app.fetch(
      new Request(`http://localhost${presignBody.upload_url}`, {
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
          file_id: presignBody.file_id,
          checksum_sha256: sha256Hex(payload),
        }),
      }),
    );

    expect(commitResponse.status).toBe(200);

    const cancelUploadingResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/cancel`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
        },
      }),
    );

    expect(cancelUploadingResponse.status).toBe(200);

    const restoreUploadingResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/restore`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
        },
      }),
    );

    expect(restoreUploadingResponse.status).toBe(200);
    const restoreUploadingBody = (await restoreUploadingResponse.json()) as {
      ingestion: { status: string };
    };
    expect(restoreUploadingBody.ingestion.status).toBe("UPLOADING");
  });

  test("cancel is a no-op on canceled ingestions", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(buildIngestionBody({ batch_label: "batch-cancel-noop-001" })),
      }),
    );

    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as {
      ingestion: { id: string };
    };
    const ingestionId = createBody.ingestion.id;

    const cancelResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/cancel`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
        },
      }),
    );

    expect(cancelResponse.status).toBe(200);
    const cancelBody = (await cancelResponse.json()) as {
      ingestion: { status: string };
    };
    expect(cancelBody.ingestion.status).toBe("CANCELED");

    const cancelAgainResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/cancel`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
        },
      }),
    );

    expect(cancelAgainResponse.status).toBe(200);
    const cancelAgainBody = (await cancelAgainResponse.json()) as {
      ingestion: { status: string };
    };
    expect(cancelAgainBody.ingestion.status).toBe("CANCELED");
  });

  test("does not delete a queued ingestion", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(buildIngestionBody({ batch_label: "batch-delete-queued-001" })),
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
          filename: "queued.txt",
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

    const payload = "data";
    const uploadResponse = await app.fetch(
      new Request(`http://localhost${presignBody.upload_url}`, {
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
          file_id: presignBody.file_id,
          checksum_sha256: sha256Hex(payload),
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

    const deleteResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}`, {
        method: "DELETE",
        headers: {
          authorization: `Bearer ${authToken}`,
        },
      }),
    );

    expect(deleteResponse.status).toBe(409);
  });

  test("deletes a canceled ingestion", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(buildIngestionBody({ batch_label: "batch-delete-canceled-001" })),
      }),
    );

    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as {
      ingestion: { id: string };
    };
    const ingestionId = createBody.ingestion.id;

    const cancelResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/cancel`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
        },
      }),
    );

    expect(cancelResponse.status).toBe(200);

    const deleteResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}`, {
        method: "DELETE",
        headers: {
          authorization: `Bearer ${authToken}`,
        },
      }),
    );

    expect(deleteResponse.status).toBe(200);

    const getResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}`, {
        method: "GET",
        headers: {
          authorization: `Bearer ${authToken}`,
        },
      }),
    );

    expect(getResponse.status).toBe(404);
  });

  test("removes a committed ingestion file while uploading", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(buildIngestionBody({ batch_label: "batch-remove-file-001" })),
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
          filename: "remove-me.jpg",
          content_type: "image/jpeg",
          size_bytes: 4,
        }),
      }),
    );

    expect(presignResponse.status).toBe(201);
    const presignBody = (await presignResponse.json()) as {
      file_id: string;
      upload_url: string;
    };

    const payload = "data";
    const uploadResponse = await app.fetch(
      new Request(`http://localhost${presignBody.upload_url}`, {
        method: "PUT",
        headers: {
          "content-type": "image/jpeg",
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
          file_id: presignBody.file_id,
          checksum_sha256: sha256Hex(payload),
        }),
      }),
    );

    expect(commitResponse.status).toBe(200);

    const deleteResponse = await app.fetch(
      new Request(
        `http://localhost/api/ingestions/${ingestionId}/files/${presignBody.file_id}`,
        {
          method: "DELETE",
          headers: {
            authorization: `Bearer ${authToken}`,
          },
        },
      ),
    );

    expect(deleteResponse.status).toBe(200);

    const getResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}`, {
        method: "GET",
        headers: {
          authorization: `Bearer ${authToken}`,
        },
      }),
    );

    expect(getResponse.status).toBe(200);
    const getBody = (await getResponse.json()) as {
      files: Array<{ id: string }>;
    };
    expect(getBody.files.length).toBe(0);
  });

  test("stores per-file processing overrides", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(buildIngestionBody({ batch_label: "batch-file-overrides-001" })),
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
          filename: "override.pdf",
          content_type: "application/pdf",
          size_bytes: 4,
        }),
      }),
    );

    expect(presignResponse.status).toBe(201);
    const presignBody = (await presignResponse.json()) as {
      file_id: string;
    };

    const overrideResponse = await app.fetch(
      new Request(
        `http://localhost/api/ingestions/${ingestionId}/files/${presignBody.file_id}/overrides`,
        {
          method: "POST",
          headers: {
            authorization: `Bearer ${authToken}`,
            "content-type": "application/json",
          },
          body: JSON.stringify({
            processing_overrides: {
              ocr_text: { enabled: true, language: "tg" },
              video_transcript: { enabled: false },
            },
          }),
        },
      ),
    );

    expect(overrideResponse.status).toBe(200);
    const overrideBody = (await overrideResponse.json()) as {
      file: { processing_overrides: Record<string, unknown> };
    };
    expect(overrideBody.file.processing_overrides).toMatchObject({
      ocr_text: { enabled: true, language: "tg" },
      video_transcript: { enabled: false },
    });

    const getResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}`, {
        method: "GET",
        headers: {
          authorization: `Bearer ${authToken}`,
        },
      }),
    );

    expect(getResponse.status).toBe(200);
    const getBody = (await getResponse.json()) as {
      files: Array<{ processing_overrides: Record<string, unknown> }>;
    };
    expect(getBody.files[0]?.processing_overrides).toMatchObject({
      ocr_text: { enabled: true, language: "tg" },
      video_transcript: { enabled: false },
    });
  });

  test("allows mixed image types in one ingestion", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(buildIngestionBody({ batch_label: "batch-jpeg-jpg-001" })),
      }),
    );

    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as {
      ingestion: { id: string };
    };
    const ingestionId = createBody.ingestion.id;

    const jpegPresign = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/presign`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          filename: "photo-a.jpeg",
          content_type: "image/jpeg",
          size_bytes: 8,
        }),
      }),
    );

    expect(jpegPresign.status).toBe(201);
    const jpegBody = (await jpegPresign.json()) as {
      file_id: string;
      upload_url: string;
    };

    const webpPresign = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/presign`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          filename: "photo-b.webp",
          content_type: "image/webp",
          size_bytes: 7,
        }),
      }),
    );

    expect(webpPresign.status).toBe(201);
    const webpBody = (await webpPresign.json()) as {
      file_id: string;
      upload_url: string;
    };

    const jpegPayload = "jpegdata";
    const jpegUpload = await app.fetch(
      new Request(`http://localhost${jpegBody.upload_url}`, {
        method: "PUT",
        headers: {
          "content-type": "image/jpeg",
          "content-length": String(jpegPayload.length),
        },
        body: jpegPayload,
      }),
    );

    expect(jpegUpload.status).toBe(200);

    const webpPayload = "webpdat";
    const webpUpload = await app.fetch(
      new Request(`http://localhost${webpBody.upload_url}`, {
        method: "PUT",
        headers: {
          "content-type": "image/webp",
          "content-length": String(webpPayload.length),
        },
        body: webpPayload,
      }),
    );

    expect(webpUpload.status).toBe(200);

    const jpegCommit = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/commit`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          file_id: jpegBody.file_id,
          checksum_sha256: sha256Hex(jpegPayload),
        }),
      }),
    );

    expect(jpegCommit.status).toBe(200);

    const webpCommit = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/commit`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          file_id: webpBody.file_id,
          checksum_sha256: sha256Hex(webpPayload),
        }),
      }),
    );

    expect(webpCommit.status).toBe(200);
  });

  test("allows mp3 and mpeg content types in one ingestion", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(buildIngestionBody({ batch_label: "batch-mp3-mpeg-001" })),
      }),
    );

    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as {
      ingestion: { id: string };
    };
    const ingestionId = createBody.ingestion.id;

    const mp3Presign = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/presign`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          filename: "audio-a.mp3",
          content_type: "audio/mp3",
          size_bytes: 8,
        }),
      }),
    );

    expect(mp3Presign.status).toBe(201);
    const mp3Body = (await mp3Presign.json()) as {
      file_id: string;
      upload_url: string;
    };

    const mpegPresign = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/presign`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          filename: "audio-b.mpeg",
          content_type: "audio/mpeg",
          size_bytes: 7,
        }),
      }),
    );

    expect(mpegPresign.status).toBe(201);
    const mpegBody = (await mpegPresign.json()) as {
      file_id: string;
      upload_url: string;
    };

    const mp3Payload = "mp3audio";
    const mp3Upload = await app.fetch(
      new Request(`http://localhost${mp3Body.upload_url}`, {
        method: "PUT",
        headers: {
          "content-type": "audio/mp3",
          "content-length": String(mp3Payload.length),
        },
        body: mp3Payload,
      }),
    );

    expect(mp3Upload.status).toBe(200);

    const mpegPayload = "mpegdat";
    const mpegUpload = await app.fetch(
      new Request(`http://localhost${mpegBody.upload_url}`, {
        method: "PUT",
        headers: {
          "content-type": "audio/mpeg",
          "content-length": String(mpegPayload.length),
        },
        body: mpegPayload,
      }),
    );

    expect(mpegUpload.status).toBe(200);

    const mp3Commit = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/commit`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          file_id: mp3Body.file_id,
          checksum_sha256: sha256Hex(mp3Payload),
        }),
      }),
    );

    expect(mp3Commit.status).toBe(200);

    const mpegCommit = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/commit`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          file_id: mpegBody.file_id,
          checksum_sha256: sha256Hex(mpegPayload),
        }),
      }),
    );

    expect(mpegCommit.status).toBe(200);
  });

  test("allows x-pdf and pdf content types in one ingestion", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(buildIngestionBody({ batch_label: "batch-xpdf-pdf-001" })),
      }),
    );

    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as {
      ingestion: { id: string };
    };
    const ingestionId = createBody.ingestion.id;

    const xpdfPresign = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/presign`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          filename: "doc-a.xpdf",
          content_type: "application/x-pdf",
          size_bytes: 8,
        }),
      }),
    );

    expect(xpdfPresign.status).toBe(201);
    const xpdfBody = (await xpdfPresign.json()) as {
      file_id: string;
      upload_url: string;
    };

    const pdfPresign = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/presign`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          filename: "doc-b.pdf",
          content_type: "application/pdf",
          size_bytes: 7,
        }),
      }),
    );

    expect(pdfPresign.status).toBe(201);
    const pdfBody = (await pdfPresign.json()) as {
      file_id: string;
      upload_url: string;
    };

    const xpdfPayload = "xpdfdata";
    const xpdfUpload = await app.fetch(
      new Request(`http://localhost${xpdfBody.upload_url}`, {
        method: "PUT",
        headers: {
          "content-type": "application/x-pdf",
          "content-length": String(xpdfPayload.length),
        },
        body: xpdfPayload,
      }),
    );

    expect(xpdfUpload.status).toBe(200);

    const pdfPayload = "pdfdata";
    const pdfUpload = await app.fetch(
      new Request(`http://localhost${pdfBody.upload_url}`, {
        method: "PUT",
        headers: {
          "content-type": "application/pdf",
          "content-length": String(pdfPayload.length),
        },
        body: pdfPayload,
      }),
    );

    expect(pdfUpload.status).toBe(200);

    const xpdfCommit = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/commit`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          file_id: xpdfBody.file_id,
          checksum_sha256: sha256Hex(xpdfPayload),
        }),
      }),
    );

    expect(xpdfCommit.status).toBe(200);

    const pdfCommit = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/commit`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          file_id: pdfBody.file_id,
          checksum_sha256: sha256Hex(pdfPayload),
        }),
      }),
    );

    expect(pdfCommit.status).toBe(200);
  });

  test("allows m4v and mp4 content types in one ingestion", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(buildIngestionBody({ batch_label: "batch-m4v-mp4-001" })),
      }),
    );

    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as {
      ingestion: { id: string };
    };
    const ingestionId = createBody.ingestion.id;

    const m4vPresign = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/presign`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          filename: "video-a.m4v",
          content_type: "video/x-m4v",
          size_bytes: 7,
        }),
      }),
    );

    expect(m4vPresign.status).toBe(201);
    const m4vBody = (await m4vPresign.json()) as {
      file_id: string;
      upload_url: string;
    };

    const mp4Presign = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/presign`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          filename: "video-b.mp4",
          content_type: "video/mp4",
          size_bytes: 7,
        }),
      }),
    );

    expect(mp4Presign.status).toBe(201);
    const mp4Body = (await mp4Presign.json()) as {
      file_id: string;
      upload_url: string;
    };

    const m4vPayload = "m4vdata";
    const m4vUpload = await app.fetch(
      new Request(`http://localhost${m4vBody.upload_url}`, {
        method: "PUT",
        headers: {
          "content-type": "video/x-m4v",
          "content-length": String(m4vPayload.length),
        },
        body: m4vPayload,
      }),
    );

    expect(m4vUpload.status).toBe(200);

    const mp4Payload = "mp4data";
    const mp4Upload = await app.fetch(
      new Request(`http://localhost${mp4Body.upload_url}`, {
        method: "PUT",
        headers: {
          "content-type": "video/mp4",
          "content-length": String(mp4Payload.length),
        },
        body: mp4Payload,
      }),
    );

    expect(mp4Upload.status).toBe(200);

    const m4vCommit = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/commit`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          file_id: m4vBody.file_id,
          checksum_sha256: sha256Hex(m4vPayload),
        }),
      }),
    );

    expect(m4vCommit.status).toBe(200);

    const mp4Commit = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/commit`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          file_id: mp4Body.file_id,
          checksum_sha256: sha256Hex(mp4Payload),
        }),
      }),
    );

    expect(mp4Commit.status).toBe(200);
  });

  test("rejects mixed media kinds in one ingestion", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(buildIngestionBody({ batch_label: "batch-mixed-types-001" })),
      }),
    );

    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as {
      ingestion: { id: string };
    };
    const ingestionId = createBody.ingestion.id;

    const jpegPresign = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/presign`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          filename: "photo-a.jpeg",
          content_type: "image/jpeg",
          size_bytes: 8,
        }),
      }),
    );

    expect(jpegPresign.status).toBe(201);
    const jpegBody = (await jpegPresign.json()) as {
      file_id: string;
      upload_url: string;
    };

    const pdfPresign = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/presign`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          filename: "doc-a.pdf",
          content_type: "application/pdf",
          size_bytes: 6,
        }),
      }),
    );

    expect(pdfPresign.status).toBe(201);
    const pdfBody = (await pdfPresign.json()) as {
      file_id: string;
      upload_url: string;
    };

    const jpegPayload = "jpegdata";
    const jpegUpload = await app.fetch(
      new Request(`http://localhost${jpegBody.upload_url}`, {
        method: "PUT",
        headers: {
          "content-type": "image/jpeg",
          "content-length": String(jpegPayload.length),
        },
        body: jpegPayload,
      }),
    );

    expect(jpegUpload.status).toBe(200);

    const pdfPayload = "pdfdoc";
    const pdfUpload = await app.fetch(
      new Request(`http://localhost${pdfBody.upload_url}`, {
        method: "PUT",
        headers: {
          "content-type": "application/pdf",
          "content-length": String(pdfPayload.length),
        },
        body: pdfPayload,
      }),
    );

    expect(pdfUpload.status).toBe(200);

    const commitResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/commit`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          file_id: jpegBody.file_id,
          checksum_sha256: sha256Hex(jpegPayload),
        }),
      }),
    );

    expect(commitResponse.status).toBe(409);
  });

  test("rejects unsupported content types during commit", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(buildIngestionBody({ batch_label: "batch-unsupported-001" })),
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
          filename: "blob.bin",
          content_type: "application/octet-stream",
          size_bytes: 4,
        }),
      }),
    );

    expect(presignResponse.status).toBe(201);
    const presignBody = (await presignResponse.json()) as {
      file_id: string;
      upload_url: string;
    };

    const payload = "data";
    const uploadResponse = await app.fetch(
      new Request(`http://localhost${presignBody.upload_url}`, {
        method: "PUT",
        headers: {
          "content-type": "application/octet-stream",
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
          file_id: presignBody.file_id,
          checksum_sha256: sha256Hex(payload),
        }),
      }),
    );

    expect(commitResponse.status).toBe(400);
  });

  test("stores summary metadata when provided", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(
          buildIngestionBody({
            batch_label: "batch-catalog-001",
            summary: buildSummary({
              title: {
                primary: "Catalog from UI",
                original_script: null,
                translations: [],
              },
            }),
          }),
        ),
      }),
    );

    expect(createResponse.status).toBe(201);
      const createBody = (await createResponse.json()) as {
        ingestion: {
          id: string;
          summary: { title?: { primary?: string } };
        };
      };

      expect(createBody.ingestion.summary.title?.primary).toBe("Catalog from UI");

    const detailResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${createBody.ingestion.id}`, {
        method: "GET",
        headers: {
          authorization: `Bearer ${authToken}`,
        },
      }),
    );

    expect(detailResponse.status).toBe(200);
      const detailBody = (await detailResponse.json()) as {
        ingestion: { summary: { title?: { primary?: string } } };
      };
      expect(detailBody.ingestion.summary.title?.primary).toBe("Catalog from UI");
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
        body: JSON.stringify(buildIngestionBody({ batch_label: "batch-represign-001" })),
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
        body: JSON.stringify(buildIngestionBody({ batch_label: "batch-mismatch-001" })),
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
        body: JSON.stringify(buildIngestionBody({ batch_label: "batch-represign-after-commit-001" })),
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
        body: JSON.stringify(buildIngestionBody({ batch_label: "batch-submitted-file-guard-001" })),
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
        body: JSON.stringify(buildIngestionBody({ batch_label: "batch-submitted-commit-guard-001" })),
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
