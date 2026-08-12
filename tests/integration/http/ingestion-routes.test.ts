import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { sql as sqlIdentifier } from "bun";

import { createAppWithOptions as createApp } from "../../../src/app.ts";
import { createSqlClient } from "../../../src/db/client.ts";
import { runMigrations } from "../../../src/db/migrate.ts";
import { TEST_DATABASE_URL } from "../test-database.ts";

function sha256Hex(value: string): string {
  return new Bun.CryptoHasher("sha256").update(value).digest("hex");
}

async function createAndLinkItem(params: {
  app: ReturnType<typeof createApp>;
  token: string;
  ingestionId: string;
  fileId: string;
  itemIndex?: number;
}): Promise<string> {
  const createItemResponse = await params.app.fetch(
    new Request(`http://localhost/api/ingestions/${params.ingestionId}/items`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${params.token}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({ item_index: params.itemIndex ?? 1 }),
    }),
  );
  expect(createItemResponse.status).toBe(201);
  const createItemBody = (await createItemResponse.json()) as { item: { id: string } };

  const linkResponse = await params.app.fetch(
    new Request(
      `http://localhost/api/ingestions/${params.ingestionId}/items/${createItemBody.item.id}/files`,
      {
        method: "POST",
        headers: {
          authorization: `Bearer ${params.token}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          ingestion_file_id: params.fileId,
          sort_order: 1,
        }),
      },
    ),
  );
  expect(linkResponse.status).toBe(201);

  return createItemBody.item.id;
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

describe("ingestion routes", () => {
  let schema = "";
  let stagingRoot = "";
  let authToken = "";

  function createTestApp() {
    return createApp({
      runtimeConfig: {
        databaseUrl: TEST_DATABASE_URL,
        dbSchema: schema,
        stagingRoot,
        workerAuthToken: "worker-secret",
        uploadSigningSecret: "ingestion-routes-upload-secret-00001",
        leaseSigningSecret: "ingestion-routes-lease-secret-000001",
      },
    });
  }

  function workerHeaders(workerId = "preview-worker") {
    return {
      "x-worker-auth-token": "worker-secret",
      "x-worker-id": workerId,
    };
  }

  async function createCommittedFile(params: {
    app: ReturnType<typeof createTestApp>;
    ingestionId: string;
    filename: string;
    contentType: string;
    payload: string;
  }): Promise<{ fileId: string }> {
    const presignResponse = await params.app.fetch(
      new Request(`http://localhost/api/ingestions/${params.ingestionId}/files/presign`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          filename: params.filename,
          content_type: params.contentType,
          size_bytes: params.payload.length,
        }),
      }),
    );

    expect(presignResponse.status).toBe(201);
    const presignBody = (await presignResponse.json()) as {
      file_id: string;
      upload_url: string;
    };

    const uploadResponse = await params.app.fetch(
      new Request(`http://localhost${presignBody.upload_url}`, {
        method: "PUT",
        headers: {
          "content-type": params.contentType,
          "content-length": String(params.payload.length),
        },
        body: params.payload,
      }),
    );

    expect(uploadResponse.status).toBe(200);

    const commitResponse = await params.app.fetch(
      new Request(`http://localhost/api/ingestions/${params.ingestionId}/files/commit`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          file_id: presignBody.file_id,
          checksum_sha256: sha256Hex(params.payload),
        }),
      }),
    );

    expect(commitResponse.status).toBe(200);

    return {
      fileId: presignBody.file_id,
    };
  }

  async function createIngestionDraft(params: {
    app: ReturnType<typeof createTestApp>;
    body?: Record<string, unknown>;
  }): Promise<string> {
    const createResponse = await params.app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(buildIngestionBody(params.body)),
      }),
    );

    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as {
      ingestion: { id: string };
    };

    return createBody.ingestion.id;
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

    await createAndLinkItem({
      app,
      token: authToken,
      ingestionId,
      fileId: presignBody.file_id,
    });

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

  test("marks unsupported files with preview status unsupported", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(buildIngestionBody({ batch_label: "batch-preview-unsupported" })),
      }),
    );

    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as {
      ingestion: { id: string };
    };

    const file = await createCommittedFile({
      app,
      ingestionId: createBody.ingestion.id,
      filename: "sample.txt",
      contentType: "text/plain",
      payload: "hello world",
    });

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
      files: Array<{
        id: string;
        preview: { status: string; url: string | null };
      }>;
    };

    expect(detailBody.files.find((entry) => entry.id === file.fileId)?.preview).toMatchObject({
      status: "unsupported",
      url: null,
    });
  });

  test("returns pending previews for committed images and serves ready preview bytes", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(buildIngestionBody({
          batch_label: "batch-preview-image",
          classification_type: "image",
          item_kind: "photo",
        })),
      }),
    );

    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as {
      ingestion: { id: string };
    };

    const file = await createCommittedFile({
      app,
      ingestionId: createBody.ingestion.id,
      filename: "sample.png",
      contentType: "image/png",
      payload: "pngbytes",
    });

    let detailResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${createBody.ingestion.id}`, {
        method: "GET",
        headers: {
          authorization: `Bearer ${authToken}`,
        },
      }),
    );

    expect(detailResponse.status).toBe(200);
    let detailBody = (await detailResponse.json()) as {
      files: Array<{
        id: string;
        preview: {
          status: string;
          content_type: string | null;
          width: number | null;
          height: number | null;
          url: string | null;
        };
      }>;
    };

    expect(detailBody.files.find((entry) => entry.id === file.fileId)?.preview).toMatchObject({
      status: "pending",
      content_type: null,
      width: null,
      height: null,
      url: null,
    });

    const previewStorageKey = `tenants/00000000-0000-0000-0000-000000000001/ingestions/${createBody.ingestion.id}/preview/${file.fileId}.jpg`;
    const sql = createSqlClient(TEST_DATABASE_URL!);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`
        UPDATE ingestion_files
        SET preview_status = 'ready',
            preview_storage_key = ${previewStorageKey},
            preview_content_type = ${"image/jpeg"},
            preview_size_bytes = ${5},
            preview_width = ${64},
            preview_height = ${64},
            preview_generated_at = now()
        WHERE id = ${file.fileId}
      `;
    } finally {
      await sql.close();
    }

    const previewPath = join(stagingRoot, previewStorageKey);
    await mkdir(dirname(previewPath), { recursive: true });
    await Bun.write(previewPath, "thumb");

    detailResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${createBody.ingestion.id}`, {
        method: "GET",
        headers: {
          authorization: `Bearer ${authToken}`,
        },
      }),
    );

    expect(detailResponse.status).toBe(200);
    detailBody = (await detailResponse.json()) as {
      files: Array<{
        id: string;
        preview: {
          status: string;
          content_type: string | null;
          width: number | null;
          height: number | null;
          url: string | null;
        };
      }>;
    };

    expect(detailBody.files.find((entry) => entry.id === file.fileId)?.preview).toMatchObject({
      status: "ready",
      content_type: "image/jpeg",
      width: 64,
      height: 64,
      url: `/api/ingestions/${createBody.ingestion.id}/files/${file.fileId}/preview`,
    });

    const previewResponse = await app.fetch(
      new Request(
        `http://localhost/api/ingestions/${createBody.ingestion.id}/files/${file.fileId}/preview`,
        {
          method: "GET",
          headers: {
            authorization: `Bearer ${authToken}`,
          },
        },
      ),
    );

    expect(previewResponse.status).toBe(200);
    expect(previewResponse.headers.get("content-type")).toBe("image/jpeg");
    expect(await previewResponse.text()).toBe("thumb");
  });

  test("generates a ready image preview inline on commit", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(buildIngestionBody({
          batch_label: "batch-preview-inline",
          classification_type: "image",
          item_kind: "photo",
        })),
      }),
    );

    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as {
      ingestion: { id: string };
    };

    const file = await createCommittedFile({
      app,
      ingestionId: createBody.ingestion.id,
      filename: "sample.svg",
      contentType: "image/svg+xml",
      payload: `<svg xmlns="http://www.w3.org/2000/svg" width="100" height="60"><rect width="100" height="60" fill="rgb(200,120,40)"/></svg>`,
    });

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
      files: Array<{
        id: string;
        preview: {
          status: string;
          content_type: string | null;
          width: number | null;
          height: number | null;
          url: string | null;
        };
      }>;
    };

    const preview = detailBody.files.find((entry) => entry.id === file.fileId)?.preview;
    expect(preview).toMatchObject({
      status: "ready",
      content_type: "image/jpeg",
      width: 100,
      height: 60,
      url: `/api/ingestions/${createBody.ingestion.id}/files/${file.fileId}/preview`,
    });

    const previewResponse = await app.fetch(
      new Request(
        `http://localhost/api/ingestions/${createBody.ingestion.id}/files/${file.fileId}/preview`,
        {
          method: "GET",
          headers: {
            authorization: `Bearer ${authToken}`,
          },
        },
      ),
    );

    expect(previewResponse.status).toBe(200);
    expect(previewResponse.headers.get("content-type")).toBe("image/jpeg");
  });

  test("worker can claim, upload, and complete an ingestion preview", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(buildIngestionBody({
          batch_label: "batch-preview-worker-complete",
          classification_type: "image",
          item_kind: "photo",
        })),
      }),
    );

    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as {
      ingestion: { id: string };
    };

    const file = await createCommittedFile({
      app,
      ingestionId: createBody.ingestion.id,
      filename: "sample.png",
      contentType: "image/png",
      payload: "pngbytes",
    });

    const claimResponse = await app.fetch(
      new Request("http://localhost/api/worker/ingestion-previews/claim", {
        method: "POST",
        headers: workerHeaders(),
      }),
    );

    expect(claimResponse.status).toBe(200);
    const claimBody = (await claimResponse.json()) as {
      preview: { ingestion_id: string; file_id: string; download_url: string } | null;
    };
    expect(claimBody.preview?.ingestion_id).toBe(createBody.ingestion.id);
    expect(claimBody.preview?.file_id).toBe(file.fileId);

    const presignResponse = await app.fetch(
      new Request(
        `http://localhost/api/worker/ingestion-previews/${createBody.ingestion.id}/files/${file.fileId}/presign`,
        {
          method: "POST",
          headers: {
            ...workerHeaders(),
            "content-type": "application/json",
          },
          body: JSON.stringify({
            content_type: "image/jpeg",
            size_bytes: 5,
            extension: "jpg",
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
        `http://localhost/api/worker/ingestion-previews/${createBody.ingestion.id}/files/${file.fileId}/presign`,
        {
          method: "POST",
          headers: {
            ...workerHeaders(),
            "content-type": "application/json",
          },
          body: JSON.stringify({
            content_type: "image/jpeg",
            size_bytes: 5,
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
          "content-type": "image/jpeg",
          "content-length": "5",
        },
        body: "stale",
      }),
    );

    expect(staleUploadResponse.status).toBe(409);

    const uploadResponse = await app.fetch(
      new Request(`http://localhost${represignBody.upload_url}`, {
        method: "PUT",
        headers: {
          "content-type": "image/jpeg",
          "content-length": "5",
        },
        body: "thumb",
      }),
    );

    expect(uploadResponse.status).toBe(200);

    const completeResponse = await app.fetch(
      new Request(
        `http://localhost/api/worker/ingestion-previews/${createBody.ingestion.id}/files/${file.fileId}/complete`,
        {
          method: "POST",
          headers: {
            ...workerHeaders(),
            "content-type": "application/json",
          },
          body: JSON.stringify({
            upload_token: represignBody.upload_token,
            width: 64,
            height: 64,
          }),
        },
      ),
    );

    expect(completeResponse.status).toBe(200);

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
      files: Array<{
        id: string;
        preview: {
          status: string;
          content_type: string | null;
          size_bytes: number | null;
          width: number | null;
          height: number | null;
          url: string | null;
        };
      }>;
    };
    expect(detailBody.files.find((entry) => entry.id === file.fileId)?.preview).toMatchObject({
      status: "ready",
      content_type: "image/jpeg",
      size_bytes: 5,
      width: 64,
      height: 64,
      url: `/api/ingestions/${createBody.ingestion.id}/files/${file.fileId}/preview`,
    });
  });

  test("worker preview presign rejects unsupported thumbnail output types", async () => {
    const app = createTestApp();
    const ingestionId = await createIngestionDraft({
      app,
      body: {
        batch_label: "batch-preview-worker-bad-type",
        classification_type: "image",
        item_kind: "photo",
      },
    });
    const file = await createCommittedFile({
      app,
      ingestionId,
      filename: "sample.png",
      contentType: "image/png",
      payload: "pngbytes",
    });

    const claimResponse = await app.fetch(
      new Request("http://localhost/api/worker/ingestion-previews/claim", {
        method: "POST",
        headers: workerHeaders("preview-worker-bad-type"),
      }),
    );

    expect(claimResponse.status).toBe(200);

    const presignResponse = await app.fetch(
      new Request(
        `http://localhost/api/worker/ingestion-previews/${ingestionId}/files/${file.fileId}/presign`,
        {
          method: "POST",
          headers: {
            ...workerHeaders("preview-worker-bad-type"),
            "content-type": "application/json",
          },
          body: JSON.stringify({
            content_type: "text/html",
            size_bytes: 5,
          }),
        },
      ),
    );

    expect(presignResponse.status).toBe(400);
  });

  test("worker preview presign rejects oversized thumbnails", async () => {
    const app = createTestApp();
    const ingestionId = await createIngestionDraft({
      app,
      body: {
        batch_label: "batch-preview-worker-oversized",
        classification_type: "image",
        item_kind: "photo",
      },
    });
    const file = await createCommittedFile({
      app,
      ingestionId,
      filename: "sample.png",
      contentType: "image/png",
      payload: "pngbytes",
    });

    const claimResponse = await app.fetch(
      new Request("http://localhost/api/worker/ingestion-previews/claim", {
        method: "POST",
        headers: workerHeaders("preview-worker-oversized"),
      }),
    );

    expect(claimResponse.status).toBe(200);

    const presignResponse = await app.fetch(
      new Request(
        `http://localhost/api/worker/ingestion-previews/${ingestionId}/files/${file.fileId}/presign`,
        {
          method: "POST",
          headers: {
            ...workerHeaders("preview-worker-oversized"),
            "content-type": "application/json",
          },
          body: JSON.stringify({
            content_type: "image/webp",
            size_bytes: 5 * 1024 * 1024 + 1,
          }),
        },
      ),
    );

    expect(presignResponse.status).toBe(400);
  });

  test("worker preview completion requires dimensions", async () => {
    const app = createTestApp();
    const ingestionId = await createIngestionDraft({
      app,
      body: {
        batch_label: "batch-preview-worker-dimensions",
        classification_type: "image",
        item_kind: "photo",
      },
    });
    const file = await createCommittedFile({
      app,
      ingestionId,
      filename: "sample.png",
      contentType: "image/png",
      payload: "pngbytes",
    });

    const claimResponse = await app.fetch(
      new Request("http://localhost/api/worker/ingestion-previews/claim", {
        method: "POST",
        headers: workerHeaders("preview-worker-dimensions"),
      }),
    );

    expect(claimResponse.status).toBe(200);

    const presignResponse = await app.fetch(
      new Request(
        `http://localhost/api/worker/ingestion-previews/${ingestionId}/files/${file.fileId}/presign`,
        {
          method: "POST",
          headers: {
            ...workerHeaders("preview-worker-dimensions"),
            "content-type": "application/json",
          },
          body: JSON.stringify({
            content_type: "image/jpeg",
            size_bytes: 5,
          }),
        },
      ),
    );

    expect(presignResponse.status).toBe(200);
    const presignBody = (await presignResponse.json()) as {
      upload_token: string;
      upload_url: string;
    };

    const uploadResponse = await app.fetch(
      new Request(`http://localhost${presignBody.upload_url}`, {
        method: "PUT",
        headers: {
          "content-type": "image/jpeg",
          "content-length": "5",
        },
        body: "thumb",
      }),
    );

    expect(uploadResponse.status).toBe(200);

    const completeResponse = await app.fetch(
      new Request(
        `http://localhost/api/worker/ingestion-previews/${ingestionId}/files/${file.fileId}/complete`,
        {
          method: "POST",
          headers: {
            ...workerHeaders("preview-worker-dimensions"),
            "content-type": "application/json",
          },
          body: JSON.stringify({
            upload_token: presignBody.upload_token,
          }),
        },
      ),
    );

    expect(completeResponse.status).toBe(400);
  });

  test("worker can mark an ingestion preview as failed", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(buildIngestionBody({
          batch_label: "batch-preview-worker-fail",
          classification_type: "image",
          item_kind: "photo",
        })),
      }),
    );

    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as {
      ingestion: { id: string };
    };

    const file = await createCommittedFile({
      app,
      ingestionId: createBody.ingestion.id,
      filename: "sample.png",
      contentType: "image/png",
      payload: "pngbytes",
    });

    const claimResponse = await app.fetch(
      new Request("http://localhost/api/worker/ingestion-previews/claim", {
        method: "POST",
        headers: workerHeaders("preview-worker-fail"),
      }),
    );

    expect(claimResponse.status).toBe(200);

    const presignResponse = await app.fetch(
      new Request(
        `http://localhost/api/worker/ingestion-previews/${createBody.ingestion.id}/files/${file.fileId}/presign`,
        {
          method: "POST",
          headers: {
            ...workerHeaders("preview-worker-fail"),
            "content-type": "application/json",
          },
          body: JSON.stringify({
            content_type: "image/jpeg",
            size_bytes: 5,
          }),
        },
      ),
    );

    expect(presignResponse.status).toBe(200);

    const failResponse = await app.fetch(
      new Request(
        `http://localhost/api/worker/ingestion-previews/${createBody.ingestion.id}/files/${file.fileId}/fail`,
        {
          method: "POST",
          headers: {
            ...workerHeaders("preview-worker-fail"),
            "content-type": "application/json",
          },
          body: JSON.stringify({
            error: {
              message: "ffmpeg failed",
              code: "PREVIEW_GENERATION_FAILED",
              retryable: true,
            },
          }),
        },
      ),
    );

    expect(failResponse.status).toBe(200);

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
      files: Array<{
        id: string;
        preview: {
          status: string;
          content_type: string | null;
          size_bytes: number | null;
          width: number | null;
          height: number | null;
          url: string | null;
          error: Record<string, unknown> | null;
        };
      }>;
    };
    expect(detailBody.files.find((entry) => entry.id === file.fileId)?.preview).toMatchObject({
      status: "failed",
      content_type: null,
      size_bytes: null,
      width: null,
      height: null,
      url: null,
      error: {
        message: "ffmpeg failed",
        code: "PREVIEW_GENERATION_FAILED",
        retryable: true,
      },
    });
  });

  test("only current preview source kinds are marked pending", async () => {
    const app = createTestApp();
    const videoIngestionId = await createIngestionDraft({
      app,
      body: {
        batch_label: "batch-preview-video-source",
        classification_type: "interview",
        item_kind: "video",
      },
    });
    const videoFile = await createCommittedFile({
      app,
      ingestionId: videoIngestionId,
      filename: "sample.mp4",
      contentType: "video/mp4",
      payload: "mp4data",
    });
    const audioIngestionId = await createIngestionDraft({
      app,
      body: {
        batch_label: "batch-preview-audio-source",
        classification_type: "interview",
        item_kind: "audio",
      },
    });
    const audioFile = await createCommittedFile({
      app,
      ingestionId: audioIngestionId,
      filename: "sample.mp3",
      contentType: "audio/mpeg",
      payload: "mp3data",
    });

    const videoDetailResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${videoIngestionId}`, {
        method: "GET",
        headers: {
          authorization: `Bearer ${authToken}`,
        },
      }),
    );
    const audioDetailResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${audioIngestionId}`, {
        method: "GET",
        headers: {
          authorization: `Bearer ${authToken}`,
        },
      }),
    );

    expect(videoDetailResponse.status).toBe(200);
    expect(audioDetailResponse.status).toBe(200);
    const videoDetailBody = (await videoDetailResponse.json()) as {
      files: Array<{ id: string; preview: { status: string } }>;
    };
    const audioDetailBody = (await audioDetailResponse.json()) as {
      files: Array<{ id: string; preview: { status: string } }>;
    };

    expect(videoDetailBody.files.find((entry) => entry.id === videoFile.fileId)?.preview)
      .toMatchObject({ status: "pending" });
    expect(audioDetailBody.files.find((entry) => entry.id === audioFile.fileId)?.preview)
      .toMatchObject({ status: "unsupported" });
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

  test("exposes purge-aware action capabilities and rejects actions after purge intent", async () => {
    const app = createTestApp();
    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(buildIngestionBody({ batch_label: "batch-purge-capabilities-001" })),
      }),
    );
    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as { ingestion: { id: string } };
    const ingestionId = createBody.ingestion.id;
    const sql = createSqlClient(TEST_DATABASE_URL!);

    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`
        UPDATE ingestions
        SET status = 'FAILED',
            staging_purge_started_at = NULL,
            staging_purged_at = NULL
        WHERE id = ${ingestionId}
      `;

      const retainedResponse = await app.fetch(
        new Request(`http://localhost/api/ingestions/${ingestionId}`, {
          headers: { authorization: `Bearer ${authToken}` },
        }),
      );
      expect(retainedResponse.status).toBe(200);
      const retainedBody = (await retainedResponse.json()) as {
        ingestion: {
          staging_purge: { state: string; started_at: string | null; purged_at: string | null };
          action_capabilities: Record<string, boolean>;
        };
      };
      expect(retainedBody.ingestion.staging_purge).toEqual({
        state: "NOT_SCHEDULED",
        started_at: null,
        purged_at: null,
      });
      expect(retainedBody.ingestion.action_capabilities).toEqual({
        can_resume: false,
        can_retry: true,
        can_cancel: false,
        can_restore: false,
        can_delete: false,
      });

      await sql`
        UPDATE ingestions
        SET staging_purge_started_at = now()
        WHERE id = ${ingestionId}
      `;

      const pendingResponse = await app.fetch(
        new Request(`http://localhost/api/ingestions/${ingestionId}`, {
          headers: { authorization: `Bearer ${authToken}` },
        }),
      );
      const pendingBody = (await pendingResponse.json()) as {
        ingestion: {
          staging_purge: { state: string; started_at: string | null; purged_at: string | null };
          action_capabilities: Record<string, boolean>;
        };
      };
      expect(pendingBody.ingestion.staging_purge.state).toBe("PENDING");
      expect(pendingBody.ingestion.staging_purge.started_at).not.toBeNull();
      expect(pendingBody.ingestion.action_capabilities).toEqual({
        can_resume: false,
        can_retry: false,
        can_cancel: false,
        can_restore: false,
        can_delete: false,
      });

      const retryResponse = await app.fetch(
        new Request(`http://localhost/api/ingestions/${ingestionId}/retry`, {
          method: "POST",
          headers: { authorization: `Bearer ${authToken}` },
        }),
      );
      expect(retryResponse.status).toBe(409);

      await sql`UPDATE ingestions SET status = 'CANCELED' WHERE id = ${ingestionId}`;
      const restoreResponse = await app.fetch(
        new Request(`http://localhost/api/ingestions/${ingestionId}/restore`, {
          method: "POST",
          headers: { authorization: `Bearer ${authToken}` },
        }),
      );
      expect(restoreResponse.status).toBe(409);
    } finally {
      await sql.close();
    }
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

    await createAndLinkItem({
      app,
      token: authToken,
      ingestionId,
      fileId: presignBody.file_id,
    });

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

    await createAndLinkItem({
      app,
      token: authToken,
      ingestionId,
      fileId: presignBody.file_id,
    });

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
        body: JSON.stringify(buildIngestionBody({
          batch_label: "batch-remove-file-001",
          classification_type: "image",
          item_kind: "photo",
        })),
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
        body: JSON.stringify(buildIngestionBody({
          batch_label: "batch-jpeg-jpg-001",
          classification_type: "image",
          item_kind: "photo",
        })),
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
        body: JSON.stringify(buildIngestionBody({
          batch_label: "batch-mp3-mpeg-001",
          classification_type: "interview",
          item_kind: "audio",
        })),
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
        body: JSON.stringify(buildIngestionBody({
          batch_label: "batch-m4v-mp4-001",
          classification_type: "interview",
          item_kind: "video",
        })),
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
        body: JSON.stringify(buildIngestionBody({
          batch_label: "batch-mixed-types-001",
          classification_type: "document",
          item_kind: "scanned_document",
        })),
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
    expect(secondPresignBody.storage_key).not.toBe(firstPresignBody.storage_key);
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

    const replayResponse = await app.fetch(
      new Request(`http://localhost${secondPresignBody.upload_url}`, {
        method: "PUT",
        headers: {
          "content-type": "text/plain",
          "content-length": String(payload.length),
        },
        body: "changed-text!",
      }),
    );

    expect(replayResponse.status).toBe(409);

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

    await createAndLinkItem({
      app,
      token: authToken,
      ingestionId,
      fileId: firstPresignBody.file_id,
    });

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

    await createAndLinkItem({
      app,
      token: authToken,
      ingestionId,
      fileId: presignBody.file_id,
    });

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

  test("reorders ingestion items and file order within items", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(buildIngestionBody({
          batch_label: "batch-items-order-001",
          classification_type: "document",
          item_kind: "scanned_document",
        })),
      }),
    );

    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as { ingestion: { id: string } };
    const ingestionId = createBody.ingestion.id;

    const createItemOneResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/items`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          item_index: 1,
          title: "Item One",
          summary: { custom: "item-one" },
        }),
      }),
    );
    expect(createItemOneResponse.status).toBe(201);
    const createItemOneBody = (await createItemOneResponse.json()) as {
      item: { id: string };
    };

    const createItemTwoResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/items`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          item_index: 2,
          title: "Item Two",
          summary: { custom: "item-two" },
        }),
      }),
    );
    expect(createItemTwoResponse.status).toBe(201);
    const createItemTwoBody = (await createItemTwoResponse.json()) as {
      item: { id: string };
    };

    const fileOnePresign = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/presign`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          filename: "page-1.tif",
          content_type: "image/jpeg",
          size_bytes: 5,
        }),
      }),
    );
    expect(fileOnePresign.status).toBe(201);
    const fileOneBody = (await fileOnePresign.json()) as { file_id: string };

    const fileTwoPresign = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/presign`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          filename: "page-2.tif",
          content_type: "image/jpeg",
          size_bytes: 6,
        }),
      }),
    );
    expect(fileTwoPresign.status).toBe(201);
    const fileTwoBody = (await fileTwoPresign.json()) as { file_id: string };

    const linkFileOneResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/items/${createItemOneBody.item.id}/files`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          ingestion_file_id: fileOneBody.file_id,
          sort_order: 1,
        }),
      }),
    );
    expect(linkFileOneResponse.status).toBe(201);

    const linkFileTwoResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/items/${createItemOneBody.item.id}/files`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          ingestion_file_id: fileTwoBody.file_id,
          sort_order: 2,
        }),
      }),
    );
    expect(linkFileTwoResponse.status).toBe(201);

    const reorderFilesResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/items/${createItemOneBody.item.id}/files/order`, {
        method: "PATCH",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          files: [
            { ingestion_file_id: fileTwoBody.file_id, sort_order: 1 },
            { ingestion_file_id: fileOneBody.file_id, sort_order: 2 },
          ],
        }),
      }),
    );
    expect(reorderFilesResponse.status).toBe(200);

    const reorderItemsResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/items/order`, {
        method: "PATCH",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          items: [
            { ingestion_item_id: createItemOneBody.item.id, item_index: 2 },
            { ingestion_item_id: createItemTwoBody.item.id, item_index: 1 },
          ],
        }),
      }),
    );
    expect(reorderItemsResponse.status).toBe(200);

    const listItemsResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/items`, {
        method: "GET",
        headers: {
          authorization: `Bearer ${authToken}`,
        },
      }),
    );
    expect(listItemsResponse.status).toBe(200);
    const listItemsBody = (await listItemsResponse.json()) as {
      items: Array<{ id: string; item_index: number }>;
    };

    expect(listItemsBody.items.map((item) => item.id)).toEqual([
      createItemTwoBody.item.id,
      createItemOneBody.item.id,
    ]);
    expect(listItemsBody.items.map((item) => item.item_index)).toEqual([1, 2]);

    const listItemFilesResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/items/${createItemOneBody.item.id}/files`, {
        method: "GET",
        headers: {
          authorization: `Bearer ${authToken}`,
        },
      }),
    );
    expect(listItemFilesResponse.status).toBe(200);
    const listItemFilesBody = (await listItemFilesResponse.json()) as {
      files: Array<{ ingestion_file_id: string; sort_order: number }>;
    };

    expect(listItemFilesBody.files.map((file) => file.ingestion_file_id)).toEqual([
      fileTwoBody.file_id,
      fileOneBody.file_id,
    ]);
    expect(listItemFilesBody.files.map((file) => file.sort_order)).toEqual([1, 2]);
  });

  test("merges ingestion item metadata for dates, tags, and description", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(buildIngestionBody({ batch_label: "batch-item-metadata-001" })),
      }),
    );

    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as { ingestion: { id: string } };
    const ingestionId = createBody.ingestion.id;

    const createItemResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/items`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          item_index: 1,
          title: "Original Item Title",
          summary: {
            classification: {
              tags: ["existing"],
              summary: "seed description",
            },
            dates: {
              created: {
                value: "1950",
                approximate: true,
                confidence: "low",
                note: "initial",
              },
            },
            custom_field: {
              keep: true,
            },
            people: {
              authors: ["Existing Author"],
              contributors: ["Existing Contributor"],
              subjects: ["Existing Subject"],
              mentioned: ["Existing Mention"],
            },
          },
        }),
      }),
    );
    expect(createItemResponse.status).toBe(201);
    const createItemBody = (await createItemResponse.json()) as { item: { id: string } };

    const patchResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/items/${createItemBody.item.id}`, {
        method: "PATCH",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          title: "Updated Item Title",
          description: "Updated description",
          tags: ["tag-a", "tag-b", "tag-a"],
          people: [" Ada Lovelace ", "Grace Hopper", "Ada Lovelace"],
          dates: {
            published: {
              value: "1960-05",
              approximate: false,
              confidence: "high",
              note: null,
            },
          },
        }),
      }),
    );
    expect(patchResponse.status).toBe(200);

    const patchBody = (await patchResponse.json()) as {
      item: {
        title: string;
        summary: {
          classification: { tags: string[]; summary: string | null };
          dates: {
            created: { value: string; approximate: boolean; confidence: string; note: string | null };
            published: { value: string | null; approximate: boolean; confidence: string; note: string | null };
          };
          custom_field: { keep: boolean };
          people: {
            authors: string[];
            contributors: string[];
            subjects: string[];
            mentioned: string[];
          };
        };
      };
    };

    expect(patchBody.item.title).toBe("Updated Item Title");
    expect(patchBody.item.summary.classification.summary).toBe("Updated description");
    expect(patchBody.item.summary.classification.tags).toEqual(["tag-a", "tag-b"]);
    expect(patchBody.item.summary.dates.created.value).toBe("1950");
    expect(patchBody.item.summary.dates.published.value).toBe("1960-05");
    expect(patchBody.item.summary.custom_field.keep).toBe(true);
    expect(patchBody.item.summary.people).toEqual({
      authors: ["Existing Author"],
      contributors: ["Existing Contributor"],
      subjects: ["Existing Subject"],
      mentioned: ["Ada Lovelace", "Grace Hopper"],
    });

    const omitPeopleResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/items/${createItemBody.item.id}`, {
        method: "PATCH",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({ description: "People remain unchanged" }),
      }),
    );
    expect(omitPeopleResponse.status).toBe(200);
    const omitPeopleBody = (await omitPeopleResponse.json()) as {
      item: { summary: { people: { mentioned: string[] } } };
    };
    expect(omitPeopleBody.item.summary.people.mentioned).toEqual(["Ada Lovelace", "Grace Hopper"]);

    const clearPeopleResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/items/${createItemBody.item.id}`, {
        method: "PATCH",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({ people: [] }),
      }),
    );
    expect(clearPeopleResponse.status).toBe(200);
    const clearPeopleBody = (await clearPeopleResponse.json()) as {
      item: {
        summary: {
          people: {
            authors: string[];
            contributors: string[];
            subjects: string[];
            mentioned: string[];
          };
        };
      };
    };
    expect(clearPeopleBody.item.summary.people).toEqual({
      authors: ["Existing Author"],
      contributors: ["Existing Contributor"],
      subjects: ["Existing Subject"],
      mentioned: [],
    });

    const blankPeopleResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/items/${createItemBody.item.id}`, {
        method: "PATCH",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({ people: ["   "] }),
      }),
    );
    expect(blankPeopleResponse.status).toBe(400);
  });

  test("rejects incompatible classification type and item kind on ingestion create", async () => {
    const app = createTestApp();

    const response = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(
          buildIngestionBody({
            batch_label: "batch-invalid-kind-001",
            classification_type: "image",
            item_kind: "video",
          }),
        ),
      }),
    );

    expect(response.status).toBe(409);
  });

  test("rejects incompatible classification type and item kind on ingestion update", async () => {
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
            batch_label: "batch-invalid-update-001",
            classification_type: "interview",
            item_kind: "audio",
          }),
        ),
      }),
    );
    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as { ingestion: { id: string } };

    const patchResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${createBody.ingestion.id}`, {
        method: "PATCH",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          classification_type: "book",
        }),
      }),
    );

    expect(patchResponse.status).toBe(409);
  });

  test("rejects document uploads when ingestion item kind is video", async () => {
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
            batch_label: "batch-video-ebook-001",
            classification_type: "interview",
            item_kind: "video",
          }),
        ),
      }),
    );
    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as { ingestion: { id: string } };

    const presignResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${createBody.ingestion.id}/files/presign`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          filename: "ebook.pdf",
          content_type: "application/pdf",
          size_bytes: 7,
        }),
      }),
    );
    expect(presignResponse.status).toBe(201);
    const presignBody = (await presignResponse.json()) as { file_id: string; upload_url: string };

    const uploadResponse = await app.fetch(
      new Request(`http://localhost${presignBody.upload_url}`, {
        method: "PUT",
        headers: {
          "content-type": "application/pdf",
          "content-length": "7",
        },
        body: "pdfdata",
      }),
    );
    expect(uploadResponse.status).toBe(200);

    const commitResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${createBody.ingestion.id}/files/commit`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          file_id: presignBody.file_id,
          checksum_sha256: sha256Hex("pdfdata"),
        }),
      }),
    );

    expect(commitResponse.status).toBe(409);
  });

  test("allows scanned document ingestions to commit image uploads", async () => {
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
            batch_label: "batch-scanned-image-001",
            classification_type: "interview",
            item_kind: "scanned_document",
          }),
        ),
      }),
    );
    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as { ingestion: { id: string } };

    const file = await createCommittedFile({
      app,
      ingestionId: createBody.ingestion.id,
      filename: "page.jpg",
      contentType: "image/jpeg",
      payload: "imgdata",
    });

    expect(file.fileId).toBeTruthy();
  });

  test("rejects incompatible classification type and item kind on ingestion item create", async () => {
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
            batch_label: "batch-item-create-invalid-001",
            classification_type: "interview",
            item_kind: "audio",
          }),
        ),
      }),
    );
    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as { ingestion: { id: string } };

    const itemResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${createBody.ingestion.id}/items`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          item_index: 1,
          classification_type: "book",
        }),
      }),
    );

    expect(itemResponse.status).toBe(409);
  });

  test("rejects incompatible classification type and item kind on ingestion item update", async () => {
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
            batch_label: "batch-item-update-invalid-001",
            classification_type: "interview",
            item_kind: "audio",
          }),
        ),
      }),
    );
    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as { ingestion: { id: string } };

    const itemCreateResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${createBody.ingestion.id}/items`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          item_index: 1,
          title: "Interview item",
        }),
      }),
    );
    expect(itemCreateResponse.status).toBe(201);
    const itemCreateBody = (await itemCreateResponse.json()) as { item: { id: string } };

    const patchResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${createBody.ingestion.id}/items/${itemCreateBody.item.id}`, {
        method: "PATCH",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          classification_type: "book",
        }),
      }),
    );

    expect(patchResponse.status).toBe(409);
  });

  test("rejects linking document files to video ingestion items", async () => {
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
            batch_label: "batch-item-link-invalid-001",
            classification_type: "interview",
            item_kind: "video",
          }),
        ),
      }),
    );
    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as { ingestion: { id: string } };
    const ingestionId = createBody.ingestion.id;

    const itemResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/items`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          item_index: 1,
          title: "Video item",
        }),
      }),
    );
    expect(itemResponse.status).toBe(201);
    const itemBody = (await itemResponse.json()) as { item: { id: string } };

    const presignResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/presign`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          filename: "ebook.pdf",
          content_type: "application/pdf",
          size_bytes: 7,
        }),
      }),
    );
    expect(presignResponse.status).toBe(201);
    const presignBody = (await presignResponse.json()) as { file_id: string };

    const linkResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/items/${itemBody.item.id}/files`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          ingestion_file_id: presignBody.file_id,
          sort_order: 1,
        }),
      }),
    );

    expect(linkResponse.status).toBe(409);
  });

  test("replays a completed ingestion create request and rejects a mismatched reuse", async () => {
    const app = createTestApp();
    const idempotencyKey = `create-replay-${crypto.randomUUID()}`;
    const body = buildIngestionBody({ batch_label: `batch-idempotency-${crypto.randomUUID()}` });

    const request = () => new Request("http://localhost/api/ingestions", {
      method: "POST",
      headers: {
        authorization: `Bearer ${authToken}`,
        "content-type": "application/json",
        "x-idempotency-key": idempotencyKey,
      },
      body: JSON.stringify(body),
    });

    const firstResponse = await app.fetch(request());
    const firstBody = await firstResponse.json();
    expect(firstResponse.status).toBe(201);

    const replayResponse = await app.fetch(request());
    expect(replayResponse.status).toBe(201);
    expect(await replayResponse.json()).toEqual(firstBody);

    const mismatchResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
          "x-idempotency-key": idempotencyKey,
        },
        body: JSON.stringify({ ...body, batch_label: `batch-mismatch-${crypto.randomUUID()}` }),
      }),
    );
    expect(mismatchResponse.status).toBe(409);
  });

  test("serializes concurrent ingestion create requests with the same idempotency key", async () => {
    const app = createTestApp();
    const idempotencyKey = `create-concurrent-${crypto.randomUUID()}`;
    const body = buildIngestionBody({ batch_label: `batch-idempotency-${crypto.randomUUID()}` });
    const request = () => new Request("http://localhost/api/ingestions", {
      method: "POST",
      headers: {
        authorization: `Bearer ${authToken}`,
        "content-type": "application/json",
        "x-idempotency-key": idempotencyKey,
      },
      body: JSON.stringify(body),
    });

    const [firstResponse, secondResponse] = await Promise.all([
      app.fetch(request()),
      app.fetch(request()),
    ]);
    expect(firstResponse.status).toBe(201);
    expect(secondResponse.status).toBe(201);
    expect(await firstResponse.json()).toEqual(await secondResponse.json());
  });

  test("does not retain a failed ingestion mutation as an idempotency replay", async () => {
    const app = createTestApp();
    const idempotencyKey = `create-failure-${crypto.randomUUID()}`;
    const headers = {
      authorization: `Bearer ${authToken}`,
      "content-type": "application/json",
      "x-idempotency-key": idempotencyKey,
    };

    const failedResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers,
        body: JSON.stringify(buildIngestionBody({
          classification_type: "image",
          item_kind: "video",
        })),
      }),
    );
    expect(failedResponse.status).toBe(409);

    const successfulResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers,
        body: JSON.stringify(buildIngestionBody({
          batch_label: `batch-idempotency-${crypto.randomUUID()}`,
        })),
      }),
    );
    expect(successfulResponse.status).toBe(201);
  });
});
