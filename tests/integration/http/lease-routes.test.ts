import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterAll, beforeAll, beforeEach, describe, expect, test } from "bun:test";
import { sql as sqlIdentifier } from "bun";

import { createAppWithOptions as createApp } from "../../../src/app.ts";
import { createSqlClient } from "../../../src/db/client.ts";
import { runMigrations } from "../../../src/db/migrate.ts";
import { runWithRuntimeConfig } from "../../../src/runtime/config.ts";
import { createDownloadToken } from "../../../src/storage/staging.ts";
import { TEST_DATABASE_URL } from "../test-database.ts";

function sha256Hex(value: string): string {
  return new Bun.CryptoHasher("sha256").update(value).digest("hex");
}

const LEASE_PAYLOAD = "lease flow";

function buildSummary(overrides?: Record<string, unknown>): Record<string, unknown> {
  return {
    title: {
      primary: "Lease catalog payload",
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

async function cancelQueuedIngestions(schema: string): Promise<void> {
  const sql = createSqlClient(TEST_DATABASE_URL!);

  try {
    await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
    await sql`
      UPDATE ingestions
      SET status = ${"CANCELED"}::ingestion_status,
          updated_at = now()
      WHERE status = ${"QUEUED"}::ingestion_status
    `;
  } finally {
    await sql.close();
  }
}

async function expireActiveLease(schema: string, ingestionId: string): Promise<void> {
  const sql = createSqlClient(TEST_DATABASE_URL!);

  try {
    await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
    await sql`
      UPDATE ingestion_leases
      SET lease_started_at = now() - interval '2 minute',
          lease_expires_at = now() - interval '1 minute'
      WHERE ingestion_id = ${ingestionId}
        AND released_at IS NULL
    `;
  } finally {
    await sql.close();
  }
}

async function resetActiveIngestions(schema: string): Promise<void> {
  const sql = createSqlClient(TEST_DATABASE_URL!);

  try {
    await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;

    await sql`
      UPDATE ingestions
      SET status = ${"CANCELED"}::ingestion_status,
          updated_at = now()
      WHERE status IN (
        ${"DRAFT"}::ingestion_status,
        ${"UPLOADING"}::ingestion_status,
        ${"QUEUED"}::ingestion_status,
        ${"PROCESSING"}::ingestion_status
      )
    `;

    await sql`
      UPDATE ingestion_leases
      SET released_at = now()
      WHERE released_at IS NULL
    `;
  } finally {
    await sql.close();
  }
}

async function getLeaseState(
  schema: string,
  ingestionId: string,
): Promise<{ status: string; activeLeaseCount: number }> {
  const sql = createSqlClient(TEST_DATABASE_URL!);

  try {
    await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
    const rows = await sql<Array<{ status: string; active_lease_count: number }>>`
      SELECT
        ing.status,
        COUNT(lease.id) FILTER (
          WHERE lease.released_at IS NULL
            AND lease.lease_expires_at > now()
        )::int AS active_lease_count
      FROM ingestions ing
      LEFT JOIN ingestion_leases lease ON lease.ingestion_id = ing.id
      WHERE ing.id = ${ingestionId}
      GROUP BY ing.id
    `;
    const row = rows[0];
    if (!row) {
      throw new Error(`Ingestion '${ingestionId}' was not found.`);
    }

    return {
      status: row.status,
      activeLeaseCount: Number(row.active_lease_count),
    };
  } finally {
    await sql.close();
  }
}

async function createQueuedIngestion(
  app: ReturnType<typeof createApp>,
  token: string,
  summary?: Record<string, unknown>,
  processingOverrides?: Record<string, unknown>,
): Promise<string> {
  const createResponse = await app.fetch(
    new Request("http://localhost/api/ingestions", {
      method: "POST",
      headers: {
        authorization: `Bearer ${token}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({
        ...buildIngestionBody({
          summary: summary ?? buildSummary(),
        }),
      }),
    }),
  );
  expect(createResponse.status).toBe(201);

  const created = (await createResponse.json()) as { ingestion: { id: string } };
  const ingestionId = created.ingestion.id;

  const payload = LEASE_PAYLOAD;
  const presignResponse = await app.fetch(
    new Request(`http://localhost/api/ingestions/${ingestionId}/files/presign`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${token}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({
        filename: "lease.txt",
        content_type: "text/plain",
        size_bytes: payload.length,
      }),
    }),
  );
  expect(presignResponse.status).toBe(201);

  const presignBody = (await presignResponse.json()) as {
    file_id: string;
    upload_url: string;
  };

  if (processingOverrides) {
    const overrideResponse = await app.fetch(
      new Request(
        `http://localhost/api/ingestions/${ingestionId}/files/${presignBody.file_id}/overrides`,
        {
          method: "POST",
          headers: {
            authorization: `Bearer ${token}`,
            "content-type": "application/json",
          },
          body: JSON.stringify({
            processing_overrides: processingOverrides,
          }),
        },
      ),
    );

    expect(overrideResponse.status).toBe(200);
  }

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
        authorization: `Bearer ${token}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({
        file_id: presignBody.file_id,
        checksum_sha256: sha256Hex(payload),
      }),
    }),
  );
  expect(commitResponse.status).toBe(200);

  const createItemResponse = await app.fetch(
    new Request(`http://localhost/api/ingestions/${ingestionId}/items`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${token}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({
        item_index: 1,
      }),
    }),
  );

  expect(createItemResponse.status).toBe(201);
  const createItemBody = (await createItemResponse.json()) as { item: { id: string } };

  const linkFileResponse = await app.fetch(
    new Request(
      `http://localhost/api/ingestions/${ingestionId}/items/${createItemBody.item.id}/files`,
      {
        method: "POST",
        headers: {
          authorization: `Bearer ${token}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          ingestion_file_id: presignBody.file_id,
          sort_order: 1,
        }),
      },
    ),
  );

  expect(linkFileResponse.status).toBe(201);

  const submitResponse = await app.fetch(
    new Request(`http://localhost/api/ingestions/${ingestionId}/submit`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${token}`,
      },
    }),
  );
  expect(submitResponse.status).toBe(200);

  return ingestionId;
}

describe("lease routes", () => {
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
        uploadSigningSecret: "lease-routes-upload-signing-secret-000",
        leaseSigningSecret: "lease-routes-lease-signing-secret-0000",
      },
    });
  }

  beforeAll(async () => {
    schema = `lease_${Date.now()}_${Math.floor(Math.random() * 100000)}`;
    stagingRoot = await mkdtemp(join(tmpdir(), "osimi-lease-staging-"));

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
        VALUES (${"00000000-0000-4000-8000-000000000001"}, ${"tenant-one"}, ${"Tenant One"})
      `;

      await sql`
        INSERT INTO users (id, username, username_normalized, password_hash)
        VALUES (${"10000000-0000-4000-8000-000000000002"}, ${"archiver@osimi.local"}, ${"archiver@osimi.local"}, ${operatorHash})
      `;

      await sql`
        INSERT INTO tenant_memberships (id, tenant_id, user_id, role)
        VALUES (${"20000000-0000-4000-8000-000000000002"}, ${"00000000-0000-4000-8000-000000000001"}, ${"10000000-0000-4000-8000-000000000002"}, ${"archiver"})
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

    const loginBody = (await loginResponse.json()) as { token: string };
    authToken = loginBody.token;
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

  beforeEach(async () => {
    await resetActiveIngestions(schema);
  });

  test("leases queued ingestion, supports heartbeat, serves download, and releases", async () => {
    const app = createTestApp();
    const ingestionId = await createQueuedIngestion(app, authToken);

    const leaseResponse = await app.fetch(
      new Request("http://localhost/api/ingestions/lease", {
        method: "POST",
        headers: {
          "x-worker-auth-token": "worker-secret",
          "x-worker-id": "worker-a",
        },
      }),
    );

    expect(leaseResponse.status).toBe(200);
    const leaseBody = (await leaseResponse.json()) as {
      lease: {
        ingestion_id: string;
        lease_token: string;
        items: Array<{
          catalog_json: Record<string, unknown>;
          files: Array<{ download_url: string; checksum_sha256: string | null }>;
        }>;
      };
    };

    expect(leaseBody.lease.ingestion_id).toBe(ingestionId);
    expect(leaseBody.lease.items.length).toBe(1);
    expect(leaseBody.lease.items[0]?.catalog_json).not.toBeNull();
    expect(leaseBody.lease.items[0]?.files.length).toBe(1);
    expect(leaseBody.lease.items[0]?.files[0]?.checksum_sha256).toBe(
      sha256Hex(LEASE_PAYLOAD),
    );

    const downloadResponse = await app.fetch(
      new Request(`http://localhost${leaseBody.lease.items[0]!.files[0]!.download_url}`),
    );

    expect(downloadResponse.status).toBe(200);
    expect(await downloadResponse.text()).toBe("lease flow");

    const heartbeatResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/lease/heartbeat`, {
        method: "POST",
        headers: {
          "x-worker-auth-token": "worker-secret",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          lease_token: leaseBody.lease.lease_token,
        }),
      }),
    );

    expect(heartbeatResponse.status).toBe(200);
    const heartbeatBody = (await heartbeatResponse.json()) as {
      lease: {
        items: Array<{ catalog_json: Record<string, unknown> }>;
      };
    };
    expect(heartbeatBody.lease.items[0]?.catalog_json).not.toBeNull();

    const releaseResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/lease/release`, {
        method: "POST",
        headers: {
          "x-worker-auth-token": "worker-secret",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          lease_token: leaseBody.lease.lease_token,
        }),
      }),
    );

    expect(releaseResponse.status).toBe(200);

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
    };
    expect(detailBody.ingestion.status).toBe("QUEUED");
  });

  test("concurrent lease attempts produce a single winner", async () => {
    const app = createTestApp();
    await cancelQueuedIngestions(schema);
    await createQueuedIngestion(app, authToken);

    const [first, second] = await Promise.all([
      app.fetch(
        new Request("http://localhost/api/ingestions/lease", {
          method: "POST",
          headers: {
            "x-worker-auth-token": "worker-secret",
            "x-worker-id": "worker-one",
          },
        }),
      ),
      app.fetch(
        new Request("http://localhost/api/ingestions/lease", {
          method: "POST",
          headers: {
            "x-worker-auth-token": "worker-secret",
            "x-worker-id": "worker-two",
          },
        }),
      ),
    ]);

    const firstBody = (await first.json()) as { lease: null | Record<string, unknown> };
    const secondBody = (await second.json()) as { lease: null | Record<string, unknown> };

    const winnerCount = Number(firstBody.lease != null) + Number(secondBody.lease != null);
    expect(winnerCount).toBe(1);
  });

  test("leases a specific queued ingestion by id", async () => {
    const app = createTestApp();
    await cancelQueuedIngestions(schema);
    await createQueuedIngestion(app, authToken);
    const targetIngestionId = await createQueuedIngestion(app, authToken);

    const response = await app.fetch(
      new Request(`http://localhost/api/ingestions/${targetIngestionId}/lease`, {
        method: "POST",
        headers: {
          "x-worker-auth-token": "worker-secret",
          "x-worker-id": "worker-targeted",
        },
      }),
    );

    expect(response.status).toBe(200);
    const body = (await response.json()) as {
      lease: {
        ingestion_id: string;
      };
    };

    expect(body.lease.ingestion_id).toBe(targetIngestionId);
  });

  test("rejects specific lease request when ingestion already has an active lease", async () => {
    const app = createTestApp();
    await cancelQueuedIngestions(schema);
    const ingestionId = await createQueuedIngestion(app, authToken);

    const firstLeaseResponse = await app.fetch(
      new Request("http://localhost/api/ingestions/lease", {
        method: "POST",
        headers: {
          "x-worker-auth-token": "worker-secret",
          "x-worker-id": "worker-primary",
        },
      }),
    );
    expect(firstLeaseResponse.status).toBe(200);

    const secondResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/lease`, {
        method: "POST",
        headers: {
          "x-worker-auth-token": "worker-secret",
          "x-worker-id": "worker-targeted",
        },
      }),
    );

    expect(secondResponse.status).toBe(409);
  });

  test("reacquires specific ingestion after lease expiry", async () => {
    const app = createTestApp();
    await cancelQueuedIngestions(schema);
    const ingestionId = await createQueuedIngestion(app, authToken);

    const firstLease = await app.fetch(
      new Request("http://localhost/api/ingestions/lease", {
        method: "POST",
        headers: {
          "x-worker-auth-token": "worker-secret",
          "x-worker-id": "worker-expire-a",
        },
      }),
    );

    expect(firstLease.status).toBe(200);
    await expireActiveLease(schema, ingestionId);

    const specificLease = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/lease`, {
        method: "POST",
        headers: {
          "x-worker-auth-token": "worker-secret",
          "x-worker-id": "worker-expire-b",
        },
      }),
    );

    expect(specificLease.status).toBe(200);
    const specificBody = (await specificLease.json()) as {
      lease: {
        ingestion_id: string;
      };
    };

    expect(specificBody.lease.ingestion_id).toBe(ingestionId);
  });

  test("returns not found when requesting lease for unknown ingestion id", async () => {
    const app = createTestApp();

    const response = await app.fetch(
      new Request("http://localhost/api/ingestions/00000000-0000-4000-8000-000000000099/lease", {
        method: "POST",
        headers: {
          "x-worker-auth-token": "worker-secret",
          "x-worker-id": "worker-targeted",
        },
      }),
    );

    expect(response.status).toBe(404);
  });

  test("rejects heartbeat when ingestion id does not match lease token", async () => {
    const app = createTestApp();
    const sourceIngestionId = await createQueuedIngestion(app, authToken);
    const targetIngestionId = await createQueuedIngestion(app, authToken);

    const leaseResponse = await app.fetch(
      new Request("http://localhost/api/ingestions/lease", {
        method: "POST",
        headers: {
          "x-worker-auth-token": "worker-secret",
          "x-worker-id": "worker-heartbeat",
        },
      }),
    );

    const leaseBody = (await leaseResponse.json()) as {
      lease: {
        ingestion_id: string;
        lease_token: string;
      };
    };

    expect(leaseBody.lease.ingestion_id).toBe(sourceIngestionId);

    const mismatchResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${targetIngestionId}/lease/heartbeat`, {
        method: "POST",
        headers: {
          "x-worker-auth-token": "worker-secret",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          lease_token: leaseBody.lease.lease_token,
        }),
      }),
    );

    expect(mismatchResponse.status).toBe(401);
  });

  test("re-queues ingestion when active lease expires and worker requests next lease", async () => {
    const app = createTestApp();
    await cancelQueuedIngestions(schema);
    const ingestionId = await createQueuedIngestion(app, authToken);

    const firstLease = await app.fetch(
      new Request("http://localhost/api/ingestions/lease", {
        method: "POST",
        headers: {
          "x-worker-auth-token": "worker-secret",
          "x-worker-id": "worker-expire-a",
        },
      }),
    );

    expect(firstLease.status).toBe(200);
    await expireActiveLease(schema, ingestionId);

    const secondLease = await app.fetch(
      new Request("http://localhost/api/ingestions/lease", {
        method: "POST",
        headers: {
          "x-worker-auth-token": "worker-secret",
          "x-worker-id": "worker-expire-b",
        },
      }),
    );

    expect(secondLease.status).toBe(200);
    const secondBody = (await secondLease.json()) as {
      lease: {
        ingestion_id: string;
      };
    };

    expect(secondBody.lease.ingestion_id).toBe(ingestionId);
  });

  test("includes item catalog_json in lease and heartbeat when provided at ingestion creation", async () => {
    const app = createTestApp();
    const summary = buildSummary({
      title: {
        primary: "Lease catalog payload",
        original_script: null,
        translations: [],
      },
    });

    const ingestionId = await createQueuedIngestion(app, authToken, summary);

    const leaseResponse = await app.fetch(
      new Request("http://localhost/api/ingestions/lease", {
        method: "POST",
        headers: {
          "x-worker-auth-token": "worker-secret",
          "x-worker-id": "worker-catalog",
        },
      }),
    );

    expect(leaseResponse.status).toBe(200);
    const leaseBody = (await leaseResponse.json()) as {
      lease: {
        ingestion_id: string;
        lease_token: string;
        items: Array<{
          catalog_json: {
            schema_version: string;
            title: { primary: string };
            object_id: string | null;
          };
        }>;
      };
    };

    expect(leaseBody.lease.ingestion_id).toBe(ingestionId);
    expect(leaseBody.lease.items[0]?.catalog_json).not.toBeNull();
    expect(leaseBody.lease.items[0]?.catalog_json?.schema_version).toBe("1.0");
    expect(leaseBody.lease.items[0]?.catalog_json?.title.primary).toBe("Lease catalog payload");
    expect(leaseBody.lease.items[0]?.catalog_json?.object_id).toBeNull();

    const heartbeatResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/lease/heartbeat`, {
        method: "POST",
        headers: {
          "x-worker-auth-token": "worker-secret",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          lease_token: leaseBody.lease.lease_token,
        }),
      }),
    );

    expect(heartbeatResponse.status).toBe(200);
    const heartbeatBody = (await heartbeatResponse.json()) as {
      lease: {
        items: Array<{
          catalog_json: {
            schema_version: string;
            title: { primary: string };
            object_id: string | null;
          };
        }>;
      };
    };

    expect(heartbeatBody.lease.items[0]?.catalog_json).not.toBeNull();
    expect(heartbeatBody.lease.items[0]?.catalog_json?.schema_version).toBe("1.0");
    expect(heartbeatBody.lease.items[0]?.catalog_json?.title.primary).toBe("Lease catalog payload");
    expect(heartbeatBody.lease.items[0]?.catalog_json?.object_id).toBeNull();
  });

  test("includes per-file processing overrides in lease payload", async () => {
    const app = createTestApp();
    const processingOverrides = {
      ocr_text: { enabled: true, language: "tg" },
      video_transcript: { enabled: false },
    };

    const ingestionId = await createQueuedIngestion(
      app,
      authToken,
      undefined,
      processingOverrides,
    );

    const leaseResponse = await app.fetch(
      new Request("http://localhost/api/ingestions/lease", {
        method: "POST",
        headers: {
          "x-worker-auth-token": "worker-secret",
          "x-worker-id": "worker-overrides",
        },
      }),
    );

    expect(leaseResponse.status).toBe(200);
    const leaseBody = (await leaseResponse.json()) as {
      lease: {
        ingestion_id: string;
        items: Array<{ files: Array<{ processing_overrides: Record<string, unknown> }> }>;
      };
    };

    expect(leaseBody.lease.ingestion_id).toBe(ingestionId);
    expect(leaseBody.lease.items[0]?.files[0]?.processing_overrides).toMatchObject(
      processingOverrides,
    );
  });

  test("rejects expired worker download token", async () => {
    const app = createTestApp();
    const ingestionId = await createQueuedIngestion(app, authToken);

    const leaseResponse = await app.fetch(
      new Request("http://localhost/api/ingestions/lease", {
        method: "POST",
        headers: {
          "x-worker-auth-token": "worker-secret",
          "x-worker-id": "worker-expired-download",
        },
      }),
    );

    const leaseBody = (await leaseResponse.json()) as {
      lease: {
        items: Array<{
          files: Array<{ storage_key: string; file_id: string; content_type: string; size_bytes: number }>;
        }>;
      };
    };

    const file = leaseBody.lease.items[0]?.files[0];
    expect(file).toBeDefined();

    const expiredToken = runWithRuntimeConfig({
      uploadSigningSecret: "lease-routes-upload-signing-secret-000",
    }, () => createDownloadToken({
      ingestion_id: ingestionId,
      file_id: file!.file_id,
      tenant_id: "00000000-0000-4000-8000-000000000001",
      storage_key: file!.storage_key,
      content_type: file!.content_type,
      size_bytes: file!.size_bytes,
      expires_at: new Date(Date.now() - 1000).toISOString(),
    }));

    const downloadResponse = await app.fetch(
      new Request(`http://localhost/api/worker/downloads/${expiredToken}`),
    );

    expect(downloadResponse.status).toBe(401);
  });

  test("leases grouped items in deterministic item/file order", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(buildIngestionBody({ batch_label: "batch-lease-grouped-001" })),
      }),
    );
    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as { ingestion: { id: string } };
    const ingestionId = createBody.ingestion.id;

    const filePayloads = [
      { filename: "b-page-2.txt", content: "item1-page2" },
      { filename: "a-page-1.txt", content: "item1-page1" },
      { filename: "z-cover.txt", content: "item2-cover" },
    ];

    const fileIds: string[] = [];
    for (const payload of filePayloads) {
      const presignResponse = await app.fetch(
        new Request(`http://localhost/api/ingestions/${ingestionId}/files/presign`, {
          method: "POST",
          headers: {
            authorization: `Bearer ${authToken}`,
            "content-type": "application/json",
          },
          body: JSON.stringify({
            filename: payload.filename,
            content_type: "text/plain",
            size_bytes: payload.content.length,
          }),
        }),
      );
      expect(presignResponse.status).toBe(201);
      const presignBody = (await presignResponse.json()) as { file_id: string; upload_url: string };
      fileIds.push(presignBody.file_id);

      const uploadResponse = await app.fetch(
        new Request(`http://localhost${presignBody.upload_url}`, {
          method: "PUT",
          headers: {
            "content-type": "text/plain",
            "content-length": String(payload.content.length),
          },
          body: payload.content,
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
            checksum_sha256: sha256Hex(payload.content),
          }),
        }),
      );
      expect(commitResponse.status).toBe(200);
    }

    const createItemTwo = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/items`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({ item_index: 2, title: "Item Two" }),
      }),
    );
    expect(createItemTwo.status).toBe(201);
    const itemTwoBody = (await createItemTwo.json()) as { item: { id: string } };

    const createItemOne = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/items`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({ item_index: 1, title: "Item One" }),
      }),
    );
    expect(createItemOne.status).toBe(201);
    const itemOneBody = (await createItemOne.json()) as { item: { id: string } };

    const links = [
      { itemId: itemOneBody.item.id, fileId: fileIds[0]!, sortOrder: 2 },
      { itemId: itemOneBody.item.id, fileId: fileIds[1]!, sortOrder: 1 },
      { itemId: itemTwoBody.item.id, fileId: fileIds[2]!, sortOrder: 1 },
    ];

    for (const link of links) {
      const response = await app.fetch(
        new Request(`http://localhost/api/ingestions/${ingestionId}/items/${link.itemId}/files`, {
          method: "POST",
          headers: {
            authorization: `Bearer ${authToken}`,
            "content-type": "application/json",
          },
          body: JSON.stringify({
            ingestion_file_id: link.fileId,
            sort_order: link.sortOrder,
          }),
        }),
      );
      expect(response.status).toBe(201);
    }

    const submitResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/submit`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
        },
      }),
    );
    expect(submitResponse.status).toBe(200);

    const leaseResponse = await app.fetch(
      new Request("http://localhost/api/ingestions/lease", {
        method: "POST",
        headers: {
          "x-worker-auth-token": "worker-secret",
          "x-worker-id": "worker-grouped-order",
        },
      }),
    );
    expect(leaseResponse.status).toBe(200);

    const leaseBody = (await leaseResponse.json()) as {
      lease: {
        items: Array<{
          ingestion_item_id: string;
          item_index: number;
          files: Array<{ file_id: string; sort_order: number }>;
        }>;
      };
    };

    expect(leaseBody.lease.items.length).toBe(2);
    expect(leaseBody.lease.items.map((item) => item.ingestion_item_id)).toEqual([
      itemOneBody.item.id,
      itemTwoBody.item.id,
    ]);
    expect(leaseBody.lease.items.map((item) => item.item_index)).toEqual([1, 2]);
    expect(leaseBody.lease.items[0]?.files.map((file) => file.file_id)).toEqual([
      fileIds[1]!,
      fileIds[0]!,
    ]);
    expect(leaseBody.lease.items[0]?.files.map((file) => file.sort_order)).toEqual([1, 2]);
  });

  test("rejects submission when committed files are not linked to ingestion items", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(buildIngestionBody({ batch_label: "batch-unlinked-lease-001" })),
      }),
    );
    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as { ingestion: { id: string } };
    const ingestionId = createBody.ingestion.id;

    const payload = "unlinked";
    const presignResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/presign`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          filename: "unlinked.txt",
          content_type: "text/plain",
          size_bytes: payload.length,
        }),
      }),
    );
    expect(presignResponse.status).toBe(201);
    const presignBody = (await presignResponse.json()) as { file_id: string; upload_url: string };

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
    expect(submitResponse.status).toBe(409);
    expect(await getLeaseState(schema, ingestionId)).toEqual({
      status: "UPLOADING",
      activeLeaseCount: 0,
    });
  });
});
