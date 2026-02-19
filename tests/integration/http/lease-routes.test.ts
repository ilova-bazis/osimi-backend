import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterAll, beforeAll, beforeEach, describe, expect, test } from "bun:test";

import { createAppWithOptions as createApp } from "../../../src/app.ts";
import { createSqlClient } from "../../../src/db/client.ts";
import { runMigrations } from "../../../src/db/migrate.ts";
import { createDownloadToken } from "../../../src/storage/staging.ts";

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

async function cancelQueuedIngestions(schema: string): Promise<void> {
  const sql = createSqlClient(TEST_DATABASE_URL!);

  try {
    const ingestionsTable = qualifiedTable(schema, "ingestions");
    await sql.unsafe(
      `
        UPDATE ${ingestionsTable}
        SET status = 'CANCELED',
            updated_at = now()
        WHERE status = 'QUEUED'
      `,
    );
  } finally {
    await sql.close();
  }
}

async function expireActiveLease(schema: string, ingestionId: string): Promise<void> {
  const sql = createSqlClient(TEST_DATABASE_URL!);

  try {
    const leasesTable = qualifiedTable(schema, "ingestion_leases");
    await sql.unsafe(
      `
        UPDATE ${leasesTable}
        SET lease_expires_at = now() - interval '1 minute'
        WHERE ingestion_id = $1
          AND released_at IS NULL
      `,
      [ingestionId],
    );
  } finally {
    await sql.close();
  }
}

async function resetActiveIngestions(schema: string): Promise<void> {
  const sql = createSqlClient(TEST_DATABASE_URL!);

  try {
    const ingestionsTable = qualifiedTable(schema, "ingestions");
    const leasesTable = qualifiedTable(schema, "ingestion_leases");

    await sql.unsafe(
      `
        UPDATE ${ingestionsTable}
        SET status = 'CANCELED',
            updated_at = now()
        WHERE status IN ('DRAFT', 'UPLOADING', 'QUEUED', 'PROCESSING')
      `,
    );

    await sql.unsafe(
      `
        UPDATE ${leasesTable}
        SET released_at = now()
        WHERE released_at IS NULL
      `,
    );
  } finally {
    await sql.close();
  }
}

async function createQueuedIngestion(app: ReturnType<typeof createApp>, token: string): Promise<string> {
  const createResponse = await app.fetch(
    new Request("http://localhost/api/ingestions", {
      method: "POST",
      headers: {
        authorization: `Bearer ${token}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({
        batch_label: `batch-${Date.now()}-${Math.floor(Math.random() * 100000)}`,
      }),
    }),
  );

  const created = (await createResponse.json()) as { ingestion: { id: string } };
  const ingestionId = created.ingestion.id;

  const payload = "lease flow";
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

  const presignBody = (await presignResponse.json()) as {
    file_id: string;
    upload_url: string;
  };

  await app.fetch(
    new Request(`http://localhost${presignBody.upload_url}`, {
      method: "PUT",
      headers: {
        "content-type": "text/plain",
        "content-length": String(payload.length),
      },
      body: payload,
    }),
  );

  await app.fetch(
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

  await app.fetch(
    new Request(`http://localhost/api/ingestions/${ingestionId}/submit`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${token}`,
      },
    }),
  );

  return ingestionId;
}

describe.skipIf(!TEST_DATABASE_URL)("lease routes", () => {
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

    const loginBody = (await loginResponse.json()) as { token: string };
    authToken = loginBody.token;
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
        download_urls: Array<{ download_url: string }>;
      };
    };

    expect(leaseBody.lease.ingestion_id).toBe(ingestionId);
    expect(leaseBody.lease.download_urls.length).toBe(1);

    const downloadResponse = await app.fetch(
      new Request(`http://localhost${leaseBody.lease.download_urls[0]!.download_url}`),
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
        download_urls: Array<{ storage_key: string; file_id: string; content_type: string; size_bytes: number }>;
      };
    };

    const file = leaseBody.lease.download_urls[0];
    expect(file).toBeDefined();

    const expiredToken = createDownloadToken({
      ingestion_id: ingestionId,
      file_id: file!.file_id,
      tenant_id: "00000000-0000-0000-0000-000000000001",
      storage_key: file!.storage_key,
      content_type: file!.content_type,
      size_bytes: file!.size_bytes,
      expires_at: new Date(Date.now() - 1000).toISOString(),
    });

    const downloadResponse = await app.fetch(
      new Request(`http://localhost/api/worker/downloads/${expiredToken}`),
    );

    expect(downloadResponse.status).toBe(401);
  });
});
