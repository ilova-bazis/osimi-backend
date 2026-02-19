import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterAll, beforeAll, beforeEach, describe, expect, test } from "bun:test";

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

async function createQueuedIngestion(app: ReturnType<typeof createApp>, token: string): Promise<string> {
  const createResponse = await app.fetch(
    new Request("http://localhost/api/ingestions", {
      method: "POST",
      headers: {
        authorization: `Bearer ${token}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({
        batch_label: `batch-events-${Date.now()}-${Math.floor(Math.random() * 100000)}`,
      }),
    }),
  );

  const created = (await createResponse.json()) as { ingestion: { id: string } };
  const ingestionId = created.ingestion.id;

  const payload = "events flow";
  const presignResponse = await app.fetch(
    new Request(`http://localhost/api/ingestions/${ingestionId}/files/presign`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${token}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({
        filename: "events.txt",
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

async function leaseIngestion(app: ReturnType<typeof createApp>): Promise<{
  ingestionId: string;
  leaseToken: string;
}> {
  const leaseResponse = await app.fetch(
    new Request("http://localhost/api/ingestions/lease", {
      method: "POST",
      headers: {
        "x-worker-auth-token": "worker-secret",
        "x-worker-id": "worker-events",
      },
    }),
  );

  expect(leaseResponse.status).toBe(200);
  const leaseBody = (await leaseResponse.json()) as {
    lease: {
      ingestion_id: string;
      lease_token: string;
    };
  };

  return {
    ingestionId: leaseBody.lease.ingestion_id,
    leaseToken: leaseBody.lease.lease_token,
  };
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

describe.skipIf(!TEST_DATABASE_URL)("event routes", () => {
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
    schema = `events_${Date.now()}_${Math.floor(Math.random() * 100000)}`;
    stagingRoot = await mkdtemp(join(tmpdir(), "osimi-events-staging-"));

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

  test("ingests worker events with dedupe and completion object finalization", async () => {
    const app = createTestApp();
    const ingestionId = await createQueuedIngestion(app, authToken);

    const lease = await leaseIngestion(app);
    expect(lease.ingestionId).toBe(ingestionId);

    const eventPayload = {
      lease_token: lease.leaseToken,
      events: [
        {
          event_id: crypto.randomUUID(),
          event_type: "FILE_VALIDATED",
          timestamp: new Date().toISOString(),
          payload: {
            file: "events.txt",
          },
        },
        {
          event_id: crypto.randomUUID(),
          event_type: "INGESTION_COMPLETED",
          object_id: "OBJ-20260213-EVT001",
          timestamp: new Date().toISOString(),
          payload: {
            title: "Event-completed object",
            ingest_json: {
              schema_version: "1.0",
              ingest: {
                ingest_id: "ING-1",
              },
            },
          },
        },
      ],
    };

    const firstEventsResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/events`, {
        method: "POST",
        headers: {
          "x-worker-auth-token": "worker-secret",
          "content-type": "application/json",
        },
        body: JSON.stringify(eventPayload),
      }),
    );

    expect(firstEventsResponse.status).toBe(200);
    const firstBody = (await firstEventsResponse.json()) as {
      inserted_events: number;
      duplicate_events: number;
      object_id: string | null;
    };

    expect(firstBody.inserted_events).toBe(2);
    expect(firstBody.duplicate_events).toBe(0);
    expect(typeof firstBody.object_id).toBe("string");

    const secondEventsResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/events`, {
        method: "POST",
        headers: {
          "x-worker-auth-token": "worker-secret",
          "content-type": "application/json",
        },
        body: JSON.stringify(eventPayload),
      }),
    );

    expect(secondEventsResponse.status).toBe(200);
    const secondBody = (await secondEventsResponse.json()) as {
      inserted_events: number;
      duplicate_events: number;
      object_id: string | null;
    };

    expect(secondBody.inserted_events).toBe(0);
    expect(secondBody.duplicate_events).toBe(2);

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
    expect(detailBody.ingestion.status).toBe("COMPLETED");

    const sql = createSqlClient(TEST_DATABASE_URL!);
    try {
      const objectsTable = qualifiedTable(schema, "objects");
      const eventsTable = qualifiedTable(schema, "object_events");

      const objectRows = (await sql.unsafe(
        `SELECT object_id, ingest_manifest, processing_state, availability_state FROM ${objectsTable} WHERE source_ingestion_id = $1`,
        [ingestionId],
      )) as Array<{
        object_id: string;
        ingest_manifest: unknown;
        processing_state: string;
        availability_state: string;
      }>;

      expect(objectRows.length).toBe(1);
      expect(objectRows[0]?.ingest_manifest).toMatchObject({
        schema_version: "1.0",
      });
      expect(objectRows[0]?.processing_state).toBe("index_done");
      expect(objectRows[0]?.availability_state).toBe("AVAILABLE");

      const eventRows = (await sql.unsafe(
        `SELECT id FROM ${eventsTable} WHERE ingestion_id = $1`,
        [ingestionId],
      )) as Array<{ id: string }>;

      expect(eventRows.length).toBe(2);
    } finally {
      await sql.close();
    }
  });

  test("rejects events when lease token does not match ingestion id", async () => {
    const app = createTestApp();
    const ingestionOne = await createQueuedIngestion(app, authToken);
    const ingestionTwo = await createQueuedIngestion(app, authToken);
    const lease = await leaseIngestion(app);
    expect(lease.ingestionId).toBe(ingestionOne);

    const response = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionTwo}/events`, {
        method: "POST",
        headers: {
          "x-worker-auth-token": "worker-secret",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          lease_token: lease.leaseToken,
          events: [
            {
              event_id: crypto.randomUUID(),
              event_type: "FILE_VALIDATED",
              timestamp: new Date().toISOString(),
              payload: {
                step: "validate",
              },
            },
          ],
        }),
      }),
    );

    expect(response.status).toBe(401);
  });

  test("rejects completion event without object_id", async () => {
    const app = createTestApp();
    const ingestionId = await createQueuedIngestion(app, authToken);
    const lease = await leaseIngestion(app);
    expect(lease.ingestionId).toBe(ingestionId);

    const response = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/events`, {
        method: "POST",
        headers: {
          "x-worker-auth-token": "worker-secret",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          lease_token: lease.leaseToken,
          events: [
            {
              event_id: crypto.randomUUID(),
              event_type: "INGESTION_COMPLETED",
              timestamp: new Date().toISOString(),
              payload: {
                title: "Missing object id",
              },
            },
          ],
        }),
      }),
    );

    expect(response.status).toBe(400);
    const body = (await response.json()) as {
      error: {
        code: string;
        message: string;
      };
    };
    expect(body.error.code).toBe("BAD_REQUEST");
  });

  test("rejects completion event with invalid object_id format", async () => {
    const app = createTestApp();
    const ingestionId = await createQueuedIngestion(app, authToken);
    const lease = await leaseIngestion(app);
    expect(lease.ingestionId).toBe(ingestionId);

    const response = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/events`, {
        method: "POST",
        headers: {
          "x-worker-auth-token": "worker-secret",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          lease_token: lease.leaseToken,
          events: [
            {
              event_id: crypto.randomUUID(),
              event_type: "INGESTION_COMPLETED",
              object_id: "not-an-object-id",
              timestamp: new Date().toISOString(),
              payload: {
                title: "Invalid object id",
              },
            },
          ],
        }),
      }),
    );

    expect(response.status).toBe(400);
    const body = (await response.json()) as {
      error: {
        code: string;
        message: string;
      };
    };
    expect(body.error.code).toBe("BAD_REQUEST");
  });

  test("rejects invalid event_type in worker events", async () => {
    const app = createTestApp();
    const ingestionId = await createQueuedIngestion(app, authToken);
    const lease = await leaseIngestion(app);
    expect(lease.ingestionId).toBe(ingestionId);

    const response = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/events`, {
        method: "POST",
        headers: {
          "x-worker-auth-token": "worker-secret",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          lease_token: lease.leaseToken,
          events: [
            {
              event_id: crypto.randomUUID(),
              event_type: "UNKNOWN_EVENT_TYPE",
              timestamp: new Date().toISOString(),
              payload: {
                title: "Invalid event type",
              },
            },
          ],
        }),
      }),
    );

    expect(response.status).toBe(400);
    const body = (await response.json()) as {
      error: {
        code: string;
        message: string;
      };
    };
    expect(body.error.code).toBe("BAD_REQUEST");
  });

  test("accepts out-of-order events and keeps ingestion completed", async () => {
    const app = createTestApp();
    const ingestionId = await createQueuedIngestion(app, authToken);
    const lease = await leaseIngestion(app);
    expect(lease.ingestionId).toBe(ingestionId);

    const response = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/events`, {
        method: "POST",
        headers: {
          "x-worker-auth-token": "worker-secret",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          lease_token: lease.leaseToken,
          events: [
            {
              event_id: crypto.randomUUID(),
              event_type: "INGESTION_COMPLETED",
              object_id: "OBJ-20260213-OOO001",
              timestamp: new Date().toISOString(),
              payload: {
                title: "Out of order object",
              },
            },
            {
              event_id: crypto.randomUUID(),
              event_type: "PIPELINE_STEP_STARTED",
              timestamp: new Date().toISOString(),
              payload: {
                step: "OCR",
              },
            },
          ],
        }),
      }),
    );

    expect(response.status).toBe(200);

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
    expect(detailBody.ingestion.status).toBe("COMPLETED");
  });

  test("does not duplicate object and keeps latest ingest manifest on repeated completion events", async () => {
    const app = createTestApp();
    const ingestionId = await createQueuedIngestion(app, authToken);
    const lease = await leaseIngestion(app);
    expect(lease.ingestionId).toBe(ingestionId);

    const completionPayload = {
      title: "Idempotent completion object",
      ingest_json: {
        schema_version: "1.0",
        ingest: { ingest_id: "ING-repeat" },
      },
    };
    const completionPayloadUpdated = {
      title: "Idempotent completion object",
      ingest_json: {
        schema_version: "2.0",
        ingest: { ingest_id: "ING-repeat-updated" },
      },
    };
    const completionObjectId = "OBJ-20260213-IDEMP01";

    const first = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/events`, {
        method: "POST",
        headers: {
          "x-worker-auth-token": "worker-secret",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          lease_token: lease.leaseToken,
          events: [
            {
              event_id: crypto.randomUUID(),
              event_type: "INGESTION_COMPLETED",
              object_id: completionObjectId,
              timestamp: new Date().toISOString(),
              payload: completionPayloadUpdated,
            },
          ],
        }),
      }),
    );

    expect(first.status).toBe(200);

    const second = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/events`, {
        method: "POST",
        headers: {
          "x-worker-auth-token": "worker-secret",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          lease_token: lease.leaseToken,
          events: [
            {
              event_id: crypto.randomUUID(),
              event_type: "INGESTION_COMPLETED",
              object_id: completionObjectId,
              timestamp: new Date().toISOString(),
              payload: completionPayload,
            },
          ],
        }),
      }),
    );

    expect(second.status).toBe(200);

    const sql = createSqlClient(TEST_DATABASE_URL!);
    try {
      const objectsTable = qualifiedTable(schema, "objects");
      const objects = (await sql.unsafe(
        `SELECT object_id, ingest_manifest, processing_state, availability_state FROM ${objectsTable} WHERE source_ingestion_id = $1`,
        [ingestionId],
      )) as Array<{
        object_id: string;
        ingest_manifest: unknown;
        processing_state: string;
        availability_state: string;
      }>;

      expect(objects.length).toBe(1);
      expect(objects[0]?.ingest_manifest).toMatchObject({
        schema_version: "1.0",
        ingest: { ingest_id: "ING-repeat" },
      });
      expect(objects[0]?.processing_state).toBe("index_done");
      expect(objects[0]?.availability_state).toBe("AVAILABLE");
    } finally {
      await sql.close();
    }
  });

  test("does not create duplicate objects under concurrent completion requests", async () => {
    const app = createTestApp();
    const ingestionId = await createQueuedIngestion(app, authToken);
    const lease = await leaseIngestion(app);
    expect(lease.ingestionId).toBe(ingestionId);

    const requestBodyOne = JSON.stringify({
      lease_token: lease.leaseToken,
      events: [
        {
          event_id: crypto.randomUUID(),
          event_type: "INGESTION_COMPLETED",
          object_id: "OBJ-20260213-CONCUR1",
          timestamp: new Date().toISOString(),
          payload: {
            title: "Concurrent completion A",
          },
        },
      ],
    });

    const requestBodyTwo = JSON.stringify({
      lease_token: lease.leaseToken,
      events: [
        {
          event_id: crypto.randomUUID(),
          event_type: "INGESTION_COMPLETED",
          object_id: "OBJ-20260213-CONCUR1",
          timestamp: new Date().toISOString(),
          payload: {
            title: "Concurrent completion B",
          },
        },
      ],
    });

    const [responseOne, responseTwo] = await Promise.all([
      app.fetch(
        new Request(`http://localhost/api/ingestions/${ingestionId}/events`, {
          method: "POST",
          headers: {
            "x-worker-auth-token": "worker-secret",
            "content-type": "application/json",
          },
          body: requestBodyOne,
        }),
      ),
      app.fetch(
        new Request(`http://localhost/api/ingestions/${ingestionId}/events`, {
          method: "POST",
          headers: {
            "x-worker-auth-token": "worker-secret",
            "content-type": "application/json",
          },
          body: requestBodyTwo,
        }),
      ),
    ]);

    expect(responseOne.status).toBe(200);
    expect(responseTwo.status).toBe(200);

    const sql = createSqlClient(TEST_DATABASE_URL!);
    try {
      const objectsTable = qualifiedTable(schema, "objects");
      const objects = (await sql.unsafe(
        `SELECT object_id FROM ${objectsTable} WHERE source_ingestion_id = $1`,
        [ingestionId],
      )) as Array<{ object_id: string }>;

      expect(objects.length).toBe(1);
    } finally {
      await sql.close();
    }
  });
});
