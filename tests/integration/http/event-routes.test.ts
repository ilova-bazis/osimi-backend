import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterAll, beforeAll, beforeEach, describe, expect, test } from "bun:test";
import { sql as sqlIdentifier } from "bun";

import { createAppWithOptions as createApp } from "../../../src/app.ts";
import { createSqlClient } from "../../../src/db/client.ts";
import { runMigrations } from "../../../src/db/migrate.ts";
import { TEST_DATABASE_URL } from "../test-database.ts";

function sha256Hex(value: string): string {
  return new Bun.CryptoHasher("sha256").update(value).digest("hex");
}

function buildSummary(overrides?: Record<string, unknown>): Record<string, unknown> {
  return {
    title: {
      primary: "Event catalog payload",
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
    batch_label: `batch-events-${Date.now()}-${Math.floor(Math.random() * 100000)}`,
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

async function createQueuedIngestion(
  app: ReturnType<typeof createApp>,
  token: string,
  overrides?: Record<string, unknown>,
): Promise<string> {
  const createResponse = await app.fetch(
    new Request("http://localhost/api/ingestions", {
      method: "POST",
      headers: {
        authorization: `Bearer ${token}`,
        "content-type": "application/json",
      },
      body: JSON.stringify(buildIngestionBody(overrides)),
    }),
  );
  expect(createResponse.status).toBe(201);

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
        title: "Events Item 001",
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

async function leaseIngestion(app: ReturnType<typeof createApp>): Promise<{
  ingestionId: string;
  leaseToken: string;
  ingestionItemId: string;
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
      items: Array<{ ingestion_item_id: string }>;
    };
  };

  return {
    ingestionId: leaseBody.lease.ingestion_id,
    leaseToken: leaseBody.lease.lease_token,
    ingestionItemId: leaseBody.lease.items[0]!.ingestion_item_id,
  };
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

describe("event routes", () => {
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
        uploadSigningSecret: "event-routes-upload-signing-secret-000",
        leaseSigningSecret: "event-routes-lease-signing-secret-0000",
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

  test("ingests item completion events with dedupe and object finalization", async () => {
    const app = createTestApp();
    const ingestionId = await createQueuedIngestion(app, authToken, {
      item_kind: "scanned_document",
      access_level: "family",
      embargo_until: "2030-01-01T00:00:00.000Z",
      rights_note: "family-rights",
      sensitivity_note: "private-note",
      summary: buildSummary({
        title: {
          primary: "Summary-driven title",
          original_script: null,
          translations: [],
        },
        classification: {
          tags: ["source:test", "subject:letters"],
          summary: null,
        },
      }),
    });

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
          event_type: "INGESTION_ITEM_COMPLETED",
          ingestion_item_id: lease.ingestionItemId,
          object_id: "OBJ-20260213-EVT001",
          timestamp: new Date().toISOString(),
          payload: {
            title: "Event title should not win",
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
      object_ids: string[];
    };

    expect(firstBody.inserted_events).toBe(2);
    expect(firstBody.duplicate_events).toBe(0);
    expect(firstBody.object_ids).toEqual(["OBJ-20260213-EVT001"]);

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
      object_ids: string[];
    };

    expect(secondBody.inserted_events).toBe(0);
    expect(secondBody.duplicate_events).toBe(2);
    expect(secondBody.object_ids).toEqual(["OBJ-20260213-EVT001"]);

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
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      const objectRows = await sql<
        {
          object_id: string;
          type: string;
          title: string;
          language_code: string | null;
          access_level: string;
          embargo_kind: string;
          embargo_until: Date | null;
          rights_note: string | null;
          sensitivity_note: string | null;
          tags: string[];
          ingest_manifest: unknown;
          processing_state: string;
          availability_state: string;
        }[]
      >`
        SELECT
          obj.object_id,
          obj.type,
          obj.title,
          obj.language_code,
          obj.access_level,
          obj.embargo_kind,
          obj.embargo_until,
          obj.rights_note,
          obj.sensitivity_note,
          COALESCE((
            SELECT array_agg(tag.name_normalized ORDER BY tag.name_normalized)
            FROM object_tags otag
            INNER JOIN tags tag ON tag.id = otag.tag_id
            WHERE otag.object_id = obj.object_id
          ), ARRAY[]::text[]) AS tags,
          obj.ingest_manifest,
          obj.processing_state,
          obj.availability_state
        FROM objects obj
        WHERE source_ingestion_id = ${ingestionId}
      `;

      expect(objectRows.length).toBe(1);
      expect(objectRows[0]?.type).toBe("DOCUMENT");
      expect(objectRows[0]?.title).toBe("Events Item 001");
      expect(objectRows[0]?.language_code).toBe("en");
      expect(objectRows[0]?.access_level).toBe("family");
      expect(objectRows[0]?.embargo_kind).toBe("timed");
      expect(objectRows[0]?.embargo_until).not.toBeNull();
      expect(objectRows[0]?.rights_note).toBe("family-rights");
      expect(objectRows[0]?.sensitivity_note).toBe("private-note");
      expect(objectRows[0]?.tags).toEqual(["source:test", "subject:letters"]);
      expect(objectRows[0]?.ingest_manifest).toMatchObject({
        schema_version: "1.0",
      });
      expect(objectRows[0]?.processing_state).toBe("index_done");
      expect(objectRows[0]?.availability_state).toBe("AVAILABLE");

      const eventRows = await sql<{ id: string }[]>`
        SELECT id
        FROM object_events
        WHERE ingestion_id = ${ingestionId}
      `;

      expect(eventRows.length).toBe(2);
    } finally {
      await sql.close();
    }
  });

  test("replays object-id-bearing event envelopes without conflict", async () => {
    const app = createTestApp();
    const ingestionId = await createQueuedIngestion(app, authToken);
    const lease = await leaseIngestion(app);
    expect(lease.ingestionId).toBe(ingestionId);

    const eventPayload = {
      lease_token: lease.leaseToken,
      events: [
        {
          event_id: crypto.randomUUID(),
          event_type: "OBJECT_CREATED",
          object_id: "OBJ-20260213-EVT002",
          timestamp: new Date().toISOString(),
          payload: { source: "worker" },
        },
        {
          event_id: crypto.randomUUID(),
          event_type: "ARTIFACT_CREATED",
          object_id: "OBJ-20260213-EVT003",
          timestamp: new Date().toISOString(),
          payload: { kind: "original" },
        },
      ],
    };

    const submit = (): Promise<Response> => app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/events`, {
        method: "POST",
        headers: {
          "x-worker-auth-token": "worker-secret",
          "content-type": "application/json",
        },
        body: JSON.stringify(eventPayload),
      }),
    );

    const first = await submit();
    expect(first.status).toBe(200);
    expect(await first.json()).toMatchObject({
      inserted_events: 2,
      duplicate_events: 0,
    });

    const replay = await submit();
    expect(replay.status).toBe(200);
    expect(await replay.json()).toMatchObject({
      inserted_events: 0,
      duplicate_events: 2,
    });
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

  test("rejects item completion event without object_id", async () => {
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
              event_type: "INGESTION_ITEM_COMPLETED",
              ingestion_item_id: lease.ingestionItemId,
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

  test("rejects aggregate completion events scoped to an object or item", async () => {
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
              object_id: "OBJ-20260213-INVALID1",
              ingestion_item_id: lease.ingestionItemId,
              timestamp: new Date().toISOString(),
              payload: {
                title: "Invalid aggregate scope",
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

  test("keeps the ingestion processing when aggregate completion arrives before item outcomes", async () => {
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
                step: "aggregate-completion",
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
    const body = (await response.json()) as { object_ids: string[] };
    expect(body.object_ids).toEqual([]);

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
    expect(detailBody.ingestion.status).toBe("PROCESSING");

    const sql = createSqlClient(TEST_DATABASE_URL!);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      const objects = await sql<Array<{ count: number }>>`
        SELECT COUNT(*)::int AS count
        FROM objects
        WHERE source_ingestion_id = ${ingestionId}
      `;
      expect(objects[0]?.count).toBe(0);
    } finally {
      await sql.close();
    }
  });

  test("uses ingestion item language override when creating item object", async () => {
    const app = createTestApp();

    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(buildIngestionBody({
          batch_label: "batch-item-language-override-001",
          language_code: "tj",
        })),
      }),
    );
    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as { ingestion: { id: string } };
    const ingestionId = createBody.ingestion.id;

    const payload = "item language override";
    const presignResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/files/presign`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          filename: "item-language.txt",
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

    const createItemResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/items`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          item_index: 1,
          title: "Language override item",
          language_code: "en",
        }),
      }),
    );
    expect(createItemResponse.status).toBe(201);
    const itemBody = (await createItemResponse.json()) as { item: { id: string } };

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
    expect(linkResponse.status).toBe(201);

    const submitResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/submit`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
        },
      }),
    );
    expect(submitResponse.status).toBe(200);

    const lease = await leaseIngestion(app);
    expect(lease.ingestionId).toBe(ingestionId);

    const eventsResponse = await app.fetch(
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
              event_type: "INGESTION_PROCESSING",
              timestamp: new Date().toISOString(),
              payload: { step: "processing" },
            },
            {
              event_id: crypto.randomUUID(),
              event_type: "INGESTION_ITEM_COMPLETED",
              ingestion_item_id: lease.ingestionItemId,
              object_id: "OBJ-20260318-LNG001",
              timestamp: new Date().toISOString(),
              payload: {
                ingest_json: { schema_version: "1.0" },
              },
            },
          ],
        }),
      }),
    );
    expect(eventsResponse.status).toBe(200);

    const sql = createSqlClient(TEST_DATABASE_URL!);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      const rows = await sql<{ language_code: string | null }[]>`
        SELECT language_code
        FROM objects
        WHERE source_ingestion_item_id = ${lease.ingestionItemId}
        LIMIT 1
      `;

      expect(rows.length).toBe(1);
      expect(rows[0]?.language_code).toBe("en");
    } finally {
      await sql.close();
    }
  });

  test("falls back to ingestion language when ingestion item language is unset", async () => {
    const app = createTestApp();
    const ingestionId = await createQueuedIngestion(app, authToken, {
      batch_label: "batch-item-language-fallback-001",
      language_code: "tj",
    });

    const lease = await leaseIngestion(app);
    expect(lease.ingestionId).toBe(ingestionId);

    const eventsResponse = await app.fetch(
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
              event_type: "INGESTION_PROCESSING",
              timestamp: new Date().toISOString(),
              payload: { step: "processing" },
            },
            {
              event_id: crypto.randomUUID(),
              event_type: "INGESTION_ITEM_COMPLETED",
              ingestion_item_id: lease.ingestionItemId,
              object_id: "OBJ-20260318-LNG002",
              timestamp: new Date().toISOString(),
              payload: {
                ingest_json: { schema_version: "1.0" },
              },
            },
          ],
        }),
      }),
    );
    expect(eventsResponse.status).toBe(200);

    const sql = createSqlClient(TEST_DATABASE_URL!);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      const rows = await sql<{ language_code: string | null }[]>`
        SELECT language_code
        FROM objects
        WHERE source_ingestion_item_id = ${lease.ingestionItemId}
        LIMIT 1
      `;

      expect(rows.length).toBe(1);
      expect(rows[0]?.language_code).toBe("tj");
    } finally {
      await sql.close();
    }
  });

  test("derives COMPLETED_WITH_ERRORS from item terminal outcomes", async () => {
    const app = createTestApp();
    const createResponse = await app.fetch(
      new Request("http://localhost/api/ingestions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${authToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(buildIngestionBody({ batch_label: "batch-item-outcomes-001" })),
      }),
    );
    expect(createResponse.status).toBe(201);
    const createBody = (await createResponse.json()) as { ingestion: { id: string } };
    const ingestionId = createBody.ingestion.id;

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

    const files = [
      { filename: "events-1.txt", content: "events flow 1", itemId: itemOneBody.item.id },
      { filename: "events-2.txt", content: "events flow 2", itemId: itemTwoBody.item.id },
    ];

    for (const file of files) {
      const presignResponse = await app.fetch(
        new Request(`http://localhost/api/ingestions/${ingestionId}/files/presign`, {
          method: "POST",
          headers: {
            authorization: `Bearer ${authToken}`,
            "content-type": "application/json",
          },
          body: JSON.stringify({
            filename: file.filename,
            content_type: "text/plain",
            size_bytes: file.content.length,
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
            "content-length": String(file.content.length),
          },
          body: file.content,
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
            checksum_sha256: sha256Hex(file.content),
          }),
        }),
      );
      expect(commitResponse.status).toBe(200);

      const linkResponse = await app.fetch(
        new Request(`http://localhost/api/ingestions/${ingestionId}/items/${file.itemId}/files`, {
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
      expect(linkResponse.status).toBe(201);
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

    const lease = await leaseIngestion(app);
    expect(lease.ingestionId).toBe(ingestionId);

    const eventsResponse = await app.fetch(
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
              event_type: "INGESTION_PROCESSING",
              timestamp: new Date().toISOString(),
              payload: {
                step: "item-processing",
              },
            },
            {
              event_id: crypto.randomUUID(),
              event_type: "INGESTION_ITEM_COMPLETED",
              ingestion_item_id: itemOneBody.item.id,
              object_id: "OBJ-20260317-ITM001",
              timestamp: new Date().toISOString(),
              payload: {
                ingest_json: { schema_version: "1.0" },
              },
            },
            {
              event_id: crypto.randomUUID(),
              event_type: "INGESTION_ITEM_FAILED",
              ingestion_item_id: itemTwoBody.item.id,
              timestamp: new Date().toISOString(),
              payload: {
                reason: "simulated failure",
              },
            },
          ],
        }),
      }),
    );

    expect(eventsResponse.status).toBe(200);

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
    expect(detailBody.ingestion.status).toBe("COMPLETED_WITH_ERRORS");
  });

  test("does not duplicate an item object and keeps the latest manifest", async () => {
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
              event_type: "INGESTION_ITEM_COMPLETED",
              ingestion_item_id: lease.ingestionItemId,
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
              event_type: "INGESTION_ITEM_COMPLETED",
              ingestion_item_id: lease.ingestionItemId,
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
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      const objects = await sql<
        {
          object_id: string;
          ingest_manifest: unknown;
          processing_state: string;
          availability_state: string;
        }[]
      >`
        SELECT object_id, ingest_manifest, processing_state, availability_state
        FROM objects
        WHERE source_ingestion_id = ${ingestionId}
      `;

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

  test("does not create duplicate item objects under concurrent completion requests", async () => {
    const app = createTestApp();
    const ingestionId = await createQueuedIngestion(app, authToken);
    const lease = await leaseIngestion(app);
    expect(lease.ingestionId).toBe(ingestionId);

    const requestBodyOne = JSON.stringify({
      lease_token: lease.leaseToken,
      events: [
        {
          event_id: crypto.randomUUID(),
          event_type: "INGESTION_ITEM_COMPLETED",
          ingestion_item_id: lease.ingestionItemId,
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
          event_type: "INGESTION_ITEM_COMPLETED",
          ingestion_item_id: lease.ingestionItemId,
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
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      const objects = await sql<{ object_id: string }[]>`
        SELECT object_id
        FROM objects
        WHERE source_ingestion_id = ${ingestionId}
      `;

      expect(objects.length).toBe(1);
    } finally {
      await sql.close();
    }
  });

  test("returns every item-scoped object id from a multi-item completion batch", async () => {
    const app = createTestApp();
    const ingestionId = await createQueuedIngestion(app, authToken);
    const lease = await leaseIngestion(app);
    expect(lease.ingestionId).toBe(ingestionId);

    const secondItemId = crypto.randomUUID();
    const sql = createSqlClient(TEST_DATABASE_URL!);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`
        INSERT INTO ingestion_items (id, ingestion_id, item_index, title)
        VALUES (${secondItemId}, ${ingestionId}, 2, ${"Events Item 002"})
      `;
    } finally {
      await sql.close();
    }

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
              event_type: "INGESTION_ITEM_COMPLETED",
              ingestion_item_id: lease.ingestionItemId,
              object_id: "OBJ-20260319-MULTI01",
              timestamp: new Date().toISOString(),
              payload: { ingest_json: { schema_version: "1.0" } },
            },
            {
              event_id: crypto.randomUUID(),
              event_type: "INGESTION_ITEM_COMPLETED",
              ingestion_item_id: secondItemId,
              object_id: "OBJ-20260319-MULTI02",
              timestamp: new Date().toISOString(),
              payload: { ingest_json: { schema_version: "1.0" } },
            },
          ],
        }),
      }),
    );

    expect(response.status).toBe(200);
    const body = (await response.json()) as { object_ids: string[] };
    expect(body.object_ids).toEqual([
      "OBJ-20260319-MULTI01",
      "OBJ-20260319-MULTI02",
    ]);

    const itemResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/items`, {
        headers: { authorization: `Bearer ${authToken}` },
      }),
    );
    expect(itemResponse.status).toBe(200);
    const itemBody = (await itemResponse.json()) as {
      items: Array<{ id: string; object_id: string | null }>;
    };
    expect(itemBody.items).toEqual(expect.arrayContaining([
      expect.objectContaining({
        id: lease.ingestionItemId,
        object_id: "OBJ-20260319-MULTI01",
      }),
      expect.objectContaining({
        id: secondItemId,
        object_id: "OBJ-20260319-MULTI02",
      }),
    ]));
  });

  test("derives completed from all skipped items", async () => {
    const app = createTestApp();
    const ingestionId = await createQueuedIngestion(app, authToken);
    const lease = await leaseIngestion(app);
    expect(lease.ingestionId).toBe(ingestionId);

    const sql = createSqlClient(TEST_DATABASE_URL!);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`
        UPDATE ingestion_items
        SET status = 'SKIPPED'
        WHERE id = ${lease.ingestionItemId}
      `;
    } finally {
      await sql.close();
    }

    const response = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/events`, {
        method: "POST",
        headers: {
          "x-worker-auth-token": "worker-secret",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          lease_token: lease.leaseToken,
          events: [{
            event_id: crypto.randomUUID(),
            event_type: "INGESTION_COMPLETED",
            timestamp: new Date().toISOString(),
            payload: { step: "aggregate-completion" },
          }],
        }),
      }),
    );
    expect(response.status).toBe(200);

    const detailResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}`, {
        headers: { authorization: `Bearer ${authToken}` },
      }),
    );
    const detailBody = (await detailResponse.json()) as { ingestion: { status: string } };
    expect(detailBody.ingestion.status).toBe("COMPLETED");
  });

  test("derives failed from failed and skipped items without a completed item", async () => {
    const app = createTestApp();
    const ingestionId = await createQueuedIngestion(app, authToken);
    const lease = await leaseIngestion(app);
    expect(lease.ingestionId).toBe(ingestionId);

    const secondItemId = crypto.randomUUID();
    const sql = createSqlClient(TEST_DATABASE_URL!);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`
        INSERT INTO ingestion_items (id, ingestion_id, item_index, title)
        VALUES (${secondItemId}, ${ingestionId}, 2, ${"Skipped and failed item"})
      `;
      await sql`
        UPDATE ingestion_items
        SET status = 'SKIPPED'
        WHERE id = ${lease.ingestionItemId}
      `;
    } finally {
      await sql.close();
    }

    const response = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}/events`, {
        method: "POST",
        headers: {
          "x-worker-auth-token": "worker-secret",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          lease_token: lease.leaseToken,
          events: [{
            event_id: crypto.randomUUID(),
            event_type: "INGESTION_ITEM_FAILED",
            ingestion_item_id: secondItemId,
            timestamp: new Date().toISOString(),
            payload: { reason: "simulated failure" },
          }],
        }),
      }),
    );
    expect(response.status).toBe(200);

    const detailResponse = await app.fetch(
      new Request(`http://localhost/api/ingestions/${ingestionId}`, {
        headers: { authorization: `Bearer ${authToken}` },
      }),
    );
    const detailBody = (await detailResponse.json()) as { ingestion: { status: string } };
    expect(detailBody.ingestion.status).toBe("FAILED");
  });

});
