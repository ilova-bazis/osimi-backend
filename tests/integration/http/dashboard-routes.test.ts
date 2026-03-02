import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { sql as sqlIdentifier } from "bun";

import { createAppWithOptions as createApp } from "../../../src/app.ts";
import { createSqlClient } from "../../../src/db/client.ts";
import { runMigrations } from "../../../src/db/migrate.ts";

const TEST_DATABASE_URL =
  process.env.TEST_DATABASE_URL ?? process.env.DATABASE_URL;

describe.skipIf(!TEST_DATABASE_URL)("dashboard routes", () => {
  let schema = "";
  let viewerToken = "";

  function createTestApp() {
    return createApp({
      runtimeConfig: {
        databaseUrl: TEST_DATABASE_URL,
        dbSchema: schema,
      },
    });
  }

  beforeAll(async () => {
    schema = `dashboard_${Date.now()}_${Math.floor(Math.random() * 100000)}`;

    await runMigrations({
      databaseUrl: TEST_DATABASE_URL,
      schema,
    });

    const sql = createSqlClient(TEST_DATABASE_URL!);

    try {
      const viewerHash = await Bun.password.hash("viewer123");
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;

      await sql`
        INSERT INTO tenants (id, slug, name)
        VALUES
          (${"00000000-0000-4000-8000-000000000001"}, ${"tenant-one"}, ${"Tenant One"}),
          (${"00000000-0000-4000-8000-000000000002"}, ${"tenant-two"}, ${"Tenant Two"})
      `;

      await sql`
        INSERT INTO users (id, username, username_normalized, password_hash)
        VALUES (${"10000000-0000-4000-8000-000000000001"}, ${"viewer@osimi.local"}, ${"viewer@osimi.local"}, ${viewerHash})
      `;

      await sql`
        INSERT INTO tenant_memberships (id, tenant_id, user_id, role)
        VALUES (${"20000000-0000-4000-8000-000000000001"}, ${"00000000-0000-4000-8000-000000000001"}, ${"10000000-0000-4000-8000-000000000001"}, ${"viewer"})
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
          access_level,
          summary,
          error_summary
        )
        VALUES
          (
            ${"30000000-0000-4000-8000-000000000001"},
            ${"batch-1"},
            ${"00000000-0000-4000-8000-000000000001"},
            ${"COMPLETED"}::ingestion_status,
            ${"10000000-0000-4000-8000-000000000001"},
            ${"1.0"},
            ${"document"}::ingestion_classification_type,
            ${"document"}::ingest_item_kind,
            ${"en"},
            ${"auto"}::ingestion_pipeline_preset,
            ${"private"}::object_access_level,
            ${{}},
            ${{}}
          ),
          (
            ${"30000000-0000-4000-8000-000000000002"},
            ${"batch-2"},
            ${"00000000-0000-4000-8000-000000000001"},
            ${"COMPLETED"}::ingestion_status,
            ${"10000000-0000-4000-8000-000000000001"},
            ${"1.0"},
            ${"document"}::ingestion_classification_type,
            ${"document"}::ingest_item_kind,
            ${"en"},
            ${"auto"}::ingestion_pipeline_preset,
            ${"private"}::object_access_level,
            ${{}},
            ${{}}
          ),
          (
            ${"30000000-0000-4000-8000-000000000003"},
            ${"batch-3"},
            ${"00000000-0000-4000-8000-000000000001"},
            ${"FAILED"}::ingestion_status,
            ${"10000000-0000-4000-8000-000000000001"},
            ${"1.0"},
            ${"document"}::ingestion_classification_type,
            ${"document"}::ingest_item_kind,
            ${"en"},
            ${"auto"}::ingestion_pipeline_preset,
            ${"private"}::object_access_level,
            ${{}},
            ${{}}
          ),
          (
            ${"30000000-0000-4000-8000-000000000004"},
            ${"batch-4"},
            ${"00000000-0000-4000-8000-000000000002"},
            ${"COMPLETED"}::ingestion_status,
            ${"10000000-0000-4000-8000-000000000001"},
            ${"1.0"},
            ${"document"}::ingestion_classification_type,
            ${"document"}::ingest_item_kind,
            ${"en"},
            ${"auto"}::ingestion_pipeline_preset,
            ${"private"}::object_access_level,
            ${{}},
            ${{}}
          )
      `;

      await sql`
        UPDATE ingestions
        SET updated_at = CASE id
          WHEN ${"30000000-0000-4000-8000-000000000001"} THEN now()
          WHEN ${"30000000-0000-4000-8000-000000000002"} THEN date_trunc('week', now()) + interval '1 hour'
          WHEN ${"30000000-0000-4000-8000-000000000003"} THEN now()
          WHEN ${"30000000-0000-4000-8000-000000000004"} THEN now()
        END
        WHERE id IN (
          ${"30000000-0000-4000-8000-000000000001"},
          ${"30000000-0000-4000-8000-000000000002"},
          ${"30000000-0000-4000-8000-000000000003"},
          ${"30000000-0000-4000-8000-000000000004"}
        )
      `;

      await sql`
        INSERT INTO objects (object_id, tenant_id, type, title, metadata, source_ingestion_id, availability_state)
        VALUES
          (
            ${"OBJ-20260210-AAA111"},
            ${"00000000-0000-4000-8000-000000000001"},
            ${"DOCUMENT"}::object_type,
            ${"Object A"},
            ${{}},
            ${"30000000-0000-4000-8000-000000000001"},
            ${"AVAILABLE"}::object_availability_state
          ),
          (
            ${"OBJ-20260210-BBB222"},
            ${"00000000-0000-4000-8000-000000000001"},
            ${"IMAGE"}::object_type,
            ${"Object B"},
            ${{}},
            ${"30000000-0000-4000-8000-000000000002"},
            ${"AVAILABLE"}::object_availability_state
          ),
          (
            ${"OBJ-20260210-CCC333"},
            ${"00000000-0000-4000-8000-000000000002"},
            ${"AUDIO"}::object_type,
            ${"Object C"},
            ${{}},
            ${"30000000-0000-4000-8000-000000000004"},
            ${"AVAILABLE"}::object_availability_state
          )
      `;

      await sql`
        INSERT INTO object_events (id, event_id, tenant_id, type, ingestion_id, object_id, payload, actor_user_id, created_at)
        VALUES
          (
            ${"40000000-0000-4000-8000-000000000001"},
            ${"41000000-0000-4000-8000-000000000001"},
            ${"00000000-0000-4000-8000-000000000001"},
            ${"INGESTION_COMPLETED"}::object_event_type,
            ${"30000000-0000-4000-8000-000000000001"},
            NULL,
            ${{}},
            ${"10000000-0000-4000-8000-000000000001"},
            now()
          ),
          (
            ${"40000000-0000-4000-8000-000000000002"},
            ${"41000000-0000-4000-8000-000000000002"},
            ${"00000000-0000-4000-8000-000000000001"},
            ${"OBJECT_CREATED"}::object_event_type,
            ${"30000000-0000-4000-8000-000000000002"},
            ${"OBJ-20260210-BBB222"},
            ${{}},
            ${"10000000-0000-4000-8000-000000000001"},
            now() - interval '1 minute'
          ),
          (
            ${"40000000-0000-4000-8000-000000000003"},
            ${"41000000-0000-4000-8000-000000000003"},
            ${"00000000-0000-4000-8000-000000000001"},
            ${"FILE_VALIDATED"}::object_event_type,
            ${"30000000-0000-4000-8000-000000000003"},
            NULL,
            ${{}},
            ${"10000000-0000-4000-8000-000000000001"},
            now() - interval '2 minute'
          ),
          (
            ${"40000000-0000-4000-8000-000000000004"},
            ${"41000000-0000-4000-8000-000000000004"},
            ${"00000000-0000-4000-8000-000000000002"},
            ${"INGESTION_COMPLETED"}::object_event_type,
            ${"30000000-0000-4000-8000-000000000004"},
            NULL,
            ${{}},
            ${"10000000-0000-4000-8000-000000000001"},
            now()
          )
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
          username: "viewer@osimi.local",
          password: "viewer123",
        }),
      }),
    );
    const loginBody = (await loginResponse.json()) as { token: string };
    viewerToken = loginBody.token;
  });

  afterAll(async () => {
    if (TEST_DATABASE_URL && schema) {
      const sql = createSqlClient(TEST_DATABASE_URL!);

      try {
        await sql`DROP SCHEMA IF EXISTS ${sqlIdentifier(schema)} CASCADE`;
      } finally {
        await sql.close();
      }
    }

  });

  test("returns tenant-scoped dashboard summary", async () => {
    const app = createTestApp();
    const response = await app.fetch(
      new Request("http://localhost/api/dashboard/summary", {
        method: "GET",
        headers: {
          authorization: `Bearer ${viewerToken}`,
        },
      }),
    );

    expect(response.status).toBe(200);

    const body = (await response.json()) as {
      summary: {
        total_ingestions: number;
        total_objects: number;
        processed_today: number;
        processed_week: number;
        failed_count: number;
      };
    };

    expect(body.summary.total_ingestions).toBe(3);
    expect(body.summary.total_objects).toBe(2);
    expect(body.summary.failed_count).toBe(1);
    expect(body.summary.processed_today).toBeGreaterThanOrEqual(1);
    expect(body.summary.processed_week).toBeGreaterThanOrEqual(
      body.summary.processed_today,
    );
  });

  test("returns tenant-scoped activity feed with cursor pagination", async () => {
    const app = createTestApp();

    const firstPageResponse = await app.fetch(
      new Request("http://localhost/api/dashboard/activity?limit=2", {
        method: "GET",
        headers: {
          authorization: `Bearer ${viewerToken}`,
        },
      }),
    );

    expect(firstPageResponse.status).toBe(200);
    const firstPage = (await firstPageResponse.json()) as {
      activity: Array<{ id: string; created_at: string }>;
      next_cursor: string | null;
    };

    expect(firstPage.activity.length).toBe(2);
    expect(firstPage.next_cursor).not.toBeNull();
    expect(
      new Date(firstPage.activity[0]!.created_at).getTime(),
    ).toBeGreaterThanOrEqual(
      new Date(firstPage.activity[1]!.created_at).getTime(),
    );

    const secondPageResponse = await app.fetch(
      new Request(
        `http://localhost/api/dashboard/activity?limit=2&cursor=${firstPage.next_cursor!}`,
        {
          method: "GET",
          headers: {
            authorization: `Bearer ${viewerToken}`,
          },
        },
      ),
    );

    expect(secondPageResponse.status).toBe(200);
    const secondPage = (await secondPageResponse.json()) as {
      activity: Array<{ id: string }>;
      next_cursor: string | null;
    };

    expect(secondPage.activity.length).toBe(1);
    expect(secondPage.next_cursor).toBeNull();
  });

  test("filters activity by ingestion id", async () => {
    const app = createTestApp();

    const response = await app.fetch(
      new Request(
        "http://localhost/api/dashboard/activity?ingestion_id=30000000-0000-4000-8000-000000000002",
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
      activity: Array<{ ingestion_id: string | null; event_id: string }>;
      next_cursor: string | null;
    };

    expect(body.activity.length).toBe(1);
    expect(body.activity[0]?.ingestion_id).toBe(
      "30000000-0000-4000-8000-000000000002",
    );
    expect(body.activity[0]?.event_id).toBe("41000000-0000-4000-8000-000000000002");
    expect(body.next_cursor).toBeNull();
  });

  test("rejects invalid activity cursor", async () => {
    const app = createTestApp();

    const response = await app.fetch(
      new Request("http://localhost/api/dashboard/activity?cursor=not-base64", {
        method: "GET",
        headers: {
          authorization: `Bearer ${viewerToken}`,
        },
      }),
    );

    expect(response.status).toBe(400);
  });
});
