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

describe.skipIf(!TEST_DATABASE_URL)("dashboard routes", () => {
  let schema = "";
  let viewerToken = "";

  let previousDatabaseUrl: string | undefined;
  let previousSchema: string | undefined;

  beforeAll(async () => {
    schema = `dashboard_${Date.now()}_${Math.floor(Math.random() * 100000)}`;

    await runMigrations({
      databaseUrl: TEST_DATABASE_URL,
      schema,
    });

    previousDatabaseUrl = process.env.DATABASE_URL;
    previousSchema = process.env.DB_SCHEMA;

    process.env.DATABASE_URL = TEST_DATABASE_URL;
    process.env.DB_SCHEMA = schema;

    const sql = createSqlClient(TEST_DATABASE_URL!);

    try {
      const tenantsTable = qualifiedTable(schema, "tenants");
      const usersTable = qualifiedTable(schema, "users");
      const membershipsTable = qualifiedTable(schema, "tenant_memberships");
      const ingestionsTable = qualifiedTable(schema, "ingestions");
      const objectsTable = qualifiedTable(schema, "objects");
      const eventsTable = qualifiedTable(schema, "object_events");

      const viewerHash = await Bun.password.hash("viewer123");

      await sql.unsafe(
        `
          INSERT INTO ${tenantsTable} (id, slug, name)
          VALUES
            ($1, $2, $3),
            ($4, $5, $6)
        `,
        [
          "00000000-0000-0000-0000-000000000001",
          "tenant-one",
          "Tenant One",
          "00000000-0000-0000-0000-000000000002",
          "tenant-two",
          "Tenant Two",
        ],
      );

      await sql.unsafe(
        `
          INSERT INTO ${usersTable} (id, username, username_normalized, password_hash)
          VALUES ($1, $2, $3, $4)
        `,
        [
          "10000000-0000-0000-0000-000000000001",
          "viewer@osimi.local",
          "viewer@osimi.local",
          viewerHash,
        ],
      );

      await sql.unsafe(
        `
          INSERT INTO ${membershipsTable} (id, tenant_id, user_id, role)
          VALUES ($1, $2, $3, $4)
        `,
        [
          "20000000-0000-0000-0000-000000000001",
          "00000000-0000-0000-0000-000000000001",
          "10000000-0000-0000-0000-000000000001",
          "viewer",
        ],
      );

      await sql.unsafe(
        `
          INSERT INTO ${ingestionsTable} (id, upload_id, tenant_id, status, created_by, summary, error_summary)
          VALUES
            ($1, $2, $3, 'COMPLETED', $4, '{}'::jsonb, '{}'::jsonb),
            ($5, $6, $7, 'COMPLETED', $8, '{}'::jsonb, '{}'::jsonb),
            ($9, $10, $11, 'FAILED', $12, '{}'::jsonb, '{}'::jsonb),
            ($13, $14, $15, 'COMPLETED', $16, '{}'::jsonb, '{}'::jsonb)
        `,
        [
          "30000000-0000-0000-0000-000000000001",
          "batch-1",
          "00000000-0000-0000-0000-000000000001",
          "10000000-0000-0000-0000-000000000001",
          "30000000-0000-0000-0000-000000000002",
          "batch-2",
          "00000000-0000-0000-0000-000000000001",
          "10000000-0000-0000-0000-000000000001",
          "30000000-0000-0000-0000-000000000003",
          "batch-3",
          "00000000-0000-0000-0000-000000000001",
          "10000000-0000-0000-0000-000000000001",
          "30000000-0000-0000-0000-000000000004",
          "batch-4",
          "00000000-0000-0000-0000-000000000002",
          "10000000-0000-0000-0000-000000000001",
        ],
      );

      await sql.unsafe(
        `
          UPDATE ${ingestionsTable}
          SET updated_at = CASE id
            WHEN $1 THEN now()
            WHEN $2 THEN date_trunc('week', now()) + interval '1 hour'
            WHEN $3 THEN now()
            WHEN $4 THEN now()
          END
          WHERE id IN ($1, $2, $3, $4)
        `,
        [
          "30000000-0000-0000-0000-000000000001",
          "30000000-0000-0000-0000-000000000002",
          "30000000-0000-0000-0000-000000000003",
          "30000000-0000-0000-0000-000000000004",
        ],
      );

      await sql.unsafe(
        `
          INSERT INTO ${objectsTable} (object_id, tenant_id, type, title, metadata, source_ingestion_id, status)
          VALUES
            ($1, $2, 'DOCUMENT', $3, '{}'::jsonb, $4, 'ACTIVE'),
            ($5, $6, 'IMAGE', $7, '{}'::jsonb, $8, 'ACTIVE'),
            ($9, $10, 'AUDIO', $11, '{}'::jsonb, $12, 'ACTIVE')
        `,
        [
          "OBJ-20260210-AAA111",
          "00000000-0000-0000-0000-000000000001",
          "Object A",
          "30000000-0000-0000-0000-000000000001",
          "OBJ-20260210-BBB222",
          "00000000-0000-0000-0000-000000000001",
          "Object B",
          "30000000-0000-0000-0000-000000000002",
          "OBJ-20260210-CCC333",
          "00000000-0000-0000-0000-000000000002",
          "Object C",
          "30000000-0000-0000-0000-000000000004",
        ],
      );

      await sql.unsafe(
        `
          INSERT INTO ${eventsTable} (id, event_id, tenant_id, type, ingestion_id, object_id, payload, actor_user_id, created_at)
          VALUES
            ($1, $2, $3, 'INGESTION_COMPLETED', $4, NULL, '{}'::jsonb, $5, now()),
            ($6, $7, $8, 'OBJECT_CREATED', $9, $10, '{}'::jsonb, $11, now() - interval '1 minute'),
            ($12, $13, $14, 'FILE_VALIDATED', $15, NULL, '{}'::jsonb, $16, now() - interval '2 minute'),
            ($17, $18, $19, 'INGESTION_COMPLETED', $20, NULL, '{}'::jsonb, $21, now())
        `,
        [
          "40000000-0000-0000-0000-000000000001",
          "41000000-0000-0000-0000-000000000001",
          "00000000-0000-0000-0000-000000000001",
          "30000000-0000-0000-0000-000000000001",
          "10000000-0000-0000-0000-000000000001",
          "40000000-0000-0000-0000-000000000002",
          "41000000-0000-0000-0000-000000000002",
          "00000000-0000-0000-0000-000000000001",
          "30000000-0000-0000-0000-000000000002",
          "OBJ-20260210-BBB222",
          "10000000-0000-0000-0000-000000000001",
          "40000000-0000-0000-0000-000000000003",
          "41000000-0000-0000-0000-000000000003",
          "00000000-0000-0000-0000-000000000001",
          "30000000-0000-0000-0000-000000000003",
          "10000000-0000-0000-0000-000000000001",
          "40000000-0000-0000-0000-000000000004",
          "41000000-0000-0000-0000-000000000004",
          "00000000-0000-0000-0000-000000000002",
          "30000000-0000-0000-0000-000000000004",
          "10000000-0000-0000-0000-000000000001",
        ],
      );
    } finally {
      await sql.close();
    }

    const app = createApp();
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
        await sql.unsafe(`DROP SCHEMA IF EXISTS ${quoteIdentifier(schema)} CASCADE`);
      } finally {
        await sql.close();
      }
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
  });

  test("returns tenant-scoped dashboard summary", async () => {
    const app = createApp();
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
    expect(body.summary.processed_week).toBeGreaterThanOrEqual(body.summary.processed_today);
  });

  test("returns tenant-scoped activity feed with cursor pagination", async () => {
    const app = createApp();

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
    expect(new Date(firstPage.activity[0]!.created_at).getTime()).toBeGreaterThanOrEqual(
      new Date(firstPage.activity[1]!.created_at).getTime(),
    );

    const secondPageResponse = await app.fetch(
      new Request(`http://localhost/api/dashboard/activity?limit=2&cursor=${firstPage.next_cursor!}`, {
        method: "GET",
        headers: {
          authorization: `Bearer ${viewerToken}`,
        },
      }),
    );

    expect(secondPageResponse.status).toBe(200);
    const secondPage = (await secondPageResponse.json()) as {
      activity: Array<{ id: string }>;
      next_cursor: string | null;
    };

    expect(secondPage.activity.length).toBe(1);
    expect(secondPage.next_cursor).toBeNull();
  });
});
