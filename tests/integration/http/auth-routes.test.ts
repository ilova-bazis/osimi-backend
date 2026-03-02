import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { sql as sqlIdentifier } from "bun";

import { createAppWithOptions as createApp } from "../../../src/app.ts";
import { createSqlClient } from "../../../src/db/client.ts";
import { runMigrations } from "../../../src/db/migrate.ts";

const TEST_DATABASE_URL = process.env.TEST_DATABASE_URL ?? process.env.DATABASE_URL;

function getJson(response: Response): Promise<any> {
  return response.json() as Promise<any>;
}

describe.skipIf(!TEST_DATABASE_URL)("auth routes", () => {
  let schema = "";

  function createTestApp() {
    return createApp({
      runtimeConfig: {
        databaseUrl: TEST_DATABASE_URL,
        dbSchema: schema,
      },
    });
  }

  beforeAll(async () => {
    schema = `auth_${Date.now()}_${Math.floor(Math.random() * 100000)}`;

    await runMigrations({
      databaseUrl: TEST_DATABASE_URL,
      schema,
    });

    const sql = createSqlClient(TEST_DATABASE_URL);

    try {
      const adminHash = await Bun.password.hash("admin123");
      const operatorHash = await Bun.password.hash("operator123");
      const viewerHash = await Bun.password.hash("viewer123");
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;

      await sql`
        INSERT INTO tenants (id, slug, name)
        VALUES
          (${"00000000-0000-0000-0000-000000000001"}, ${"tenant-one"}, ${"Tenant One"}),
          (${"00000000-0000-0000-0000-000000000002"}, ${"tenant-two"}, ${"Tenant Two"})
      `;

      await sql`
        INSERT INTO users (id, username, username_normalized, password_hash)
        VALUES
          (${"10000000-0000-0000-0000-000000000001"}, ${"admin@osimi.local"}, ${"admin@osimi.local"}, ${adminHash}),
          (${"10000000-0000-0000-0000-000000000002"}, ${"archiver@osimi.local"}, ${"archiver@osimi.local"}, ${operatorHash}),
          (${"10000000-0000-0000-0000-000000000003"}, ${"viewer@osimi.local"}, ${"viewer@osimi.local"}, ${viewerHash})
      `;

      await sql`
        INSERT INTO tenant_memberships (id, tenant_id, user_id, role)
        VALUES
          (${"20000000-0000-0000-0000-000000000001"}, ${"00000000-0000-0000-0000-000000000001"}, ${"10000000-0000-0000-0000-000000000001"}, ${"admin"}),
          (${"20000000-0000-0000-0000-000000000002"}, ${"00000000-0000-0000-0000-000000000001"}, ${"10000000-0000-0000-0000-000000000002"}, ${"archiver"}),
          (${"20000000-0000-0000-0000-000000000003"}, ${"00000000-0000-0000-0000-000000000002"}, ${"10000000-0000-0000-0000-000000000003"}, ${"viewer"})
      `;
    } finally {
      await sql.close();
    }
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

  });

  test("login succeeds and me returns authenticated user", async () => {
    const app = createTestApp();

    const loginResponse = await app.fetch(
      new Request("http://localhost/api/auth/login", {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({
          username: "admin@osimi.local",
          password: "admin123",
        }),
      }),
    );

    expect(loginResponse.status).toBe(200);
    const loginBody = await getJson(loginResponse);
    expect(loginBody.token_type).toBe("Bearer");
    expect(typeof loginBody.token).toBe("string");
    expect(loginBody.user.role).toBe("admin");

    const meResponse = await app.fetch(
      new Request("http://localhost/api/auth/me", {
        method: "GET",
        headers: {
          authorization: `Bearer ${loginBody.token}`,
        },
      }),
    );

    expect(meResponse.status).toBe(200);
    const meBody = await getJson(meResponse);
    expect(meBody.user.username).toBe("admin@osimi.local");
    expect(meBody.user.tenant_id).toBe("00000000-0000-0000-0000-000000000001");
    expect(meBody.user.role).toBe("admin");
  });

  test("login rejects invalid credentials", async () => {
    const app = createTestApp();

    const loginResponse = await app.fetch(
      new Request("http://localhost/api/auth/login", {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({
          username: "admin@osimi.local",
          password: "wrong-password",
        }),
      }),
    );

    expect(loginResponse.status).toBe(401);
    const body = await getJson(loginResponse);
    expect(body.error.code).toBe("UNAUTHORIZED");
  });

  test("me returns 401 without authentication", async () => {
    const app = createTestApp();

    const response = await app.fetch(new Request("http://localhost/api/auth/me", { method: "GET" }));
    expect(response.status).toBe(401);

    const body = await getJson(response);
    expect(body.error.code).toBe("UNAUTHORIZED");
  });

  test("tenant mismatch between header and session returns 403", async () => {
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
    const loginBody = await getJson(loginResponse);

    const meResponse = await app.fetch(
      new Request("http://localhost/api/auth/me", {
        method: "GET",
        headers: {
          authorization: `Bearer ${loginBody.token}`,
          "x-tenant-id": "00000000-0000-0000-0000-000000000002",
        },
      }),
    );

    expect(meResponse.status).toBe(403);
    const meBody = await getJson(meResponse);
    expect(meBody.error.code).toBe("FORBIDDEN");
  });

  test("logout invalidates active session token", async () => {
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

    const loginBody = await getJson(loginResponse);

    const logoutResponse = await app.fetch(
      new Request("http://localhost/api/auth/logout", {
        method: "POST",
        headers: {
          authorization: `Bearer ${loginBody.token}`,
        },
      }),
    );

    expect(logoutResponse.status).toBe(200);

    const meResponse = await app.fetch(
      new Request("http://localhost/api/auth/me", {
        method: "GET",
        headers: {
          authorization: `Bearer ${loginBody.token}`,
        },
      }),
    );

    expect(meResponse.status).toBe(401);
    const meBody = await getJson(meResponse);
    expect(meBody.error.code).toBe("UNAUTHORIZED");
  });

  test("records auth audit events for login and logout", async () => {
    const app = createTestApp();

    const loginResponse = await app.fetch(
      new Request("http://localhost/api/auth/login", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-request-id": "audit_login_req_123",
          "user-agent": "bun-test-agent",
        },
        body: JSON.stringify({
          username: "archiver@osimi.local",
          password: "operator123",
        }),
      }),
    );

    expect(loginResponse.status).toBe(200);
    const loginBody = await getJson(loginResponse);

    const logoutResponse = await app.fetch(
      new Request("http://localhost/api/auth/logout", {
        method: "POST",
        headers: {
          authorization: `Bearer ${loginBody.token}`,
          "x-request-id": "audit_logout_req_456",
          "user-agent": "bun-test-agent",
        },
      }),
    );

    expect(logoutResponse.status).toBe(200);

    const sql = createSqlClient(TEST_DATABASE_URL!);

    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      const rows = await sql<{ request_id: string; event_type: string; success: boolean }[]>`
        SELECT request_id, event_type, success
        FROM auth_audit_events
        WHERE request_id IN (${"audit_login_req_123"}, ${"audit_logout_req_456"})
        ORDER BY created_at ASC
      `;

      expect(rows.length).toBe(2);
      expect(rows[0]).toEqual({
        request_id: "audit_login_req_123",
        event_type: "LOGIN_SUCCEEDED",
        success: true,
      });
      expect(rows[1]).toEqual({
        request_id: "audit_logout_req_456",
        event_type: "LOGOUT_SUCCEEDED",
        success: true,
      });
    } finally {
      await sql.close();
    }
  });
});
