import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { sql as sqlIdentifier } from "bun";

import { createAppWithOptions as createApp } from "../../../src/app.ts";
import { createSqlClient } from "../../../src/db/client.ts";
import { runMigrations } from "../../../src/db/migrate.ts";
import type { RuntimeConfig } from "../../../src/runtime/config.ts";
import { TEST_DATABASE_URL } from "../test-database.ts";

function getJson(response: Response): Promise<any> {
  return response.json() as Promise<any>;
}

describe("auth routes", () => {
  let schema = "";

  function createTestApp(runtimeConfig: Partial<RuntimeConfig> = {}) {
    return createApp({
      runtimeConfig: {
        databaseUrl: TEST_DATABASE_URL,
        dbSchema: schema,
        uploadSigningSecret: "auth-routes-upload-signing-secret-0001",
        leaseSigningSecret: "auth-routes-lease-signing-secret-00001",
        ...runtimeConfig,
      },
    });
  }

  function makeLoginRequest(
    username: string,
    password: string,
    tenantId?: string,
    requestId?: string,
    authorization?: string,
  ): Request {
    return new Request("http://localhost/api/auth/login", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...(requestId ? { "x-request-id": requestId } : {}),
        ...(authorization ? { authorization } : {}),
      },
      body: JSON.stringify({
        username,
        password,
        ...(tenantId ? { tenant_id: tenantId } : {}),
      }),
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
      const multiHash = await Bun.password.hash("multi123");
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
          (${"10000000-0000-0000-0000-000000000003"}, ${"viewer@osimi.local"}, ${"viewer@osimi.local"}, ${viewerHash}),
          (${"10000000-0000-0000-0000-000000000004"}, ${"multi@osimi.local"}, ${"multi@osimi.local"}, ${multiHash})
      `;

      await sql`
        INSERT INTO tenant_memberships (id, tenant_id, user_id, role)
        VALUES
          (${"20000000-0000-0000-0000-000000000001"}, ${"00000000-0000-0000-0000-000000000001"}, ${"10000000-0000-0000-0000-000000000001"}, ${"admin"}),
          (${"20000000-0000-0000-0000-000000000002"}, ${"00000000-0000-0000-0000-000000000001"}, ${"10000000-0000-0000-0000-000000000002"}, ${"archiver"}),
          (${"20000000-0000-0000-0000-000000000003"}, ${"00000000-0000-0000-0000-000000000002"}, ${"10000000-0000-0000-0000-000000000003"}, ${"viewer"}),
          (${"20000000-0000-0000-0000-000000000004"}, ${"00000000-0000-0000-0000-000000000001"}, ${"10000000-0000-0000-0000-000000000004"}, ${"archiver"}),
          (${"20000000-0000-0000-0000-000000000005"}, ${"00000000-0000-0000-0000-000000000002"}, ${"10000000-0000-0000-0000-000000000004"}, ${"viewer"})
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

  test("throttles repeated failed logins and recovers after the cooldown", async () => {
    const app = createTestApp({
      loginRateLimit: { maxFailures: 2, windowMs: 60_000, cooldownMs: 1_000, maxEntries: 100 },
    });

    const first = await app.fetch(makeLoginRequest("admin@osimi.local", "wrong-password"));
    expect(first.status).toBe(401);

    const second = await app.fetch(makeLoginRequest("admin@osimi.local", "wrong-password"));
    expect(second.status).toBe(429);
    const blockedBody = await getJson(second);
    expect(blockedBody.error.code).toBe("RATE_LIMITED");
    expect(Number(second.headers.get("retry-after"))).toBeGreaterThanOrEqual(1);

    const stillBlocked = await app.fetch(makeLoginRequest("admin@osimi.local", "wrong-password"));
    expect(stillBlocked.status).toBe(429);

    await new Promise((resolve) => setTimeout(resolve, 1_100));

    const recovered = await app.fetch(makeLoginRequest("admin@osimi.local", "admin123"));
    expect(recovered.status).toBe(200);
  });

  test("an invalid Bearer header cannot bypass the login limiter", async () => {
    const app = createTestApp({
      loginRateLimit: { maxFailures: 2, windowMs: 60_000, cooldownMs: 60_000, maxEntries: 100 },
    });
    const invalidBearer = "Bearer invalid-session-token-000000000000";

    const first = await app.fetch(makeLoginRequest(
      "admin@osimi.local",
      "wrong-password",
      undefined,
      undefined,
      invalidBearer,
    ));
    expect(first.status).toBe(401);

    const second = await app.fetch(makeLoginRequest(
      "admin@osimi.local",
      "wrong-password",
      undefined,
      undefined,
      invalidBearer,
    ));
    expect(second.status).toBe(429);
    expect((await getJson(second)).error.code).toBe("RATE_LIMITED");
  });

  test("a successful login clears previous failures", async () => {
    const app = createTestApp({
      loginRateLimit: { maxFailures: 2, windowMs: 60_000, cooldownMs: 60_000, maxEntries: 100 },
    });

    expect((await app.fetch(makeLoginRequest("admin@osimi.local", "wrong-password"))).status).toBe(401);
    expect((await app.fetch(makeLoginRequest("admin@osimi.local", "admin123"))).status).toBe(200);

    expect((await app.fetch(makeLoginRequest("admin@osimi.local", "wrong-password"))).status).toBe(401);
    expect((await app.fetch(makeLoginRequest("admin@osimi.local", "wrong-password"))).status).toBe(429);
  });

  test("a blocked login query times out without counting a credential failure and later logins recover", async () => {
    const app = createTestApp({
      loginDependencyTimeoutMs: 50,
      loginRateLimit: { maxFailures: 2, windowMs: 60_000, cooldownMs: 60_000, maxEntries: 100 },
    });
    const blockerPool = createSqlClient(TEST_DATABASE_URL!);
    const blocker = await blockerPool.reserve();

    try {
      await blocker`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await blocker`BEGIN`;
      await blocker`LOCK TABLE users IN ACCESS EXCLUSIVE MODE`;

      const timedOut = await app.fetch(makeLoginRequest(
        "admin@osimi.local",
        "admin123",
        undefined,
        "timeout_login_req_123",
      ));
      expect(timedOut.status).toBe(503);
      const body = await getJson(timedOut);
      expect(body.error.code).toBe("DEPENDENCY_TIMEOUT");
    } finally {
      await blocker`ROLLBACK`;
      blocker.release();
      await blockerPool.close();
    }

    const firstCredentialFailure = await app.fetch(
      makeLoginRequest("admin@osimi.local", "wrong-password"),
    );
    expect(firstCredentialFailure.status).toBe(401);

    const recovered = await app.fetch(makeLoginRequest("admin@osimi.local", "admin123"));
    expect(recovered.status).toBe(200);

    const sql = createSqlClient(TEST_DATABASE_URL!);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      const rows = await sql<Array<{ count: number }>>`
        SELECT count(*)::int AS count
        FROM auth_audit_events
        WHERE request_id = ${"timeout_login_req_123"}
          AND event_type = ${"LOGIN_FAILED"}::auth_audit_event_type
      `;
      expect(rows[0]?.count).toBe(0);
    } finally {
      await sql.close();
    }
  });

  test("throttles unknown usernames and does not allow tenant variation to bypass", async () => {
    const app = createTestApp({
      loginRateLimit: { maxFailures: 2, windowMs: 60_000, cooldownMs: 60_000, maxEntries: 100 },
    });

    expect((await app.fetch(makeLoginRequest("nobody@osimi.local", "wrong-password"))).status).toBe(401);
    expect((await app.fetch(makeLoginRequest("nobody@osimi.local", "wrong-password"))).status).toBe(429);

    const withTenant = await app.fetch(
      makeLoginRequest("nobody@osimi.local", "wrong-password", "00000000-0000-0000-0000-000000000001"),
    );
    expect(withTenant.status).toBe(429);
  });

  test("records the blocking login failure as a rate-limited audit event", async () => {
    const app = createTestApp({
      loginRateLimit: { maxFailures: 2, windowMs: 60_000, cooldownMs: 60_000, maxEntries: 100 },
    });

    await app.fetch(makeLoginRequest("viewer@osimi.local", "wrong-password"));
    await app.fetch(makeLoginRequest("viewer@osimi.local", "wrong-password"));

    const sql = createSqlClient(TEST_DATABASE_URL!);

    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      const rows = await sql<{ error_code: string; reason: string | null }[]>`
        SELECT error_code, payload ->> 'reason' AS reason
        FROM auth_audit_events
        WHERE username_normalized = ${"viewer@osimi.local"}
          AND event_type = ${"LOGIN_FAILED"}::auth_audit_event_type
          AND error_code = ${"RATE_LIMITED"}
      `;

      expect(rows.length).toBe(1);
      expect(rows[0]?.reason).toBe("rate_limited");
    } finally {
      await sql.close();
    }
  });

  test("concurrent failed logins respect the admission budget", async () => {
    const app = createTestApp({
      loginRateLimit: { maxFailures: 2, windowMs: 60_000, cooldownMs: 60_000, maxEntries: 100 },
    });

    const responses = await Promise.all(
      Array.from({ length: 6 }, () => app.fetch(makeLoginRequest("burst@osimi.local", "wrong-password"))),
    );

    const statuses = responses.map((response) => response.status);
    expect(statuses.filter((status) => status === 401).length).toBeLessThanOrEqual(2);
    expect(statuses.filter((status) => status === 429).length).toBeGreaterThanOrEqual(4);

    const sql = createSqlClient(TEST_DATABASE_URL!);

    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      const rows = await sql<{ count: number }[]>`
        SELECT count(*)::int AS count
        FROM auth_audit_events
        WHERE username_normalized = ${"burst@osimi.local"}
          AND event_type = ${"LOGIN_FAILED"}::auth_audit_event_type
      `;

      expect(rows[0]?.count ?? 0).toBe(2);
    } finally {
      await sql.close();
    }
  });

  test("throttles repeated attempts that omit a required tenant_id", async () => {
    const app = createTestApp({
      loginRateLimit: { maxFailures: 2, windowMs: 60_000, cooldownMs: 60_000, maxEntries: 100 },
    });

    const first = await app.fetch(makeLoginRequest("multi@osimi.local", "whatever-password"));
    expect(first.status).toBe(400);

    const second = await app.fetch(makeLoginRequest("multi@osimi.local", "whatever-password"));
    expect(second.status).toBe(429);

    const third = await app.fetch(makeLoginRequest("multi@osimi.local", "whatever-password"));
    expect(third.status).toBe(429);
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
