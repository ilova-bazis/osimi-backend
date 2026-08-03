import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { sql as sqlIdentifier } from "bun";

import { createAppWithOptions } from "../../../src/app.ts";
import { createSqlClient } from "../../../src/db/client.ts";
import { runMigrations } from "../../../src/db/migrate.ts";
import { createReadinessService } from "../../../src/services/readiness-service.ts";
import { LifecycleController } from "../../../src/runtime/lifecycle.ts";
import { TEST_DATABASE_URL } from "../test-database.ts";

const runtimeConfig = (schema: string) => ({
  databaseUrl: TEST_DATABASE_URL,
  dbSchema: schema,
  stagingRoot: "/tmp/osimi-health-routes",
  workerAuthToken: "worker-health-secret",
  uploadSigningSecret: "health-upload-signing-secret-000001",
  leaseSigningSecret: "health-lease-signing-secret-000001",
  readinessTimeoutMs: 250,
});

describe("health routes", () => {
  let schema = "";

  beforeAll(async () => {
    schema = `health_${Date.now()}_${Math.floor(Math.random() * 100000)}`;
    await runMigrations({ databaseUrl: TEST_DATABASE_URL, schema });
  });

  afterAll(async () => {
    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`DROP SCHEMA IF EXISTS ${sqlIdentifier(schema)} CASCADE`;
    } finally {
      await sql.close();
    }
  });

  test("keeps liveness independent from auth and readiness dependencies", async () => {
    const app = createAppWithOptions({
      runtimeConfig: {
        ...runtimeConfig(schema),
        databaseUrl: "postgres://postgres:postgres@localhost:1/unavailable",
      },
    });

    const response = await app.fetch(new Request("http://localhost/healthz", {
      headers: { authorization: "Bearer malformed" },
    }));

    expect(response.status).toBe(200);
    expect((await response.json() as { status: string }).status).toBe("ok");
  });

  test("reports a healthy ready state without authenticating probe headers", async () => {
    const app = createAppWithOptions({ runtimeConfig: runtimeConfig(schema) });

    const response = await app.fetch(new Request("http://localhost/readyz", {
      headers: {
        authorization: "Bearer malformed",
        "x-tenant-id": "bad",
        "x-idempotency-key": "bad key",
      },
    }));

    expect(response.status).toBe(200);
    const body = await response.json() as {
      status: string;
      checks: Array<{ name: string; ready: boolean }>;
    };
    expect(body.status).toBe("ready");
    expect(body.checks).toEqual([
      { name: "lifecycle", ready: true },
      { name: "configuration", ready: true },
      { name: "database", ready: true },
      { name: "migrations", ready: true },
    ]);
  });

  test("reports invalid required configuration without exposing secrets", async () => {
    const app = createAppWithOptions({
      runtimeConfig: {
        ...runtimeConfig(schema),
        workerAuthToken: "",
      },
    });

    const response = await app.fetch(new Request("http://localhost/readyz"));

    expect(response.status).toBe(503);
    const body = await response.json() as { status: string; checks: Array<Record<string, unknown>> };
    expect(body.status).toBe("not_ready");
    expect(body.checks.find((check) => check.name === "configuration")).toEqual({
      name: "configuration",
      ready: false,
      reason: "configuration_invalid",
    });
  });

  test("reports an unavailable database as not ready", async () => {
    const app = createAppWithOptions({
      runtimeConfig: {
        ...runtimeConfig(schema),
        databaseUrl: "postgres://postgres:postgres@localhost:1/unavailable",
      },
    });

    const response = await app.fetch(new Request("http://localhost/readyz"));

    expect(response.status).toBe(503);
    const body = await response.json() as { checks: Array<Record<string, unknown>> };
    expect(body.checks.find((check) => check.name === "database")).toEqual({
      name: "database",
      ready: false,
      reason: "database_unavailable",
    });
  });

  test("reports missing migration tracking as not ready", async () => {
    const emptySchema = `health_empty_${Date.now()}_${Math.floor(Math.random() * 100000)}`;
    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`CREATE SCHEMA ${sqlIdentifier(emptySchema)}`;
      const app = createAppWithOptions({ runtimeConfig: runtimeConfig(emptySchema) });

      const response = await app.fetch(new Request("http://localhost/readyz"));

      expect(response.status).toBe(503);
      const body = await response.json() as { checks: Array<Record<string, unknown>> };
      expect(body.checks.find((check) => check.name === "migrations")).toEqual({
        name: "migrations",
        ready: false,
        reason: "migration_tracking_missing",
      });
    } finally {
      await sql`DROP SCHEMA IF EXISTS ${sqlIdentifier(emptySchema)} CASCADE`;
      await sql.close();
    }
  });

  test("reports migration checksum mismatches as not ready", async () => {
    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      const original = await sql<Array<{ name: string; checksum_sha256: string }>>`
        SELECT name, checksum_sha256
        FROM schema_migrations
        ORDER BY name
        LIMIT 1
      `;
      await sql`
        UPDATE schema_migrations
        SET checksum_sha256 = ${"0".repeat(64)}
        WHERE name = (SELECT name FROM schema_migrations ORDER BY name LIMIT 1)
      `;
      const app = createAppWithOptions({ runtimeConfig: runtimeConfig(schema) });

      const response = await app.fetch(new Request("http://localhost/readyz"));

      expect(response.status).toBe(503);
      const body = await response.json() as { checks: Array<Record<string, unknown>> };
      expect(body.checks.find((check) => check.name === "migrations")).toEqual({
        name: "migrations",
        ready: false,
        reason: "migration_checksum_mismatch",
      });

      await sql`
        UPDATE schema_migrations
        SET checksum_sha256 = ${original[0]!.checksum_sha256}
        WHERE name = ${original[0]!.name}
      `;
    } finally {
      await sql.close();
    }
  });

  test("reports applied migrations unknown to this build as not ready", async () => {
    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`
        INSERT INTO schema_migrations (name, checksum_sha256)
        VALUES (${"9999_unknown.sql"}, ${"0".repeat(64)})
      `;
      const app = createAppWithOptions({ runtimeConfig: runtimeConfig(schema) });

      const response = await app.fetch(new Request("http://localhost/readyz"));

      expect(response.status).toBe(503);
      const body = await response.json() as { checks: Array<Record<string, unknown>> };
      expect(body.checks.find((check) => check.name === "migrations")).toEqual({
        name: "migrations",
        ready: false,
        reason: "unknown_migration",
      });
    } finally {
      await sql`DELETE FROM schema_migrations WHERE name = ${"9999_unknown.sql"}`;
      await sql.close();
    }
  });

  test("reports draining and bounded check timeouts as not ready", async () => {
    const lifecycle = new LifecycleController();
    const readiness = createReadinessService({
      lifecycle,
      checkDatabase: async () => {
        await new Promise((resolve) => setTimeout(resolve, 100));
        return [
          { name: "database" as const, ready: true },
          { name: "migrations" as const, ready: true },
        ];
      },
    });
    const app = createAppWithOptions({
      runtimeConfig: {
        ...runtimeConfig(schema),
        readinessTimeoutMs: 50,
      },
      lifecycle,
      readinessService: readiness,
    });

    const timeoutResponse = await app.fetch(new Request("http://localhost/readyz"));
    expect(timeoutResponse.status).toBe(503);
    const timeoutBody = await timeoutResponse.json() as { checks: Array<Record<string, unknown>> };
    expect(timeoutBody.checks.find((check) => check.name === "database")).toEqual({
      name: "database",
      ready: false,
      reason: "check_timed_out",
    });

    lifecycle.beginDrain();
    const drainingResponse = await app.fetch(new Request("http://localhost/readyz"));
    expect(drainingResponse.status).toBe(503);
    const drainingBody = await drainingResponse.json() as { checks: Array<Record<string, unknown>> };
    expect(drainingBody.checks).toEqual([
      { name: "lifecycle", ready: false, reason: "draining" },
    ]);
  });
});
