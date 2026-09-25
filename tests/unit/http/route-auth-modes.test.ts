import { describe, expect, test } from "bun:test";

import { createAppWithOptions } from "../../../src/app.ts";
import { authRoutes } from "../../../src/routes/auth.ts";
import { routes } from "../../../src/routes/index.ts";
import { withWorkerAuth } from "../../../src/routes/middleware.ts";
import type { RouteDefinition } from "../../../src/routes/types.ts";

const UPLOAD_SECRET = "route-auth-upload-signing-secret-000001";
const LEASE_SECRET = "route-auth-lease-signing-secret-0000001";
const WORKER_TOKEN = "route-auth-worker-token-00000000000001";

function createTestApp(routeDefinitions: RouteDefinition[]) {
  return createAppWithOptions({
    runtimeConfig: {
      uploadSigningSecret: UPLOAD_SECRET,
      leaseSigningSecret: LEASE_SECRET,
      workerAuthToken: WORKER_TOKEN,
      workerAuthRateLimit: {
        maxFailures: 2,
        windowMs: 60_000,
        cooldownMs: 60_000,
        maxEntries: 100,
      },
    },
    routeDefinitions,
  });
}

async function errorCode(response: Response): Promise<string | undefined> {
  const body = await response.json() as { error?: { code?: string } };
  return body.error?.code;
}

describe("route authentication modes", () => {
  test("all public signed-token routes explicitly skip session authentication", () => {
    for (const path of [
      "/api/uploads/:token",
      "/api/archive-requests/uploads/:token",
      "/api/object-download-requests/uploads/:token",
      "/api/worker/downloads/:token",
    ]) {
      expect(routes.find((route) => route.path === path)?.auth).toBe("none");
    }
  });

  test("login ignores an incidental invalid Bearer header before body validation", async () => {
    const loginRoute = authRoutes.find((route) => route.path === "/api/auth/login");
    if (!loginRoute) {
      throw new Error("Login route is not registered.");
    }
    const app = createTestApp([loginRoute]);

    for (const url of [
      "http://api.test/api/auth/login?probe=1",
      "http://api.test/api/auth/login/?probe=1",
    ]) {
      const response = await app.fetch(new Request(url, {
        method: "POST",
        headers: {
          authorization: "Bearer invalid",
          "content-type": "application/json",
        },
        body: "{}",
      }));

      expect(response.status).toBe(400);
      expect(await errorCode(response)).toBe("BAD_REQUEST");
    }
  });

  test("worker routes ignore Bearer headers and enforce the worker limiter", async () => {
    const workerRoute: RouteDefinition = {
      method: "POST",
      path: "/api/worker/:id",
      handler: withWorkerAuth(() => new Response("ok")),
    };
    const app = createTestApp([workerRoute]);
    const makeRequest = (workerToken: string) => new Request(
      "http://api.test/api/worker/123/?probe=1",
      {
        method: "POST",
        headers: {
          authorization: "Bearer invalid",
          "x-worker-auth-token": workerToken,
        },
      },
    );

    const first = await app.fetch(makeRequest("wrong-token"));
    expect(first.status).toBe(401);

    const second = await app.fetch(makeRequest("wrong-token"));
    expect(second.status).toBe(429);
    expect(await errorCode(second)).toBe("RATE_LIMITED");

    const valid = await app.fetch(makeRequest(WORKER_TOKEN));
    expect(valid.status).toBe(200);
  });

  test("session-mode routes still reject malformed Bearer credentials", async () => {
    let handlerCalls = 0;
    const app = createTestApp([{
      method: "GET",
      path: "/api/session-route",
      handler: () => {
        handlerCalls += 1;
        return new Response("ok");
      },
    }]);

    const response = await app.fetch(new Request("http://api.test/api/session-route", {
      headers: { authorization: "Bearer invalid" },
    }));

    expect(response.status).toBe(401);
    expect(handlerCalls).toBe(0);
  });
});
