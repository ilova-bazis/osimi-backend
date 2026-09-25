import { describe, expect, test } from "bun:test";

import { createRateLimiter } from "../../../src/auth/rate-limit.ts";
import { requireWorkerAuthentication } from "../../../src/auth/worker.ts";
import { runWithRuntimeConfig } from "../../../src/runtime/config.ts";

const WORKER_TOKEN = "worker-auth-test-token-000000000001";

function makeRequest(headers: Record<string, string> = {}): Request {
  return new Request("http://worker.test/api/ingestions/lease", {
    method: "POST",
    headers,
  });
}

function createLimiter() {
  return createRateLimiter({
    maxFailures: 3,
    windowMs: 60_000,
    cooldownMs: 60_000,
    maxEntries: 100,
  });
}

function authenticate(
  request: Request,
  rateLimiter = createLimiter(),
): ReturnType<typeof requireWorkerAuthentication> {
  return runWithRuntimeConfig({ workerAuthToken: WORKER_TOKEN }, () =>
    requireWorkerAuthentication(request, {
      rateLimiter,
      requestId: "req-0001",
    }));
}

describe("worker authentication rate limiting", () => {
  test("valid credentials succeed and do not consume the failure budget", () => {
    const limiter = createLimiter();
    const principal = authenticate(makeRequest({
      "x-worker-auth-token": WORKER_TOKEN,
      "x-worker-id": "worker-1",
    }), limiter);

    expect(principal.workerId).toBe("worker-1");
    expect(limiter.size).toBe(0);
  });

  test("missing credentials are rejected without immediately blocking", () => {
    const limiter = createLimiter();
    expect(() => authenticate(makeRequest(), limiter)).toThrow("is required");
    expect(limiter.size).toBe(1);
  });

  test("repeated invalid credentials block with a RATE_LIMITED error", () => {
    const limiter = createLimiter();
    for (let attempt = 0; attempt < 2; attempt += 1) {
      expect(() => authenticate(makeRequest({
        "x-worker-auth-token": "wrong-token",
      }), limiter)).toThrow("Worker authentication token is invalid.");
    }

    let caught: unknown;
    try {
      authenticate(makeRequest({ "x-worker-auth-token": "wrong-token" }), limiter);
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeDefined();
    const error = caught as { status?: number; code?: string; retryAfterSeconds?: number };
    expect(error.status).toBe(429);
    expect(error.code).toBe("RATE_LIMITED");
    expect(error.retryAfterSeconds).toBeGreaterThanOrEqual(1);
  });

  test("valid credentials still succeed while invalid attempts are blocked", () => {
    const limiter = createLimiter();
    for (let attempt = 0; attempt < 3; attempt += 1) {
      try {
        authenticate(makeRequest({ "x-worker-auth-token": "wrong-token" }), limiter);
      } catch {
        // Expected 401/429 failures.
      }
    }

    const principal = authenticate(makeRequest({
      "x-worker-auth-token": WORKER_TOKEN,
    }), limiter);
    expect(principal).toBeDefined();
  });

  test("missing server-side configuration fails before any limiter interaction", () => {
    const limiter = createLimiter();
    expect(() => runWithRuntimeConfig({}, () =>
      requireWorkerAuthentication(makeRequest({ "x-worker-auth-token": "wrong-token" }), {
        rateLimiter: limiter,
      }))).toThrow("WORKER_AUTH_TOKEN");
    expect(limiter.size).toBe(0);
  });
});
