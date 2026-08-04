import { createHmac } from "node:crypto";

import { describe, expect, test } from "bun:test";

import { parseLeaseToken } from "../../../src/auth/worker-lease.ts";
import {
  DEFAULT_CORS_ALLOWED_ORIGINS,
  DEFAULT_MAX_UPLOAD_SIZE_BYTES,
  DEFAULT_READINESS_TIMEOUT_MS,
  DEFAULT_SHUTDOWN_GRACE_PERIOD_MS,
  parseCorsAllowedOrigins,
  resolveCorsAllowedOrigins,
  resolveMaxUploadSizeBytes,
  resolveReadinessTimeoutMs,
  resolveShutdownGracePeriodMs,
  runWithRuntimeConfig,
  validateSigningConfiguration,
  validateRuntimeConfiguration,
  validateWorkerConfiguration,
} from "../../../src/runtime/config.ts";
import { startServer } from "../../../src/server.ts";
import { parseUploadToken } from "../../../src/storage/staging.ts";

const UPLOAD_SECRET = "unit-upload-signing-secret-0000000001";
const LEASE_SECRET = "unit-lease-signing-secret-00000000001";

function legacyToken(payload: Record<string, unknown>, secret: string): string {
  const encodedPayload = Buffer.from(JSON.stringify(payload), "utf8").toString("base64url");
  const signature = createHmac("sha256", secret).update(encodedPayload).digest("base64url");
  return `${encodedPayload}.${signature}`;
}

describe("signing configuration", () => {
  test("rejects blank and too-short signing secrets", () => {
    expect(() => validateSigningConfiguration({
      uploadSigningSecret: "",
      leaseSigningSecret: LEASE_SECRET,
    })).toThrow("UPLOAD_SIGNING_SECRET");

    expect(() => validateSigningConfiguration({
      uploadSigningSecret: UPLOAD_SECRET,
      leaseSigningSecret: "too-short",
    })).toThrow("LEASE_SIGNING_SECRET");
  });

  test("refuses server startup without explicit signing configuration", () => {
    expect(() => startServer({
      runtimeConfig: {
        uploadSigningSecret: "",
        leaseSigningSecret: LEASE_SECRET,
      },
    })).toThrow("UPLOAD_SIGNING_SECRET");
  });

  test("rejects upload tokens signed with the removed default secret", () => {
    const token = legacyToken({
      ingestion_id: crypto.randomUUID(),
      file_id: crypto.randomUUID(),
      tenant_id: crypto.randomUUID(),
      storage_key: "tenants/test/ingestions/test/original/file.txt",
      content_type: "text/plain",
      size_bytes: 1,
      expires_at: new Date(Date.now() + 60_000).toISOString(),
    }, "dev-local-signing-secret");

    expect(() => runWithRuntimeConfig({ uploadSigningSecret: UPLOAD_SECRET }, () => {
      parseUploadToken(token);
    })).toThrow("Upload token signature is invalid.");
  });

  test("rejects lease tokens signed with the removed default secret", () => {
    const token = legacyToken({
      lease_id: crypto.randomUUID(),
      lease_token_id: crypto.randomUUID(),
      ingestion_id: crypto.randomUUID(),
      tenant_id: crypto.randomUUID(),
      exp: new Date(Date.now() + 60_000).toISOString(),
    }, "dev-local-lease-secret");

    expect(() => runWithRuntimeConfig({ leaseSigningSecret: LEASE_SECRET }, () => {
      parseLeaseToken(token);
    })).toThrow("Lease token signature is invalid.");
  });

  test("does not accept tokens from another configured signing secret", () => {
    const token = runWithRuntimeConfig({ uploadSigningSecret: UPLOAD_SECRET }, () => {
      const payload = {
        ingestion_id: crypto.randomUUID(),
        file_id: crypto.randomUUID(),
        tenant_id: crypto.randomUUID(),
        storage_key: "tenants/test/ingestions/test/original/file.txt",
        content_type: "text/plain",
        size_bytes: 1,
        expires_at: new Date(Date.now() + 60_000).toISOString(),
      };
      return legacyToken(payload, UPLOAD_SECRET);
    });

    expect(() => runWithRuntimeConfig({
      uploadSigningSecret: "another-upload-signing-secret-000000",
    }, () => {
      parseUploadToken(token);
    })).toThrow("Upload token signature is invalid.");
  });
});

describe("upload size configuration", () => {
  test("uses a 2 GiB default and accepts a runtime override", () => {
    expect(resolveMaxUploadSizeBytes({})).toBe(DEFAULT_MAX_UPLOAD_SIZE_BYTES);
    expect(resolveMaxUploadSizeBytes({ maxUploadSizeBytes: 1234 })).toBe(1234);
  });

  test("rejects invalid runtime upload limits", () => {
    for (const maxUploadSizeBytes of [0, -1, 1.5, Number.POSITIVE_INFINITY]) {
      expect(() => resolveMaxUploadSizeBytes({ maxUploadSizeBytes })).toThrow(
        "Runtime upload size limit",
      );
    }
  });
});

describe("CORS origin configuration", () => {
  test("uses localhost defaults and accepts runtime overrides", () => {
    expect(DEFAULT_CORS_ALLOWED_ORIGINS).toEqual([
      "http://localhost:4444",
      "http://localhost:5173",
    ]);
    expect(resolveCorsAllowedOrigins({
      corsAllowedOrigins: ["https://archive.example"],
    })).toEqual(["https://archive.example"]);
    expect(resolveCorsAllowedOrigins({ corsAllowedOrigins: [] })).toEqual([]);
  });

  test("normalizes comma-separated origins", () => {
    expect(parseCorsAllowedOrigins(
      " https://archive.example/ , http://localhost:4444,https://archive.example ",
    )).toEqual([
      "https://archive.example",
      "http://localhost:4444",
    ]);
    expect(parseCorsAllowedOrigins("")).toEqual([]);
  });

  test("rejects malformed CORS origins", () => {
    for (const value of [
      "*",
      "archive.example",
      "file:///tmp/ui",
      "https://user:pass@archive.example",
      "https://archive.example/ui",
      "https://archive.example?tenant=one",
      "https://archive.example#upload",
      "https://archive.example,,https://admin.example",
    ]) {
      expect(() => parseCorsAllowedOrigins(value)).toThrow("CORS_ALLOWED_ORIGINS");
    }

    expect(() => validateRuntimeConfiguration({
      uploadSigningSecret: UPLOAD_SECRET,
      leaseSigningSecret: LEASE_SECRET,
      corsAllowedOrigins: ["https://archive.example/ui"],
    })).toThrow("Runtime CORS allowed origins");
  });
});

describe("readiness configuration", () => {
  test("uses the default readiness timeout and accepts a runtime override", () => {
    expect(resolveReadinessTimeoutMs({})).toBe(DEFAULT_READINESS_TIMEOUT_MS);
    expect(resolveReadinessTimeoutMs({ readinessTimeoutMs: 250 })).toBe(250);
  });

  test("rejects invalid readiness timeout and missing worker authentication", () => {
    expect(() => resolveReadinessTimeoutMs({ readinessTimeoutMs: 0 })).toThrow(
      "READINESS_TIMEOUT_MS",
    );
    expect(() => validateWorkerConfiguration({ workerAuthToken: "" })).toThrow(
      "WORKER_AUTH_TOKEN",
    );
  });
});

describe("shutdown configuration", () => {
  test("uses a 60 second default and accepts a runtime override", () => {
    expect(resolveShutdownGracePeriodMs({})).toBe(DEFAULT_SHUTDOWN_GRACE_PERIOD_MS);
    expect(resolveShutdownGracePeriodMs({ shutdownGracePeriodMs: 250 })).toBe(250);
  });

  test("rejects invalid shutdown grace periods", () => {
    expect(() => resolveShutdownGracePeriodMs({ shutdownGracePeriodMs: 0 })).toThrow(
      "SHUTDOWN_GRACE_PERIOD_MS",
    );
  });
});
