import { createHmac } from "node:crypto";

import { describe, expect, test } from "bun:test";

import { parseLeaseToken } from "../../../src/auth/worker-lease.ts";
import {
  runWithRuntimeConfig,
  validateSigningConfiguration,
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
