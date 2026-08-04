import { describe, expect, test } from "bun:test";

import { createAppWithOptions } from "../../../src/app.ts";
import type { RouteDefinition } from "../../../src/routes/types.ts";

const UPLOAD_SECRET = "cors-upload-signing-secret-0000000001";
const LEASE_SECRET = "cors-lease-signing-secret-00000000001";

function createTestApp(origins: readonly string[] = ["https://archive.example"]) {
  let putCalls = 0;
  const routeDefinitions: RouteDefinition[] = [{
    method: "PUT",
    path: "/api/uploads/:token",
    handler: () => {
      putCalls += 1;
      return new Response("uploaded", { headers: { vary: "Accept-Encoding" } });
    },
  }];

  return {
    app: createAppWithOptions({
      runtimeConfig: {
        uploadSigningSecret: UPLOAD_SECRET,
        leaseSigningSecret: LEASE_SECRET,
        corsAllowedOrigins: origins,
      },
      routeDefinitions,
    }),
    getPutCalls: () => putCalls,
  };
}

describe("CORS", () => {
  test("grants a configured production origin a direct-upload preflight", async () => {
    const { app } = createTestApp();

    const response = await app.fetch(new Request("http://api.test/api/uploads/token", {
      method: "OPTIONS",
      headers: {
        origin: "https://archive.example",
        "access-control-request-method": "PUT",
        "access-control-request-headers": "content-type",
      },
    }));

    expect(response.status).toBe(204);
    expect(response.headers.get("access-control-allow-origin")).toBe("https://archive.example");
    expect(response.headers.get("access-control-allow-methods")).toContain("PUT");
    expect(response.headers.get("access-control-allow-headers")).toContain("content-type");
    expect(response.headers.get("access-control-max-age")).toBe("600");
    expect(response.headers.get("access-control-allow-credentials")).toBeNull();
    expect(response.headers.get("vary")).toBe("Origin");
  });

  test("applies CORS headers and merges Vary on actual and error PUT responses", async () => {
    const { app, getPutCalls } = createTestApp();
    const response = await app.fetch(new Request("http://api.test/api/uploads/token", {
      method: "PUT",
      headers: { origin: "https://archive.example" },
    }));

    expect(response.status).toBe(200);
    expect(getPutCalls()).toBe(1);
    expect(response.headers.get("access-control-allow-origin")).toBe("https://archive.example");
    expect(response.headers.get("access-control-allow-credentials")).toBeNull();
    expect(response.headers.get("vary")).toBe("Accept-Encoding, Origin");

    const errorApp = createAppWithOptions({
      runtimeConfig: {
        uploadSigningSecret: UPLOAD_SECRET,
        leaseSigningSecret: LEASE_SECRET,
        corsAllowedOrigins: ["https://archive.example"],
      },
      routeDefinitions: [{
        method: "PUT",
        path: "/api/uploads/:token",
        handler: () => {
          throw new Error("upload failed");
        },
      }],
    });
    const errorResponse = await errorApp.fetch(new Request("http://api.test/api/uploads/token", {
      method: "PUT",
      headers: { origin: "https://archive.example" },
    }));

    expect(errorResponse.status).toBe(500);
    expect(errorResponse.headers.get("access-control-allow-origin")).toBe("https://archive.example");
    expect(errorResponse.headers.get("vary")).toBe("Origin");
  });

  test("does not grant unapproved origins or requests without an Origin header", async () => {
    const { app, getPutCalls } = createTestApp();
    const rejectedPreflight = await app.fetch(new Request("http://api.test/api/uploads/token", {
      method: "OPTIONS",
      headers: { origin: "https://unapproved.example" },
    }));

    expect(rejectedPreflight.status).toBe(204);
    expect(rejectedPreflight.headers.get("access-control-allow-origin")).toBeNull();
    expect(rejectedPreflight.headers.get("access-control-allow-methods")).toBeNull();
    expect(rejectedPreflight.headers.get("vary")).toBe("Origin");

    const unapprovedResponse = await app.fetch(new Request("http://api.test/api/uploads/token", {
      method: "PUT",
      headers: { origin: "https://unapproved.example" },
    }));
    expect(unapprovedResponse.status).toBe(200);
    expect(unapprovedResponse.headers.get("access-control-allow-origin")).toBeNull();
    expect(unapprovedResponse.headers.get("vary")).toBe("Accept-Encoding, Origin");

    const noOriginResponse = await app.fetch(new Request("http://api.test/api/uploads/token", {
      method: "PUT",
    }));
    expect(noOriginResponse.status).toBe(200);
    expect(noOriginResponse.headers.get("access-control-allow-origin")).toBeNull();
    expect(noOriginResponse.headers.get("vary")).toBe("Accept-Encoding");
    expect(getPutCalls()).toBe(2);
  });

  test("accepts each configured origin and supports an explicit deny-all list", async () => {
    const { app } = createTestApp([
      "https://archive.example",
      "https://admin.archive.example",
    ]);

    for (const origin of ["https://archive.example", "https://admin.archive.example"]) {
      const response = await app.fetch(new Request("http://api.test/api/uploads/token", {
        method: "OPTIONS",
        headers: { origin },
      }));
      expect(response.headers.get("access-control-allow-origin")).toBe(origin);
    }

    const { app: denyAllApp } = createTestApp([]);
    const deniedResponse = await denyAllApp.fetch(new Request("http://api.test/api/uploads/token", {
      method: "OPTIONS",
      headers: { origin: "https://archive.example" },
    }));
    expect(deniedResponse.headers.get("access-control-allow-origin")).toBeNull();
  });
});
