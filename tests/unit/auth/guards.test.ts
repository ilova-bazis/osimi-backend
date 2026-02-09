import { describe, expect, test } from "bun:test";

import { requireAuthenticated, requireRole } from "../../../src/auth/guards.ts";
import type { RequestContext } from "../../../src/http/context.ts";

function createContext(overrides: Partial<RequestContext> = {}): RequestContext {
  return {
    requestId: crypto.randomUUID(),
    startedAt: new Date(),
    method: "GET",
    pathname: "/",
    ...overrides,
  };
}

describe("auth guards", () => {
  test("requireAuthenticated returns principal when context is authenticated", () => {
    const context = createContext({
      authToken: "token-1234567890123456",
      userId: "10000000-0000-0000-0000-000000000001",
      username: "admin@osimi.local",
      tenantId: "00000000-0000-0000-0000-000000000001",
      role: "admin",
    });

    const principal = requireAuthenticated(context);

    expect(principal.userId).toBe("10000000-0000-0000-0000-000000000001");
    expect(principal.role).toBe("admin");
  });

  test("requireAuthenticated throws when context has no session", () => {
    const context = createContext();

    expect(() => requireAuthenticated(context)).toThrow("Authentication is required.");
  });

  test("requireRole accepts allowed role", () => {
    const context = createContext({
      authToken: "token-1234567890123456",
      userId: "10000000-0000-0000-0000-000000000002",
      username: "operator@osimi.local",
      tenantId: "00000000-0000-0000-0000-000000000001",
      role: "operator",
    });

    const principal = requireRole(context, ["operator", "admin"]);
    expect(principal.role).toBe("operator");
  });

  test("requireRole rejects disallowed role", () => {
    const context = createContext({
      authToken: "token-1234567890123456",
      userId: "10000000-0000-0000-0000-000000000003",
      username: "viewer@osimi.local",
      tenantId: "00000000-0000-0000-0000-000000000002",
      role: "viewer",
    });

    expect(() => requireRole(context, ["admin"])).toThrow(
      "You do not have permission to access this resource.",
    );
  });
});
