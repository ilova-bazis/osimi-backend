import { createAuthAuditContext, logoutByToken, loginWithPassword } from "../auth/service.ts";
import { requireAuthenticated } from "../auth/guards.ts";
import { jsonResponse } from "../http/response.ts";
import {
  parseJsonBody,
  requireObject,
  requireOptionalStringField,
  requireStringField,
} from "../http/validation.ts";
import type { RouteDefinition } from "./types.ts";

function parseLoginBody(payload: unknown): { username: string; password: string; tenantId?: string } {
  const data = requireObject(payload);
  const username = requireStringField(data, "username");
  const password = requireStringField(data, "password");
  const tenant_id = requireOptionalStringField(data, "tenant_id");

  const normalizedTenant = tenant_id?.trim();

  return {
    username,
    password,
    tenantId: normalizedTenant && normalizedTenant.length > 0 ? normalizedTenant : undefined,
  };
}

const loginRoute: RouteDefinition = {
  method: "POST",
  path: "/api/auth/login",
  handler: async (request, context) => {
    const body = parseLoginBody(await parseJsonBody(request));
    const principal = await loginWithPassword(body, createAuthAuditContext(request, context.requestId));

    return jsonResponse({
      token: principal.sessionToken,
      token_type: "Bearer",
      user: {
        id: principal.userId,
        username: principal.username,
        tenant_id: principal.tenantId,
        role: principal.role,
      },
    });
  },
};

const logoutRoute: RouteDefinition = {
  method: "POST",
  path: "/api/auth/logout",
  handler: async (request, context) => {
    const authenticated = requireAuthenticated(context);
    await logoutByToken(authenticated.authToken, createAuthAuditContext(request, context.requestId));

    return jsonResponse({
      status: "ok",
      request_id: context.requestId,
    });
  },
};

const meRoute: RouteDefinition = {
  method: "GET",
  path: "/api/auth/me",
  handler: async (_request, context) => {
    const authenticated = requireAuthenticated(context);

    return jsonResponse({
      user: {
        id: authenticated.userId,
        username: authenticated.username,
        tenant_id: authenticated.tenantId,
        role: authenticated.role,
      },
    });
  },
};

export const authRoutes: RouteDefinition[] = [loginRoute, logoutRoute, meRoute];
