import { createAuthAuditContext, logoutByToken, loginWithPassword } from "../auth/service.ts";
import { requireAuthenticated } from "../auth/guards.ts";
import { ValidationError } from "../http/errors.ts";
import { jsonResponse } from "../http/response.ts";
import type { RouteDefinition } from "./types.ts";

interface LoginBody {
  username?: unknown;
  password?: unknown;
  tenant_id?: unknown;
}

async function parseJsonBody(request: Request): Promise<unknown> {
  try {
    return await request.json();
  } catch {
    throw new ValidationError("Request body must be valid JSON.");
  }
}

function parseLoginBody(payload: unknown): { username: string; password: string; tenantId?: string } {
  if (payload === null || typeof payload !== "object" || Array.isArray(payload)) {
    throw new ValidationError("Request body must be an object.");
  }

  const { username, password, tenant_id } = payload as LoginBody;

  if (typeof username !== "string") {
    throw new ValidationError("Field 'username' must be a string.");
  }

  if (typeof password !== "string") {
    throw new ValidationError("Field 'password' must be a string.");
  }

  if (tenant_id !== undefined && typeof tenant_id !== "string") {
    throw new ValidationError("Field 'tenant_id' must be a string when provided.");
  }

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
