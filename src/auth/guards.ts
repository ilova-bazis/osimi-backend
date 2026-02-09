import { ForbiddenError, UnauthorizedError } from "../http/errors.ts";
import type { RequestContext } from "../http/context.ts";
import type { UserRole } from "./types.ts";

export interface AuthenticatedContext {
  authToken: string;
  userId: string;
  username: string;
  tenantId: string;
  role: UserRole;
}

export function requireAuthenticated(context: RequestContext): AuthenticatedContext {
  if (!context.authToken || !context.userId || !context.username || !context.tenantId || !context.role) {
    throw new UnauthorizedError("Authentication is required.");
  }

  return {
    authToken: context.authToken,
    userId: context.userId,
    username: context.username,
    tenantId: context.tenantId,
    role: context.role,
  };
}

export function requireRole(context: RequestContext, allowedRoles: readonly UserRole[]): AuthenticatedContext {
  const authenticated = requireAuthenticated(context);

  if (!allowedRoles.includes(authenticated.role)) {
    throw new ForbiddenError("You do not have permission to access this resource.", {
      required_roles: allowedRoles,
      actual_role: authenticated.role,
    });
  }

  return authenticated;
}

export function requireTenantScope(context: RequestContext): string {
  const authenticated = requireAuthenticated(context);
  return authenticated.tenantId;
}
