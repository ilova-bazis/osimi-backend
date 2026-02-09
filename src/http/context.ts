import { ForbiddenError, InternalServerError, ValidationError, createErrorResponse, isAppError } from "./errors.ts";
import { IDEMPOTENCY_KEY_HEADER, parseIdempotencyKey } from "./idempotency.ts";
import { logHttpRequest, logLevelFromStatus } from "./logging.ts";
import { createAuthAuditContext, resolvePrincipalFromRequest } from "../auth/service.ts";
import type { UserRole } from "../auth/types.ts";

export interface RequestContext {
  requestId: string;
  authToken?: string;
  tenantId?: string;
  userId?: string;
  username?: string;
  role?: UserRole;
  idempotencyKey?: string;
  startedAt: Date;
  method: string;
  pathname: string;
}

type ContextHandler = (context: RequestContext) => Response | Promise<Response>;

const REQUEST_ID_HEADER = "x-request-id";
const TENANT_ID_HEADER = "x-tenant-id";

const REQUEST_ID_PATTERN = /^[A-Za-z0-9_-]{8,128}$/;
const TENANT_ID_PATTERN = /^[a-zA-Z0-9-]{8,128}$/;

function normalizeHeaderValue(rawValue: string | null): string | undefined {
  if (rawValue === null) {
    return undefined;
  }

  const normalized = rawValue.trim();
  return normalized.length > 0 ? normalized : undefined;
}

export function resolveRequestId(rawValue: string | null): string {
  const normalized = normalizeHeaderValue(rawValue);

  if (!normalized) {
    return crypto.randomUUID();
  }

  if (!REQUEST_ID_PATTERN.test(normalized)) {
    throw new ValidationError(`Header '${REQUEST_ID_HEADER}' is invalid.`, {
      expected_pattern: REQUEST_ID_PATTERN.source,
    });
  }

  return normalized;
}

function parseTenantId(rawValue: string | undefined): string | undefined {
  if (!rawValue) {
    return undefined;
  }

  if (!TENANT_ID_PATTERN.test(rawValue)) {
    throw new ValidationError(`Header '${TENANT_ID_HEADER}' is invalid.`, {
      expected_pattern: TENANT_ID_PATTERN.source,
    });
  }

  return rawValue;
}

async function buildRequestContext(request: Request, requestId: string): Promise<RequestContext> {
  const tenantHeader = parseTenantId(normalizeHeaderValue(request.headers.get(TENANT_ID_HEADER)));
  const principal = await resolvePrincipalFromRequest(request, createAuthAuditContext(request, requestId));

  if (principal && tenantHeader && tenantHeader !== principal.tenantId) {
    throw new ForbiddenError("Header 'x-tenant-id' does not match the authenticated session tenant.");
  }

  const tenantId = principal?.tenantId ?? tenantHeader;
  const idempotencyKey = parseIdempotencyKey(request.headers.get(IDEMPOTENCY_KEY_HEADER));
  const url = new URL(request.url);

  return {
    requestId,
    authToken: principal?.sessionToken,
    tenantId,
    userId: principal?.userId,
    username: principal?.username,
    role: principal?.role,
    idempotencyKey,
    startedAt: new Date(),
    method: request.method.toUpperCase(),
    pathname: url.pathname,
  };
}

export async function withRequestContext(request: Request, handler: ContextHandler): Promise<Response> {
  const fallbackRequestId: string = crypto.randomUUID();
  let requestId: string = fallbackRequestId;
  const startedAt = performance.now();
  const url = new URL(request.url);
  const method = request.method.toUpperCase();
  const pathname = url.pathname;

  try {
    requestId = resolveRequestId(request.headers.get(REQUEST_ID_HEADER));
  } catch (error) {
    const response = createErrorResponse(error, fallbackRequestId);
    response.headers.set(REQUEST_ID_HEADER, fallbackRequestId);

    const durationMs = Math.round(performance.now() - startedAt);
    logHttpRequest({
      timestamp: new Date().toISOString(),
      level: logLevelFromStatus(response.status),
      event: "http_request",
      request_id: fallbackRequestId,
      method,
      path: pathname,
      status: response.status,
      duration_ms: durationMs,
      error_code: isAppError(error) ? error.code : "INTERNAL_SERVER_ERROR",
      error_message: isAppError(error) ? error.message : "An unexpected error occurred.",
    });

    return response;
  }

  try {
    const context = await buildRequestContext(request, requestId);
    const response = await handler(context);

    if (!(response instanceof Response)) {
      throw new InternalServerError("Route handler did not return a Response object.");
    }

    response.headers.set(REQUEST_ID_HEADER, requestId);

    const durationMs = Math.round(performance.now() - startedAt);
    logHttpRequest({
      timestamp: new Date().toISOString(),
      level: logLevelFromStatus(response.status),
      event: "http_request",
      request_id: requestId,
      method: context.method,
      path: context.pathname,
      status: response.status,
      duration_ms: durationMs,
      tenant_id: context.tenantId,
      user_id: context.userId,
      role: context.role,
      idempotency_key: context.idempotencyKey,
    });

    return response;
  } catch (error) {
    const response = createErrorResponse(error, requestId);
    response.headers.set(REQUEST_ID_HEADER, requestId);

    const durationMs = Math.round(performance.now() - startedAt);
    logHttpRequest({
      timestamp: new Date().toISOString(),
      level: logLevelFromStatus(response.status),
      event: "http_request",
      request_id: requestId,
      method,
      path: pathname,
      status: response.status,
      duration_ms: durationMs,
      error_code: isAppError(error) ? error.code : "INTERNAL_SERVER_ERROR",
      error_message: isAppError(error) ? error.message : "An unexpected error occurred.",
    });

    return response;
  }
}
