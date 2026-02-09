import { UnauthorizedError, ValidationError } from "../http/errors.ts";
import {
  createSession,
  findActiveSessionByTokenHash,
  findLoginCandidates,
  insertAuthAuditEvent,
  revokeSessionByTokenHash,
  touchSessionLastSeenAt,
  updateUserLastLoginAt,
} from "../repos/auth-repo.ts";
import type { AuthenticatedPrincipal } from "./types.ts";

const AUTHORIZATION_HEADER = "authorization";
const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

const DEFAULT_SESSION_TTL_SECONDS = 60 * 60 * 24;

interface LoginInput {
  username: string;
  password: string;
  tenantId?: string;
}

export interface AuthAuditContext {
  requestId: string;
  ip?: string;
  userAgent?: string;
}

function generateSessionToken(): string {
  return `${crypto.randomUUID()}${crypto.randomUUID().replaceAll("-", "")}`;
}

function hashSessionToken(token: string): string {
  return new Bun.CryptoHasher("sha256").update(token).digest("hex");
}

function resolveSessionTtlSeconds(): number {
  const rawValue = process.env.SESSION_TTL_SECONDS;

  if (!rawValue) {
    return DEFAULT_SESSION_TTL_SECONDS;
  }

  const parsed = Number.parseInt(rawValue, 10);

  if (
    !Number.isFinite(parsed) ||
    Number.isNaN(parsed) ||
    parsed < 60 ||
    parsed > 60 * 60 * 24 * 30
  ) {
    throw new ValidationError(
      "Environment variable 'SESSION_TTL_SECONDS' is invalid.",
    );
  }

  return parsed;
}

function computeSessionExpiry(now: Date): Date {
  return new Date(now.getTime() + resolveSessionTtlSeconds() * 1000);
}

function toPrincipal(params: {
  token: string;
  userId: string;
  username: string;
  tenantId: string;
  role: AuthenticatedPrincipal["role"];
  createdAt: Date;
  lastSeenAt: Date;
}): AuthenticatedPrincipal {
  return {
    sessionToken: params.token,
    userId: params.userId,
    username: params.username,
    tenantId: params.tenantId,
    role: params.role,
    createdAt: params.createdAt,
    lastSeenAt: params.lastSeenAt,
  };
}

function parseBearerToken(headerValue: string | null): string | undefined {
  if (headerValue === null) {
    return undefined;
  }

  const trimmed = headerValue.trim();
  if (trimmed.length === 0) {
    throw new UnauthorizedError("Authorization header cannot be empty.");
  }

  const [scheme, token, ...rest] = trimmed.split(/\s+/);
  if (
    rest.length > 0 ||
    !scheme ||
    !token ||
    scheme.toLowerCase() !== "bearer"
  ) {
    throw new UnauthorizedError(
      "Authorization header must use Bearer token format.",
    );
  }

  if (token.length < 16) {
    throw new UnauthorizedError("Authorization token is invalid.");
  }

  return token;
}

function normalizeClientIp(rawIp: string | null): string | undefined {
  if (!rawIp) {
    return undefined;
  }

  const first = rawIp.split(",")[0]?.trim();
  return first && first.length > 0 ? first : undefined;
}

function maybeUuid(value: string | undefined): string | undefined {
  if (!value) {
    return undefined;
  }

  return UUID_PATTERN.test(value) ? value : undefined;
}

export function createAuthAuditContext(
  request: Request,
  requestId: string,
): AuthAuditContext {
  return {
    requestId,
    ip: normalizeClientIp(request.headers.get("x-forwarded-for")),
    userAgent: request.headers.get("user-agent") ?? undefined,
  };
}

async function safeRecordAuthAuditEvent(params: {
  context?: AuthAuditContext;
  eventType:
    | "LOGIN_SUCCEEDED"
    | "LOGIN_FAILED"
    | "SESSION_REJECTED"
    | "LOGOUT_SUCCEEDED"
    | "LOGOUT_FAILED";
  success: boolean;
  tenantId?: string;
  userId?: string;
  sessionId?: string;
  usernameNormalized?: string;
  errorCode?: string;
  payload?: Record<string, unknown>;
}): Promise<void> {
  if (!params.context) {
    return;
  }

  try {
    await insertAuthAuditEvent({
      requestId: params.context.requestId,
      eventType: params.eventType,
      success: params.success,
      tenantId: maybeUuid(params.tenantId),
      userId: maybeUuid(params.userId),
      sessionId: maybeUuid(params.sessionId),
      usernameNormalized: params.usernameNormalized,
      errorCode: params.errorCode,
      ip: params.context.ip,
      userAgent: params.context.userAgent,
      payload: params.payload,
    });
  } catch (error) {
    console.error(
      `[auth-audit:${params.context.requestId}] failed to persist auth audit event`,
      error,
    );
  }
}

export async function loginWithPassword(
  input: LoginInput,
  auditContext?: AuthAuditContext,
): Promise<AuthenticatedPrincipal> {
  const username = input.username.trim().toLowerCase();
  const password = input.password;

  if (username.length === 0 || password.length === 0) {
    throw new ValidationError("Username and password are required.");
  }

  const candidates = await findLoginCandidates(username, input.tenantId);
  if (candidates.length === 0) {
    await safeRecordAuthAuditEvent({
      context: auditContext,
      eventType: "LOGIN_FAILED",
      success: false,
      tenantId: input.tenantId,
      usernameNormalized: username,
      errorCode: "UNAUTHORIZED",
    });
    throw new UnauthorizedError("Invalid credentials.");
  }

  if (!input.tenantId && candidates.length > 1) {
    await safeRecordAuthAuditEvent({
      context: auditContext,
      eventType: "LOGIN_FAILED",
      success: false,
      usernameNormalized: username,
      errorCode: "BAD_REQUEST",
      payload: {
        reason: "tenant_required",
      },
    });
    throw new ValidationError(
      "Field 'tenant_id' is required for multi-tenant accounts.",
    );
  }

  const candidate = candidates.at(0);

  if (!candidate) {
    throw new UnauthorizedError("Invalid credentials.");
  }
  const isPasswordValid = await Bun.password.verify(
    password,
    candidate.passwordHash,
  );
  if (!isPasswordValid) {
    await safeRecordAuthAuditEvent({
      context: auditContext,
      eventType: "LOGIN_FAILED",
      success: false,
      tenantId: candidate.tenantId,
      userId: candidate.userId,
      usernameNormalized: username,
      errorCode: "UNAUTHORIZED",
    });
    throw new UnauthorizedError("Invalid credentials.");
  }

  const now = new Date();
  const expiresAt = computeSessionExpiry(now);
  const sessionToken = generateSessionToken();
  const sessionTokenHash = hashSessionToken(sessionToken);

  await createSession({
    sessionId: crypto.randomUUID(),
    tokenHash: sessionTokenHash,
    userId: candidate.userId,
    tenantId: candidate.tenantId,
    membershipId: candidate.membershipId,
    expiresAt,
  });

  await updateUserLastLoginAt(candidate.userId);

  await safeRecordAuthAuditEvent({
    context: auditContext,
    eventType: "LOGIN_SUCCEEDED",
    success: true,
    tenantId: candidate.tenantId,
    userId: candidate.userId,
    usernameNormalized: username,
    errorCode: undefined,
  });

  return toPrincipal({
    token: sessionToken,
    userId: candidate.userId,
    username: candidate.username,
    tenantId: candidate.tenantId,
    role: candidate.role,
    createdAt: now,
    lastSeenAt: now,
  });
}

export async function resolvePrincipalFromRequest(
  request: Request,
  auditContext?: AuthAuditContext,
): Promise<AuthenticatedPrincipal | undefined> {
  let token: string | undefined;

  try {
    token = parseBearerToken(request.headers.get(AUTHORIZATION_HEADER));
  } catch (error) {
    await safeRecordAuthAuditEvent({
      context: auditContext,
      eventType: "SESSION_REJECTED",
      success: false,
      errorCode: "UNAUTHORIZED",
      payload: {
        reason: "invalid_authorization_header",
      },
    });
    throw error;
  }

  if (!token) {
    return undefined;
  }

  const session = await findActiveSessionByTokenHash(hashSessionToken(token));
  if (!session) {
    await safeRecordAuthAuditEvent({
      context: auditContext,
      eventType: "SESSION_REJECTED",
      success: false,
      errorCode: "UNAUTHORIZED",
    });
    throw new UnauthorizedError("Session is invalid or has expired.");
  }

  await touchSessionLastSeenAt(session.sessionId);

  return toPrincipal({
    token,
    userId: session.userId,
    username: session.username,
    tenantId: session.tenantId,
    role: session.role,
    createdAt: session.issuedAt,
    lastSeenAt: new Date(),
  });
}

export async function logoutByToken(
  token: string | undefined,
  auditContext?: AuthAuditContext,
): Promise<void> {
  if (!token) {
    await safeRecordAuthAuditEvent({
      context: auditContext,
      eventType: "LOGOUT_FAILED",
      success: false,
      errorCode: "UNAUTHORIZED",
      payload: {
        reason: "missing_token",
      },
    });
    throw new UnauthorizedError("Authentication is required.");
  }

  const tokenHash = hashSessionToken(token);
  const activeSession = await findActiveSessionByTokenHash(tokenHash);
  const revoked = await revokeSessionByTokenHash(tokenHash);

  if (!revoked) {
    await safeRecordAuthAuditEvent({
      context: auditContext,
      eventType: "LOGOUT_FAILED",
      success: false,
      errorCode: "UNAUTHORIZED",
      payload: {
        reason: "session_not_found",
      },
    });
    throw new UnauthorizedError("Session is invalid or has expired.");
  }

  await safeRecordAuthAuditEvent({
    context: auditContext,
    eventType: "LOGOUT_SUCCEEDED",
    success: true,
    tenantId: activeSession?.tenantId,
    userId: activeSession?.userId,
    sessionId: activeSession?.sessionId,
  });
}
