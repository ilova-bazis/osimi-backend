import type { UserRole } from "../auth/types.ts";
import { withSchemaClient } from "../db/client.ts";

type AuthAuditEventType =
  | "LOGIN_SUCCEEDED"
  | "LOGIN_FAILED"
  | "SESSION_REJECTED"
  | "LOGOUT_SUCCEEDED"
  | "LOGOUT_FAILED";

interface LoginCandidateRow {
  user_id: string;
  username: string;
  password_hash: string;
  tenant_id: string;
  role: UserRole;
  membership_id: string;
}

interface ActiveSessionRow {
  session_id: string;
  user_id: string;
  username: string;
  tenant_id: string;
  role: UserRole;
  issued_at: Date;
  last_seen_at: Date;
}

export interface LoginCandidate {
  userId: string;
  username: string;
  passwordHash: string;
  tenantId: string;
  role: UserRole;
  membershipId: string;
}

export interface ActiveSession {
  sessionId: string;
  userId: string;
  username: string;
  tenantId: string;
  role: UserRole;
  issuedAt: Date;
  lastSeenAt: Date;
}

function mapLoginCandidate(row: LoginCandidateRow): LoginCandidate {
  return {
    userId: row.user_id,
    username: row.username,
    passwordHash: row.password_hash,
    tenantId: row.tenant_id,
    role: row.role,
    membershipId: row.membership_id,
  };
}

function mapActiveSession(row: ActiveSessionRow): ActiveSession {
  return {
    sessionId: row.session_id,
    userId: row.user_id,
    username: row.username,
    tenantId: row.tenant_id,
    role: row.role,
    issuedAt: new Date(row.issued_at),
    lastSeenAt: new Date(row.last_seen_at),
  };
}

export async function findLoginCandidates(
  usernameNormalized: string,
  tenantId?: string,
): Promise<LoginCandidate[]> {
  const rows = await withSchemaClient(async (sql) => {
    if (tenantId) {
      return await sql<LoginCandidateRow[]>`
        SELECT
          u.id AS user_id,
          u.username,
          u.password_hash,
          tm.tenant_id,
          tm.role,
          tm.id AS membership_id
        FROM users u
        INNER JOIN tenant_memberships tm ON tm.user_id = u.id
        INNER JOIN tenants t ON t.id = tm.tenant_id
        WHERE u.username_normalized = ${usernameNormalized}
          AND tm.tenant_id = ${tenantId}
          AND u.is_active = true
          AND tm.is_active = true
          AND t.is_active = true
        ORDER BY tm.created_at ASC
      `;
    }

    return await sql<LoginCandidateRow[]>`
      SELECT
        u.id AS user_id,
        u.username,
        u.password_hash,
        tm.tenant_id,
        tm.role,
        tm.id AS membership_id
      FROM users u
      INNER JOIN tenant_memberships tm ON tm.user_id = u.id
      INNER JOIN tenants t ON t.id = tm.tenant_id
      WHERE u.username_normalized = ${usernameNormalized}
        AND u.is_active = true
        AND tm.is_active = true
        AND t.is_active = true
      ORDER BY tm.created_at ASC
    `;
  });

  return rows.map(mapLoginCandidate);
}

export async function updateUserLastLoginAt(userId: string): Promise<void> {
  await withSchemaClient(async (sql) => {
    await sql`
      UPDATE users
      SET last_login_at = now(), updated_at = now()
      WHERE id = ${userId}
    `;
  });
}

export async function createSession(params: {
  sessionId: string;
  tokenHash: string;
  userId: string;
  tenantId: string;
  membershipId: string;
  expiresAt: Date;
  ip?: string;
  userAgent?: string;
}): Promise<void> {
  await withSchemaClient(async (sql) => {
    await sql`
      INSERT INTO auth_sessions (
        id,
        session_token_hash,
        user_id,
        tenant_id,
        membership_id,
        expires_at,
        ip,
        user_agent
      )
      VALUES (
        ${params.sessionId},
        ${params.tokenHash},
        ${params.userId},
        ${params.tenantId},
        ${params.membershipId},
        ${params.expiresAt.toISOString()},
        ${params.ip ?? null},
        ${params.userAgent ?? null}
      )
    `;
  });
}

export async function findActiveSessionByTokenHash(
  tokenHash: string,
): Promise<ActiveSession | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<ActiveSessionRow[]>`
      SELECT
        s.id AS session_id,
        s.user_id,
        u.username,
        s.tenant_id,
        tm.role,
        s.issued_at,
        s.last_seen_at
      FROM auth_sessions s
      INNER JOIN users u ON u.id = s.user_id
      INNER JOIN tenants t ON t.id = s.tenant_id
      INNER JOIN tenant_memberships tm ON tm.id = s.membership_id
      WHERE s.session_token_hash = ${tokenHash}
        AND s.revoked_at IS NULL
        AND s.expires_at > now()
        AND u.is_active = true
        AND t.is_active = true
        AND tm.is_active = true
        AND tm.user_id = s.user_id
        AND tm.tenant_id = s.tenant_id
      LIMIT 1
    `;
  });

  const row = rows.at(0);

  if (!row) {
    return undefined;
  }

  return mapActiveSession(row);
}

export async function touchSessionLastSeenAt(sessionId: string): Promise<void> {
  await withSchemaClient(async (sql) => {
    await sql`
      UPDATE auth_sessions
      SET last_seen_at = now()
      WHERE id = ${sessionId}
    `;
  });
}

export async function revokeSessionByTokenHash(
  tokenHash: string,
): Promise<boolean> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<Array<{ id: string }>>`
      UPDATE auth_sessions
      SET revoked_at = now(), revoked_reason = 'logout'
      WHERE session_token_hash = ${tokenHash}
        AND revoked_at IS NULL
      RETURNING id
    `;
  });

  return rows.length > 0;
}

export async function insertAuthAuditEvent(params: {
  requestId: string;
  eventType: AuthAuditEventType;
  success: boolean;
  tenantId?: string;
  userId?: string;
  sessionId?: string;
  usernameNormalized?: string;
  errorCode?: string;
  ip?: string;
  userAgent?: string;
  payload?: Record<string, unknown>;
}): Promise<void> {
  await withSchemaClient(async (sql) => {
    await sql`
      INSERT INTO auth_audit_events (
        id,
        request_id,
        event_type,
        success,
        tenant_id,
        user_id,
        session_id,
        username_normalized,
        error_code,
        ip,
        user_agent,
        payload
      )
      VALUES (
        ${crypto.randomUUID()},
        ${params.requestId},
        ${params.eventType},
        ${params.success},
        ${params.tenantId ?? null},
        ${params.userId ?? null},
        ${params.sessionId ?? null},
        ${params.usernameNormalized ?? null},
        ${params.errorCode ?? null},
        ${params.ip ?? null},
        ${params.userAgent ?? null},
        CAST(${JSON.stringify(params.payload ?? {})} AS jsonb)
      )
    `;
  });
}

interface TenantRow {
  id: string;
  slug: string;
  name: string;
}

export interface TenantSummary {
  id: string;
  slug: string;
  name: string;
}

function mapTenant(row: TenantRow): TenantSummary {
  return {
    id: row.id,
    slug: row.slug,
    name: row.name,
  };
}

export async function findAllActiveTenants(): Promise<TenantSummary[]> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<TenantRow[]>`
      SELECT id, slug, name
      FROM tenants
      WHERE is_active = true
      ORDER BY name
    `;
  });

  return rows.map(mapTenant);
}

interface UserRow {
  id: string;
  username: string;
  username_normalized: string;
}

export interface UserSummary {
  id: string;
  username: string;
  usernameNormalized: string;
}

function mapUser(row: UserRow): UserSummary {
  return {
    id: row.id,
    username: row.username,
    usernameNormalized: row.username_normalized,
  };
}

export async function findUserByUsername(
  usernameNormalized: string,
): Promise<UserSummary | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<UserRow[]>`
      SELECT id, username, username_normalized
      FROM users
      WHERE username_normalized = ${usernameNormalized}
      LIMIT 1
    `;
  });

  const row = rows.at(0);
  if (!row) {
    return undefined;
  }

  return mapUser(row);
}

export async function createUser(params: {
  userId: string;
  username: string;
  usernameNormalized: string;
  passwordHash: string;
  tenantId: string;
  role: UserRole;
  membershipId: string;
}): Promise<void> {
  await withSchemaClient(async (sql) => {
    await sql.begin(async (tx) => {
      await tx`
            INSERT INTO users (id, username, username_normalized, password_hash, is_active, created_at, updated_at)
            VALUES (
              ${params.userId},
              ${params.username},
              ${params.usernameNormalized},
              ${params.passwordHash},
              true,
              now(),
              now()
            )
          `;

      await tx`
            INSERT INTO tenant_memberships (id, tenant_id, user_id, role, is_active, created_at, updated_at)
            VALUES (
              ${params.membershipId},
              ${params.tenantId},
              ${params.userId},
              ${params.role},
              true,
              now(),
              now()
            )
          `;
    });
  });
}

interface UserWithRoleRow {
  id: string;
  username: string;
  username_normalized: string;
  role: UserRole;
}

export interface UserWithRole extends UserSummary {
  role: UserRole;
}

function mapUserWithRole(row: UserWithRoleRow): UserWithRole {
  return {
    id: row.id,
    username: row.username,
    usernameNormalized: row.username_normalized,
    role: row.role,
  };
}

export async function findUsersByTenant(
  tenantId: string,
): Promise<UserWithRole[]> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<UserWithRoleRow[]>`
      SELECT
        u.id,
        u.username,
        u.username_normalized,
        tm.role
      FROM users u
      INNER JOIN tenant_memberships tm ON tm.user_id = u.id
      WHERE tm.tenant_id = ${tenantId}
        AND u.is_active = true
        AND tm.is_active = true
      ORDER BY u.username
    `;
  });

  return rows.map(mapUserWithRole);
}

export async function getUserRole(
  userId: string,
  tenantId: string,
): Promise<UserRole | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<Array<{ role: UserRole }>>`
      SELECT role
      FROM tenant_memberships
      WHERE user_id = ${userId}
        AND tenant_id = ${tenantId}
        AND is_active = true
      LIMIT 1
    `;
  });

  return rows[0]?.role;
}

export async function updateUserPassword(
  userId: string,
  passwordHash: string,
): Promise<void> {
  await withSchemaClient(async (sql) => {
    await sql`
      UPDATE users
      SET password_hash = ${passwordHash}, updated_at = now()
      WHERE id = ${userId}
    `;
  });
}

export async function updateUserRole(
  userId: string,
  tenantId: string,
  role: UserRole,
): Promise<void> {
  await withSchemaClient(async (sql) => {
    await sql`
      UPDATE tenant_memberships
      SET role = ${role}, updated_at = now()
      WHERE user_id = ${userId}
        AND tenant_id = ${tenantId}
    `;
  });
}

export async function deleteUser(userId: string): Promise<void> {
  await withSchemaClient(async (sql) => {
    await sql`
      UPDATE users
      SET is_active = false, updated_at = now()
      WHERE id = ${userId}
    `;
  });
}
