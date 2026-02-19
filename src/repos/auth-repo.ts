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
          usr.id AS user_id,
          usr.username,
          usr.password_hash,
          mem.tenant_id,
          mem.role,
          mem.id AS membership_id
        FROM users usr
        INNER JOIN tenant_memberships mem ON mem.user_id = usr.id
        INNER JOIN tenants ten ON ten.id = mem.tenant_id
        WHERE usr.username_normalized = ${usernameNormalized}
          AND mem.tenant_id = ${tenantId}
          AND usr.is_active = true
          AND mem.is_active = true
          AND ten.is_active = true
        ORDER BY mem.created_at ASC
      `;
    }

    return await sql<LoginCandidateRow[]>`
      SELECT
        usr.id AS user_id,
        usr.username,
        usr.password_hash,
        mem.tenant_id,
        mem.role,
        mem.id AS membership_id
      FROM users usr
      INNER JOIN tenant_memberships mem ON mem.user_id = usr.id
      INNER JOIN tenants ten ON ten.id = mem.tenant_id
      WHERE usr.username_normalized = ${usernameNormalized}
        AND usr.is_active = true
        AND mem.is_active = true
        AND ten.is_active = true
      ORDER BY mem.created_at ASC
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
        ses.id AS session_id,
        ses.user_id,
        usr.username,
        ses.tenant_id,
        mem.role,
        ses.issued_at,
        ses.last_seen_at
      FROM auth_sessions ses
      INNER JOIN users usr ON usr.id = ses.user_id
      INNER JOIN tenants ten ON ten.id = ses.tenant_id
      INNER JOIN tenant_memberships mem ON mem.id = ses.membership_id
      WHERE ses.session_token_hash = ${tokenHash}
        AND ses.revoked_at IS NULL
        AND ses.expires_at > now()
        AND usr.is_active = true
        AND ten.is_active = true
        AND mem.is_active = true
        AND mem.user_id = ses.user_id
        AND mem.tenant_id = ses.tenant_id
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
        usr.id,
        usr.username,
        usr.username_normalized,
        mem.role
      FROM users usr
      INNER JOIN tenant_memberships mem ON mem.user_id = usr.id
      WHERE mem.tenant_id = ${tenantId}
        AND usr.is_active = true
        AND mem.is_active = true
      ORDER BY usr.username
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

export async function findTenantBySlug(slug: string): Promise<TenantSummary | undefined> {
  return await withSchemaClient(async (sql) => {
    const rows = await sql<TenantRow[]>`
      SELECT id, slug, name FROM tenants WHERE slug = ${slug} LIMIT 1
    `;

    const row = rows.at(0);
    if (!row) {
      return undefined;
    }

    return mapTenant(row);
  });
}

export async function createTenant(params: {
  tenantId: string;
  slug: string;
  name: string;
}): Promise<void> {
  await withSchemaClient(async (sql) => {
    await sql`
      INSERT INTO tenants (id, slug, name, is_active, created_at, updated_at)
      VALUES (${params.tenantId}, ${params.slug}, ${params.name}, true, now(), now())
    `;
  });
}
