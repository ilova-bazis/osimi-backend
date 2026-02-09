import { db, qualifiedTableName } from "../db/runtime.ts";
import type { UserRole } from "../auth/types.ts";

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
  const sql = db();
  const usersTable = qualifiedTableName("users");
  const tenantsTable = qualifiedTableName("tenants");
  const membershipsTable = qualifiedTableName("tenant_memberships");

  const tenantClause = tenantId ? " AND tm.tenant_id = $2" : "";
  const values = tenantId ? [usernameNormalized, tenantId] : [usernameNormalized];

  const rows = (await sql.unsafe(
    `
      SELECT
        u.id AS user_id,
        u.username,
        u.password_hash,
        tm.tenant_id,
        tm.role,
        tm.id AS membership_id
      FROM ${usersTable} u
      INNER JOIN ${membershipsTable} tm ON tm.user_id = u.id
      INNER JOIN ${tenantsTable} t ON t.id = tm.tenant_id
      WHERE u.username_normalized = $1
        AND u.is_active = true
        AND tm.is_active = true
        AND t.is_active = true
        ${tenantClause}
      ORDER BY tm.created_at ASC
    `,
    values,
  )) as LoginCandidateRow[];

  return rows.map(mapLoginCandidate);
}

export async function updateUserLastLoginAt(userId: string): Promise<void> {
  const sql = db();
  const usersTable = qualifiedTableName("users");

  await sql.unsafe(`UPDATE ${usersTable} SET last_login_at = now(), updated_at = now() WHERE id = $1`, [userId]);
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
  const sql = db();
  const sessionsTable = qualifiedTableName("auth_sessions");

  await sql.unsafe(
    `
      INSERT INTO ${sessionsTable} (
        id,
        session_token_hash,
        user_id,
        tenant_id,
        membership_id,
        expires_at,
        ip,
        user_agent
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    `,
    [
      params.sessionId,
      params.tokenHash,
      params.userId,
      params.tenantId,
      params.membershipId,
      params.expiresAt.toISOString(),
      params.ip ?? null,
      params.userAgent ?? null,
    ],
  );
}

export async function findActiveSessionByTokenHash(tokenHash: string): Promise<ActiveSession | undefined> {
  const sql = db();
  const sessionsTable = qualifiedTableName("auth_sessions");
  const usersTable = qualifiedTableName("users");
  const tenantsTable = qualifiedTableName("tenants");
  const membershipsTable = qualifiedTableName("tenant_memberships");

  const rows = (await sql.unsafe(
    `
      SELECT
        s.id AS session_id,
        s.user_id,
        u.username,
        s.tenant_id,
        tm.role,
        s.issued_at,
        s.last_seen_at
      FROM ${sessionsTable} s
      INNER JOIN ${usersTable} u ON u.id = s.user_id
      INNER JOIN ${tenantsTable} t ON t.id = s.tenant_id
      INNER JOIN ${membershipsTable} tm ON tm.id = s.membership_id
      WHERE s.session_token_hash = $1
        AND s.revoked_at IS NULL
        AND s.expires_at > now()
        AND u.is_active = true
        AND t.is_active = true
        AND tm.is_active = true
        AND tm.user_id = s.user_id
        AND tm.tenant_id = s.tenant_id
      LIMIT 1
    `,
    [tokenHash],
  )) as ActiveSessionRow[];

  const row = rows.at(0);

  if (!row) {
    return undefined;
  }

  return mapActiveSession(row);
}

export async function touchSessionLastSeenAt(sessionId: string): Promise<void> {
  const sql = db();
  const sessionsTable = qualifiedTableName("auth_sessions");

  await sql.unsafe(`UPDATE ${sessionsTable} SET last_seen_at = now() WHERE id = $1`, [sessionId]);
}

export async function revokeSessionByTokenHash(tokenHash: string): Promise<boolean> {
  const sql = db();
  const sessionsTable = qualifiedTableName("auth_sessions");

  const rows = (await sql.unsafe(
    `
      UPDATE ${sessionsTable}
      SET revoked_at = now(), revoked_reason = 'logout'
      WHERE session_token_hash = $1
        AND revoked_at IS NULL
      RETURNING id
    `,
    [tokenHash],
  )) as Array<{ id: string }>;

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
  const sql = db();
  const auditTable = qualifiedTableName("auth_audit_events");

  await sql.unsafe(
    `
      INSERT INTO ${auditTable} (
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
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb)
    `,
    [
      crypto.randomUUID(),
      params.requestId,
      params.eventType,
      params.success,
      params.tenantId ?? null,
      params.userId ?? null,
      params.sessionId ?? null,
      params.usernameNormalized ?? null,
      params.errorCode ?? null,
      params.ip ?? null,
      params.userAgent ?? null,
      JSON.stringify(params.payload ?? {}),
    ],
  );
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
  const sql = db();
  const tenantsTable = qualifiedTableName("tenants");

  const rows = (await sql.unsafe(
    `SELECT id, slug, name FROM ${tenantsTable} WHERE is_active = true ORDER BY name`,
  )) as TenantRow[];

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

export async function findUserByUsername(usernameNormalized: string): Promise<UserSummary | undefined> {
  const sql = db();
  const usersTable = qualifiedTableName("users");

  const rows = (await sql.unsafe(
    `SELECT id, username, username_normalized FROM ${usersTable} WHERE username_normalized = $1 LIMIT 1`,
    [usernameNormalized],
  )) as UserRow[];

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
  const sql = db();
  const usersTable = qualifiedTableName("users");
  const membershipsTable = qualifiedTableName("tenant_memberships");

  await sql.unsafe(
    `
      INSERT INTO ${usersTable} (id, username, username_normalized, password_hash, is_active, created_at, updated_at)
      VALUES ($1, $2, $3, $4, true, now(), now())
    `,
    [params.userId, params.username, params.usernameNormalized, params.passwordHash],
  );

  await sql.unsafe(
    `
      INSERT INTO ${membershipsTable} (id, tenant_id, user_id, role, is_active, created_at, updated_at)
      VALUES ($1, $2, $3, $4, true, now(), now())
    `,
    [params.membershipId, params.tenantId, params.userId, params.role],
  );
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

export async function findUsersByTenant(tenantId: string): Promise<UserWithRole[]> {
  const sql = db();
  const usersTable = qualifiedTableName("users");
  const membershipsTable = qualifiedTableName("tenant_memberships");

  const rows = (await sql.unsafe(
    `
      SELECT 
        u.id,
        u.username,
        u.username_normalized,
        tm.role
      FROM ${usersTable} u
      INNER JOIN ${membershipsTable} tm ON tm.user_id = u.id
      WHERE tm.tenant_id = $1
        AND u.is_active = true
        AND tm.is_active = true
      ORDER BY u.username
    `,
    [tenantId],
  )) as UserWithRoleRow[];

  return rows.map(mapUserWithRole);
}

export async function getUserRole(userId: string, tenantId: string): Promise<UserRole | undefined> {
  const sql = db();
  const membershipsTable = qualifiedTableName("tenant_memberships");

  const rows = (await sql.unsafe(
    `SELECT role FROM ${membershipsTable} WHERE user_id = $1 AND tenant_id = $2 AND is_active = true LIMIT 1`,
    [userId, tenantId],
  )) as Array<{ role: UserRole }>;

  return rows[0]?.role;
}

export async function updateUserPassword(userId: string, passwordHash: string): Promise<void> {
  const sql = db();
  const usersTable = qualifiedTableName("users");

  await sql.unsafe(
    `UPDATE ${usersTable} SET password_hash = $1, updated_at = now() WHERE id = $2`,
    [passwordHash, userId],
  );
}

export async function updateUserRole(userId: string, tenantId: string, role: UserRole): Promise<void> {
  const sql = db();
  const membershipsTable = qualifiedTableName("tenant_memberships");

  await sql.unsafe(
    `UPDATE ${membershipsTable} SET role = $1, updated_at = now() WHERE user_id = $2 AND tenant_id = $3`,
    [role, userId, tenantId],
  );
}
