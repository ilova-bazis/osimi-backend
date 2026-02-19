DO $$
BEGIN
  CREATE TYPE user_role AS ENUM (
    'viewer',
    'archiver',
    'admin'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS tenants (
  id uuid PRIMARY KEY,
  slug text NOT NULL UNIQUE,
  name text NOT NULL,
  is_active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (length(trim(slug)) > 0),
  CHECK (length(trim(name)) > 0)
);

CREATE INDEX IF NOT EXISTS tenants_is_active_idx
  ON tenants (is_active);

CREATE TABLE IF NOT EXISTS users (
  id uuid PRIMARY KEY,
  username text NOT NULL,
  username_normalized text NOT NULL UNIQUE,
  password_hash text NOT NULL,
  is_active boolean NOT NULL DEFAULT true,
  last_login_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (length(trim(username)) > 0),
  CHECK (length(trim(username_normalized)) > 0),
  CHECK (username_normalized = lower(username_normalized))
);

CREATE INDEX IF NOT EXISTS users_is_active_idx
  ON users (is_active);

CREATE TABLE IF NOT EXISTS tenant_memberships (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  role user_role NOT NULL,
  is_active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, user_id)
);

CREATE INDEX IF NOT EXISTS tenant_memberships_user_idx
  ON tenant_memberships (user_id);

CREATE INDEX IF NOT EXISTS tenant_memberships_tenant_role_idx
  ON tenant_memberships (tenant_id, role)
  WHERE is_active = true;

CREATE TABLE IF NOT EXISTS auth_sessions (
  id uuid PRIMARY KEY,
  session_token_hash char(64) NOT NULL UNIQUE,
  user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  membership_id uuid NOT NULL REFERENCES tenant_memberships(id) ON DELETE CASCADE,
  issued_at timestamptz NOT NULL DEFAULT now(),
  last_seen_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL,
  revoked_at timestamptz,
  revoked_reason text,
  ip inet,
  user_agent text,
  CHECK (expires_at > issued_at),
  CHECK (revoked_at IS NULL OR revoked_at >= issued_at)
);

CREATE INDEX IF NOT EXISTS auth_sessions_user_idx
  ON auth_sessions (user_id);

CREATE INDEX IF NOT EXISTS auth_sessions_tenant_idx
  ON auth_sessions (tenant_id);

CREATE INDEX IF NOT EXISTS auth_sessions_active_lookup_idx
  ON auth_sessions (session_token_hash)
  WHERE revoked_at IS NULL;

CREATE INDEX IF NOT EXISTS auth_sessions_active_expiry_idx
  ON auth_sessions (expires_at)
  WHERE revoked_at IS NULL;
