DO $$
BEGIN
  CREATE TYPE auth_audit_event_type AS ENUM (
    'LOGIN_SUCCEEDED',
    'LOGIN_FAILED',
    'SESSION_REJECTED',
    'LOGOUT_SUCCEEDED',
    'LOGOUT_FAILED'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS auth_audit_events (
  id uuid PRIMARY KEY,
  request_id text NOT NULL,
  event_type auth_audit_event_type NOT NULL,
  success boolean NOT NULL,
  tenant_id uuid REFERENCES tenants(id) ON DELETE SET NULL,
  user_id uuid REFERENCES users(id) ON DELETE SET NULL,
  session_id uuid REFERENCES auth_sessions(id) ON DELETE SET NULL,
  username_normalized text,
  error_code text,
  ip inet,
  user_agent text,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (length(trim(request_id)) > 0)
);

CREATE INDEX IF NOT EXISTS auth_audit_events_created_idx
  ON auth_audit_events (created_at DESC);

CREATE INDEX IF NOT EXISTS auth_audit_events_request_idx
  ON auth_audit_events (request_id);

CREATE INDEX IF NOT EXISTS auth_audit_events_tenant_created_idx
  ON auth_audit_events (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS auth_audit_events_user_created_idx
  ON auth_audit_events (user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS auth_audit_events_event_created_idx
  ON auth_audit_events (event_type, created_at DESC);
