DO $$
BEGIN
  CREATE TYPE archive_request_status AS ENUM (
    'PENDING',
    'PROCESSING',
    'COMPLETED',
    'FAILED',
    'CANCELED'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
  CREATE TYPE archive_request_target_type AS ENUM (
    'object',
    'ingestion'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
  CREATE TYPE archive_request_action_type AS ENUM (
    'object_resync',
    'artifact_fetch'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS archive_requests (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL,
  target_type archive_request_target_type NOT NULL,
  target_id text NOT NULL,
  action_type archive_request_action_type NOT NULL,
  action_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  requested_by uuid NOT NULL,
  dedupe_key text,
  status archive_request_status NOT NULL DEFAULT 'PENDING',
  failure_reason text,
  failure_details jsonb,
  lease_id uuid,
  lease_token_id uuid,
  lease_expires_at timestamptz,
  leased_by text,
  released_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz
);

CREATE INDEX IF NOT EXISTS archive_requests_pending_idx
  ON archive_requests (action_type, created_at ASC, id ASC)
  WHERE status = 'PENDING';

CREATE INDEX IF NOT EXISTS archive_requests_active_lease_idx
  ON archive_requests (id, lease_id, lease_token_id, lease_expires_at)
  WHERE status = 'PROCESSING' AND released_at IS NULL;

CREATE INDEX IF NOT EXISTS archive_requests_tenant_target_idx
  ON archive_requests (tenant_id, target_type, target_id, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS archive_requests_active_dedupe_idx
  ON archive_requests (tenant_id, action_type, dedupe_key)
  WHERE dedupe_key IS NOT NULL AND status IN ('PENDING', 'PROCESSING');
