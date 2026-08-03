CREATE TABLE IF NOT EXISTS ingestion_idempotency_records (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  actor_user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  endpoint text NOT NULL,
  idempotency_key text NOT NULL,
  request_fingerprint char(64) NOT NULL,
  state text NOT NULL,
  owner_token uuid NOT NULL,
  locked_until timestamptz NOT NULL,
  status_code integer,
  response_body jsonb,
  completed_at timestamptz,
  expires_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT ingestion_idempotency_records_scope_unique
    UNIQUE (tenant_id, actor_user_id, endpoint, idempotency_key),
  CONSTRAINT ingestion_idempotency_records_state_check CHECK (
    state IN ('PROCESSING', 'COMPLETED')
  ),
  CONSTRAINT ingestion_idempotency_records_fingerprint_check CHECK (
    request_fingerprint ~ '^[a-f0-9]{64}$'
  ),
  CONSTRAINT ingestion_idempotency_records_response_check CHECK (
    (state = 'PROCESSING'
      AND status_code IS NULL
      AND response_body IS NULL
      AND completed_at IS NULL)
    OR
    (state = 'COMPLETED'
      AND status_code BETWEEN 200 AND 299
      AND response_body IS NOT NULL
      AND completed_at IS NOT NULL
      AND expires_at IS NOT NULL)
  )
);

CREATE INDEX IF NOT EXISTS ingestion_idempotency_records_expiry_idx
  ON ingestion_idempotency_records (expires_at)
  WHERE state = 'COMPLETED';
