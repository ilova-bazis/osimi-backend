CREATE TABLE IF NOT EXISTS archive_artifact_upload_attempts (
  upload_token_id uuid PRIMARY KEY,
  request_id uuid NOT NULL REFERENCES archive_requests(id) ON DELETE CASCADE,
  authorized_lease_id uuid NOT NULL,
  authorized_lease_token_id uuid NOT NULL,
  storage_key text NOT NULL UNIQUE,
  content_type text NOT NULL,
  size_bytes bigint NOT NULL,
  expected_sha256 char(64),
  computed_sha256 char(64),
  state text NOT NULL DEFAULT 'AUTHORIZED',
  created_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now(),
  verified_at timestamptz,
  artifact_id uuid UNIQUE REFERENCES object_artifacts(id),
  materialized_at timestamptz,
  invalidated_at timestamptz,
  finalization_claim_token uuid,
  finalization_claimed_at timestamptz,
  finalization_attempt_count integer NOT NULL DEFAULT 0,
  finalization_next_retry_at timestamptz,
  finalization_last_error text,
  CONSTRAINT archive_artifact_upload_attempts_storage_key_check
    CHECK (length(trim(storage_key)) > 0),
  CONSTRAINT archive_artifact_upload_attempts_content_type_check
    CHECK (length(trim(content_type)) > 0),
  CONSTRAINT archive_artifact_upload_attempts_size_check
    CHECK (size_bytes >= 0),
  CONSTRAINT archive_artifact_upload_attempts_expected_sha256_check
    CHECK (expected_sha256 IS NULL OR expected_sha256 ~ '^[a-f0-9]{64}$'),
  CONSTRAINT archive_artifact_upload_attempts_computed_sha256_check
    CHECK (computed_sha256 IS NULL OR computed_sha256 ~ '^[a-f0-9]{64}$'),
  CONSTRAINT archive_artifact_upload_attempts_checksum_match_check
    CHECK (expected_sha256 IS NULL OR computed_sha256 IS NULL OR expected_sha256 = computed_sha256),
  CONSTRAINT archive_artifact_upload_attempts_state_check
    CHECK (state IN ('AUTHORIZED', 'VERIFIED', 'MATERIALIZED')),
  CONSTRAINT archive_artifact_upload_attempts_attempt_count_check
    CHECK (finalization_attempt_count >= 0),
  CONSTRAINT archive_artifact_upload_attempts_claim_check
    CHECK ((finalization_claim_token IS NULL) = (finalization_claimed_at IS NULL)),
  CONSTRAINT archive_artifact_upload_attempts_lifecycle_check CHECK (
    (
      state = 'AUTHORIZED'
      AND computed_sha256 IS NULL
      AND verified_at IS NULL
      AND artifact_id IS NULL
      AND materialized_at IS NULL
      AND finalization_claim_token IS NULL
      AND finalization_claimed_at IS NULL
      AND finalization_attempt_count = 0
      AND finalization_next_retry_at IS NULL
      AND finalization_last_error IS NULL
    )
    OR (
      state = 'VERIFIED'
      AND computed_sha256 IS NOT NULL
      AND verified_at IS NOT NULL
      AND artifact_id IS NULL
      AND materialized_at IS NULL
      AND invalidated_at IS NULL
    )
    OR (
      state = 'MATERIALIZED'
      AND computed_sha256 IS NOT NULL
      AND verified_at IS NOT NULL
      AND artifact_id IS NOT NULL
      AND materialized_at IS NOT NULL
      AND invalidated_at IS NULL
      AND finalization_claim_token IS NULL
      AND finalization_claimed_at IS NULL
      AND finalization_next_retry_at IS NULL
      AND finalization_last_error IS NULL
    )
  )
);

CREATE UNIQUE INDEX IF NOT EXISTS archive_artifact_upload_attempts_one_active_authorized_idx
  ON archive_artifact_upload_attempts (request_id)
  WHERE state = 'AUTHORIZED' AND invalidated_at IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS archive_artifact_upload_attempts_one_accepted_idx
  ON archive_artifact_upload_attempts (request_id)
  WHERE state IN ('VERIFIED', 'MATERIALIZED');

CREATE INDEX IF NOT EXISTS archive_artifact_upload_attempts_finalization_candidates_idx
  ON archive_artifact_upload_attempts (
    (COALESCE(finalization_next_retry_at, verified_at)),
    finalization_claimed_at,
    upload_token_id
  )
  WHERE state = 'VERIFIED';

INSERT INTO archive_artifact_upload_attempts (
  upload_token_id,
  request_id,
  authorized_lease_id,
  authorized_lease_token_id,
  storage_key,
  content_type,
  size_bytes,
  computed_sha256,
  state,
  created_at,
  updated_at,
  verified_at,
  artifact_id,
  materialized_at,
  invalidated_at
)
SELECT
  req.artifact_upload_token_id,
  req.id,
  req.lease_id,
  req.lease_token_id,
  req.artifact_upload_storage_key,
  req.artifact_upload_content_type,
  req.artifact_upload_size_bytes,
  req.artifact_upload_checksum_sha256,
  CASE
    WHEN req.status = 'COMPLETED'
      AND req.artifact_upload_checksum_sha256 IS NOT NULL
      AND art.id IS NOT NULL THEN 'MATERIALIZED'
    WHEN req.artifact_upload_checksum_sha256 IS NOT NULL
      AND req.status IN ('PENDING', 'PROCESSING') THEN 'VERIFIED'
    ELSE 'AUTHORIZED'
  END,
  req.created_at,
  req.updated_at,
  CASE WHEN req.artifact_upload_checksum_sha256 IS NOT NULL THEN req.updated_at END,
  CASE WHEN req.status = 'COMPLETED'
    AND req.artifact_upload_checksum_sha256 IS NOT NULL THEN art.id END,
  CASE WHEN req.status = 'COMPLETED'
    AND req.artifact_upload_checksum_sha256 IS NOT NULL
    AND art.id IS NOT NULL
    THEN COALESCE(req.completed_at, req.updated_at) END,
  CASE WHEN req.artifact_upload_checksum_sha256 IS NULL
    AND NOT (
      req.status = 'PROCESSING'
      AND req.released_at IS NULL
      AND req.lease_expires_at > now()
    ) THEN now() END
FROM archive_requests req
LEFT JOIN object_artifacts art
  ON art.storage_key = req.artifact_upload_storage_key
  AND art.object_id = req.target_id
WHERE req.action_type = 'artifact_fetch'
  AND req.target_type = 'object'
  AND req.artifact_upload_token_id IS NOT NULL
  AND req.artifact_upload_storage_key IS NOT NULL
  AND req.artifact_upload_content_type IS NOT NULL
  AND req.artifact_upload_size_bytes IS NOT NULL
  AND req.lease_id IS NOT NULL
  AND req.lease_token_id IS NOT NULL
  AND (
    req.status <> 'COMPLETED'
    OR art.id IS NOT NULL
  )
  AND (
    req.artifact_upload_checksum_sha256 IS NULL
    OR req.status IN ('PENDING', 'PROCESSING')
    OR (req.status = 'COMPLETED' AND art.id IS NOT NULL)
  )
ON CONFLICT (upload_token_id) DO NOTHING;

UPDATE archive_requests req
SET status = 'PROCESSING',
    released_at = COALESCE(req.released_at, now()),
    lease_expires_at = NULL,
    updated_at = now()
WHERE EXISTS (
  SELECT 1
  FROM archive_artifact_upload_attempts attempt
  WHERE attempt.request_id = req.id
    AND attempt.state = 'VERIFIED'
);
