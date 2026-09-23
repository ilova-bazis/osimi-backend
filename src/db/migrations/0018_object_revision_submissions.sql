CREATE TABLE archive_request_sources (
  request_id uuid PRIMARY KEY REFERENCES archive_requests(id) ON DELETE CASCADE,
  tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  source_name text NOT NULL,
  storage_key text NOT NULL UNIQUE,
  content_type text NOT NULL,
  size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
  checksum_sha256 char(64) NOT NULL CHECK (checksum_sha256 ~ '^[0-9a-f]{64}$'),
  created_at timestamptz NOT NULL DEFAULT now(),
  cleanup_eligible_at timestamptz,
  cleanup_claim_id uuid,
  cleanup_claimed_at timestamptz,
  cleanup_attempt_count integer NOT NULL DEFAULT 0 CHECK (cleanup_attempt_count >= 0),
  cleanup_next_attempt_at timestamptz,
  cleanup_last_error text,
  purged_at timestamptz,
  UNIQUE (request_id, source_name)
);

CREATE INDEX archive_request_sources_cleanup_idx
  ON archive_request_sources (cleanup_next_attempt_at, cleanup_eligible_at, created_at)
  WHERE cleanup_eligible_at IS NOT NULL AND purged_at IS NULL;

CREATE TABLE object_revision_submissions (
  submission_id uuid PRIMARY KEY,
  request_id uuid NOT NULL UNIQUE REFERENCES archive_requests(id) ON DELETE CASCADE,
  tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  object_id text NOT NULL REFERENCES objects(object_id) ON DELETE CASCADE,
  edit_revision integer NOT NULL CHECK (edit_revision >= 0),
  package_schema_version text NOT NULL,
  package_checksum_sha256 char(64) NOT NULL CHECK (package_checksum_sha256 ~ '^[0-9a-f]{64}$'),
  submitted_by uuid REFERENCES users(id) ON DELETE SET NULL,
  submission_note text,
  archive_result jsonb,
  applied_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, object_id, edit_revision, package_schema_version)
);

CREATE INDEX object_revision_submissions_object_idx
  ON object_revision_submissions (tenant_id, object_id, created_at DESC, submission_id DESC);
