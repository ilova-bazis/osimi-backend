CREATE TABLE curation_publications (
  request_id uuid PRIMARY KEY REFERENCES archive_requests(id) ON DELETE CASCADE,
  tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  object_id text NOT NULL REFERENCES objects(object_id) ON DELETE CASCADE,
  curated_kind text NOT NULL CHECK (curated_kind IN ('ocr_curated')),
  publication_revision integer NOT NULL CHECK (publication_revision >= 1),
  target_version text NOT NULL,
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
  UNIQUE (tenant_id, object_id, curated_kind, publication_revision)
);

CREATE INDEX curation_publications_cleanup_idx
  ON curation_publications (cleanup_next_attempt_at, cleanup_eligible_at, created_at)
  WHERE cleanup_eligible_at IS NOT NULL AND purged_at IS NULL;
