ALTER TABLE ingestion_files
  ADD COLUMN IF NOT EXISTS upload_token_id uuid,
  ADD COLUMN IF NOT EXISTS upload_checksum_sha256 char(64),
  ADD COLUMN IF NOT EXISTS preview_upload_token_id uuid;

ALTER TABLE ingestion_files
  DROP CONSTRAINT IF EXISTS ingestion_files_upload_checksum_sha256_check;

ALTER TABLE ingestion_files
  ADD CONSTRAINT ingestion_files_upload_checksum_sha256_check CHECK (
    upload_checksum_sha256 IS NULL
    OR upload_checksum_sha256 ~ '^[a-f0-9]{64}$'
  );

ALTER TABLE archive_requests
  ADD COLUMN IF NOT EXISTS artifact_upload_token_id uuid,
  ADD COLUMN IF NOT EXISTS artifact_upload_storage_key text,
  ADD COLUMN IF NOT EXISTS artifact_upload_content_type text,
  ADD COLUMN IF NOT EXISTS artifact_upload_size_bytes bigint,
  ADD COLUMN IF NOT EXISTS artifact_upload_checksum_sha256 char(64);

ALTER TABLE archive_requests
  DROP CONSTRAINT IF EXISTS archive_requests_artifact_upload_size_check;

ALTER TABLE archive_requests
  ADD CONSTRAINT archive_requests_artifact_upload_size_check CHECK (
    artifact_upload_size_bytes IS NULL
    OR artifact_upload_size_bytes >= 0
  );

ALTER TABLE archive_requests
  DROP CONSTRAINT IF EXISTS archive_requests_artifact_upload_checksum_sha256_check;

ALTER TABLE archive_requests
  ADD CONSTRAINT archive_requests_artifact_upload_checksum_sha256_check CHECK (
    artifact_upload_checksum_sha256 IS NULL
    OR artifact_upload_checksum_sha256 ~ '^[a-f0-9]{64}$'
  );
