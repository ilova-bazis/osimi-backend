ALTER TABLE ingestions
  ADD COLUMN IF NOT EXISTS staging_purge_started_at timestamptz,
  ADD COLUMN IF NOT EXISTS staging_purge_claim_token uuid,
  ADD COLUMN IF NOT EXISTS staging_purge_claimed_at timestamptz,
  ADD COLUMN IF NOT EXISTS staging_purge_attempt_count integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS staging_purge_next_attempt_at timestamptz,
  ADD COLUMN IF NOT EXISTS staging_purge_last_error jsonb,
  ADD COLUMN IF NOT EXISTS staging_purged_at timestamptz;

ALTER TABLE ingestions
  ADD CONSTRAINT ingestions_staging_purge_attempt_count_check
  CHECK (staging_purge_attempt_count >= 0),
  ADD CONSTRAINT ingestions_staging_purge_error_check
  CHECK (staging_purge_last_error IS NULL OR jsonb_typeof(staging_purge_last_error) = 'object');

ALTER TABLE ingestion_files
  DROP CONSTRAINT IF EXISTS ingestion_files_preview_status_check;

ALTER TABLE ingestion_files
  ADD CONSTRAINT ingestion_files_preview_status_check CHECK (
    preview_status IS NULL
    OR preview_status IN ('pending', 'processing', 'ready', 'failed', 'unsupported', 'purged')
  );

CREATE INDEX IF NOT EXISTS ingestions_staging_purge_pending_idx
  ON ingestions (updated_at ASC, id ASC)
  WHERE staging_purged_at IS NULL;

CREATE INDEX IF NOT EXISTS ingestions_staging_purge_retry_idx
  ON ingestions (staging_purge_next_attempt_at ASC, id ASC)
  WHERE staging_purge_started_at IS NOT NULL AND staging_purged_at IS NULL;
