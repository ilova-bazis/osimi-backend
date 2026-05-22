ALTER TABLE ingestion_files
  ADD COLUMN IF NOT EXISTS preview_status text,
  ADD COLUMN IF NOT EXISTS preview_claimed_by text,
  ADD COLUMN IF NOT EXISTS preview_claimed_at timestamptz,
  ADD COLUMN IF NOT EXISTS preview_storage_key text,
  ADD COLUMN IF NOT EXISTS preview_content_type text,
  ADD COLUMN IF NOT EXISTS preview_size_bytes bigint,
  ADD COLUMN IF NOT EXISTS preview_width integer,
  ADD COLUMN IF NOT EXISTS preview_height integer,
  ADD COLUMN IF NOT EXISTS preview_error jsonb,
  ADD COLUMN IF NOT EXISTS preview_generated_at timestamptz;

ALTER TABLE ingestion_files
  DROP CONSTRAINT IF EXISTS ingestion_files_preview_status_check;

ALTER TABLE ingestion_files
  ADD CONSTRAINT ingestion_files_preview_status_check CHECK (
    preview_status IS NULL
    OR preview_status IN ('pending', 'processing', 'ready', 'failed', 'unsupported')
  );

ALTER TABLE ingestion_files
  DROP CONSTRAINT IF EXISTS ingestion_files_preview_size_bytes_check;

ALTER TABLE ingestion_files
  ADD CONSTRAINT ingestion_files_preview_size_bytes_check CHECK (
    preview_size_bytes IS NULL
    OR preview_size_bytes >= 0
  );

ALTER TABLE ingestion_files
  DROP CONSTRAINT IF EXISTS ingestion_files_preview_width_check;

ALTER TABLE ingestion_files
  ADD CONSTRAINT ingestion_files_preview_width_check CHECK (
    preview_width IS NULL
    OR preview_width > 0
  );

ALTER TABLE ingestion_files
  DROP CONSTRAINT IF EXISTS ingestion_files_preview_height_check;

ALTER TABLE ingestion_files
  ADD CONSTRAINT ingestion_files_preview_height_check CHECK (
    preview_height IS NULL
    OR preview_height > 0
  );

ALTER TABLE ingestion_files
  DROP CONSTRAINT IF EXISTS ingestion_files_preview_error_check;

ALTER TABLE ingestion_files
  ADD CONSTRAINT ingestion_files_preview_error_check CHECK (
    preview_error IS NULL
    OR jsonb_typeof(preview_error) = 'object'
  );
