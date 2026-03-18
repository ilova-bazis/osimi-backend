DO $$
BEGIN
  CREATE TYPE ingestion_status AS ENUM (
    'DRAFT',
    'UPLOADING',
    'QUEUED',
    'PROCESSING',
    'COMPLETED',
    'COMPLETED_WITH_ERRORS',
    'FAILED',
    'CANCELED'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
  CREATE TYPE ingestion_classification_type AS ENUM (
    'newspaper_article',
    'magazine_article',
    'book_chapter',
    'book',
    'letter',
    'speech',
    'interview',
    'report',
    'manuscript',
    'image',
    'document',
    'other'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
  CREATE TYPE ingest_item_kind AS ENUM (
    'photo',
    'audio',
    'video',
    'scanned_document',
    'document',
    'other'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
  CREATE TYPE ingestion_pipeline_preset AS ENUM (
    'auto',
    'none',
    'ocr_text',
    'audio_transcript',
    'video_transcript',
    'ocr_and_audio_transcript',
    'ocr_and_video_transcript'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
  CREATE TYPE ingestion_file_status AS ENUM (
    'PENDING',
    'UPLOADED',
    'VALIDATED',
    'FAILED'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
  CREATE TYPE ingestion_item_status AS ENUM (
    'PENDING',
    'READY',
    'PROCESSING',
    'COMPLETED',
    'FAILED',
    'SKIPPED'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
  CREATE TYPE ingestion_item_file_role AS ENUM (
    'primary',
    'front',
    'back',
    'page',
    'attachment',
    'transcript_source',
    'side_a',
    'side_b',
    'other'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
  CREATE TYPE object_processing_state AS ENUM (
    'queued',
    'ingesting',
    'ingested',
    'derivatives_running',
    'derivatives_done',
    'ocr_running',
    'ocr_done',
    'index_running',
    'index_done',
    'processing_failed',
    'processing_skipped'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
  CREATE TYPE object_curation_state AS ENUM (
    'needs_review',
    'review_in_progress',
    'reviewed',
    'curation_failed'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
  CREATE TYPE object_availability_state AS ENUM (
    'AVAILABLE',
    'ARCHIVED',
    'RESTORE_PENDING',
    'RESTORING',
    'UNAVAILABLE'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
  CREATE TYPE object_access_level AS ENUM (
    'private',
    'family',
    'public'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
  CREATE TYPE object_embargo_kind AS ENUM (
    'none',
    'timed',
    'curation_state'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
  CREATE TYPE object_access_granted_level AS ENUM (
    'family',
    'private'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
  CREATE TYPE object_access_request_status AS ENUM (
    'PENDING',
    'APPROVED',
    'REJECTED',
    'CANCELED'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
  CREATE TYPE object_type AS ENUM (
    'GENERIC',
    'IMAGE',
    'AUDIO',
    'VIDEO',
    'DOCUMENT'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
  CREATE TYPE artifact_kind AS ENUM (
    'ingest_json',
    'pipeline_json',
    'catalog_json',
    'original',
    'preview',
    'ocr',
    'transcript',
    'metadata',
    'pdf',
    'ocr_text',
    'thumbnail',
    'web_version',
    'other'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
  CREATE TYPE object_download_request_status AS ENUM (
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
  CREATE TYPE object_event_type AS ENUM (
    'INGESTION_SUBMITTED',
    'INGESTION_QUEUED',
    'INGESTION_PROCESSING',
    'INGESTION_COMPLETED',
    'INGESTION_FAILED',
    'INGESTION_CANCELED',
    'LEASE_GRANTED',
    'LEASE_RENEWED',
    'LEASE_EXPIRED',
    'LEASE_RELEASED',
    'FILE_VALIDATED',
    'FILE_FAILED',
    'PIPELINE_STEP_STARTED',
    'PIPELINE_STEP_COMPLETED',
    'PIPELINE_STEP_FAILED',
    'INGESTION_ITEM_CREATED',
    'INGESTION_ITEM_UPDATED',
    'INGESTION_ITEM_PROCESSING',
    'INGESTION_ITEM_COMPLETED',
    'INGESTION_ITEM_FAILED',
    'OBJECT_CREATED',
    'ARTIFACT_CREATED'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS ingestions (
  id uuid PRIMARY KEY,
  batch_label text NOT NULL,
  tenant_id uuid NOT NULL,
  status ingestion_status NOT NULL DEFAULT 'DRAFT',
  created_by uuid NOT NULL,
  schema_version text NOT NULL DEFAULT '1.0',
  classification_type ingestion_classification_type NOT NULL,
  item_kind ingest_item_kind NOT NULL,
  language_code text NOT NULL,
  pipeline_preset ingestion_pipeline_preset NOT NULL DEFAULT 'auto',
  access_level object_access_level NOT NULL DEFAULT 'private',
  embargo_until timestamptz,
  rights_note text,
  sensitivity_note text,
  summary jsonb NOT NULL DEFAULT '{}'::jsonb,
  error_summary jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
  ,
  CHECK (length(trim(batch_label)) > 0),
  CHECK (length(trim(language_code)) > 0),
  CHECK (length(trim(schema_version)) > 0),
  CHECK (jsonb_typeof(summary) = 'object'),
  CHECK (jsonb_typeof(error_summary) = 'object')
);

CREATE INDEX IF NOT EXISTS ingestions_tenant_created_idx
  ON ingestions (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS ingestions_tenant_batch_label_idx
  ON ingestions (tenant_id, batch_label);

CREATE INDEX IF NOT EXISTS ingestions_status_created_idx
  ON ingestions (status, created_at ASC, id ASC);

CREATE INDEX IF NOT EXISTS ingestions_status_updated_idx
  ON ingestions (status, updated_at ASC, id ASC);

CREATE TABLE IF NOT EXISTS ingestion_files (
  id uuid PRIMARY KEY,
  ingestion_id uuid NOT NULL REFERENCES ingestions(id) ON DELETE CASCADE,
  filename text NOT NULL,
  content_type text NOT NULL,
  size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
  storage_key text NOT NULL,
  status ingestion_file_status NOT NULL DEFAULT 'PENDING',
  checksum_sha256 char(64),
  processing_overrides jsonb NOT NULL DEFAULT '{}'::jsonb,
  error jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (jsonb_typeof(processing_overrides) = 'object'),
  CHECK (jsonb_typeof(error) = 'object'),
  CHECK (checksum_sha256 IS NULL OR checksum_sha256 ~ '^[a-f0-9]{64}$'),
  CHECK (
    (status = 'PENDING' AND checksum_sha256 IS NULL)
    OR (status IN ('UPLOADED', 'VALIDATED') AND checksum_sha256 IS NOT NULL)
    OR (status = 'FAILED')
  ),
  UNIQUE (id, ingestion_id),
  UNIQUE (ingestion_id, storage_key)
);

CREATE INDEX IF NOT EXISTS ingestion_files_ingestion_idx
  ON ingestion_files (ingestion_id);

CREATE TABLE IF NOT EXISTS ingestion_items (
  id uuid PRIMARY KEY,
  ingestion_id uuid NOT NULL REFERENCES ingestions(id) ON DELETE CASCADE,
  item_index integer NOT NULL,
  status ingestion_item_status NOT NULL DEFAULT 'PENDING',
  classification_type ingestion_classification_type,
  item_kind ingest_item_kind,
  language_code text,
  title text,
  summary jsonb NOT NULL DEFAULT '{}'::jsonb,
  error_summary jsonb NOT NULL DEFAULT '{}'::jsonb,
  object_id text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (item_index > 0),
  CHECK (language_code IS NULL OR length(trim(language_code)) > 0),
  CHECK (jsonb_typeof(summary) = 'object'),
  CHECK (jsonb_typeof(error_summary) = 'object'),
  UNIQUE (ingestion_id, item_index),
  UNIQUE (id, ingestion_id)
);

CREATE INDEX IF NOT EXISTS ingestion_items_ingestion_idx
  ON ingestion_items (ingestion_id, item_index);

CREATE INDEX IF NOT EXISTS ingestion_items_status_idx
  ON ingestion_items (status, updated_at ASC, id ASC);

CREATE INDEX IF NOT EXISTS ingestion_items_object_idx
  ON ingestion_items (object_id)
  WHERE object_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS ingestion_item_files (
  id uuid PRIMARY KEY,
  ingestion_item_id uuid NOT NULL,
  ingestion_file_id uuid NOT NULL,
  ingestion_id uuid NOT NULL REFERENCES ingestions(id) ON DELETE CASCADE,
  role ingestion_item_file_role NOT NULL DEFAULT 'primary',
  sort_order integer NOT NULL,
  page_number integer,
  is_primary boolean NOT NULL DEFAULT false,
  logical_label text,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (sort_order > 0),
  CHECK (page_number IS NULL OR page_number > 0),
  UNIQUE (ingestion_item_id, ingestion_file_id),
  UNIQUE (ingestion_item_id, sort_order),
  FOREIGN KEY (ingestion_item_id, ingestion_id)
    REFERENCES ingestion_items(id, ingestion_id)
    ON DELETE CASCADE,
  FOREIGN KEY (ingestion_file_id, ingestion_id)
    REFERENCES ingestion_files(id, ingestion_id)
    ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ingestion_item_files_item_idx
  ON ingestion_item_files (ingestion_item_id, sort_order, id);

CREATE INDEX IF NOT EXISTS ingestion_item_files_file_idx
  ON ingestion_item_files (ingestion_file_id);

CREATE UNIQUE INDEX IF NOT EXISTS ingestion_item_files_one_primary_per_item_idx
  ON ingestion_item_files (ingestion_item_id)
  WHERE is_primary = true;

CREATE TABLE IF NOT EXISTS ingestion_leases (
  id uuid PRIMARY KEY,
  ingestion_id uuid NOT NULL REFERENCES ingestions(id) ON DELETE CASCADE,
  leased_by text,
  lease_token_id uuid NOT NULL,
  lease_started_at timestamptz NOT NULL DEFAULT now(),
  lease_expires_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  released_at timestamptz,
  CHECK (lease_expires_at > lease_started_at),
  CHECK (released_at IS NULL OR released_at >= lease_started_at)
);

CREATE EXTENSION IF NOT EXISTS btree_gist;

ALTER TABLE ingestion_leases
  ADD CONSTRAINT ingestion_leases_no_overlap
  EXCLUDE USING gist (
    ingestion_id WITH =,
    tstzrange(lease_started_at, lease_expires_at, '[)') WITH &&
  )
  WHERE (released_at IS NULL);

CREATE INDEX IF NOT EXISTS ingestion_leases_expiry_idx
  ON ingestion_leases (lease_expires_at);

CREATE INDEX IF NOT EXISTS ingestion_leases_ingestion_idx
  ON ingestion_leases (ingestion_id);

CREATE TABLE IF NOT EXISTS objects (
  object_id text PRIMARY KEY,
  tenant_id uuid NOT NULL,
  type object_type NOT NULL DEFAULT 'GENERIC',
  title text NOT NULL DEFAULT '',
  processing_state object_processing_state NOT NULL DEFAULT 'queued',
  curation_state object_curation_state NOT NULL DEFAULT 'needs_review',
  availability_state object_availability_state NOT NULL DEFAULT 'AVAILABLE',
  access_level object_access_level NOT NULL DEFAULT 'private',
  embargo_kind object_embargo_kind NOT NULL DEFAULT 'none',
  embargo_until timestamptz,
  embargo_curation_state object_curation_state,
  rights_note text,
  sensitivity_note text,
  language_code text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  ingest_manifest jsonb,
  source_ingestion_id uuid REFERENCES ingestions(id) ON DELETE SET NULL,
  source_ingestion_item_id uuid REFERENCES ingestion_items(id) ON DELETE SET NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (object_id ~ '^OBJ-[0-9]{8}-[A-Z0-9]+$'),
  CHECK (ingest_manifest IS NULL OR jsonb_typeof(ingest_manifest) = 'object'),
  CHECK (language_code IS NULL OR length(trim(language_code)) > 0),
  CHECK (
    (embargo_kind = 'none' AND embargo_until IS NULL AND embargo_curation_state IS NULL)
    OR (embargo_kind = 'timed' AND embargo_until IS NOT NULL AND embargo_curation_state IS NULL)
    OR (embargo_kind = 'curation_state' AND embargo_until IS NULL AND embargo_curation_state IS NOT NULL)
  )
);

CREATE INDEX IF NOT EXISTS objects_tenant_created_idx
  ON objects (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS objects_tenant_language_created_idx
  ON objects (tenant_id, language_code, created_at DESC, object_id DESC);

DROP INDEX IF EXISTS objects_source_ingestion_unique_idx;

CREATE UNIQUE INDEX IF NOT EXISTS objects_source_ingestion_item_unique_idx
  ON objects (source_ingestion_item_id)
  WHERE source_ingestion_item_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS objects_source_ingestion_idx
  ON objects (source_ingestion_id)
  WHERE source_ingestion_id IS NOT NULL;

ALTER TABLE ingestion_items
  DROP CONSTRAINT IF EXISTS ingestion_items_object_id_fkey;

ALTER TABLE ingestion_items
  ADD CONSTRAINT ingestion_items_object_id_fkey
  FOREIGN KEY (object_id)
  REFERENCES objects(object_id)
  ON DELETE SET NULL;

CREATE TABLE IF NOT EXISTS tags (
  id uuid PRIMARY KEY,
  name_normalized text NOT NULL UNIQUE,
  display_name text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (length(trim(name_normalized)) > 0),
  CHECK (length(trim(display_name)) > 0),
  CHECK (name_normalized = lower(name_normalized))
);

CREATE TABLE IF NOT EXISTS object_tags (
  object_id text NOT NULL REFERENCES objects(object_id) ON DELETE CASCADE,
  tag_id uuid NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (object_id, tag_id)
);

CREATE INDEX IF NOT EXISTS object_tags_tag_idx
  ON object_tags (tag_id);

CREATE INDEX IF NOT EXISTS object_tags_object_idx
  ON object_tags (object_id);

CREATE TABLE IF NOT EXISTS object_access_assignments (
  object_id text NOT NULL REFERENCES objects(object_id) ON DELETE CASCADE,
  tenant_id uuid NOT NULL,
  user_id uuid NOT NULL,
  granted_level object_access_granted_level NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  created_by uuid NOT NULL,
  PRIMARY KEY (object_id, user_id)
);

CREATE INDEX IF NOT EXISTS object_access_assignments_user_idx
  ON object_access_assignments (tenant_id, user_id, granted_level);

CREATE INDEX IF NOT EXISTS object_access_assignments_object_idx
  ON object_access_assignments (object_id, granted_level);

CREATE TABLE IF NOT EXISTS object_access_requests (
  id uuid PRIMARY KEY,
  object_id text NOT NULL REFERENCES objects(object_id) ON DELETE CASCADE,
  tenant_id uuid NOT NULL,
  requester_user_id uuid NOT NULL,
  requested_level object_access_granted_level NOT NULL,
  reason text,
  status object_access_request_status NOT NULL DEFAULT 'PENDING',
  reviewed_by uuid,
  reviewed_at timestamptz,
  decision_note text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS object_access_requests_object_status_idx
  ON object_access_requests (object_id, status, created_at DESC);

CREATE INDEX IF NOT EXISTS object_access_requests_requester_idx
  ON object_access_requests (tenant_id, requester_user_id, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS object_access_requests_one_pending_per_user_idx
  ON object_access_requests (object_id, requester_user_id)
  WHERE status = 'PENDING';

CREATE TABLE IF NOT EXISTS object_download_requests (
  id uuid PRIMARY KEY,
  object_id text NOT NULL REFERENCES objects(object_id) ON DELETE CASCADE,
  tenant_id uuid NOT NULL,
  available_file_id uuid,
  requested_by uuid NOT NULL,
  artifact_kind artifact_kind NOT NULL,
  variant text,
  status object_download_request_status NOT NULL DEFAULT 'PENDING',
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

CREATE INDEX IF NOT EXISTS object_download_requests_object_status_idx
  ON object_download_requests (object_id, status, created_at DESC);

CREATE INDEX IF NOT EXISTS object_download_requests_tenant_created_idx
  ON object_download_requests (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS object_download_requests_active_lease_idx
  ON object_download_requests (id, lease_id, lease_token_id, lease_expires_at)
  WHERE status = 'PROCESSING' AND released_at IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS object_download_requests_one_active_kind_idx
  ON object_download_requests (tenant_id, object_id, artifact_kind, COALESCE(variant, ''))
  WHERE status IN ('PENDING', 'PROCESSING');

CREATE TABLE IF NOT EXISTS object_available_files (
  id uuid PRIMARY KEY,
  object_id text NOT NULL REFERENCES objects(object_id) ON DELETE CASCADE,
  tenant_id uuid NOT NULL,
  archive_file_key text NOT NULL,
  artifact_kind artifact_kind NOT NULL,
  variant text,
  display_name text NOT NULL,
  content_type text,
  size_bytes bigint CHECK (size_bytes >= 0),
  checksum_sha256 text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  is_available boolean NOT NULL DEFAULT true,
  synced_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS object_available_files_archive_key_unique_idx
  ON object_available_files (tenant_id, object_id, archive_file_key);

CREATE INDEX IF NOT EXISTS object_available_files_object_available_idx
  ON object_available_files (object_id, is_available, synced_at DESC);

ALTER TABLE object_download_requests
  DROP CONSTRAINT IF EXISTS object_download_requests_available_file_id_fkey;

ALTER TABLE object_download_requests
  ADD CONSTRAINT object_download_requests_available_file_id_fkey
  FOREIGN KEY (available_file_id)
  REFERENCES object_available_files(id)
  ON DELETE SET NULL;

CREATE TABLE IF NOT EXISTS object_artifacts (
  id uuid PRIMARY KEY,
  object_id text NOT NULL REFERENCES objects(object_id) ON DELETE CASCADE,
  kind artifact_kind NOT NULL,
  variant text,
  storage_key text NOT NULL,
  content_type text NOT NULL,
  size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (storage_key)
);

CREATE INDEX IF NOT EXISTS object_artifacts_object_idx
  ON object_artifacts (object_id);

CREATE TABLE IF NOT EXISTS object_events (
  id uuid PRIMARY KEY,
  event_id uuid NOT NULL UNIQUE,
  tenant_id uuid NOT NULL,
  type object_event_type NOT NULL,
  ingestion_id uuid REFERENCES ingestions(id) ON DELETE SET NULL,
  ingestion_item_id uuid REFERENCES ingestion_items(id) ON DELETE SET NULL,
  object_id text REFERENCES objects(object_id) ON DELETE SET NULL,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  actor_user_id uuid,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS object_events_tenant_created_idx
  ON object_events (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS object_events_ingestion_created_idx
  ON object_events (ingestion_id, created_at DESC);

CREATE INDEX IF NOT EXISTS object_events_ingestion_item_created_idx
  ON object_events (ingestion_item_id, created_at DESC);

CREATE INDEX IF NOT EXISTS object_events_object_created_idx
  ON object_events (object_id, created_at DESC);
