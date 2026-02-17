DO $$
BEGIN
  CREATE TYPE ingestion_status AS ENUM (
    'DRAFT',
    'UPLOADING',
    'QUEUED',
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
  CREATE TYPE object_status AS ENUM (
    'ACTIVE',
    'ARCHIVED',
    'DELETED'
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
    'original',
    'preview',
    'ocr',
    'transcript',
    'metadata',
    'other'
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
  summary jsonb NOT NULL DEFAULT '{}'::jsonb,
  error_summary jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ingestions_tenant_created_idx
  ON ingestions (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS ingestions_tenant_batch_label_idx
  ON ingestions (tenant_id, batch_label);

CREATE TABLE IF NOT EXISTS ingestion_files (
  id uuid PRIMARY KEY,
  ingestion_id uuid NOT NULL REFERENCES ingestions(id) ON DELETE CASCADE,
  filename text NOT NULL,
  content_type text NOT NULL,
  size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
  storage_key text NOT NULL,
  status ingestion_file_status NOT NULL DEFAULT 'PENDING',
  checksum_sha256 char(64),
  error jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (ingestion_id, storage_key)
);

CREATE INDEX IF NOT EXISTS ingestion_files_ingestion_idx
  ON ingestion_files (ingestion_id);

CREATE TABLE IF NOT EXISTS ingestion_leases (
  id uuid PRIMARY KEY,
  ingestion_id uuid NOT NULL REFERENCES ingestions(id) ON DELETE CASCADE,
  leased_by text,
  lease_token_id uuid NOT NULL,
  lease_expires_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  released_at timestamptz,
  CHECK (released_at IS NULL OR released_at >= created_at)
);

CREATE UNIQUE INDEX IF NOT EXISTS ingestion_leases_one_active_idx
  ON ingestion_leases (ingestion_id)
  WHERE released_at IS NULL;

CREATE INDEX IF NOT EXISTS ingestion_leases_expiry_idx
  ON ingestion_leases (lease_expires_at);

CREATE INDEX IF NOT EXISTS ingestion_leases_ingestion_idx
  ON ingestion_leases (ingestion_id);

CREATE TABLE IF NOT EXISTS objects (
  object_id text PRIMARY KEY,
  tenant_id uuid NOT NULL,
  type object_type NOT NULL DEFAULT 'GENERIC',
  title text NOT NULL DEFAULT '',
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  ingest_manifest jsonb,
  source_ingestion_id uuid REFERENCES ingestions(id) ON DELETE SET NULL,
  status object_status NOT NULL DEFAULT 'ACTIVE',
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (object_id ~ '^OBJ-[0-9]{8}-[A-Z0-9]+$'),
  CHECK (ingest_manifest IS NULL OR jsonb_typeof(ingest_manifest) = 'object')
);

CREATE INDEX IF NOT EXISTS objects_tenant_created_idx
  ON objects (tenant_id, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS objects_source_ingestion_unique_idx
  ON objects (source_ingestion_id)
  WHERE source_ingestion_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS object_artifacts (
  id uuid PRIMARY KEY,
  object_id text NOT NULL REFERENCES objects(object_id) ON DELETE CASCADE,
  kind artifact_kind NOT NULL,
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
  object_id text REFERENCES objects(object_id) ON DELETE SET NULL,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  actor_user_id uuid,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS object_events_tenant_created_idx
  ON object_events (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS object_events_ingestion_created_idx
  ON object_events (ingestion_id, created_at DESC);

CREATE INDEX IF NOT EXISTS object_events_object_created_idx
  ON object_events (object_id, created_at DESC);
