DO $$
BEGIN
  CREATE TYPE object_date_precision AS ENUM (
    'none',
    'year',
    'month',
    'day'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
  ALTER TYPE archive_request_action_type ADD VALUE IF NOT EXISTS 'curation_apply';
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
  ALTER TABLE objects ADD COLUMN publication_date text NOT NULL DEFAULT '';
EXCEPTION
  WHEN duplicate_column THEN NULL;
END $$;

DO $$
BEGIN
  ALTER TABLE objects ADD COLUMN date_precision object_date_precision NOT NULL DEFAULT 'none';
EXCEPTION
  WHEN duplicate_column THEN NULL;
END $$;

DO $$
BEGIN
  ALTER TABLE objects ADD COLUMN date_approximate boolean NOT NULL DEFAULT false;
EXCEPTION
  WHEN duplicate_column THEN NULL;
END $$;

DO $$
BEGIN
  ALTER TABLE objects ADD COLUMN description text;
EXCEPTION
  WHEN duplicate_column THEN NULL;
END $$;

DO $$
BEGIN
  ALTER TABLE objects
    ADD CONSTRAINT objects_date_precision_consistency_check
    CHECK (
      (date_precision = 'none' AND publication_date = '' AND date_approximate = false)
      OR (date_precision <> 'none')
    );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

UPDATE objects
SET
  publication_date = CASE
    WHEN metadata ? 'publication_date'
      AND jsonb_typeof(metadata -> 'publication_date') = 'string'
      AND (metadata ->> 'publication_date') ~ '^\d{4}(-\d{2})?(-\d{2})?$'
    THEN metadata ->> 'publication_date'
    ELSE publication_date
  END,
  date_precision = CASE
    WHEN metadata ? 'date_precision'
      AND jsonb_typeof(metadata -> 'date_precision') = 'string'
      AND metadata ->> 'date_precision' IN ('none', 'year', 'month', 'day')
    THEN (metadata ->> 'date_precision')::object_date_precision
    ELSE date_precision
  END,
  date_approximate = CASE
    WHEN metadata ? 'date_approximate'
      AND jsonb_typeof(metadata -> 'date_approximate') = 'boolean'
    THEN (metadata ->> 'date_approximate')::boolean
    ELSE date_approximate
  END,
  description = CASE
    WHEN description IS NULL
      AND metadata ? 'description'
      AND jsonb_typeof(metadata -> 'description') = 'string'
      AND length(trim(metadata ->> 'description')) > 0
    THEN metadata ->> 'description'
    ELSE description
  END,
  language_code = CASE
    WHEN language_code IS NULL
      AND metadata ? 'language'
      AND jsonb_typeof(metadata -> 'language') = 'string'
      AND length(trim(metadata ->> 'language')) > 0
    THEN trim(metadata ->> 'language')
    ELSE language_code
  END;

UPDATE objects
SET
  publication_date = '',
  date_approximate = false
WHERE date_precision = 'none';

CREATE TABLE IF NOT EXISTS object_people (
  object_id text NOT NULL REFERENCES objects(object_id) ON DELETE CASCADE,
  person_name text NOT NULL,
  sort_order integer NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (object_id, person_name),
  CHECK (length(trim(person_name)) > 0),
  CHECK (sort_order >= 0)
);

CREATE INDEX IF NOT EXISTS object_people_object_order_idx
  ON object_people (object_id, sort_order ASC, person_name ASC);

INSERT INTO object_people (object_id, person_name, sort_order)
SELECT
  obj.object_id,
  person_name,
  sort_order - 1
FROM objects obj,
LATERAL (
  SELECT value AS person_name, ordinality AS sort_order
  FROM jsonb_array_elements_text(
    CASE
      WHEN obj.metadata ? 'people' AND jsonb_typeof(obj.metadata -> 'people') = 'array'
      THEN obj.metadata -> 'people'
      ELSE '[]'::jsonb
    END
  ) WITH ORDINALITY
) extracted
ON CONFLICT (object_id, person_name) DO NOTHING;

CREATE TABLE IF NOT EXISTS object_edit_revisions (
  object_id text PRIMARY KEY REFERENCES objects(object_id) ON DELETE CASCADE,
  revision integer NOT NULL DEFAULT 0 CHECK (revision >= 0),
  updated_at timestamptz NOT NULL DEFAULT now(),
  updated_by uuid REFERENCES users(id) ON DELETE SET NULL
);

INSERT INTO object_edit_revisions (object_id, revision)
SELECT object_id, 0
FROM objects
ON CONFLICT (object_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS object_edit_events (
  id uuid PRIMARY KEY,
  object_id text NOT NULL REFERENCES objects(object_id) ON DELETE CASCADE,
  tenant_id uuid NOT NULL,
  type text NOT NULL,
  actor_user_id uuid REFERENCES users(id) ON DELETE SET NULL,
  revision_before integer NOT NULL CHECK (revision_before >= 0),
  revision_after integer NOT NULL CHECK (revision_after >= 0),
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (type IN ('METADATA_UPDATED', 'RIGHTS_UPDATED', 'DOCUMENT_PAGE_UPDATED', 'CURATION_SUBMITTED'))
);

ALTER TABLE object_edit_events
  DROP CONSTRAINT IF EXISTS object_edit_events_type_check;

ALTER TABLE object_edit_events
  ADD CONSTRAINT object_edit_events_type_check
  CHECK (type IN ('METADATA_UPDATED', 'RIGHTS_UPDATED', 'DOCUMENT_PAGE_UPDATED', 'CURATION_SUBMITTED'));

CREATE INDEX IF NOT EXISTS object_edit_events_object_created_idx
  ON object_edit_events (object_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS object_edit_events_tenant_created_idx
  ON object_edit_events (tenant_id, created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS object_curated_document_pages (
  object_id text NOT NULL REFERENCES objects(object_id) ON DELETE CASCADE,
  page_number integer NOT NULL CHECK (page_number > 0),
  curated_text text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  updated_by uuid REFERENCES users(id) ON DELETE SET NULL,
  PRIMARY KEY (object_id, page_number)
);

CREATE INDEX IF NOT EXISTS object_curated_document_pages_object_idx
  ON object_curated_document_pages (object_id, page_number ASC);
