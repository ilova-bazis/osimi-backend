# Ingestion Pivot - Final DDL (Best Practice)

This is the finalized DDL direction for moving from `1 ingestion -> 1 object` to
`1 ingestion -> many items -> many objects`, with strict DB-level integrity.

## Decisions Locked

- Use `sort_order` (not `source_order`).
- Use 1-based ordering (`item_index > 0`, `sort_order > 0`).
- Enforce deterministic ordering at insert time (`sort_order NOT NULL`).
- Keep strict cross-ingestion integrity in DB using composite foreign keys.
- Pivot object provenance to item-level (`source_ingestion_item_id`).

---

```sql
-- 1) New enums
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


-- 2) Item boundary table
CREATE TABLE IF NOT EXISTS ingestion_items (
  id uuid PRIMARY KEY,
  ingestion_id uuid NOT NULL REFERENCES ingestions(id) ON DELETE CASCADE,

  -- 1-based order of items in ingestion envelope
  item_index integer NOT NULL CHECK (item_index > 0),

  status ingestion_item_status NOT NULL DEFAULT 'PENDING',

  -- per-item metadata overrides (ingestion fields remain defaults/hints)
  classification_type ingestion_classification_type,
  item_kind ingest_item_kind,
  language_code text,
  title text,

  summary jsonb NOT NULL DEFAULT '{}'::jsonb,
  error_summary jsonb NOT NULL DEFAULT '{}'::jsonb,

  -- filled when object is created for this item
  object_id text REFERENCES objects(object_id) ON DELETE SET NULL,

  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),

  CHECK (language_code IS NULL OR length(trim(language_code)) > 0),
  CHECK (jsonb_typeof(summary) = 'object'),
  CHECK (jsonb_typeof(error_summary) = 'object'),

  UNIQUE (ingestion_id, item_index)
);

CREATE INDEX IF NOT EXISTS ingestion_items_ingestion_idx
  ON ingestion_items (ingestion_id, item_index);

CREATE INDEX IF NOT EXISTS ingestion_items_status_idx
  ON ingestion_items (status, updated_at ASC, id ASC);

CREATE INDEX IF NOT EXISTS ingestion_items_object_idx
  ON ingestion_items (object_id)
  WHERE object_id IS NOT NULL;


-- 3) File membership table (grouping + per-item order)
CREATE TABLE IF NOT EXISTS ingestion_item_files (
  id uuid PRIMARY KEY,
  ingestion_item_id uuid NOT NULL REFERENCES ingestion_items(id) ON DELETE CASCADE,
  ingestion_file_id uuid NOT NULL REFERENCES ingestion_files(id) ON DELETE CASCADE,

  -- shadow key for strict composite-FK integrity checks
  ingestion_id uuid NOT NULL,

  role ingestion_item_file_role NOT NULL DEFAULT 'primary',

  -- 1-based deterministic order inside the item
  sort_order integer NOT NULL CHECK (sort_order > 0),

  page_number integer CHECK (page_number IS NULL OR page_number > 0),
  is_primary boolean NOT NULL DEFAULT false,
  logical_label text,

  created_at timestamptz NOT NULL DEFAULT now(),

  UNIQUE (ingestion_item_id, ingestion_file_id),
  UNIQUE (ingestion_item_id, sort_order)
);

CREATE INDEX IF NOT EXISTS ingestion_item_files_item_idx
  ON ingestion_item_files (ingestion_item_id, sort_order, id);

CREATE INDEX IF NOT EXISTS ingestion_item_files_file_idx
  ON ingestion_item_files (ingestion_file_id);

CREATE UNIQUE INDEX IF NOT EXISTS ingestion_item_files_one_primary_per_item_idx
  ON ingestion_item_files (ingestion_item_id)
  WHERE is_primary = true;


-- 4) Composite FK support constraints
-- (needed so ingestion_item_files cannot link file/item across different ingestions)

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'ingestion_items_id_ingestion_unique'
  ) THEN
    ALTER TABLE ingestion_items
      ADD CONSTRAINT ingestion_items_id_ingestion_unique
      UNIQUE (id, ingestion_id);
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'ingestion_files_id_ingestion_unique'
  ) THEN
    ALTER TABLE ingestion_files
      ADD CONSTRAINT ingestion_files_id_ingestion_unique
      UNIQUE (id, ingestion_id);
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'ingestion_item_files_item_fk'
  ) THEN
    ALTER TABLE ingestion_item_files
      ADD CONSTRAINT ingestion_item_files_item_fk
      FOREIGN KEY (ingestion_item_id, ingestion_id)
      REFERENCES ingestion_items (id, ingestion_id)
      ON DELETE CASCADE;
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'ingestion_item_files_file_fk'
  ) THEN
    ALTER TABLE ingestion_item_files
      ADD CONSTRAINT ingestion_item_files_file_fk
      FOREIGN KEY (ingestion_file_id, ingestion_id)
      REFERENCES ingestion_files (id, ingestion_id)
      ON DELETE CASCADE;
  END IF;
END $$;


-- 5) Object provenance pivot: ingestion-level uniqueness -> item-level uniqueness
ALTER TABLE objects
  ADD COLUMN IF NOT EXISTS source_ingestion_item_id uuid
  REFERENCES ingestion_items(id)
  ON DELETE SET NULL;

DROP INDEX IF EXISTS objects_source_ingestion_unique_idx;

CREATE UNIQUE INDEX IF NOT EXISTS objects_source_ingestion_item_unique_idx
  ON objects (source_ingestion_item_id)
  WHERE source_ingestion_item_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS objects_source_ingestion_idx
  ON objects (source_ingestion_id)
  WHERE source_ingestion_id IS NOT NULL;


-- 6) Item-level event granularity
ALTER TABLE object_events
  ADD COLUMN IF NOT EXISTS ingestion_item_id uuid
  REFERENCES ingestion_items(id)
  ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS object_events_ingestion_item_created_idx
  ON object_events (ingestion_item_id, created_at DESC);


-- 7) Optional (recommended) ingestion aggregate status for partial success
ALTER TYPE ingestion_status ADD VALUE IF NOT EXISTS 'COMPLETED_WITH_ERRORS';
```

---

## Notes

- `ingestion_item_files.ingestion_id` is intentionally denormalized to guarantee DB-level consistency between item and file ingestion boundaries.
- Client/backend contracts should refer to `sort_order` consistently.
- Lease payload ordering target per item: `sort_order ASC`, then deterministic tie-breakers.
