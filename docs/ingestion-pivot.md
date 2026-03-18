## Ingestion Pivot

Right now the biggest blocker is this:

```sql
CREATE UNIQUE INDEX IF NOT EXISTS objects_source_ingestion_unique_idx
  ON objects (source_ingestion_id)
  WHERE source_ingestion_id IS NOT NULL;
```

That index encodes:

> one ingestion -> at most one object

But your new requirement is:

> one ingestion -> many items -> many objects

So the pivot is mostly about introducing **`ingestion_items`** and **`ingestion_item_files`**, then changing provenance from ingestion-level to item-level.

---

# What your current schema already does well

You already have:

* `ingestions` as submission envelope
* `ingestion_files` as uploaded physical files
* `ingestion_leases` for worker ownership
* `objects` as permanent archive records
* `object_events` for lifecycle/event history
* `object_available_files` / `object_artifacts` for resulting files

That is good. The missing layer is only:

* **logical archival item inside an ingestion**

So the safest evolution is:

## Keep

* `ingestions`
* `ingestion_files`
* `ingestion_leases`
* `objects`
* `object_events`

## Add

* `ingestion_items`
* `ingestion_item_files`

## Adjust

* `objects.source_ingestion_id`
* event model
* ingestion statuses and completion rules

---

# Main conceptual pivot

Your current structure behaves like this:

```text
ingestion
  -> files
  -> one object
```

You want:

```text
ingestion
  -> ingestion_items
      -> ingestion_item_files
      -> one object each
```

So the real model becomes:

```text
one ingestion = one submission envelope
one ingestion item = one future object boundary
one ingestion file = one uploaded physical file
```

---

# What I would change first

## 1. Add `ingestion_items`

This becomes the object-boundary table.

I would add a new enum first.

```sql
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
```

Optional but useful:

```sql
DO $$
BEGIN
  CREATE TYPE ingestion_item_grouping_mode AS ENUM (
    'single_file',
    'grouped_files',
    'ordered_pages',
    'front_back_pair',
    'compound'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;
```

Then the table:

```sql
CREATE TABLE IF NOT EXISTS ingestion_items (
  id uuid PRIMARY KEY,
  ingestion_id uuid NOT NULL REFERENCES ingestions(id) ON DELETE CASCADE,
  tenant_id uuid NOT NULL,

  item_index integer NOT NULL,
  status ingestion_item_status NOT NULL DEFAULT 'PENDING',

  grouping_mode ingestion_item_grouping_mode NOT NULL DEFAULT 'single_file',

  classification_type ingestion_classification_type,
  item_kind ingest_item_kind,
  language_code text,

  title text,
  summary jsonb NOT NULL DEFAULT '{}'::jsonb,
  error_summary jsonb NOT NULL DEFAULT '{}'::jsonb,

  object_id text REFERENCES objects(object_id) ON DELETE SET NULL,

  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),

  CHECK (item_index > 0),
  CHECK (language_code IS NULL OR length(trim(language_code)) > 0),
  CHECK (jsonb_typeof(summary) = 'object'),
  CHECK (jsonb_typeof(error_summary) = 'object'),

  UNIQUE (ingestion_id, item_index)
);
```

---

## 2. Add `ingestion_item_files`

This is the grouping table you were missing earlier.

Optional enum:

```sql
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
```

Table:

```sql
CREATE TABLE IF NOT EXISTS ingestion_item_files (
  id uuid PRIMARY KEY,
  ingestion_item_id uuid NOT NULL REFERENCES ingestion_items(id) ON DELETE CASCADE,
  ingestion_file_id uuid NOT NULL REFERENCES ingestion_files(id) ON DELETE CASCADE,

  role ingestion_item_file_role NOT NULL DEFAULT 'primary',
  sort_order integer NOT NULL DEFAULT 1,
  page_number integer,
  is_primary boolean NOT NULL DEFAULT false,
  logical_label text,

  created_at timestamptz NOT NULL DEFAULT now(),

  CHECK (sort_order > 0),
  CHECK (page_number IS NULL OR page_number > 0),

  UNIQUE (ingestion_item_id, ingestion_file_id),
  UNIQUE (ingestion_item_id, sort_order)
);
```

This gives you exactly:

* `item_index` = order of items within ingestion
* `sort_order` = order of files within item

---

# What to change in existing tables

## 3. Change `objects` provenance model

Right now `objects.source_ingestion_id` and its unique index imply one object per ingestion.

You have two options.

## Option A — minimal invasive change

Keep `source_ingestion_id`, but remove uniqueness and add `source_ingestion_item_id`.

This is the best path.

### Add new column

```sql
ALTER TABLE objects
  ADD COLUMN IF NOT EXISTS source_ingestion_item_id uuid
  REFERENCES ingestion_items(id)
  ON DELETE SET NULL;
```

### Drop old uniqueness constraint

```sql
DROP INDEX IF EXISTS objects_source_ingestion_unique_idx;
```

### Replace with item-level uniqueness

```sql
CREATE UNIQUE INDEX IF NOT EXISTS objects_source_ingestion_item_unique_idx
  ON objects (source_ingestion_item_id)
  WHERE source_ingestion_item_id IS NOT NULL;
```

### Keep ingestion lookup index

```sql
CREATE INDEX IF NOT EXISTS objects_source_ingestion_idx
  ON objects (source_ingestion_id)
  WHERE source_ingestion_id IS NOT NULL;
```

Now your rules become:

* many objects may share same `source_ingestion_id`
* one item may map to at most one object

That is exactly what you want.

---

# Strong recommendation about duplicated metadata

Your current `ingestions` table has:

* `classification_type`
* `item_kind`
* `language_code`

These fields made sense when ingestion implied one object.
With multi-object ingestion, they become ambiguous.

Because in one ingestion you may have:

* one photo
* one letter
* one scanned document

So I would redefine them.

## Recommendation

Keep them on `ingestions` for now, but reinterpret them as:

* default values / submission-level hints
* not authoritative per-object metadata

Then move real per-item classification to `ingestion_items`.

So:

* `ingestions.classification_type` = default classification for newly created items
* `ingestions.item_kind` = default kind
* `ingestions.language_code` = default language

And on `ingestion_items`, these become per-item override fields.

That avoids a destructive migration right now.

---

# Ingestion status model: what changes

Your current `ingestion_status` is:

* DRAFT
* UPLOADING
* QUEUED
* PROCESSING
* COMPLETED
* FAILED
* CANCELED

This is still usable, but with multiple items you should decide how to handle partial success.

Right now `COMPLETED` probably implies one successful final object.

With multi-item processing, I strongly recommend adding one more state:

```sql
ALTER TYPE ingestion_status ADD VALUE IF NOT EXISTS 'COMPLETED_WITH_ERRORS';
```

Then semantics become:

* `COMPLETED` = all items completed successfully
* `COMPLETED_WITH_ERRORS` = at least one item completed and at least one failed
* `FAILED` = no useful item completed, or ingestion-level failure prevented processing

That will help the UI a lot.

---

# Add item reference to events

Your `object_events` table is already useful, but it lacks item granularity.

Add:

```sql
ALTER TABLE object_events
  ADD COLUMN IF NOT EXISTS ingestion_item_id uuid
  REFERENCES ingestion_items(id)
  ON DELETE SET NULL;
```

Then add index:

```sql
CREATE INDEX IF NOT EXISTS object_events_ingestion_item_created_idx
  ON object_events (ingestion_item_id, created_at DESC);
```

This lets you record events like:

* item 1 started
* item 1 completed
* item 2 failed
* object created for item 3

without ambiguity.

---

# Extend `object_event_type`

Current enum is close, but it is ingestion-heavy and object-heavy, not item-aware.

You do not need to remove existing values. Just extend.

I would add:

* `INGESTION_ITEM_CREATED`
* `INGESTION_ITEM_UPDATED`
* `INGESTION_ITEM_PROCESSING`
* `INGESTION_ITEM_COMPLETED`
* `INGESTION_ITEM_FAILED`

That way the worker can post both:

* ingestion-level progress
* item-level progress
