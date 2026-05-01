CREATE TABLE IF NOT EXISTS object_edits (
  object_id text PRIMARY KEY REFERENCES objects(object_id) ON DELETE CASCADE,
  revision integer NOT NULL DEFAULT 0 CHECK (revision >= 0),
  locked_by uuid REFERENCES users(id) ON DELETE SET NULL,
  locked_until timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now(),
  updated_by uuid REFERENCES users(id) ON DELETE SET NULL
);

INSERT INTO object_edits (object_id, revision, updated_at, updated_by)
SELECT object_id, revision, updated_at, updated_by
FROM object_edit_revisions
ON CONFLICT (object_id) DO NOTHING;

DROP TABLE IF EXISTS object_edit_revisions;

ALTER TABLE object_edit_events
  ALTER COLUMN revision_before DROP NOT NULL,
  ALTER COLUMN revision_after DROP NOT NULL;
