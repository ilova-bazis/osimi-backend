DO $$
BEGIN
  CREATE TYPE object_text_manifest_media_type AS ENUM (
    'document',
    'audio',
    'video',
    'photo',
    'other'
  );
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS object_text_manifests (
  object_id text PRIMARY KEY REFERENCES objects(object_id) ON DELETE CASCADE,
  tenant_id uuid NOT NULL,
  media_type object_text_manifest_media_type NOT NULL,
  projection_version text NOT NULL,
  generated_at timestamptz NOT NULL,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  synced_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS object_text_manifests_tenant_id_idx
  ON object_text_manifests (tenant_id);