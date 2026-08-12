CREATE TABLE IF NOT EXISTS object_artifact_search_documents (
  artifact_id uuid PRIMARY KEY REFERENCES object_artifacts(id) ON DELETE CASCADE,
  available_file_id uuid REFERENCES object_available_files(id) ON DELETE SET NULL,
  text_content text,
  indexed_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT object_artifact_search_documents_content_check
    CHECK (text_content IS NULL OR length(trim(text_content)) > 0),
  CONSTRAINT object_artifact_search_documents_indexed_at_check
    CHECK ((text_content IS NULL) = (indexed_at IS NULL))
);

CREATE INDEX IF NOT EXISTS object_artifact_search_documents_available_file_idx
  ON object_artifact_search_documents (available_file_id)
  WHERE available_file_id IS NOT NULL;
