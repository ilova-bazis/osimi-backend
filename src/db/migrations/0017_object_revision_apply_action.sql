DO $$
BEGIN
  ALTER TYPE archive_request_action_type ADD VALUE IF NOT EXISTS 'object_revision_apply';
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;
