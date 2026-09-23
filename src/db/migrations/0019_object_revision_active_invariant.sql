CREATE UNIQUE INDEX archive_requests_one_active_object_mutation_per_object_idx
  ON archive_requests (tenant_id, target_id)
  WHERE target_type = 'object'
    AND action_type IN ('curation_apply', 'object_revision_apply')
    AND status IN ('PENDING', 'PROCESSING');
