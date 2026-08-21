LOCK TABLE archive_requests IN EXCLUSIVE MODE;

DO $$
DECLARE
  conflicting_group record;
BEGIN
  SELECT tenant_id, target_id, count(*)::int AS processing_count
  INTO conflicting_group
  FROM archive_requests
  WHERE target_type = 'object'
    AND action_type = 'curation_apply'
    AND status = 'PROCESSING'
  GROUP BY tenant_id, target_id
  HAVING count(*) > 1
  ORDER BY tenant_id, target_id
  LIMIT 1;

  IF FOUND THEN
    RAISE EXCEPTION USING
      ERRCODE = 'P0001',
      MESSAGE = format(
        'Migration 0015 cannot reconcile multiple PROCESSING curation_apply requests for tenant_id=%s object_id=%s processing_count=%s. Quiesce workers and reconcile these requests before retrying the migration.',
        conflicting_group.tenant_id,
        conflicting_group.target_id,
        conflicting_group.processing_count
      );
  END IF;
END $$;

WITH ranked_active_requests AS (
  SELECT
    id,
    status AS previous_status,
    first_value(id) OVER (
      PARTITION BY tenant_id, target_id
      ORDER BY (status = 'PROCESSING') DESC, created_at DESC, id DESC
    ) AS retained_request_id,
    row_number() OVER (
      PARTITION BY tenant_id, target_id
      ORDER BY (status = 'PROCESSING') DESC, created_at DESC, id DESC
    ) AS active_rank
  FROM archive_requests
  WHERE target_type = 'object'
    AND action_type = 'curation_apply'
    AND status IN ('PENDING', 'PROCESSING')
), duplicate_active_requests AS (
  SELECT id, previous_status, retained_request_id
  FROM ranked_active_requests
  WHERE active_rank > 1
)
UPDATE archive_requests req
SET status = 'CANCELED',
    failure_reason = 'Canceled by migration 0015: safe active curation_apply request retained.',
    failure_details = jsonb_build_object(
      'migration', '0015_one_active_curation_apply_per_object',
      'reason', 'duplicate_active_curation_apply',
      'survivor_policy', 'processing_then_newest_pending',
      'previous_status', dupe.previous_status::text,
      'retained_request_id', dupe.retained_request_id
    ),
    lease_expires_at = NULL,
    released_at = COALESCE(req.released_at, now()),
    completed_at = COALESCE(req.completed_at, now()),
    updated_at = now()
FROM duplicate_active_requests dupe
WHERE req.id = dupe.id;

CREATE UNIQUE INDEX archive_requests_one_active_curation_apply_per_object_idx
  ON archive_requests (tenant_id, target_id)
  WHERE target_type = 'object'
    AND action_type = 'curation_apply'
    AND status IN ('PENDING', 'PROCESSING');
