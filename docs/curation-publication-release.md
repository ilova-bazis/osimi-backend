# Curation Publication Release Runbook

This release introduces stable revision identity, durable lease-protected publication sources, and the one-active-`curation_apply` database invariant. Use a controlled maintenance window.

## Deployment Order

1. Back up `archive_requests`, `object_edits`, and `object_edit_events`, and record all active `curation_apply` rows.
2. Deploy an archive worker version that accepts both legacy `source_ref.type = signed_download_url` and new `source_ref.type = request_source`. For `request_source`, send worker auth plus the active lease token in `x-archive-request-lease-token`, and verify the payload size/SHA-256.
3. Stop publication submissions and quiesce archive-request workers. Confirm no lease/write transaction is still running.
4. Check for more than one `PROCESSING` curation request per tenant/object. Reconcile manually before migration if any exist.
5. Deploy the backend and run migrations 0015 and 0016. Migration 0015 takes an exclusive `archive_requests` lock and may wait for older transactions.
6. Verify the index, publication table, reconciliation results, and source endpoint before resuming workers.
7. Deploy the UI, resume workers, then reopen publication submissions.
8. Keep dual-source worker support until all legacy signed-URL requests are terminal and absent from retry/requeue workflows.

## Preflight SQL

```sql
SELECT tenant_id, target_id, count(*)
FROM archive_requests
WHERE action_type = 'curation_apply' AND status = 'PROCESSING'
GROUP BY tenant_id, target_id
HAVING count(*) > 1;

SELECT tenant_id, target_id, status, id, leased_by, lease_expires_at
FROM archive_requests
WHERE action_type = 'curation_apply' AND status IN ('PENDING', 'PROCESSING')
ORDER BY tenant_id, target_id, created_at DESC, id DESC;
```

The first query must return no rows. Do not automatically cancel duplicate `PROCESSING` requests because a worker may already have written archive output.

## Post-Deploy Verification

```sql
SELECT tenant_id, target_id, count(*)
FROM archive_requests
WHERE action_type = 'curation_apply' AND status IN ('PENDING', 'PROCESSING')
GROUP BY tenant_id, target_id
HAVING count(*) > 1;

SELECT indexname
FROM pg_indexes
WHERE indexname = 'archive_requests_one_active_curation_apply_per_object_idx';

SELECT count(*) FROM curation_publications;
```

- Submit one curated OCR publication and verify one request and one `curation_publications` row.
- Lease it with the compatible worker and download the source using the lease token.
- Verify the returned byte length and `x-content-sha256`, complete the request, then repeat the exact submit and confirm it returns the same completed request.
- Submit a newer revision and confirm it creates a different request.
- Check `jobs.curation_publication_source_cleanup` logs, including `failed`, `missing`, and `orphaned` counts.

## Retention

- `COMPLETED` and `CANCELED`: source becomes cleanup-eligible after 24 hours.
- `FAILED`: source becomes cleanup-eligible after 7 days.
- Active requests have no cleanup deadline.
- Missing tracked files converge to `purged_at`; untracked files under the dedicated source namespace are removed only after 24 hours.

## Rollback Limits

- Before any new request is queued, application binaries may be rolled back after restoring the pre-migration database backup.
- Migration 0015 cancellation decisions and newly queued `request_source` payloads are not safely reversible by dropping an index or table.
- After a new request is queued, keep the new backend and dual-compatible worker available until it is terminal. Do not roll back to a worker that only understands signed URLs.
- If verification fails, stop submissions and workers, preserve source files, and restore the database backup rather than attempting ad hoc reverse DDL.
