# Worker API Integration Delta

Status: review notes, prepared 2026-10-26.
Verified against: `main` at `e7ffb4c` (2026-09-25, "update: rate limit added"), clean worktree.
Method: read-only inspection of routes, services, repositories, validation schemas, tests, and git history. Nothing was executed against a database or a deployed worker.

This document records the confirmed divergences between the current worker-facing implementation and the current worker-facing documentation, so the next worker integration or handover can be updated on exact grounds.

## Scope and Limits

- The version of the worker documentation handed to the archive-system team previously is not recorded here. Findings are therefore split into "current documentation is wrong" (section 1) and "historical change, already documented" (section 2).
- The deployed archive worker was not inspected. This report does not establish that any deployed integration is currently failing.
- Line references are valid for commit `e7ffb4c`; re-audit after the next merge to main.

## 1. Confirmed Documentation Gaps

Places where current documentation and current code disagree.

### 1.1 `object_revision_apply` is missing from the documented action list

- Confidence: high. Integration impact: significant.
- `docs/api-reference.md:2383` lists `object_resync|artifact_fetch|curation_apply`.
- `src/validation/object.ts:91` (`archiveRequestActionTypeSchema`) also accepts `object_revision_apply`, and `src/repos/archive-request-repo.ts:19` joins the same union.
- The lease request filter accepts it as well: `src/validation/object.ts:668-670` (`workerLeaseArchiveRequestBodySchema` uses that enum, optional). A worker that leases without an `action_type` filter can receive it.

Evidence that this is executable behavior, not an unused type:

- The object change submission service creates such requests.
- Integration tests lease and complete them through `/api/archive-requests/lease` (`tests/integration/http/object-change-submission-routes.test.ts:343-437`).
- The queue query applies no action restriction when the worker omits the filter.

Introduced 2026-09-23 (commit `3d8e886`).

Qualification: creating new requests is gated behind `OBJECT_REVISION_APPLY_ENABLED=true` (`src/services/object-change-submission-service.ts`, 503 `SERVICE_UNAVAILABLE` when disabled; asserted at `tests/integration/http/object-change-submission-routes.test.ts:887`). Leasing is not gated, so already-pending requests stay relevant if the flag is later turned off.

### 1.2 Completion documentation omits the required `result` for `object_revision_apply`

- Confidence: high. Integration impact: significant.
- `docs/api-reference.md:2446` documents completion with `lease_token` and optional `upload_token`.
- For `object_revision_apply`, `src/services/archive-request-service.ts:402` rejects completion without a structured `result`.
- Result schema: `src/validation/object.ts:355` (`workerObjectRevisionApplyResultSchema`).

Required `result` fields:

```text
schema_version:      "1.0"
submission_id:       uuid
object_id:           OBJ-...
object_revision:     integer >= 0
package_sha256:      64 hex chars
disposition:         "applied"   (only accepted value)
archive_revision_id: string
applied_at:          ISO datetime
```

- `disposition` accepts only `"applied"`. A failed apply must use `POST /api/archive-requests/:id/fail` instead.
- The repo layer re-checks that `submission_id`, `object_id`, `object_revision`, and `package_sha256` match the queued submission before first completion (`src/repos/object-change-submission-repo.ts:534-577`), and requires an active lease.
- Re-completion is idempotent when the stored completion already carries `applied_at`.

Consequence: a completion body built from the current documentation is insufficient for this action. Existing bodies remain valid for the other three actions.

### 1.3 Source-download documentation is inconsistent

- Confidence: high. Integration impact: significant.

Two mismatches: one against the producer code, one inside the docs themselves.

a) `docs/archive-system/api-reference-worker-snippet.md:120-143` still documents the curation payload as `source_ref.type = signed_download_url` (line 129). New curation requests carry:

```text
source_ref.type = request_source
source_ref.url  = /api/archive-requests/<request_id>/source
publication_revision
size_bytes
checksum_sha256  (64 hex)
```

The newer shape is correctly documented in `docs/archive-system/generic-requests.md:320-350`, so the snippet now contradicts both the code and the sibling document.

b) `docs/api-reference.md:2423` says the source endpoint is available only for `curation_apply`. The service allows `curation_apply` and `object_revision_apply` (`src/services/archive-request-service.ts:186`), and the route delegates to it (`src/routes/objects.ts:383-409`; the older handler name `downloadCurationPublicationSource` is retained).

Route requirements: worker authentication plus `x-archive-request-lease-token`. A `x-worker-id` mismatch is rejected only when both the request and the lease carry a worker identity, so the header itself is not always mandatory.

Introduced 2026-08-21 (commit `a4e5f31`).

Qualification: the signed-download transport still exists. `docs/curation-publication-release.md:8` describes the expected dual support during rollout. Treat this as "new curation payloads use `request_source`", not "signed URLs were removed".

## 2. Worker-Facing Changes Since Early August (Timeline)

| Date | Commit | Change | Status in current docs |
|---|---|---|---|
| 2026-08-03 | `d24bca1` | Event responses return `object_ids: string[]` (`src/services/event-service.ts:448-454`); aggregate `INGESTION_COMPLETED` rejects populated `object_id`/`ingestion_item_id` (test: `tests/integration/http/event-routes.test.ts:634`) | Documented in `docs/api-reference.md` |
| 2026-08-12 | `9af1df4` | Upload conflict semantics: `details.reason` = `expected_checksum_mismatch` with `retry_action: "retry_same_upload_url"`, plus `accepted_checkpoint_mismatch` and `accepted_checkpoint_storage_conflict` (`src/services/object-service.ts:2394-2428`) | Documented in the worker snippet |
| 2026-08-21 | `a4e5f31` | Curation payloads switch to `source_ref.type = request_source` with `publication_revision`, `size_bytes`, `checksum_sha256`; publish identity `(object_id, curated_kind, publication_revision)`; one-active `curation_apply` invariant (migrations 0015/0016) | `generic-requests.md` current; snippet and `api-reference.md` stale (see 1.3) |
| 2026-09-16 | `32ee1c0` | Deployment hardening; `GET /api/worker/downloads/:token` explicitly `auth: "none"` (token-bound) | No contract change |
| 2026-09-23 | `3d8e886` | New `object_revision_apply` action (migrations 0017-0019); completion requires `result` | Not documented anywhere in the worker docs (see 1.1, 1.2) |
| 2026-09-25 | `e7ffb4c` | Repeated invalid worker credentials produce `429 RATE_LIMITED` with `Retry-After` (failure-budget based; valid credentials return before the limiter is consulted; tests: `tests/unit/auth/worker-auth-limits.test.ts:37-90`) | Documented in `docs/api-reference.md` |

Snapshot baseline used for "new since May": `006ae7b` (2026-05-01).

## 3. Compatibility Clarification (Not a Change)

Nullable `dedupe_key` was already accepted and stored on 2026-05-01 (`006ae7b`); later commits only made the documentation explicit. List this as a documentation clarification, not evidence of a post-handover API change.

## 4. `object_revision_apply` Quick Reference

Observed `action_payload` shape (`src/services/object-change-submission-service.ts:311-329`):

```json
{
  "schema_version": "1.0",
  "submission_id": "<uuid>",
  "object_id": "OBJ-...",
  "object_revision": 7,
  "package_schema_version": "1.0",
  "source_ref": {
    "type": "request_source",
    "url": "/api/archive-requests/<request_id>/source"
  },
  "content_type": "application/vnd.osimi.object-revision+json; version=1",
  "size_bytes": 12345,
  "checksum_sha256": "<64 hex>",
  "idempotency_key": "object_revision_apply:<tenant_id>:<object_id>:editrev-<revision>:pkg-v1"
}
```

The idempotency key format is defined at `src/repos/object-change-submission-repo.ts:76`.

- Package media type: `application/vnd.osimi.object-revision+json; version=1`. Verify the downloaded bytes against `size_bytes`/`checksum_sha256` (per `generic-requests.md:348-350`).
- Completion: `POST /api/archive-requests/:id/complete` with `result` as in 1.2. Failure path: `POST /api/archive-requests/:id/fail`.
- Gating: submissions require `OBJECT_REVISION_APPLY_ENABLED=true`; leasing is not gated.

## 5. Worker Integration Checklist

1. Lease handling must tolerate (or fail gracefully on) unknown `action_type` values, or explicitly support `object_revision_apply`.
2. Support `source_ref.type = "request_source"`: resolve the relative URL against the API base, send worker auth plus `x-archive-request-lease-token`, and verify `size_bytes` and `checksum_sha256`.
3. For `object_revision_apply`, send the structured completion `result`; use `/fail` for failures (no other `disposition` is accepted).
4. Treat `429 RATE_LIMITED` on worker auth as expected under repeated credential failures and honor `Retry-After`; valid credentials are never throttled.
5. Follow `details.reason` on upload 409 responses; re-read the source and retry the same upload URL only for `expected_checksum_mismatch`; never correct already-accepted bytes.
6. Parse event responses with `object_ids: string[]` and never send `object_id`/`ingestion_item_id` on aggregate `INGESTION_COMPLETED` (400 `BAD_REQUEST`).
7. Keep `checksum_sha256` values in available-file lists as 64-hex strings and accept `dedupe_key: null`.

## 6. Documentation Update Checklist (Backend)

1. `docs/api-reference.md:2383` — add `object_revision_apply` to the `action_type` enum.
2. `docs/api-reference.md:2417-2430` — source endpoint: allow `curation_apply` and `object_revision_apply`; note the worker-id match nuance.
3. `docs/api-reference.md:2446` — completion: document the optional `result` body (required for `object_revision_apply`) and its schema.
4. `docs/archive-system/api-reference-worker-snippet.md:120-143` — replace the `signed_download_url` curation payload with the `request_source` shape; mark signed URLs as legacy.
5. `docs/archive-system/api-reference-worker-snippet.md` — add an `object_revision_apply` section (payload, completion `result`, `/fail` path).
6. Cross-reference `docs/curation-publication-release.md` for the rollout window.

## 7. Open Questions

1. Which document version was handed to the archive-system team, and when? This determines exactly which rows in section 2 are post-handover.
2. Which worker version is deployed, and does it filter action types on lease?
3. Is `OBJECT_REVISION_APPLY_ENABLED=true` set in the target environment (submissions return 503 otherwise)?
4. How long will legacy signed-URL curation payloads remain in flight?

## 8. Evidence Index

- Validation: `src/validation/object.ts:91` (action enum), `:355` (completion result), `:668-670` (lease filter), `src/validation/event.ts` (aggregate completion rejection).
- Routes: `src/routes/objects.ts:383-409` (source), `:673-688` (lease).
- Services: `src/services/archive-request-service.ts:186` (source actions), `:402` (completion result), `src/services/object-change-submission-service.ts:311-329` (payload), `src/services/object-service.ts:2394-2428` (upload conflict reasons), `src/services/event-service.ts:448-454` (`object_ids`).
- Repos: `src/repos/archive-request-repo.ts:19`, `:376`, `:408` (action unions and lookups), `src/repos/object-change-submission-repo.ts:71-77` (idempotency key), `:534-577` (completion match checks).
- Auth: `src/auth/worker.ts`, `src/routes/middleware.ts`, `src/runtime/config.ts` (`DEFAULT_WORKER_AUTH_RATE_LIMIT`).
- Tests: `tests/integration/http/object-change-submission-routes.test.ts:343-437` (full flow), `:887` (disabled gate), `tests/unit/auth/worker-auth-limits.test.ts:37-90`, `tests/integration/http/event-routes.test.ts:634` (aggregate rejection), `tests/integration/http/archive-artifact-upload-routes.test.ts:405-500` (checksum conflicts).
- Docs: `docs/api-reference.md`, `docs/archive-system/api-reference-worker-snippet.md`, `docs/archive-system/generic-requests.md`, `docs/curation-publication-release.md`.
