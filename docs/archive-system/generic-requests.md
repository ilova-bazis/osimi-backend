# Archive Generic Requests Worker Guide

This guide explains the generic archive request queue used for worker-driven archive actions.

It is intended for archive-system developers implementing worker loops and handlers.

For full HTTP contracts, see `docs/api-reference.md` (`## Worker APIs`).

## 1) Why this exists

- We now support archive-facing operations through one generic queue model.
- Instead of creating a new queue table and lease API for every feature, workers consume a single archive request protocol.
- Current production action:
  - `object_resync`
- Additional supported / planned actions:
  - `artifact_fetch`
  - `curation_apply`

## 2) Concepts

- `archive_request`:
  - one queued unit of archive work
- `target_type`:
  - what entity this work applies to (`object|ingestion`)
- `target_id`:
  - concrete target id (for `object`, this is `OBJ-...`)
- `action_type`:
  - what to do (`object_resync|artifact_fetch|curation_apply`)
- `action_payload`:
  - action-specific JSON object
- `lease_token`:
  - short-lived signed token proving active lease ownership

## 3) Data model (worker-relevant fields)

Each request includes:

- identity: `id`, `tenant_id`
- routing: `target_type`, `target_id`, `action_type`, `action_payload`
- requester metadata: `requested_by`, `dedupe_key` (`string|null`; `null` has no keyed active-request deduplication identity)
- status: `PENDING|PROCESSING|COMPLETED|FAILED|CANCELED`
- failure: `failure_reason`, `failure_details`
- timestamps: `created_at`, `updated_at`, `completed_at`

Worker lease internals (`lease_id`, `lease_token_id`, `lease_expires_at`, `leased_by`, `released_at`) are backend-managed and not a worker contract except via lease response payload.

## 4) Authentication and lease rules

- All archive-request worker endpoints require:
  - `x-worker-auth-token: <WORKER_AUTH_TOKEN>`
- Optional:
  - `x-worker-id: <worker-id>`
- Lease-protected endpoints additionally require body field:
  - `lease_token`

Tenant rule (important):

- Archive workers must not provide `tenant_id` in request bodies.
- Backend resolves tenant context from request ownership and lease token context.
- `tenant_id` in lease responses is informational only for logs/observability.

Important:

- Worker does not sign or verify lease tokens. It stores and forwards them.
- Every successful heartbeat returns a refreshed token. Replace local token immediately.

## 5) Endpoints and flow

## 5.1 Lease next request

`POST /api/archive-requests/lease`

Body (optional):

```json
{
  "action_type": "object_resync"
}
```

Notes:

- Body may be omitted (`{}` is valid).
- If `action_type` is provided, leasing is filtered to that action only.
- Lease TTL is 5 minutes.
- Backend sweeps expired leases before selecting new work.
- `tenant_id` is not accepted as input.

Response (`200`):

```json
{
  "request": {
    "request_id": "uuid",
    "lease_id": "uuid",
    "lease_token": "token",
    "lease_expires_at": "2026-03-09T12:00:00.000Z",
    "tenant_id": "uuid",
    "target_type": "object",
    "target_id": "OBJ-20260306-RSYNC1",
    "action_type": "object_resync",
    "action_payload": {},
    "requested_by": "uuid",
    "dedupe_key": "object_resync:OBJ-20260306-RSYNC1"
  }
}
```

No work:

```json
{
  "request": null
}
```

## 5.2 Heartbeat lease

`POST /api/archive-requests/:id/lease/heartbeat`

Body:

```json
{
  "lease_token": "token"
}
```

- `tenant_id` is not accepted as input.

Response (`200`):

```json
{
  "request": {
    "request_id": "uuid",
    "lease_id": "uuid",
    "lease_token": "new-token",
    "lease_expires_at": "2026-03-09T12:05:00.000Z"
  }
}
```

Rule: replace local token with the new `lease_token`.

## 5.3 Release lease (requeue)

`POST /api/archive-requests/:id/lease/release`

Body:

```json
{
  "lease_token": "token"
}
```

- `tenant_id` is not accepted as input.

Response (`200`):

```json
{
  "status": "ok",
  "request_id": "uuid"
}
```

Behavior:

- request transitions from `PROCESSING` back to `PENDING`
- use for graceful shutdown or temporary abandonment

## 5.4 Complete request

`POST /api/archive-requests/:id/complete`

Body:

```json
{
  "lease_token": "token",
  "upload_token": "optional-upload-token"
}
```

- `tenant_id` is not accepted as input.
- For `artifact_fetch`, `upload_token` is required.
- For non-`artifact_fetch` actions, `upload_token` is ignored when present.

Response (`200`):

```json
{
  "status": "completed",
  "request": {
    "id": "uuid",
    "tenant_id": "uuid",
    "target_type": "object",
    "target_id": "OBJ-20260306-RSYNC1",
    "action_type": "object_resync",
    "action_payload": {},
    "requested_by": "uuid",
    "dedupe_key": "object_resync:OBJ-20260306-RSYNC1",
    "status": "COMPLETED",
    "failure_reason": null,
    "failure_details": null,
    "created_at": "2026-03-09T11:55:00.000Z",
    "updated_at": "2026-03-09T12:01:00.000Z",
    "completed_at": "2026-03-09T12:01:00.000Z"
  }
}
```

## 5.4a Presign artifact upload

`POST /api/archive-requests/:id/artifacts/presign`

Body:

```json
{
  "lease_token": "token",
  "content_type": "text/plain",
  "size_bytes": 11,
  "extension": "txt"
}
```

- Supported for `artifact_fetch` requests only.

Response (`200`):

```json
{
  "upload_token": "signed-upload-token",
  "upload_url": "/api/archive-requests/uploads/signed-upload-token",
  "storage_key": "tenants/.../objects/.../artifacts/...txt",
  "expires_at": "2026-03-09T12:15:00.000Z",
  "headers": {
    "content-type": "text/plain",
    "content-length": 11
  }
}
```

## 5.5 Fail request

`POST /api/archive-requests/:id/fail`

Body:

```json
{
  "lease_token": "token",
  "failure": {
    "code": "SYNC_FAILED",
    "message": "Archive source unavailable",
    "retryable": true,
    "details": {
      "upstream_status": 503
    }
  }
}
```

- `tenant_id` is not accepted as input.

Response (`200`):

```json
{
  "status": "failed",
  "request_id": "uuid",
  "retryable": true
}
```

Behavior:

- backend stores `failure_reason` from `failure.message`
- backend stores `failure_details` object
- if `failure.details` is omitted, backend stores `{}`

## 6) Current action contract: `object_resync`

Current producer endpoint:

- `POST /api/objects/:object_id/resync`

Request creation behavior:

- If no active request exists for the object, backend creates one and returns `201`.
- If active `PENDING|PROCESSING` request exists, backend dedupes and returns existing one with `200`.

Worker handling expectation:

- filter leasing with `{"action_type": "object_resync"}`
- when request leased:
  - call archive system to refresh available-file snapshot for `target_id`
  - write snapshot via existing endpoint:
    - `PUT /api/internal/objects/:object_id/available-files`
  - if refresh succeeded, call `/complete`
  - if refresh failed, call `/fail`

## `curation_apply` action contract

Purpose:

- replace the full contents of one curated text file version for an object
- no partial patch semantics
- canonical update unit is the file

Queue request shape:

- `target_type = object`
- `target_id = OBJ-...`
- `action_type = curation_apply`

`action_payload`:

```json
{
  "object_id": "OBJ-20260417-000001",
  "curated_kind": "transcript_curated",
  "target_version": "20260417",
  "source_ref": {
    "type": "signed_download_url",
    "url": "https://..."
  },
  "content_type": "text/plain",
  "idempotency_key": "OBJ-20260417-000001:transcript_curated:20260417"
}
```

Supported `curated_kind` values in v1:

- `transcript_curated`
- `ocr_curated`

Rules:

- `target_version` is a UTC day string in `YYYYMMDD`
- all edits for the same object/kind/day replace the same file version
- worker downloads the full replacement file from `source_ref.url`
- worker writes the target curated file atomically
- worker updates `meta/object_text_manifest.json`
- worker enqueues `object_text_manifest_snapshot`
- worker completes the request on success

Success boundary:

- `curation_apply` is considered successful once archive has durably written the target curated file and updated local `meta/object_text_manifest.json`
- republishing refreshed object text state to VPS is a downstream synchronization step and may complete asynchronously
- if republish enqueue fails after local apply succeeds, archive should log and retry separately, but should not treat the apply itself as failed

Suggested file naming:

- `transcript/transcript_curated_YYYYMMDD.txt`
- `ocr/ocr_curated_YYYYMMDD.txt`

Failure code guidance:

- `CURATION_INVALID_PAYLOAD`
- `CURATION_UNSUPPORTED_KIND`
- `CURATION_INVALID_VERSION`
- `CURATION_SOURCE_DOWNLOAD_FAILED`
- `CURATION_SOURCE_TOO_LARGE`
- `CURATION_WRITE_FAILED`
- `CURATION_MANIFEST_UPDATE_FAILED`

## 7) Status machine

Expected transitions:

- `PENDING -> PROCESSING` (lease)
- `PROCESSING -> PROCESSING` (heartbeat)
- `PROCESSING -> PENDING` (release or lease expiry sweep)
- `PROCESSING -> COMPLETED` (complete)
- `PROCESSING -> FAILED` (fail)

## 8) Error handling and retries

General handling:

- `400 BAD_REQUEST`:
  - invalid body/path
  - fix serializer/request builder; do not blind retry
- `401 UNAUTHORIZED`:
  - missing/invalid worker auth token, invalid/expired lease token
  - fix credentials/token handling; do not blind retry
- `409 CONFLICT`:
  - when `error.details.reason` is `expected_checksum_mismatch`, the body was rejected before publication; correct or re-read the source bytes and retry the same upload URL while its token and lease remain active
  - do not blindly resend the same rejected bytes, and do not call `/fail` solely for this recoverable mismatch
  - when `error.details.reason` is `accepted_checkpoint_mismatch`, backend ownership has already transferred; never replace the checkpoint, do not call `/fail` or re-lease, and resend only the exact accepted bytes if acknowledgment requires it
  - when `error.details.reason` is `accepted_checkpoint_storage_conflict`, stop upload retries and escalate backend storage repair
  - when `error.details.reason` is `artifact_source_missing`, `artifact_source_identity_changed`, or `artifact_source_checksum_invalid`, fail the stale request; re-leasing cannot repair its persisted source identity
  - for other conflicts, the lease or upload attempt may no longer be active; stop work for that lease context and reacquire with `/lease`
- `500 CONFIGURATION_ERROR`:
  - backend worker auth not configured
  - operational fix required
- other `5xx`:
  - retry with exponential backoff + jitter

Failure ownership rule:

- If archive work is definitively failed for this request, call `/fail`.
- If worker is shutting down or temporarily unable to continue, call `/lease/release`.

## 9) Polling and heartbeat recommendations

- Poll interval when no work: jittered 2-10 seconds.
- Heartbeat interval while processing: 60-120 seconds.
- Always maintain local deadline from `lease_expires_at` and refresh before expiry.

## 10) Minimal worker loop (pseudo)

```text
loop:
  lease = POST /api/archive-requests/lease {}
  if lease.request == null:
    sleep(jittered_poll_interval)
    continue

  request = lease.request
  token = request.lease_token
  start heartbeat timer

  try:
    if request.action_type == "artifact_fetch":
      download source bytes from archive storage
      presign = POST /api/archive-requests/:id/artifacts/presign { lease_token: token, ... }
      PUT presign.upload_url with bytes
      POST /api/archive-requests/:id/complete { lease_token: token, upload_token: presign.upload_token }
    else if request.action_type == "object_resync":
      refresh archive metadata for request.target_id
      PUT /api/internal/objects/:object_id/available-files
      POST /api/archive-requests/:id/complete { lease_token: token }
    else if request.action_type == "curation_apply":
      download curated replacement file
      write curated target version
      update meta/object_text_manifest.json
      enqueue object_text_manifest_snapshot
      POST /api/archive-requests/:id/complete { lease_token: token }
    else:
      POST /api/archive-requests/:id/fail { lease_token: token, failure: unsupported_action }
  catch err:
    if err.details.reason == "expected_checksum_mismatch":
      correct or re-read source bytes and retry the same active upload URL
      if correction cannot continue while the lease is active: release the lease
    else if err.details.reason == "accepted_checkpoint_mismatch":
      do not fail or re-lease backend-owned work; retry only exact accepted bytes if needed
    else if err.details.reason == "accepted_checkpoint_storage_conflict":
      stop upload retries and alert backend storage operations
    else if err.details.reason in ["artifact_source_missing", "artifact_source_identity_changed", "artifact_source_checksum_invalid"]:
      POST /api/archive-requests/:id/fail { lease_token: token, failure: stale_source_error }
    else if err.status == 409:
      stop the stale lease context and return to leasing; do not call /fail
    else:
      POST /api/archive-requests/:id/fail { lease_token: token, failure: mapped_error }
  finally:
    stop heartbeat timer
```

An available-file source becoming inactive after queueing does not cancel the
request. Presign still binds to that exact retained source and its known
checksum. A missing source or changed artifact kind/variant returns a `409`
source-identity reason before any upload authorization; malformed historical
source checksums are rejected the same way. The worker must fail or quarantine
that stale request rather than re-lease it or upload unverified replacement
bytes.

For `artifact_fetch`, a successful `PUT` is the durable handoff boundary. The
backend owns the synchronized and checksum-verified immutable bytes from that
point, may complete the request synchronously, and retries finalization in the
background. Finalization revalidates the current file against the accepted size
and SHA-256 before committing side effects. The worker still calls `/complete`
as an idempotent acknowledgment. Once the upload is materialized, that
acknowledgment does not read staged bytes or mutate or repair the search
projection. Verified work is not reassigned or uploaded again if the worker
exits after `PUT`.

## 11) Implementation checklist for archive team

- Include `x-worker-auth-token` on all archive-request control calls.
- Optionally include stable `x-worker-id` for observability.
- Store latest `lease_token` and rotate it after heartbeat.
- Branch handler by `action_type`.
- For `object_resync` and `curation_apply`, use `target_id` as canonical object id.
- Use `/complete` on success, `/fail` on business failure, `/lease/release` on graceful abandonment.
- Treat `409` as lease loss unless structured details identify a checksum case: `expected_checksum_mismatch` permits corrected bytes on the same active URL, `accepted_checkpoint_mismatch` permits only exact accepted bytes without fail/re-lease, and `accepted_checkpoint_storage_conflict` requires backend storage repair.
- Retry transient network/server failures with jittered backoff.

## 12) Relationship to other worker guides

- Ingestion pipeline workflow remains in `docs/archive-system-integration.md`.
- Available-files snapshot semantics are in `docs/archive-system-available-files-sync.md`.
- Generic queue/lease API contract is in this document.
