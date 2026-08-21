# Object Editing Support Spec (V1)

Status: Draft for backend/archive alignment  
Date: 2026-04-14

## Purpose

Define the backend and integration requirements to support object editing while respecting system architecture constraints:

- archive is outbound-only and worker-driven
- archive canonical edit persistence is file/version based
- VPS owns editor-facing read model, revisioning, conflict handling, and API surface

## Architecture Constraints

The following are non-negotiable:

1. VPS MUST NOT depend on direct synchronous VPS -> archive read/write APIs.
2. Archive worker MUST publish projection state to VPS asynchronously.
3. VPS MUST enqueue archive-side apply work asynchronously.
4. Archive worker MUST poll and process queued work asynchronously.
5. Canonical archive curation persistence MUST remain file/version based.

## Scope

### In Scope (V1 foundation)

- edit-read endpoint
- metadata write endpoint
- curation history endpoint
- optimistic concurrency for object editing state
- archive projection manifest ingestion contract
- `curation_apply` queue plumbing contract for future archive-side materialization

### Deferred / Conditional

- video captions curation endpoint
- durable segment identity guarantees from archive
- auto-rebase semantics across machine reprocessing
- direct row-level archive persistence for transcript/page segments

## Data Ownership Model

### Archive owns

- machine and curated text artifacts as versioned files
- active/current artifact designation per kind
- projection manifest publication
- async file materialization apply

### VPS owns

- editor API contracts
- object edit revision counter and conflict semantics
- editor-facing derived projections (pages/segments/file view)
- history/audit trail for edits and submits
- stale-state detection and UI-facing status

## Archive -> VPS Projection Manifest Contract

Archive worker MUST publish object-level projection payloads with at least:

- `object_id`
- `media_type` (`document|audio|video|photo|other`)
- `projection_version` (opaque object-level change token)
- `generated_at` (RFC3339 UTC)
- `text_artifacts[]` with `kind`, `version`, `is_active`, `reference`, and optional `metadata`

Rules:

1. For each `(object_id, kind)`, at most one artifact version MUST be active.
2. Projection publish is a full snapshot of known text artifacts for the object.
3. Unknown artifact kinds SHOULD be ignored and logged by VPS for forward compatibility.
4. VPS MUST treat `photo` as archive-side media naming and map it to `image` in editor-facing APIs.

## Backend Persistence Requirements

Backend MUST persist:

1. object edit revision state per object
2. current editable metadata state
3. latest archive projection state (`projection_version`, active text artifacts)
4. immutable object edit events with `revision_before` and `revision_after`
5. archive apply request linkage/result state when `curation_apply` is used

Backend MAY keep derived page/segment projections for editor UX.
Backend MUST NOT treat derived page/segment projections as archive canonical persistence units.

## Backend API Requirements

### GET `/api/objects/:object_id/edit`

Returns:

- object identity and media type
- current edit revision
- curation/editor state
- editable metadata (first-class fields)
- rights fields
- capability flags
- curation payload for the editor

For document objects in the current V1 slice, `curation_payload` MUST include page-by-page OCR editing data:

- `kind = document`
- `machine_ocr_artifact_id`
- `page_count`
- `pages[]` with:
  - `page_number`
  - `label`
  - `machine_text`
  - `curated_text`
  - `status`

### PATCH `/api/objects/:object_id/metadata`

Request MUST include:

- `revision`
- metadata payload
- optional rights payload

Behavior:

- compare-and-set on `revision`
- update editable first-class metadata fields
- increment revision
- emit immutable edit history event(s)

### GET `/api/objects/:object_id/curation/history`

Returns immutable object edit history with cursor pagination.

### PUT `/api/objects/:object_id/curation/document`

Request MUST include:

- `revision`
- `pages[]` containing `page_number` and `curated_text`

Behavior:

- valid only for document objects
- page numbers must match existing document page projection
- updates only the submitted pages
- increment revision
- emit immutable `DOCUMENT_PAGE_UPDATED` history event

### POST `/api/objects/:object_id/curation/submit`

Request MUST include:

- `revision`
- optional `review_note`

Behavior:

- valid for the currently supported curation media types only
- assembles the current curated OCR state into an archive-apply payload
- creates a `curation_apply` archive request
- increments revision
- emits immutable `CURATION_SUBMITTED` history event
- permits only one active (`PENDING` or `PROCESSING`) publication per tenant and object
- returns the original request for an exact revision-qualified retry, including after it is terminal, without incrementing revision
- rejects different content while a publication is active with `409 PUBLICATION_ALREADY_ACTIVE`, without retaining its staged bytes

## Revision and Conflict Semantics

1. All object editing write endpoints MUST require `revision`.
2. Any stale revision MUST return deterministic `409` conflict.
3. Revision increments by 1 for each successful edit write.
4. Archive-side apply completion/failure MUST NOT silently mutate user draft revision.
5. OCR page curation writes update only the submitted page set and MUST NOT overwrite unrelated curated pages.
6. Submit-for-review is revision-guarded and MUST create at most one active `curation_apply` request per tenant and object, regardless of target version.
7. An exact revision-qualified retry MUST return its original request in any status. A different publication MUST return `409 PUBLICATION_ALREADY_ACTIVE` while another request is active and MUST NOT increment revision or silently deduplicate newer content.
8. A terminal `curation_apply` request permits the next publication.

### Deployment Migration Safety

- Migration `0015_one_active_curation_apply_per_object.sql` reconciles legacy active duplicates before creating the database invariant.
- Migration takes an exclusive `archive_requests` table lock before inspecting or reconciling rows, so in-flight worker leases/writes finish first and no writer can enter between reconciliation and index creation.
- When exactly one request is `PROCESSING`, it is retained regardless of newer PENDING rows; duplicate PENDING rows are canceled with migration provenance and terminal timestamps.
- With no PROCESSING request, the newest PENDING request by `created_at DESC, id DESC` is retained.
- More than one PROCESSING request is not automatically reconcilable because workers may already be writing archive output. Migration fails before changing rows or creating the index, and operators must quiesce workers and reconcile the requests before retrying.

## Async `curation_apply` Contract

Queue payload SHOULD include:

- `object_id`
- `curated_kind`
- `target_version`
- lease-protected `request_source` reference to a backend-retained curated file body
- immutable `publication_revision`, byte length, and SHA-256 checkpoint

Worker result MUST include at least:

- `object_id`
- `curated_kind`
- `target_version`
- `status`
- `retryable`
- message or reason

Apply semantics:

- full-file replacement
- idempotent by `(object_id, curated_kind, publication_revision)`; `target_version` remains the UTC submission day used for archive naming

Source lifecycle:

- only the worker holding the active archive-request lease may read the source
- `COMPLETED` and `CANCELED` sources are retained for 24 hours; `FAILED` sources are retained for 7 days
- cleanup uses fenced claims, records missing files as successfully purged, and removes untracked source files only after they are 24 hours old

## Reprocessing and Stale Semantics

When archive publishes a new `projection_version` after curated output exists:

1. VPS MUST preserve curated history/state.
2. VPS MUST mark curated state stale or equivalent.
3. VPS MUST expose stale status in edit-read response when implemented.
4. VPS MUST NOT auto-rebase curated text in V1.

## Acceptance Criteria

1. Backend serves editing UI entirely from VPS state.
2. No direct VPS -> archive synchronous dependency is required at request time.
3. Metadata edit flow works with deterministic revision conflicts.
4. Document OCR page editing works with deterministic revision conflicts and page-number validation.
5. Submit creates `curation_apply` request state and `CURATION_SUBMITTED` history event.
6. Projection manifest updates can refresh VPS editor read model.
7. Existing read-only object endpoints remain backward compatible.
