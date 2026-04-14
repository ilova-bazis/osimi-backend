# Object Editing Backend Requirements (Production)

Last updated: 2026-04-10

## Purpose

This document defines the backend API and persistence requirements needed to support production object editing for:

- metadata editing
- document OCR curation
- audio transcript curation
- video transcript and caption curation

This is an implementation contract for backend engineering. Requirements use **MUST / MUST NOT / SHOULD** language and are intentionally concrete.

## Scope

In scope:

- new edit-read endpoint
- new metadata-write endpoint
- new media-curation write endpoints
- submit-for-review workflow endpoint
- curation audit/history endpoint
- optimistic concurrency for all write operations
- explicit validation and error contracts

Out of scope:

- client UX decisions
- changing ingestion object-boundary rules
- replacing existing object-view contract used by read-only object pages

## Current Baseline (Must Remain Compatible)

The following behavior already exists and MUST remain backward-compatible:

- `GET /api/objects/:object_id` (read-only object + viewer contract)
- `GET /api/objects/:object_id/artifacts`
- `GET /api/objects/:object_id/available-files`
- `POST /api/objects/:object_id/download-requests`
- `POST /api/objects/:object_id/resync`

Existing read-only consumers must continue to work without any required payload migration.

## Core Workflow Model

### 1) Machine output is immutable

- Machine-produced OCR/transcript/caption text is source-of-truth input and MUST NOT be overwritten by curation writes.
- Curated text is stored separately.

### 2) Segment/page timing and boundaries are system-owned (v1)

- For audio/video, clients MUST NOT provide or modify `start_seconds`/`end_seconds`.
- Backend MUST reject attempts to mutate timing boundaries (`422` validation error).
- Curation updates are text-only (plus optional speaker normalization where allowed).

### 3) Optimistic concurrency is mandatory

- Every write endpoint in this document MUST require `revision`.
- If `revision` does not match current server revision, backend MUST return `409 CONFLICT` with latest `revision` and a machine-readable conflict code.

### 4) Curation lifecycle

- Allowed values remain: `needs_review | review_in_progress | reviewed | curation_failed`.
- Save draft does not auto-set `reviewed`.
- Submit endpoint controls transition toward reviewer-ready state.

## Authorization

- Read endpoints: roles `viewer | archiver | admin`.
- Write endpoints: roles `archiver | admin`.
- Unauthorized/forbidden behavior must follow existing API conventions:
  - `401 UNAUTHORIZED` for missing/invalid auth
  - `403 FORBIDDEN` for role/policy denial

## Endpoint Contract

## 1) GET `/api/objects/:object_id/edit`

### Purpose

Returns the full edit payload for one object, including metadata, rights fields, media-specific machine text, curated text, and current revision.

### Response (200)

```json
{
  "object_id": "OBJ-20260213-ABC123",
  "media_type": "audio",
  "revision": 12,
  "curation_state": "review_in_progress",
  "draft": {
    "updated_at": "2026-04-10T09:18:44Z",
    "updated_by": "user-42"
  },
  "metadata": {
    "title": "Oral History Interview with Zarina T.",
    "publication_date": "1987-06-14",
    "date_precision": "day",
    "date_approximate": false,
    "language": "Tajik",
    "tags": ["Oral history", "Interview"],
    "people": ["Zarina T."],
    "description": "Interview segment covering family displacement..."
  },
  "rights": {
    "access_level": "family",
    "rights_note": "Listening allowed for approved researchers.",
    "sensitivity_note": "Personal names mentioned"
  },
  "capabilities": {
    "can_edit_metadata": true,
    "can_curate_text": true,
    "can_submit_review": true
  },
  "curation_payload": {
    "kind": "audio",
    "transcript_segments": [
      {
        "id": "audio-segment-1",
        "start_seconds": 0,
        "end_seconds": 46,
        "speaker": "Zarina T.",
        "machine_text": "I remember the courtyard first...",
        "curated_text": "I remember the courtyard first...",
        "confidence": 0.93,
        "status": "edited"
      }
    ]
  }
}
```

### Rules

- `curation_payload.kind` MUST be one of `document | audio | video | image`.
- `image` payload MAY omit text curation structures.
- For `document`, payload MUST include `pages[]` with machine OCR and curated text per page.
- For `video`, payload MUST include both `transcript_segments[]` and `caption_segments[]`.

### Errors

- `400` invalid `object_id`
- `401`, `403`, `404` follow existing standards

## 2) PATCH `/api/objects/:object_id/metadata`

### Purpose

Updates editable metadata and rights-related notes. Replaces title-only patching for edit workflows.

### Request

```json
{
  "revision": 12,
  "metadata": {
    "title": "Updated title",
    "publication_date": "1987-06-14",
    "date_precision": "day",
    "date_approximate": false,
    "language": "Tajik",
    "tags": ["Oral history", "Migration"],
    "people": ["Zarina T.", "M. Davlatov"],
    "description": "Updated description"
  },
  "rights": {
    "rights_note": "Updated rights note",
    "sensitivity_note": "Updated sensitivity note"
  }
}
```

### Response (200)

```json
{
  "object_id": "OBJ-20260213-ABC123",
  "revision": 13,
  "curation_state": "review_in_progress",
  "updated_at": "2026-04-10T09:22:30Z"
}
```

### Validation

- `metadata.title` MUST be non-empty after trim.
- `metadata.tags` and `metadata.people` MUST be arrays of non-empty strings.
- `date_precision` MUST be `none | year | month | day`.
- If `date_precision = none`, backend MUST normalize `publication_date = ""` and `date_approximate = false`.

### Errors

- `409 CONFLICT` when revision is stale
- `422 UNPROCESSABLE_ENTITY` with field-level validation errors

## 3) PUT `/api/objects/:object_id/curation/transcript`

### Purpose

Updates curated transcript text for audio/video segments.

### Request

```json
{
  "revision": 13,
  "segments": [
    {
      "id": "audio-segment-1",
      "curated_text": "I remember the courtyard first, because everyone gathered there before market opened.",
      "speaker": "Zarina T."
    },
    {
      "id": "audio-segment-2",
      "curated_text": "The newspapers arrived folded in cloth...",
      "speaker": "Zarina T."
    }
  ]
}
```

### Rules

- `segments[]` MUST reference existing canonical segment IDs for the object.
- Unknown segment IDs MUST return `422`.
- Timing fields MUST NOT be accepted in request payload for this endpoint.
- For `video` transcript segment speaker MAY be omitted if not modeled.

### Response (200)

```json
{
  "object_id": "OBJ-20260213-ABC123",
  "revision": 14,
  "updated_count": 2,
  "updated_at": "2026-04-10T09:25:11Z"
}
```

### Errors

- `409` stale revision
- `422` invalid segment IDs, invalid text fields, forbidden timing fields

## 4) PUT `/api/objects/:object_id/curation/captions`

### Purpose

Updates curated caption/subtitle text for video caption segments.

### Request

```json
{
  "revision": 14,
  "segments": [
    { "id": "cue-1", "curated_text": "They arrived from the upper valley." },
    { "id": "cue-2", "curated_text": "Bring the tea over here, in the shade." }
  ]
}
```

### Rules

- Endpoint is valid only for `media_type = video`.
- For non-video objects return `409 CONFLICT` with code `INVALID_MEDIA_TYPE_FOR_CAPTIONS`.
- Caption timing boundaries are immutable in v1.

### Response and errors

Same revision and validation rules as transcript endpoint.

## 5) PUT `/api/objects/:object_id/curation/document`

### Purpose

Updates curated OCR text for document pages.

### Request

```json
{
  "revision": 8,
  "pages": [
    { "page_number": 1, "curated_text": "Curated page 1 text..." },
    { "page_number": 2, "curated_text": "Curated page 2 text..." }
  ]
}
```

### Rules

- Endpoint is valid only for `media_type = document`.
- `page_number` MUST match existing page projection.
- Machine OCR is immutable.

### Response

Returns incremented `revision`, `updated_count`, and `updated_at`.

## 6) POST `/api/objects/:object_id/curation/submit`

### Purpose

Marks object curation as submitted for review and triggers post-curation processing.

### Request

```json
{
  "revision": 15,
  "review_note": "Names in segment 3 are uncertain; verify with community reviewer."
}
```

### Behavior

- Must validate latest revision before transition.
- Must persist review note as workflow event metadata.
- Must transition curation state:
  - if current state is `needs_review` -> `review_in_progress`
  - if current state is `review_in_progress` -> remains `review_in_progress`
  - backend MUST NOT auto-set `reviewed` unless a separate reviewer action exists.
- Must enqueue async processing request for downstream index/artifact refresh.

### Response (200)

```json
{
  "object_id": "OBJ-20260213-ABC123",
  "revision": 16,
  "curation_state": "review_in_progress",
  "request": {
    "id": "req-curation-001",
    "action_type": "curation_apply",
    "status": "PENDING"
  },
  "submitted_at": "2026-04-10T09:31:55Z",
  "submitted_by": "user-42"
}
```

## 7) GET `/api/objects/:object_id/curation/history`

### Purpose

Returns immutable audit events for curation and metadata edits.

### Response (200)

```json
{
  "object_id": "OBJ-20260213-ABC123",
  "events": [
    {
      "id": "evt-1001",
      "type": "TRANSCRIPT_SEGMENT_UPDATED",
      "actor_user_id": "user-42",
      "at": "2026-04-10T09:25:11Z",
      "revision_before": 13,
      "revision_after": 14,
      "payload": { "segment_ids": ["audio-segment-1", "audio-segment-2"] }
    },
    {
      "id": "evt-1002",
      "type": "CURATION_SUBMITTED",
      "actor_user_id": "user-42",
      "at": "2026-04-10T09:31:55Z",
      "revision_before": 15,
      "revision_after": 16,
      "payload": { "review_note": "Names in segment 3 are uncertain" }
    }
  ],
  "next_cursor": null
}
```

### Required event types

- `METADATA_UPDATED`
- `RIGHTS_UPDATED`
- `DOCUMENT_PAGE_UPDATED`
- `TRANSCRIPT_SEGMENT_UPDATED`
- `CAPTION_SEGMENT_UPDATED`
- `CURATION_SUBMITTED`

## Validation and Error Format

All new endpoints MUST use standard JSON error shape with:

- `error.code` (stable machine-readable)
- `error.message` (human-readable)
- optional `error.details`

Required new error codes:

- `REVISION_CONFLICT`
- `INVALID_SEGMENT_ID`
- `INVALID_PAGE_NUMBER`
- `IMMUTABLE_TIMING_FIELD`
- `INVALID_MEDIA_TYPE_FOR_CAPTIONS`
- `INVALID_MEDIA_TYPE_FOR_DOCUMENT_CURATION`
- `VALIDATION_FAILED`

`422` validation responses MUST include field pointers, e.g.:

```json
{
  "error": {
    "code": "VALIDATION_FAILED",
    "message": "Validation failed.",
    "details": [
      { "path": "segments[1].curated_text", "code": "REQUIRED" }
    ]
  }
}
```

## Persistence Requirements

Backend implementation MUST persist:

- object curation revision counter (monotonic integer)
- metadata edit snapshots by revision
- media curation content by revision (or delta with recoverable snapshots)
- immutable audit events for all edit actions
- submit workflow events and linked async request id

Minimum required logical stores:

- `object_curation_revisions`
- `object_curated_document_pages`
- `object_curated_transcript_segments`
- `object_curated_caption_segments`
- `object_curation_events`

Exact physical schema is backend-owned, but API behavior above is mandatory.

## Async Processing Requirements

On successful submit, backend MUST enqueue an async request with:

- `target_type = object`
- `target_id = :object_id`
- `action_type = curation_apply`
- payload includes submitted revision

The worker pipeline for `curation_apply` MUST be idempotent by `(object_id, revision)`.

## Performance Requirements

- `GET /api/objects/:object_id/edit` p95 <= 500ms for objects up to:
  - document: 500 pages
  - audio/video: 10,000 segments
- Write endpoints p95 <= 400ms for payloads up to 500 segment/page updates.
- History endpoint MUST support cursor pagination.

## Compatibility and Migration

- Existing `PATCH /api/objects/:object_id` MAY remain for legacy title-only clients.
- New editing clients MUST use `/metadata` + `/curation/*` endpoints.
- Existing viewer contracts remain unchanged.

## Acceptance Criteria

Backend implementation is complete only when all conditions pass:

1. All endpoints in this document are implemented and documented in `docs/api-reference.md`.
2. Revision conflicts return deterministic `409` with latest revision.
3. Timing mutation attempts are rejected with `422` and `IMMUTABLE_TIMING_FIELD`.
4. Submit creates a `curation_apply` async request and persists `CURATION_SUBMITTED` history event.
5. History endpoint returns correct revision-before/after for every edit action.
6. Existing read-only object pages remain functional without changes.
