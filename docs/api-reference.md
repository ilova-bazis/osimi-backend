# API Reference

This document is the practical route reference for the Osimi backend.

## Conventions

- Base path: `/`
- JSON APIs return `application/json` unless noted.
- All timestamps are ISO-8601 strings.
- Pagination defaults: `limit=50`, max `limit=200`.

## Authentication Modes

### 1) Client session token (UI / API clients)

- Header: `Authorization: Bearer <token>`
- Token is obtained from `POST /api/auth/login`.
- Tenant scope is derived from the authenticated user membership.

### 2) Worker shared token (worker control APIs)

- Header: `x-worker-auth-token: <WORKER_AUTH_TOKEN>`
- Optional header: `x-worker-id: <worker-id>`
- Used by lease/event worker endpoints.

### 3) Signed token URLs (upload/download transport)

- Upload and download transport endpoints use signed URL tokens in path.
- These endpoints do not require bearer token headers.

## Standard Error Shape

On non-2xx responses, JSON errors follow this shape:

```json
{
  "request_id": "uuid",
  "error": {
    "code": "BAD_REQUEST",
    "message": "Human readable message",
    "details": {}
  }
}
```

Error codes: `BAD_REQUEST`, `UNAUTHORIZED`, `FORBIDDEN`, `NOT_FOUND`, `METHOD_NOT_ALLOWED`, `CONFLICT`, `CONFIGURATION_ERROR`, `INTERNAL_SERVER_ERROR`.

Planned addition: `LOCKED` (for HTTP `423` state-based non-deliverable conditions).

---

## Public / System

### GET `/healthz`

- Auth: none
- Returns service health.
- 200 response:
  - `status`, `service`, `request_id`, `timestamp`

### PUT `/api/uploads/:token`

- Auth: signed URL token in path (no bearer required)
- Purpose: upload file bytes to staging using a presigned token.
- Required headers:
  - `content-type` must match signed token constraints
  - `content-length` must match signed token constraints
- Body: raw file bytes
- 200 response:
  - `status`, `ingestion_id`, `file_id`, `size_bytes`

---

## Client APIs (Bearer token)

## Auth

### POST `/api/auth/login`

- Auth: none
- Body:
  - `username` (string)
  - `password` (string)
  - `tenant_id` (optional string)
- 200 response:
  - `token`, `token_type` (`Bearer`), `user { id, username, tenant_id, role }`

### POST `/api/auth/logout`

- Auth: Bearer token
- 200 response:
  - `status` (`ok`), `request_id`

### GET `/api/auth/me`

- Auth: Bearer token
- 200 response:
  - `user { id, username, tenant_id, role }`

## Dashboard

### GET `/api/dashboard/summary`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- 200 response:
  - `summary { total_ingestions, total_objects, processed_today, processed_week, failed_count }`

### GET `/api/dashboard/activity`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- Query params:
  - `limit` (optional int, default `50`, max `200`)
  - `cursor` (optional opaque base64url string)
  - `ingestion_id` (optional UUID; filters activity to a single ingestion)
- 200 response:
  - `activity[]` where each item includes:
    - `id`, `event_id`, `type`, `ingestion_id`, `object_id`, `payload`, `actor_user_id`, `created_at`
  - `next_cursor` (string or `null`)

## Ingestions

### POST `/api/ingestions`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Body:
  - `batch_label` (string)
  - `schema_version` (string, must be `"1.0"`)
  - `classification_type` (string; one of `newspaper_article`, `magazine_article`, `book_chapter`, `book`, `letter`, `speech`, `interview`, `report`, `manuscript`, `image`, `document`, `other`)
  - `item_kind` (string; one of `photo`, `audio`, `video`, `scanned_document`, `document`, `other`)
  - `language_code` (string, non-empty; unrestricted)
  - `pipeline_preset` (string; one of `auto`, `none`, `ocr_text`, `audio_transcript`, `video_transcript`, `ocr_and_audio_transcript`, `ocr_and_video_transcript`)
  - `access_level` (string; `private`, `family`, `public`)
  - `embargo_until` (optional RFC3339 timestamp)
  - `rights_note` (optional string)
  - `sensitivity_note` (optional string)
  - `summary` (object; ingestion-stage metadata, based on `catalog.json` fields)
    - required shape (strict, unknown keys rejected):
      - `title`
        - `primary` (string, non-empty)
        - `original_script` (string or `null`)
        - `translations` (array of `{ lang, text }`)
      - `classification`
        - `tags` (string array, unique values)
        - `summary` (string or `null`)
      - `dates`
        - `published` and `created`
          - `value` (YYYY, YYYY-MM, YYYY-MM-DD, or `null`)
          - `approximate` (boolean)
          - `confidence` (`low`, `medium`, `high`)
          - `note` (string or `null`)
    - optional fields:
      - `processing`
        - `ocr_text`, `audio_transcript`, `video_transcript` (each optional `{ enabled, language? }`)
      - `publication` (`name`, `issue`, `volume`, `pages`, `place`)
      - `people` (`subjects`, `authors`, `contributors`, `mentioned`)
      - `links` (`related_object_ids`, `external_urls`)
      - `notes` (`internal`, `public`)
- 201 response:
  - `ingestion` (draft ingestion object)

### GET `/api/ingestions`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- Query params:
  - `limit` (optional)
  - `cursor` (optional)
- 200 response:
  - `ingestions[]`
  - `next_cursor` (string or `null`)

### GET `/api/ingestions/:id`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- 200 response:
  - `ingestion`
  - `files[]`

### PATCH `/api/ingestions/:id`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Body (partial update, optional fields):
  - `batch_label` (string)
  - `classification_type` (string; one of `newspaper_article`, `magazine_article`, `book_chapter`, `book`, `letter`, `speech`, `interview`, `report`, `manuscript`, `image`, `document`, `other`)
  - `item_kind` (string; one of `photo`, `audio`, `video`, `scanned_document`, `document`, `other`)
  - `language_code` (string, non-empty; unrestricted)
  - `pipeline_preset` (string; one of `auto`, `none`, `ocr_text`, `audio_transcript`, `video_transcript`, `ocr_and_audio_transcript`, `ocr_and_video_transcript`)
  - `access_level` (string; `private`, `family`, `public`)
  - `embargo_until` (optional RFC3339 timestamp, nullable)
  - `rights_note` (optional string, nullable)
  - `sensitivity_note` (optional string, nullable)
  - `summary` (object; ingestion-stage metadata, based on `catalog.json` fields)
    - full object replacement (same strict schema as `POST /api/ingestions`)
    - partial nested updates are not supported; send the complete summary block
- Preconditions:
  - ingestion status is `DRAFT`, `UPLOADING`, or `CANCELED`
  - ingestion has not started processing (no active lease)
- 200 response:
  - `ingestion` (updated ingestion object)

### DELETE `/api/ingestions/:id`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Preconditions:
  - ingestion status is `DRAFT`, `UPLOADING`, or `CANCELED`
  - ingestion has not started processing (no active lease)
- 200 response:
  - `status` = `deleted`
  - `ingestion_id`

### GET `/api/ingestions/capabilities`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- 200 response:
  - `media_kinds` (array)
  - `extensions_by_kind` (object)
  - `mime_by_kind` (object)
  - `mime_aliases` (object)

### POST `/api/ingestions/:id/files/presign`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Body (new file):
  - `filename`, `content_type`, `size_bytes`
- Planned (not yet implemented):
  - optional `source_order` (integer `>= 0`) to persist client-intended page/file sequence
- Body (re-presign existing):
  - `file_id`
- 201 response:
  - `file_id`, `storage_key`, `upload_url`, `expires_at`, `headers { content-type, content-length }`

### DELETE `/api/ingestions/:id/files/:fileId`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Preconditions:
  - ingestion is in `DRAFT` or `UPLOADING`
- 200 response:
  - `status` = `deleted`
  - `file_id`

### POST `/api/ingestions/:id/files/:fileId/overrides`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Body:
  - `processing_overrides` (object; per-file processing intent overrides, unknown keys allowed)
- Preconditions:
  - ingestion is in `DRAFT` or `UPLOADING`
- 200 response:
  - `file` (updated ingestion file record)

### POST `/api/ingestions/:id/files/commit`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Body:
  - `file_id`, `checksum_sha256`
- Preconditions:
  - all ingestion files must share a single media kind (`image`, `audio`, `video`, `document`)
  - image batches may include: `image/jpeg`, `image/png`, `image/tiff`, `image/webp`, `image/gif`, `image/bmp`, `image/heic`, `image/heif`, `image/svg+xml`
- 200 response:
  - `file` (updated ingestion file record)

### POST `/api/ingestions/:id/submit`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Preconditions:
  - at least one file exists
  - at least one file is committed (`UPLOADED` or `VALIDATED`)
- 200 response:
  - `ingestion` (status transitions to queued flow)

### POST `/api/ingestions/:id/cancel`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Preconditions:
  - ingestion has not started processing (no active lease)
- 200 response:
  - `ingestion`
  - behavior:
    - `QUEUED` transitions to `UPLOADING`
    - `DRAFT` or `UPLOADING` transitions to `CANCELED`
    - `CANCELED` is returned unchanged

### POST `/api/ingestions/:id/restore`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Preconditions:
  - ingestion status is `CANCELED`
  - ingestion has not started processing (no active lease)
- 200 response:
  - `ingestion` (status transitions to draft or uploading based on files)

### POST `/api/ingestions/:id/retry`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- 200 response:
  - `ingestion`

## Objects

### GET `/api/objects`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- Query params:
  - `limit` (optional integer, default `50`, max `200`)
  - `cursor` (optional opaque cursor from previous response)
  - `sort` (optional)
    - allowed: `created_at_desc` (default), `created_at_asc`, `updated_at_desc`, `updated_at_asc`, `title_asc`, `title_desc`
  - `q` (optional text search, minimum guarantee: matches `title`, `object_id`)
  - `availability_state` (optional: `AVAILABLE`, `ARCHIVED`, `RESTORE_PENDING`, `RESTORING`, `UNAVAILABLE`)
  - `access_level` (optional: `private`, `family`, `public`)
  - `language` (optional)
  - `batch_label` (optional)
    - Filters by source ingestion batch label (same dimension returned as `source_batch_label` in list rows).
  - `type` (`GENERIC|IMAGE|AUDIO|VIDEO|DOCUMENT`, optional)
  - `from` (ISO timestamp, optional)
  - `to` (ISO timestamp, optional)
  - `tag` (optional)
- 200 response:
  - `objects[]` (does **not** include `ingest_manifest`)
  - `next_cursor` (string or `null`)
  - `total_count` (total tenant-visible objects before filters)
  - `filtered_count` (total matching current filters)

List row guarantees (`objects[]`):

- guaranteed keys:
  - `id` (alias of `object_id`)
  - `object_id`
  - `title`
  - `processing_state`
    - allowed: `queued`, `ingesting`, `ingested`, `derivatives_running`, `derivatives_done`, `ocr_running`, `ocr_done`, `index_running`, `index_done`, `processing_failed`, `processing_skipped`
  - `curation_state`
    - allowed: `needs_review`, `review_in_progress`, `reviewed`, `curation_failed`
  - `availability_state`
    - allowed: `AVAILABLE`, `ARCHIVED`, `RESTORE_PENDING`, `RESTORING`, `UNAVAILABLE`
  - `access_level`
    - allowed: `private`, `family`, `public`
  - `type`
  - `tenant_id`
  - `source_ingestion_id` (`null` allowed)
  - `source_batch_label` (`null` allowed)
  - `metadata`
  - `created_at`
  - `updated_at`
  - `embargo_until` (`null` allowed)
  - `embargo_kind`
    - allowed: `none`, `timed`, `curation_state`
  - `embargo_curation_state` (`null` allowed)
    - when non-null, allowed: `needs_review`, `review_in_progress`, `reviewed`, `curation_failed`
  - `rights_note` (`null` allowed)
  - `sensitivity_note` (`null` allowed)
  - `can_download`
  - `access_reason_code` (`OK`, `FORBIDDEN_POLICY`, `EMBARGO_ACTIVE`, `RESTORE_REQUIRED`, `RESTORE_IN_PROGRESS`, `TEMP_UNAVAILABLE`)
    - computed per requester at read time from access policy + availability state
- optional nullable keys:
  - `language` (`null` if unknown)
- excluded from list payload:
  - `ingest_manifest` (detail-only)

Sort semantics:

- default sort: `created_at_desc`
- sorting is deterministic
- tie-breaker includes `object_id` for stable cursor paging
- cursor is sort-aware and preserves ordering across pages

Example response:

```json
{
  "objects": [
    {
      "id": "OBJ-20260213-ABC123",
      "object_id": "OBJ-20260213-ABC123",
      "title": "Document title",
      "processing_state": "queued",
      "curation_state": "needs_review",
      "availability_state": "AVAILABLE",
      "access_level": "private",
      "type": "DOCUMENT",
      "language": "en",
      "tenant_id": "00000000-0000-0000-0000-000000000001",
      "source_ingestion_id": "13dd3927-17be-4211-9a77-fdea3104a028",
      "source_batch_label": "batch-2026-02-13-001",
      "metadata": {},
      "embargo_kind": "none",
      "embargo_curation_state": null,
      "embargo_until": null,
      "rights_note": null,
      "sensitivity_note": null,
      "can_download": false,
      "access_reason_code": "FORBIDDEN_POLICY",
      "created_at": "2026-02-13T20:22:29.993Z",
      "updated_at": "2026-02-14T08:01:00.000Z"
    }
  ],
  "next_cursor": "...",
  "total_count": 124,
  "filtered_count": 37
}
```

### GET `/api/objects/:object_id`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- 200 response:
  - `object` including `ingest_manifest` (or `null`)
  - access projection fields:
    - `is_authorized`
    - `is_deliverable`
    - `can_download`
    - `access_reason_code` (`OK`, `FORBIDDEN_POLICY`, `EMBARGO_ACTIVE`, `RESTORE_REQUIRED`, `RESTORE_IN_PROGRESS`, `TEMP_UNAVAILABLE`)

### PATCH `/api/objects/:object_id`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Body:
  - `title` (required string, non-empty)
- Notes:
  - `metadata` patching is intentionally not supported in this phase.
- 200 response:
  - `object` (updated)

### GET `/api/objects/:object_id/artifacts`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- 200 response:
  - `object_id`
  - `artifacts[]` (`id`, `kind`, `storage_key`, `content_type`, `size_bytes`, `created_at`)

### POST `/api/objects/:object_id/download-requests`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- Body:
  - `artifact_kind` (`original`, `pdf`, `ocr_text`, `thumbnail`, `transcript`, `web_version`, `other`)
- Behavior:
  - If matching artifact already exists on backend, request is not queued and response returns `status = available` with artifact payload.
  - If artifact is missing, backend queues (or reuses existing active) request and returns `status = queued` with request payload.
- 200 response:
  - when artifact already exists: `status: "available"`, `object_id`, `artifact`
  - when an active request already exists: `status: "queued"`, `object_id`, `request`
- 201 response:
  - when a new queue request is created: `status: "queued"`, `object_id`, `request`

### GET `/api/objects/:object_id/download-requests`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- 200 response:
  - `object_id`
  - `requests[]` (`id`, `requested_by`, `artifact_kind`, `status`, `failure_reason`, `created_at`, `updated_at`, `completed_at`)

### GET `/api/objects/:object_id/artifacts/:artifact_id/download`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- Access policy:
  - Endpoint authorization is role-aware and object access-policy aware.
  - A valid role alone does not guarantee download access.
  - `admin` can override access policy restrictions in the current implementation.
- Current behavior:
  - Returns `400 BAD_REQUEST` when object exists but is not downloadable in current policy/deliverability state.
  - Returns `404 NOT_FOUND` when object or artifact does not exist (or is outside tenant scope).
  - Current non-2xx JSON `error.code` values remain from the standard set (for example `BAD_REQUEST`, `NOT_FOUND`).
- Planned behavior (future refinement):
  - Use `403 FORBIDDEN` for policy denial.
  - Use `409 CONFLICT` / `423 LOCKED` for state-based non-deliverable conditions.
  - Keep `404 NOT_FOUND` for missing/non-visible resources.
  - Planned HTTP status refinement does not require changing request/response success payload shape.
- 200 response:
  - Binary file response
  - headers include `content-type`, `content-length`, `content-disposition`

### PATCH `/api/objects/:object_id/access-policy`

- Auth: Bearer token
- Roles: `admin`
- Body:
  - `access_level` (`private|family|public`, required)
  - `embargo_kind` (`none|timed|curation_state`, required)
  - `embargo_until` (ISO timestamp, required when `embargo_kind=timed`)
  - `embargo_curation_state` (`needs_review|review_in_progress|reviewed|curation_failed`, required when `embargo_kind=curation_state`)
  - `rights_note` (optional string)
  - `sensitivity_note` (optional string)
- 200 response:
  - `object` (updated policy + object fields)

### POST `/api/objects/:object_id/access-requests`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- Body:
  - `requested_level` (`family|private`, required)
  - `reason` (optional string)
- 201 response:
  - `request`
- Conflict behavior:
  - returns `409` when the same user already has a `PENDING` request for that object

### GET `/api/objects/:object_id/access-requests`

- Auth: Bearer token
- Roles: `admin`
- 200 response:
  - `object_id`
  - `requests[]` (`id`, `requester_user_id`, `requested_level`, `reason`, `status`, `reviewed_by`, `reviewed_at`, `decision_note`, `created_at`, `updated_at`)

### POST `/api/objects/:object_id/access-requests/:request_id/approve`

- Auth: Bearer token
- Roles: `admin`
- Body:
  - `decision_note` (optional string)
  - Empty request body is allowed.
- 200 response:
  - `request` (status becomes `APPROVED`)
  - creates or updates assignment for requester
- Conflict behavior:
  - returns `409` if request status is not `PENDING`

### POST `/api/objects/:object_id/access-requests/:request_id/reject`

- Auth: Bearer token
- Roles: `admin`
- Body:
  - `decision_note` (optional string)
  - Empty request body is allowed.
- 200 response:
  - `request` (status becomes `REJECTED`)
- Conflict behavior:
  - returns `409` if request status is not `PENDING`

### GET `/api/objects/:object_id/access-assignments`

- Auth: Bearer token
- Roles: `admin`
- 200 response:
  - `object_id`
  - `assignments[]` (`user_id`, `granted_level`, `created_by`, `created_at`)

### PUT `/api/objects/:object_id/access-assignments`

- Auth: Bearer token
- Roles: `admin`
- Body:
  - `user_id` (UUID, required)
  - `granted_level` (`family|private`, required)
- 200 response:
  - `assignment`

### DELETE `/api/objects/:object_id/access-assignments/:user_id`

- Auth: Bearer token
- Roles: `admin`
- 200 response:
  - `status` (`ok`)
  - `object_id`
  - `user_id`

---

## Worker APIs

Worker APIs are for ingestion workers, not UI clients.

Integration guide for archive worker teams: `docs/archive-system-integration.md`.

## Lease lifecycle

### POST `/api/ingestions/lease`

- Auth: `x-worker-auth-token` header
- Optional: `x-worker-id`
- 200 response:
  - `lease: null` when no queued ingestion available
  - or `lease { lease_id, lease_token, lease_expires_at, ingestion_id, batch_label, tenant_id, download_urls[], catalog_json }`
  - `catalog_json` is generated from ingestion first-class fields and `summary`
- ingestion-stage leases may provide `catalog_json.object_id = null`
- each `download_urls[]` item includes `checksum_sha256` for worker validation
- each `download_urls[]` item includes `processing_overrides` (object; per-file overrides)
- Planned (not yet implemented): each `download_urls[]` item will also include `filename` and `source_order`
- Ordering rule target (planned): `source_order ASC NULLS LAST`, then `filename`, then `file_id`

### POST `/api/ingestions/:id/lease`

- Auth: `x-worker-auth-token` header
- Optional: `x-worker-id`
- Path:
  - `:id` ingestion UUID
- 200 response:
  - `lease { lease_id, lease_token, lease_expires_at, ingestion_id, batch_label, tenant_id, download_urls[], catalog_json }`
- 404 when ingestion does not exist
- 409 when ingestion exists but is not currently leasable (for example, active lease, not in `QUEUED`, or terminal state)
- no active-lease takeover is allowed; this endpoint is for deterministic reacquire/recovery

### POST `/api/ingestions/:id/lease/heartbeat`

- Auth:
  - `x-worker-auth-token` header
  - body `lease_token` must be valid and match `:id`
- Body:
  - `lease_token`
- 200 response:
  - refreshed `lease { ... }` including refreshed `lease_token`, `download_urls[]`, and `catalog_json`

### POST `/api/ingestions/:id/lease/release`

- Auth:
  - `x-worker-auth-token` header
  - body `lease_token` must be valid and match `:id`
- Body:
  - `lease_token`
- 200 response:
  - `status` (`ok`), `ingestion_id`, `lease_id`

## Worker downloads and events

### GET `/api/worker/downloads/:token`

- Auth: signed download token in path
- 200 response:
  - Binary file response
  - headers: `content-type`, `content-length`, `accept-ranges`

### POST `/api/ingestions/:id/events`

- Auth:
  - `x-worker-auth-token` header
  - body `lease_token` must be valid and match `:id`
- Body:
  - `lease_token`
  - `events[]` where each event includes:
    - `event_id` (UUID)
    - `event_type` (supported values)
      - `INGESTION_SUBMITTED`
      - `INGESTION_QUEUED`
      - `INGESTION_PROCESSING`
      - `INGESTION_COMPLETED`
      - `INGESTION_FAILED`
      - `INGESTION_CANCELED`
      - `LEASE_GRANTED`
      - `LEASE_RENEWED`
      - `LEASE_EXPIRED`
      - `LEASE_RELEASED`
      - `FILE_VALIDATED`
      - `FILE_FAILED`
      - `PIPELINE_STEP_STARTED`
      - `PIPELINE_STEP_COMPLETED`
      - `PIPELINE_STEP_FAILED`
      - `OBJECT_CREATED`
      - `ARTIFACT_CREATED`
    - `timestamp` (ISO datetime)
    - `payload` (object)
    - `object_id` (required for completion/object/artifact event types)
- Behavior:
  - idempotent by `event_id`
  - out-of-order tolerant
  - completion event creates or resolves object by source ingestion
  - state projection rules (current phase):
    - `INGESTION_COMPLETED` updates object projection to `processing_state = index_done` and `availability_state = AVAILABLE`
    - `curation_state` is not currently projected from worker event stream in this phase
    - other event types are stored for audit/activity; they do not currently mutate object projection fields directly
  - `payload.ingest_json` (when provided) updates `objects.ingest_manifest` (last-write-wins)
- 200 response:
  - `status`, `ingestion_id`, `inserted_events`, `duplicate_events`, `object_id`

---

## Notes for UI Integrators

- Use client Bearer APIs only; do not call worker lease/event endpoints from UI.
- `ingest_manifest` is available on object detail only (`GET /api/objects/:object_id`).
- List endpoints return `next_cursor`; pass it back as `cursor` for pagination.
