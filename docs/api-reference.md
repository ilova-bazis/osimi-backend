# API Reference

This document is the practical route reference for the Osimi backend.

## Conventions

- Base path: `/`
- JSON APIs return `application/json` unless noted.
- All timestamps are ISO-8601 strings.
- Pagination defaults: `limit=50`, max `limit=200`.
- Responses include header `x-request-id`.

## Authentication Modes

### 1) Client session token (UI / API clients)

- Header: `Authorization: Bearer <token>`
- Token is obtained from `POST /api/auth/login`.
- Tenant scope is derived from the authenticated user membership.
- Invalid/missing/expired session resolves to `401 UNAUTHORIZED`.

### 2) Worker shared token (worker control APIs)

- Header: `x-worker-auth-token: <WORKER_AUTH_TOKEN>`
- Optional header: `x-worker-id: <worker-id>`
- Used by worker/internal control endpoints (including ingestion lease/event routes, internal available-files sync, and object-download worker routes).
- Missing/invalid token resolves to `401 UNAUTHORIZED`.

### 3) Signed token URLs (upload/download transport)

- Upload and download transport endpoints use signed URL tokens in path.
- These endpoints do not require bearer token headers.
- Invalid/expired token resolves to `401 UNAUTHORIZED`.

## Standard Error Shape

On non-2xx responses, JSON errors follow this shape:

```json
{
  "request_id": "uuid",
  "error": {
    "code": "BAD_REQUEST",
    "message": "Human readable message"
  }
}
```

`error.details` is optional and included only when provided by the server.

Error codes: `BAD_REQUEST`, `UNAUTHORIZED`, `FORBIDDEN`, `NOT_FOUND`, `METHOD_NOT_ALLOWED`, `CONFLICT`, `REVISION_CONFLICT`, `LOCKED`, `VALIDATION_FAILED`, `SERVICE_UNAVAILABLE`, `CONFIGURATION_ERROR`, `INTERNAL_SERVER_ERROR`.

---

## Public / System

### GET `/healthz`

- Auth: none
- Process liveness only; it does not check configuration, database connectivity,
  migrations, or shutdown state.
- 200 response:
  - `status`, `service`, `request_id`, `timestamp`

### GET `/readyz`

- Auth: none. Probe requests ignore bearer, tenant, and idempotency headers.
- Returns deployment readiness for lifecycle state, required configuration,
  PostgreSQL connectivity, and exact migration history.
- 200 response: `status = "ready"`, `service`, `request_id`, `timestamp`, and
  `checks[]` with `name` and `ready`.
- 503 response: `status = "not_ready"` and `checks[]`; failed checks include a
  non-sensitive `reason` such as `draining`, `configuration_invalid`,
  `database_unavailable`, `check_timed_out`, `migrations_pending`,
  `migration_checksum_mismatch`, or `unknown_migration`.
- Readiness is false during shutdown draining. It performs no migration DDL or
  writes.
- Once draining begins, newly dispatched non-probe requests return
  `503 SERVICE_UNAVAILABLE` without invoking application handlers.

### PUT `/api/uploads/:token`

- Auth: signed URL token in path (no bearer required)
- Purpose: upload file bytes to staging using a presigned token.
- Required headers:
  - `content-type` must match signed token constraints by media type (parameters are ignored)
  - `content-length` is required and must exactly match signed token constraints
- Body: raw file bytes; byte length must exactly match signed token constraints
- Browser CORS:
  - cross-origin browser uploads require the caller's exact UI origin in `CORS_ALLOWED_ORIGINS`
  - preflight permits `PUT` with the author-controlled `content-type` header; browsers calculate `content-length` from the file body
  - signed path token is the upload authorization mechanism; no bearer token or cookie credentials are used
- 200 response:
  - `status`, `ingestion_id`, `file_id`, `size_bytes`
- Error behavior:
  - `400 BAD_REQUEST` for header/body constraint mismatch
  - `401 UNAUTHORIZED` for invalid/expired signed token

---

## Client APIs (Bearer token)

## Auth

### POST `/api/auth/login`

- Auth: none
- Body:
  - `username` (string; required)
  - `password` (string; required)
  - `tenant_id` (optional string)
    - required when username is valid across multiple active tenant memberships
    - if provided as empty/whitespace, treated as omitted
- 200 response:
  - `token`, `token_type` (`Bearer`), `user { id, username, tenant_id, role }`
  - `role` is one of `viewer|archiver|admin`
- Error behavior:
  - `400 BAD_REQUEST` for invalid body shape or when `tenant_id` is required but missing for multi-tenant account
  - `401 UNAUTHORIZED` for invalid credentials

### POST `/api/auth/logout`

- Auth: Bearer token
- 200 response:
  - `status` (`ok`), `request_id`
- Error behavior:
  - `401 UNAUTHORIZED` for missing/invalid/expired session token

### GET `/api/auth/me`

- Auth: Bearer token
- 200 response:
  - `user { id, username, tenant_id, role }`
  - `role` is one of `viewer|archiver|admin`
- Error behavior:
  - `401 UNAUTHORIZED` for missing/invalid/expired session token

## Dashboard

### GET `/api/dashboard/summary`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- 200 response:
  - `summary`:
    - `total_ingestions` (number, integer >= 0)
    - `total_objects` (number, integer >= 0)
    - `processed_today` (number, integer >= 0)
      - ingestions where `status = COMPLETED` and `updated_at >= date_trunc('day', now())`
    - `processed_week` (number, integer >= 0)
      - ingestions where `status = COMPLETED` and `updated_at >= date_trunc('week', now())`
    - `failed_count` (number, integer >= 0)
- Error behavior:
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed

### GET `/api/dashboard/activity`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- Query params:
  - `limit` (optional int, default `50`, min `1`, max `200`)
  - `cursor` (optional opaque base64url string)
    - when present, must decode to `{ created_at: string, id: string }`
  - `ingestion_id` (optional UUID; filters activity to a single ingestion)
- Ordering:
  - newest first: `created_at DESC`, then `id DESC`
- 200 response:
  - `activity[]` where each item includes:
    - `id` (string)
    - `event_id` (string)
    - `type` (string)
    - `ingestion_id` (string, nullable)
    - `object_id` (string, nullable)
    - `payload` (JSON object)
    - `actor_user_id` (string, nullable)
    - `created_at` (ISO timestamp string)
  - `next_cursor` (string or `null`)
    - `null` means there are no additional pages
- Error behavior:
  - `400 BAD_REQUEST` for invalid query params (`limit`, `cursor`, `ingestion_id`)
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed

## Ingestions

Ingestion response shapes in this section are authoritative with `src/validation/ingestion.ts`.

### Ingestion Schemas

### Ingestion Schema

- `id` (string)
- `batch_label` (string)
- `tenant_id` (string)
- `status` (`DRAFT|UPLOADING|QUEUED|PROCESSING|COMPLETED|COMPLETED_WITH_ERRORS|FAILED|CANCELED`)
- `created_by` (string)
- `schema_version` (string)
- `classification_type` (`newspaper_article|magazine_article|book_chapter|book|letter|speech|interview|report|manuscript|image|document|other`)
- `item_kind` (`photo|audio|video|scanned_document|document|other`)
- `language_code` (string)
- `pipeline_preset` (`auto|none|ocr_text|audio_transcript|video_transcript|ocr_and_audio_transcript|ocr_and_video_transcript`)
- `access_level` (`private|family|public`)
- `embargo_until` (ISO timestamp string, nullable)
- `rights_note` (string, nullable)
- `sensitivity_note` (string, nullable)
- `summary` (catalog summary object; strict schema)
- `error_summary` (JSON object)
- `created_at` (ISO timestamp string)
- `updated_at` (ISO timestamp string)
- `staging_purge` (object, included on ingestion list and detail responses)
  - `state` (`NOT_SCHEDULED|PENDING|PURGED`)
  - `started_at` (ISO timestamp string, nullable)
  - `purged_at` (ISO timestamp string, nullable)
- `action_capabilities` (object, included on ingestion list and detail responses)
  - `can_resume`, `can_retry`, `can_cancel`, `can_restore`, `can_delete` (boolean)
  - values are computed for the authenticated role, current lease, and purge state; they are advisory snapshots and mutations remain authoritative
  - clients must use these fields for action presentation and must not infer actions from status or file previews
  - `can_retry` is true only for retained `FAILED` ingestions; cancel is not available for `FAILED`

### Ingestion File Schema

- `id` (string)
- `ingestion_id` (string)
- `filename` (string)
- `content_type` (string)
- `size_bytes` (number)
- `storage_key` (string)
- `status` (`PENDING|UPLOADED|VALIDATED|FAILED`)
- `checksum_sha256` (string, nullable)
- `preview` (object)
  - `status` (`pending|ready|failed|unsupported|purged`)
  - `content_type` (string, nullable)
  - `size_bytes` (number, nullable)
  - `width` (number, nullable)
  - `height` (number, nullable)
  - `url` (string, nullable)
  - `error` (JSON object, nullable)
  - note: internal worker claim state is normalized back to `pending` in client-facing ingestion responses
- `processing_overrides` (strict object):
  - optional keys: `ocr_text`, `audio_transcript`, `video_transcript`
  - each value: `{ enabled: boolean, language?: string }`
- `error` (JSON object)
- `created_at` (ISO timestamp string)
- `updated_at` (ISO timestamp string)

### Ingestion Item Schema

- `id` (string)
- `ingestion_id` (string)
- `item_index` (number; integer >= 1)
- `status` (`PENDING|READY|PROCESSING|COMPLETED|FAILED|SKIPPED`)
- `classification_type` (`newspaper_article|magazine_article|book_chapter|book|letter|speech|interview|report|manuscript|image|document|other`, nullable)
- `item_kind` (`photo|audio|video|scanned_document|document|other`, nullable)
- `language_code` (string, nullable)
- `title` (string, nullable)
- `summary` (JSON object)
- `error_summary` (JSON object)
- `object_id` (string, nullable)
- `created_at` (ISO timestamp string)
- `updated_at` (ISO timestamp string)

### Ingestion Item File Schema

- `id` (string)
- `ingestion_item_id` (string)
- `ingestion_file_id` (string)
- `ingestion_id` (string)
- `role` (`primary|front|back|page|attachment|transcript_source|side_a|side_b|other`)
- `sort_order` (number; integer >= 1)
- `page_number` (number, nullable)
- `is_primary` (boolean)
- `logical_label` (string, nullable)
- `created_at` (ISO timestamp string)

### Ingestion Request Idempotency

- `POST /api/ingestions`, `POST /api/ingestions/:id/files/presign`, `POST /api/ingestions/:id/files/commit`, `POST /api/ingestions/:id/submit`, and `POST /api/ingestions/:id/retry` accept an optional `x-idempotency-key` header.
- A key is scoped to the authenticated tenant, actor, and endpoint. The same key with the same canonical request replays the original successful status and response body for seven days.
- Reusing a key with a different request returns `409 CONFLICT`. Failed requests are not retained as successful replays.

### POST `/api/ingestions`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Body:
  - `batch_label` (string, required, non-empty)
  - `schema_version` (string literal `"1.0"`; optional, defaults to `"1.0"`)
  - `classification_type` (string; one of `newspaper_article`, `magazine_article`, `book_chapter`, `book`, `letter`, `speech`, `interview`, `report`, `manuscript`, `image`, `document`, `other`)
  - `item_kind` (string; one of `photo`, `audio`, `video`, `scanned_document`, `document`, `other`)
  - `language_code` (string, non-empty; unrestricted)
  - `pipeline_preset` (string; one of `auto`, `none`, `ocr_text`, `audio_transcript`, `video_transcript`, `ocr_and_audio_transcript`, `ocr_and_video_transcript`)
  - `access_level` (string; `private`, `family`, `public`)
  - `embargo_until` (optional RFC3339 timestamp, nullable)
  - `rights_note` (optional string, nullable)
  - `sensitivity_note` (optional string, nullable)
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
  - `ingestion` (Ingestion Schema)
- Compatibility rules:
  - `classification_type` and `item_kind` must be semantically compatible
  - current compatibility set:
    - `newspaper_article|magazine_article|book_chapter|book|letter|report|manuscript|document` -> `scanned_document|document`
    - `image` -> `photo`
    - `speech|interview` -> `audio|video|scanned_document`
    - `other` -> any `item_kind`
- Error behavior:
  - `400 BAD_REQUEST` for invalid body/field validation
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `409 CONFLICT` when `classification_type` and `item_kind` are incompatible

### GET `/api/ingestions`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- Query params:
  - `limit` (optional int, default `50`, min `1`, max `200`)
  - `cursor` (optional opaque base64url string)
    - when present, must decode to `{ created_at: ISO timestamp, id: UUID }`
- Ordering:
  - newest first: `created_at DESC`, then `id DESC`
- 200 response:
  - `ingestions[]` (array of Ingestion Schema)
  - `next_cursor` (string or `null`)
- Error behavior:
  - `400 BAD_REQUEST` for invalid query params (`limit`, `cursor`)
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed

### GET `/api/ingestions/:id`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- 200 response:
  - `ingestion` (Ingestion Schema)
  - `files[]` (array of Ingestion File Schema)
  - preview behavior:
    - image and video uploads return `preview.status = pending` after commit until a worker-generated thumbnail preview is uploaded back to VPS staging
    - unsupported media return `preview.status = unsupported`
    - retention-purged staged previews return `preview.status = purged`
    - `preview.url` is populated only when `preview.status = ready`
- Error behavior:
  - `400 BAD_REQUEST` for invalid `:id` format
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when ingestion does not exist in tenant scope

### GET `/api/ingestions/:id/files/:fileId/preview`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- Purpose: stream a committed ingestion file preview from temporary VPS staging.
- Preconditions:
  - ingestion file exists in tenant scope
  - `preview.status = ready`
  - retention-purged previews are unavailable
- 200 response:
  - raw preview bytes with preview `Content-Type`
- Notes:
  - preview bytes are served from temporary VPS staging storage
  - preview availability follows ingestion staging retention and cleanup rules
- Error behavior:
  - `400 BAD_REQUEST` for invalid path params
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when ingestion/file does not exist in tenant scope or preview is not ready

### POST `/api/worker/ingestion-previews/claim`

- Auth: worker token (`x-worker-auth-token`)
- Purpose: claim one committed ingestion file whose preview is pending so a worker can generate a temporary upload preview before final ingestion submission.
- Behavior:
  - returns at most one file per call
  - only considers committed files with `status = UPLOADED`
  - only considers ingestions still in upload-editable states (`DRAFT`, `UPLOADING`, `CANCELED`)
  - may reclaim a stale in-progress preview job after the claim timeout elapses
- 200 response:
  - `preview` (`null` when no work available) or:
    - `ingestion_id`
    - `file_id`
    - `tenant_id`
    - `batch_label`
    - `filename`
    - `content_type`
    - `size_bytes`
    - `download_url` (short-lived worker download URL for the committed staged original)
    - `claimed_by` (string, nullable)
    - `claimed_at` (ISO timestamp string, nullable)
- Error behavior:
  - `401 UNAUTHORIZED` for missing/invalid worker token

### POST `/api/worker/ingestion-previews/:id/files/:fileId/presign`

- Auth: worker token (`x-worker-auth-token`)
- Purpose: obtain a signed upload URL for a generated preview file belonging to a claimed ingestion preview job.
- Body:
  - `content_type` (string; generated thumbnail output MIME, currently one of `image/jpeg`, `image/png`, `image/webp`, `image/gif`, `image/avif`)
  - `size_bytes` (number; maximum 5 MiB)
  - `extension` (optional string; accepted for compatibility, storage extension is derived from validated `content_type`)
- 200 response:
  - `upload_token`
  - `upload_url`
  - `storage_key`
  - `expires_at`
  - `headers { content-type, content-length }`
- Notes:
  - `upload_url` uses the normal staged upload transport (`PUT /api/uploads/:token`)
- Error behavior:
  - `400 BAD_REQUEST` for invalid path/body shape
  - `401 UNAUTHORIZED` for missing/invalid worker token
  - `409 CONFLICT` when the preview is not currently claimed by that worker or upload preparation cannot be recorded

### POST `/api/worker/ingestion-previews/:id/files/:fileId/complete`

- Auth: worker token (`x-worker-auth-token`)
- Purpose: mark a claimed preview job ready after uploading preview bytes.
- Body:
  - `upload_token` (string)
  - `width` (number; generated thumbnail width, maximum 2048)
  - `height` (number; generated thumbnail height, maximum 2048)
- 200 response:
  - `status = ready`
  - `ingestion_id`
  - `file_id`
- Error behavior:
  - `400 BAD_REQUEST` for invalid path/body shape or mismatched upload token context
  - `401 UNAUTHORIZED` for missing/invalid worker token
  - `404 NOT_FOUND` when uploaded preview bytes are missing from staging
  - `409 CONFLICT` when the preview is not currently claimed by that worker or completion cannot be recorded

### POST `/api/worker/ingestion-previews/:id/files/:fileId/fail`

- Auth: worker token (`x-worker-auth-token`)
- Purpose: mark a claimed preview job failed with structured error details.
- Body:
  - `error`
    - `message` (string)
    - `code` (optional string)
    - `retryable` (optional boolean)
    - `details` (optional object)
- 200 response:
  - `status = failed`
  - `ingestion_id`
  - `file_id`
- Error behavior:
  - `400 BAD_REQUEST` for invalid path/body shape
  - `401 UNAUTHORIZED` for missing/invalid worker token
  - `409 CONFLICT` when the preview is not currently claimed by that worker or failure cannot be recorded

### POST `/api/ingestions/:id/items`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Body:
  - `item_index` (number, required, integer >= 1)
  - `classification_type` (optional)
  - `item_kind` (optional)
  - `language_code` (optional string, non-empty)
  - `title` (optional string, non-empty)
  - `summary` (optional JSON object)
- Preconditions:
  - ingestion status is `DRAFT`, `UPLOADING`, or `CANCELED`
  - ingestion has no active lease
  - the effective `classification_type` and `item_kind` pair must be compatible (item override when provided, otherwise ingestion defaults)
- 201 response:
  - `item` (Ingestion Item Schema)
- Error behavior:
  - `400 BAD_REQUEST` for invalid path/body shape
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when ingestion does not exist in tenant scope
  - `409 CONFLICT` when ingestion cannot be modified in current status, has active lease, or the effective `classification_type`/`item_kind` pair is incompatible

### GET `/api/ingestions/:id/items`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- 200 response:
  - `items[]` (array of Ingestion Item Schema)
- Error behavior:
  - `400 BAD_REQUEST` for invalid path params
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when ingestion does not exist in tenant scope

### PATCH `/api/ingestions/:id/items/order`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Body:
  - `items[]` (required, non-empty)
    - each item: `{ ingestion_item_id, item_index }`
- Semantics:
  - full-set reorder; payload must contain exactly all current ingestion items
  - `item_index` values must be unique and >= 1
- Preconditions:
  - ingestion status is `DRAFT`, `UPLOADING`, or `CANCELED`
  - ingestion has no active lease
  - the effective `classification_type` and `item_kind` pair after patching must remain compatible
- 200 response:
  - `items[]` (array of Ingestion Item Schema, ordered by `item_index`)
- Error behavior:
  - `400 BAD_REQUEST` for invalid path/body shape
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when ingestion does not exist in tenant scope
  - `409 CONFLICT` for full-set mismatch or immutable ingestion state

### PATCH `/api/ingestions/:id/items/:itemId`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Body (merge semantics; at least one field):
  - `classification_type` (optional)
  - `item_kind` (optional)
  - `language_code` (optional string, non-empty)
  - `title` (optional string or `null`)
  - `description` (optional string or `null`; stored in `summary.classification.summary`)
  - `tags` (optional string[]; normalized and de-duplicated)
  - `people` (optional string[]; stored as `summary.people.mentioned`)
  - `dates` (optional object):
    - `published` and/or `created` (partial patch allowed)
      - `value` (YYYY, YYYY-MM, YYYY-MM-DD, or `null`)
      - `approximate` (boolean)
      - `confidence` (`low`, `medium`, `high`)
      - `note` (string or `null`)
- Semantics:
  - merges into existing item `summary`; unspecified fields are preserved
  - `people` entries are trimmed, blank entries are rejected, and duplicates are removed in first-seen order
  - a supplied `people` array replaces only `summary.people.mentioned`; empty arrays clear it while preserving `authors`, `contributors`, `subjects`, and unrelated summary fields
- Preconditions:
  - ingestion status is `DRAFT`, `UPLOADING`, or `CANCELED`
  - ingestion has no active lease
- 200 response:
  - `item` (Ingestion Item Schema)
- Error behavior:
  - `400 BAD_REQUEST` for invalid path/body shape
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when ingestion/item does not exist in tenant scope
  - `409 CONFLICT` when ingestion cannot be modified in current status, has active lease, or the effective `classification_type`/`item_kind` pair is incompatible

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
  - `ingestion` (Ingestion Schema)
- Error behavior:
  - `400 BAD_REQUEST` for invalid body/field validation
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when ingestion does not exist in tenant scope
  - `409 CONFLICT` when ingestion cannot be modified in current status, has active lease, or the effective `classification_type`/`item_kind` pair is incompatible

### DELETE `/api/ingestions/:id`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Preconditions:
  - ingestion status is `DRAFT`, `UPLOADING`, or `CANCELED`
  - ingestion has not started processing (no active lease)
- 200 response:
  - `status` = `deleted`
  - `ingestion_id`
- Error behavior:
  - `400 BAD_REQUEST` for invalid `:id` format
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when ingestion does not exist in tenant scope
  - `409 CONFLICT` when ingestion cannot be deleted in current status or has active lease

### GET `/api/ingestions/capabilities`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- 200 response:
  - `media_kinds` (array of strings; current values: `image`, `audio`, `video`, `document`)
  - `extensions_by_kind`:
    - `image[]`, `audio[]`, `video[]`, `document[]` (array of strings)
  - `mime_by_kind`:
    - `image[]`, `audio[]`, `video[]`, `document[]` (array of strings)
  - `mime_aliases` (string map from alias MIME to canonical MIME)
- Error behavior:
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed

### POST `/api/ingestions/:id/files/presign`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Body (new file):
  - `filename` (string, required, non-empty)
  - `content_type` (string, required, non-empty)
  - `size_bytes` (number, required, integer >= 1)
- Body (re-presign existing):
  - `file_id` (UUID)
- Body must match exactly one shape above.
- Note:
  - item-level file ordering is managed via ingestion item file APIs (`sort_order`), not via presign payload.
- Behavior:
  - if ingestion is `CANCELED`, backend reopens it to `DRAFT` (no files) or `UPLOADING` (has files) before presign flow
  - re-presign is rejected when target file is already committed (`UPLOADED` or `VALIDATED`)
- 201 response:
  - `file_id`, `storage_key`, `upload_url`, `expires_at`, `headers { content-type, content-length }`
  - presigned upload TTL is 1 hour
- Error behavior:
  - `400 BAD_REQUEST` for invalid path/body shape
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when ingestion or provided `file_id` does not exist in tenant scope
  - `409 CONFLICT` when ingestion cannot accept files in current status or file is already committed

### DELETE `/api/ingestions/:id/files/:fileId`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Preconditions:
  - ingestion is in `DRAFT` or `UPLOADING`
  - if ingestion is `CANCELED`, backend reopens it first (`DRAFT` or `UPLOADING`), then applies delete
- Behavior:
  - deleting a file also deletes any temporary preview derivative stored for that ingestion file
- 200 response:
  - `status` = `deleted`
  - `file_id`
- Error behavior:
  - `400 BAD_REQUEST` for invalid path params
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when ingestion or file does not exist in tenant scope
  - `409 CONFLICT` when ingestion cannot be modified in current status

### POST `/api/ingestions/:id/files/:fileId/overrides`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Body:
  - `processing_overrides` (strict object)
    - optional keys: `ocr_text`, `audio_transcript`, `video_transcript`
    - each value: `{ enabled: boolean, language?: string }`
- Preconditions:
  - ingestion is in `DRAFT` or `UPLOADING`
  - if ingestion is `CANCELED`, backend reopens it first (`DRAFT` or `UPLOADING`)
- 200 response:
  - `file` (Ingestion File Schema)
- Error behavior:
  - `400 BAD_REQUEST` for invalid path/body shape
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when ingestion or file does not exist in tenant scope
  - `409 CONFLICT` when ingestion cannot be modified in current status

### POST `/api/ingestions/:id/files/commit`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Body:
  - `file_id` (UUID)
  - `checksum_sha256` (string, 64-char hex)
- Preconditions:
  - all ingestion files must share a single media kind (`image`, `audio`, `video`, `document`)
  - each file `content_type` must map to a supported media kind from ingestion capabilities
  - file media kind must be compatible with ingestion `item_kind`
    - `photo` -> `image`
    - `audio` -> `audio`
    - `video` -> `video`
    - `scanned_document` -> `image|document`
    - `document` -> `document`
    - `other` -> any media kind
- 200 response:
  - `file` (Ingestion File Schema)
  - preview behavior:
    - committed image/video files are marked `preview.status = pending`
    - committed unsupported media are marked `preview.status = unsupported`
- Error behavior:
  - `400 BAD_REQUEST` for invalid path/body shape
    - includes unsupported ingestion file `content_type` values
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when ingestion or file does not exist in tenant scope
  - `409 CONFLICT` for uncommittable state, missing staged upload, media-kind mismatch, item-kind/media-kind incompatibility, size mismatch, or checksum mismatch

### POST `/api/ingestions/:id/items/:itemId/files`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Body:
  - `ingestion_file_id` (UUID, required)
  - `role` (optional; `primary|front|back|page|attachment|transcript_source|side_a|side_b|other`)
  - `sort_order` (number, required, integer >= 1)
  - `page_number` (optional number, integer >= 1)
  - `is_primary` (optional boolean)
  - `logical_label` (optional string, non-empty)
- Preconditions:
  - ingestion status is `DRAFT`, `UPLOADING`, or `CANCELED`
  - ingestion has no active lease
  - file media kind must be compatible with the target item's effective `item_kind` (item override when set, otherwise ingestion `item_kind`)
- 201 response:
  - `file` (Ingestion Item File Schema)
- Error behavior:
  - `400 BAD_REQUEST` for invalid path/body shape
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when ingestion/item does not exist in tenant scope
  - `409 CONFLICT` when ingestion cannot be modified in current status, has active lease, file media is incompatible with item kind, or ordering constraints are violated

### GET `/api/ingestions/:id/items/:itemId/files`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- 200 response:
  - `files[]` (array of Ingestion Item File Schema, ordered by `sort_order`)
- Error behavior:
  - `400 BAD_REQUEST` for invalid path params
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when ingestion/item does not exist in tenant scope

### PATCH `/api/ingestions/:id/items/:itemId/files/order`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Body:
  - `files[]` (required, non-empty)
    - each file: `{ ingestion_file_id, sort_order }`
- Semantics:
  - full-set reorder; payload must contain exactly all files currently linked to item
  - `sort_order` values must be unique and >= 1
- Preconditions:
  - ingestion status is `DRAFT`, `UPLOADING`, or `CANCELED`
  - ingestion has no active lease
- 200 response:
  - `files[]` (array of Ingestion Item File Schema, ordered by `sort_order`)
- Error behavior:
  - `400 BAD_REQUEST` for invalid path/body shape
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when ingestion does not exist in tenant scope
  - `409 CONFLICT` for full-set mismatch or immutable ingestion state

### POST `/api/ingestions/:id/submit`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Preconditions:
  - at least one item and one file exist
  - every file is committed (`UPLOADED` or `VALIDATED`)
  - every item has at least one linked file
  - every committed file is linked to an item
  - status transition to `QUEUED` must be allowed by ingestion state machine
- 200 response:
  - `ingestion` (status transitions to queued flow)
- Error behavior:
  - `400 BAD_REQUEST` for invalid `:id` format
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when ingestion does not exist in tenant scope
  - `409 CONFLICT` when preconditions fail or transition is not allowed

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
- Error behavior:
  - `400 BAD_REQUEST` for invalid `:id` format
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when ingestion does not exist in tenant scope
  - `409 CONFLICT` when ingestion cannot be canceled in current status or has active lease

### POST `/api/ingestions/:id/restore`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Preconditions:
  - ingestion status is `CANCELED`
  - ingestion has not started processing (no active lease)
- 200 response:
  - `ingestion` (status transitions to draft or uploading based on files)
- Error behavior:
  - `400 BAD_REQUEST` for invalid `:id` format
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when ingestion does not exist in tenant scope
  - `409 CONFLICT` when status is not `CANCELED` or active lease exists

### POST `/api/ingestions/:id/retry`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- 200 response:
  - `ingestion` (status transitions to `QUEUED`)
- Behavior:
  - transition follows ingestion state machine rules
  - commonly used for `FAILED -> QUEUED`
- Error behavior:
  - `400 BAD_REQUEST` for invalid `:id` format
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when ingestion does not exist in tenant scope
  - `409 CONFLICT` when retry transition is not allowed

## Objects

Object response shapes in this section are authoritative with `src/validation/object.ts`.

### Object Schemas

### Base Object Schema

- `id` (string)
- `object_id` (string, format `OBJ-YYYYMMDD-XXXXXX`)
- `thumbnail_artifact_id` (string, nullable)
- `tenant_id` (string)
- `type` (`GENERIC|IMAGE|AUDIO|VIDEO|DOCUMENT`)
- `title` (string)
- `language` (string, nullable)
- `tags` (array of strings)
- `metadata` (JSON object)
- `source_ingestion_id` (string, nullable)
- `source_batch_label` (string, nullable)
- `processing_state`:
  - `queued|ingesting|ingested|derivatives_running|derivatives_done|ocr_running|ocr_done|index_running|index_done|processing_failed|processing_skipped`
- `curation_state`:
  - `needs_review|review_in_progress|reviewed|curation_failed`
- `availability_state`:
  - `AVAILABLE|ARCHIVED|RESTORE_PENDING|RESTORING|UNAVAILABLE`
- `access_level`:
  - `private|family|public`
- `embargo_kind`:
  - `none|timed|curation_state`
- `embargo_until` (ISO timestamp string, nullable)
- `embargo_curation_state` (`needs_review|review_in_progress|reviewed|curation_failed`, nullable)
- `rights_note` (string, nullable)
- `sensitivity_note` (string, nullable)
- `created_at` (ISO timestamp string)
- `updated_at` (ISO timestamp string)

### Object List Item Schema

`object_list_item = base object +`

- `can_download` (boolean)
- `access_reason_code`:
  - `OK|FORBIDDEN_POLICY|EMBARGO_ACTIVE|RESTORE_REQUIRED|RESTORE_IN_PROGRESS|TEMP_UNAVAILABLE`
- `has_access_pdf` (boolean): at least one materialized `object_artifacts.kind = pdf` artifact exists
- `has_ocr` (boolean): at least one materialized `object_artifacts.kind = ocr_text` artifact exists
- artifact indicators describe inventory, not requester authorization; use `can_download` and `access_reason_code` to decide whether retrieval is allowed
- no index/embedding indicator is exposed because index availability has no first-class inventory source

### Object Detail Schema

`object_detail = base object +`

- `ingest_manifest` (JSON object, nullable)
- `is_authorized` (boolean)
- `is_deliverable` (boolean)
- `can_download` (boolean)
- `access_reason_code`:
  - `OK|FORBIDDEN_POLICY|EMBARGO_ACTIVE|RESTORE_REQUIRED|RESTORE_IN_PROGRESS|TEMP_UNAVAILABLE`

### Object Viewer Schema

`GET /api/objects/:object_id` returns `viewer` as a top-level sibling of `object`:

- response shape: `{ object, viewer }`
- `viewer` provides the canonical media rendering contract for the frontend and eliminates frontend heuristics

The `viewer` block is `null` for `GENERIC` object types until a viewer strategy is defined.

#### Viewer Top-Level Fields

- `media_type` (`document|image|audio|video`)
  - Canonical viewer media type derived from object type
  - `DOCUMENT` → `document`, `IMAGE` → `image`, `AUDIO` → `audio`, `VIDEO` → `video`

- `primary_source` (object)
  - The single canonical source the frontend should use for the main media canvas
  - `source_type` (`original|access_copy|stream|preview|other`)
    - Viewer-facing abstraction (UI should key behavior off this + status)
  - `artifact_kind` (`original|preview|pdf|ocr_text|thumbnail|web_version|transcript|other`)
    - Backend-facing concrete identity (for diagnostics, not UI decisions)
    - current primary-source selection uses viewer-facing kinds (`pdf`, `web_version`, `original`, `preview`, `other`)
    - `ocr` and `metadata` are internal backend kinds that may exist in artifact inventories but are not selected as canonical `primary_source` in current implementation
    - OCR for viewer contracts is represented via `ocr_text` references (`preview_artifacts.ocr_text`, `viewer_payload.ocr_text_artifact_id`)
  - `variant` (string, nullable)
    - Artifact variant identifier
  - `status` (`available|request_required|request_pending|restricted|temporarily_unavailable`)
    - Determines the UI state for the primary media canvas
  - `available_file_id` (string, nullable)
    - ID of requestable available-file when artifact not yet materialized
  - `artifact_id` (string, nullable)
    - ID of materialized artifact when available
  - `display_name` (string, nullable)
    - Human-readable name for the source
  - `content_type` (string, nullable)
    - MIME type of the source
  - `size_bytes` (number, nullable)
    - File size in bytes
  - `access_reason_code` (`OK|FORBIDDEN_POLICY|EMBARGO_ACTIVE|RESTORE_REQUIRED|RESTORE_IN_PROGRESS|TEMP_UNAVAILABLE`)
    - Reason for current access state

- `active_request` (object, nullable)
  - Present when an artifact fetch request is in progress for the primary source
  - `id` (string)
  - `action_type` (`artifact_fetch`)
  - `status` (`PENDING|PROCESSING`)
  - `created_at` (ISO timestamp)
  - `updated_at` (ISO timestamp)

- `preview_artifacts` (object)
  - Supporting artifacts available independently of primary source
  - `thumbnail` (object, nullable)
    - `available` (boolean, always `true` when present)
    - `artifact_id` (string)
    - `content_type` (string, nullable)
    - `display_name` (string, nullable)
    - `metadata` (JSON object)
  - `poster` (object, nullable) - Video poster image
  - `ocr_text` (object, nullable) - OCR text artifact
  - `transcript` (object, nullable) - Transcript artifact
  - `captions` (object, nullable) - Caption/subtitle artifact (reserved for future)

- `viewer_payload` (media-specific object)
  - Render-oriented payload for the viewer component
  - Schema varies by `media_type` (see below)

#### Primary Source Status Guide

| Status | Meaning | UI Action |
|--------|---------|-----------|
| `available` | Primary source ready to render | Display the media using `artifact_id` |
| `request_required` | User can access, but source must be fetched | Show "Request" button; may show `preview_artifacts` |
| `request_pending` | Fetch already in progress | Show loading/waiting state; check `active_request` |
| `restricted` | Access denied by policy/embargo | Show access denied message using `access_reason_code` |
| `temporarily_unavailable` | Source expected but not deliverable | Show unavailable message; may retry later |

#### Media-Specific Viewer Payloads

**Document Payload** (`kind: "document"`):
- `artifact_id` (string, nullable) - PDF or document artifact ID
- `content_type` (string, nullable) - e.g., `application/pdf`
- `ocr_text_artifact_id` (string, nullable) - OCR text artifact ID
- `page_count` (number, nullable) - Total page count
- `pages` (array, optional) - Page-level structure:
  - `page_number` (number)
  - `label` (string, nullable)
  - `image_artifact_id` (string, nullable)
  - `ocr_text_artifact_id` (string, nullable)

**Image Payload** (`kind: "image"`):
- `artifact_id` (string, nullable)
- `content_type` (string, nullable) - e.g., `image/jpeg`
- `width` (number, nullable) - Pixel width
- `height` (number, nullable) - Pixel height

**Audio Payload** (`kind: "audio"`):
- `artifact_id` (string, nullable) - Browser-playable audio artifact
- `content_type` (string, nullable) - e.g., `audio/mpeg`
- `transcript_artifact_id` (string, nullable)
- `duration_seconds` (number, nullable)

**Video Payload** (`kind: "video"`):
- `artifact_id` (string, nullable) - Browser-playable video artifact
- `content_type` (string, nullable) - e.g., `video/mp4`
- `poster_artifact_id` (string, nullable)
- `transcript_artifact_id` (string, nullable)
- `captions_artifact_id` (string, nullable) - Reserved for future
- `duration_seconds` (number, nullable)

#### Example: Available Document

```json
{
  "object": {
    "id": "OBJ-20260213-ABC123",
    "object_id": "OBJ-20260213-ABC123",
    "type": "DOCUMENT",
    "title": "Historical Document",
    "is_authorized": true,
    "is_deliverable": true,
    "can_download": true,
    "access_reason_code": "OK"
  },
  "viewer": {
    "media_type": "document",
    "primary_source": {
      "source_type": "access_copy",
      "artifact_kind": "pdf",
      "variant": null,
      "status": "available",
      "available_file_id": "70000000-0000-4000-8000-000000000001",
      "artifact_id": "60000000-0000-4000-8000-000000000099",
      "display_name": "Reading PDF",
      "content_type": "application/pdf",
      "size_bytes": 12345,
      "access_reason_code": "OK"
    },
    "active_request": null,
    "preview_artifacts": {
      "thumbnail": {
        "available": true,
        "artifact_id": "60000000-0000-4000-8000-000000000777",
        "content_type": "image/jpeg",
        "display_name": "Thumbnail",
        "metadata": {}
      },
      "poster": null,
      "ocr_text": {
        "available": true,
        "artifact_id": "60000000-0000-4000-8000-000000000778",
        "content_type": "text/plain",
        "display_name": "OCR Text",
        "metadata": { "page_count": 12 }
      },
      "transcript": null,
      "captions": null
    },
    "viewer_payload": {
      "kind": "document",
      "artifact_id": "60000000-0000-4000-8000-000000000099",
      "content_type": "application/pdf",
      "ocr_text_artifact_id": "60000000-0000-4000-8000-000000000778",
      "page_count": 12,
      "pages": [
        { "page_number": 1, "label": "1", "image_artifact_id": null, "ocr_text_artifact_id": null },
        { "page_number": 2, "label": "2", "image_artifact_id": null, "ocr_text_artifact_id": null }
      ]
    }
  }
}
```

#### Example: Request-Required Video

```json
{
  "object": {
    "id": "OBJ-20260213-VID001",
    "object_id": "OBJ-20260213-VID001",
    "type": "VIDEO",
    "title": "Family Interview",
    "is_authorized": true,
    "is_deliverable": true,
    "can_download": true,
    "access_reason_code": "OK"
  },
  "viewer": {
    "media_type": "video",
    "primary_source": {
      "source_type": "stream",
      "artifact_kind": "web_version",
      "variant": null,
      "status": "request_required",
      "available_file_id": "70000000-0000-4000-8000-000000000201",
      "artifact_id": null,
      "display_name": "Web Version",
      "content_type": "video/mp4",
      "size_bytes": 52428800,
      "access_reason_code": "RESTORE_REQUIRED"
    },
    "active_request": null,
    "preview_artifacts": {
      "thumbnail": {
        "available": true,
        "artifact_id": "60000000-0000-4000-8000-000000000801",
        "content_type": "image/jpeg",
        "display_name": "Thumbnail",
        "metadata": {}
      },
      "poster": {
        "available": true,
        "artifact_id": "60000000-0000-4000-8000-000000000802",
        "content_type": "image/jpeg",
        "display_name": "Poster",
        "metadata": {}
      },
      "ocr_text": null,
      "transcript": null,
      "captions": null
    },
    "viewer_payload": {
      "kind": "video",
      "artifact_id": null,
      "content_type": "video/mp4",
      "poster_artifact_id": "60000000-0000-4000-8000-000000000802",
      "transcript_artifact_id": null,
      "captions_artifact_id": null,
      "duration_seconds": 642
    }
  }
}
```

#### Frontend Usage Patterns

1. **Check `viewer` is not null** - Only `DOCUMENT`, `IMAGE`, `AUDIO`, `VIDEO` types have viewer contracts

2. **Key UI decisions off `primary_source.source_type` + `primary_source.status`**, not `artifact_kind`

3. **When `status = "available"`**:
   - Use `viewer_payload` to configure the viewer
   - Load media via `GET /api/objects/:object_id/artifacts/:artifact_id/view`
   - Also load any `preview_artifacts` the UI wants to display

4. **When `status = "request_required"`**:
   - Show request gate UI
   - Display `preview_artifacts.thumbnail` or `preview_artifacts.poster` if available
   - Call `POST /api/objects/:object_id/download-requests` to initiate fetch

5. **When `status = "request_pending"`**:
   - Show loading/waiting state
   - Poll for changes or use `active_request` to show progress

6. **Never construct direct URLs** - Always resolve artifacts through the `/view` endpoint

### GET `/api/objects`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- Query params:
  - `limit` (optional integer, default `50`, min `1`, max `200`)
  - `cursor` (optional opaque cursor from previous response)
    - cursor is sort-aware and must match the requested `sort`
    - required cursor payload by sort:
      - `created_at_desc|created_at_asc`: `{ sort, created_at, object_id }`
      - `updated_at_desc|updated_at_asc`: `{ sort, updated_at, object_id }`
      - `title_asc|title_desc`: `{ sort, title, object_id }`
  - `sort` (optional)
    - allowed: `created_at_desc` (default), `created_at_asc`, `updated_at_desc`, `updated_at_asc`, `title_asc`, `title_desc`
  - `q` (optional text search)
    - trimmed before use; an empty trimmed value is equivalent to omission
    - maximum trimmed length is 256 characters; longer values return `400 BAD_REQUEST`
    - case-insensitive literal substring match; `%`, `_`, and `\` are escaped and have no wildcard meaning
    - no relevance ranking or match snippets in V1; requested `sort` remains authoritative
    - always searches tenant-catalog `title` and `object_id`
    - also searches materialized `object_artifacts` metadata: `id`, `kind`, `variant`, and `content_type`
    - when authoritative persisted materialization provenance associates an artifact with an `object_available_files` source, also searches its `display_name` and `archive_file_key`; the association is not inferred from `kind`/`variant`, and available-file-only inventory is excluded
    - searches indexed bodies only for already-materialized, eligible OCR/transcript text artifacts, plus `object_curated_document_pages.curated_text`
    - does not extract arbitrary PDF, image, audio, or video bytes; an extracted OCR/transcript artifact must first be materialized and indexed
    - artifact metadata, artifact body, and curated-text matches apply only when the requester currently passes the same effective content-view conditions: endpoint role, access-level/assignment authorization (including admin override), inactive embargo, and object `availability_state = AVAILABLE`
    - inaccessible artifact-derived terms cannot cause an object to match; access is evaluated at query time even when text is stored in an index
    - artifact body indexing accepts only successfully materialized, decodable text for OCR/transcript artifact kinds and skips empty, malformed/non-text, unavailable, and over-limit bodies
    - maximum indexable text-body size is configurable per artifact; recommended conservative default is 10 MiB, with the configuration name deferred to implementation
  - `availability_state` (optional: `AVAILABLE`, `ARCHIVED`, `RESTORE_PENDING`, `RESTORING`, `UNAVAILABLE`)
  - `access_level` (optional: `private`, `family`, `public`)
  - `language` (optional)
  - `batch_label` (optional)
    - Filters by source ingestion batch label (same dimension returned as `source_batch_label` in list rows).
  - `type` (`GENERIC|IMAGE|AUDIO|VIDEO|DOCUMENT`, optional)
  - `from` (ISO timestamp with offset, optional)
  - `to` (ISO timestamp with offset, optional)
  - `tag` (optional non-empty string)
- 200 response:
  - `objects[]` (`object_list_item[]`; does **not** include `ingest_manifest`)
  - `next_cursor` (string or `null`)
  - `total_count` (total tenant-visible objects before filters)
  - `filtered_count` (total matching current filters)

List row guarantees (`objects[]`): all fields from base object schema, plus `can_download` and `access_reason_code`; excludes `ingest_manifest`.

Sort semantics:

- default sort: `created_at_desc`
- sorting is deterministic
- tie-breaker includes `object_id` for stable cursor paging
- cursor is sort-aware and preserves ordering across pages
- cursor is rejected when payload shape does not match the requested `sort`

Example response:

```json
{
  "objects": [
    {
      "id": "OBJ-20260213-ABC123",
      "object_id": "OBJ-20260213-ABC123",
      "thumbnail_artifact_id": "60000000-0000-4000-8000-000000000777",
      "title": "Document title",
      "processing_state": "queued",
      "curation_state": "needs_review",
      "availability_state": "AVAILABLE",
      "access_level": "private",
      "type": "DOCUMENT",
      "language": "en",
      "tags": ["source:family_archive", "subject:history"],
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

Each object item includes `thumbnail_artifact_id`:

- preferred thumbnail artifact id (`variant = null` preferred, otherwise latest)
- `null` when no thumbnail artifact currently exists
- Error behavior:
  - `400 BAD_REQUEST` for invalid query params (`limit`, `cursor`, `sort`, `q`, filters)
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed

### GET `/api/objects/:object_id`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- 200 response:
  - `object` (`object_detail`)
    - includes `tags`
    - includes `ingest_manifest` (or `null`)
    - includes `thumbnail_artifact_id` (`null` when no thumbnail artifact exists)
  - `viewer` (`object_viewer`, nullable)
    - includes canonical `media_type`
    - includes canonical `primary_source`
    - includes relevant `active_request` when present
    - includes `preview_artifacts`
    - includes media-specific `viewer_payload`
- Error behavior:
  - `400 BAD_REQUEST` for invalid `:object_id` format
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when object does not exist in tenant scope

Example response:

```json
{
  "object": {
    "id": "OBJ-20260213-ABC123",
    "object_id": "OBJ-20260213-ABC123",
    "thumbnail_artifact_id": "60000000-0000-4000-8000-000000000777",
    "tenant_id": "00000000-0000-0000-0000-000000000001",
    "type": "DOCUMENT",
    "title": "Document title",
    "language": "en",
    "tags": ["source:family_archive", "subject:history"],
    "metadata": {},
    "source_ingestion_id": "13dd3927-17be-4211-9a77-fdea3104a028",
    "source_batch_label": "batch-2026-02-13-001",
    "processing_state": "index_done",
    "curation_state": "needs_review",
    "availability_state": "AVAILABLE",
    "access_level": "private",
    "embargo_kind": "none",
    "embargo_until": null,
    "embargo_curation_state": null,
    "rights_note": null,
    "sensitivity_note": null,
    "created_at": "2026-02-13T20:22:29.993Z",
    "updated_at": "2026-02-14T08:01:00.000Z",
    "ingest_manifest": {
      "schema_version": "1.0"
    },
    "is_authorized": true,
    "is_deliverable": true,
    "can_download": true,
    "access_reason_code": "OK"
  },
  "viewer": {
    "media_type": "document",
    "primary_source": {
      "source_type": "access_copy",
      "artifact_kind": "pdf",
      "variant": null,
      "status": "available",
      "available_file_id": "70000000-0000-4000-8000-000000000001",
      "artifact_id": "60000000-0000-4000-8000-000000000099",
      "display_name": "Reading PDF",
      "content_type": "application/pdf",
      "size_bytes": 12345,
      "access_reason_code": "OK"
    },
    "active_request": null,
    "preview_artifacts": {
      "thumbnail": null,
      "poster": null,
      "ocr_text": null,
      "transcript": null,
      "captions": null
    },
    "viewer_payload": {
      "kind": "document",
      "artifact_id": "60000000-0000-4000-8000-000000000099",
      "content_type": "application/pdf",
      "ocr_text_artifact_id": null,
      "page_count": null
    }
  }
}
```

### GET `/api/archive-requests`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- Purpose:
  - unified request visibility endpoint for tenant-wide and object-specific archive tasks
  - use filters instead of separate per-task GET endpoints
- Query params:
  - `limit` (optional int `1..200`, default `50`)
  - `cursor` (optional pagination cursor)
  - `sort` (optional; currently `created_at_desc` only)
  - `target_type` (optional `object|ingestion`)
  - `target_id` (optional string; requires `target_type`; for `target_type=object` must match `OBJ-YYYYMMDD-XXXXXX`)
  - `action_type` (optional `object_resync|artifact_fetch|curation_apply`)
  - `status` (optional; can be repeated and/or comma-separated; values `PENDING|PROCESSING|COMPLETED|FAILED|CANCELED`)
  - `active_only` (optional boolean; when `true`, equivalent to filtering `PENDING|PROCESSING`)
    - precedence: when `active_only=true`, backend ignores explicit `status` filters and uses `PENDING|PROCESSING`
  - `include_payload` (optional boolean, default `false`; when `true`, each item includes `action_payload`)
- 200 response:
   - `requests[]` where each item includes:
     - `id`, `tenant_id`, `target_type`, `target_id`, `action_type`
     - `requested_by`, `dedupe_key`, `status`
    - `failure_reason`, `failure_details`
    - `created_at`, `updated_at`, `completed_at`
     - `action_payload` only when `include_payload=true`
   - `dedupe_key` is `string|null`; `null` means the request has no keyed active-request deduplication identity
  - `next_cursor` (`string|null`)
  - `filtered_count` (number)
- Error behavior:
  - `400 BAD_REQUEST` for invalid query params/cursor/filter combinations
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed

### Object Editing Authorization

The object editing endpoints below apply both their listed role requirement and object access policy. For non-admin users, `public` objects require no assignment, `family` objects require a `family` or `private` assignment, and `private` objects require a `private` assignment. Embargo and artifact availability do not limit editing. In-tenant policy denial returns `403 FORBIDDEN`; missing or cross-tenant objects return `404 NOT_FOUND`.

### GET `/api/objects/:object_id/edit`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Purpose:
  - backend object editing read model
  - returns editable first-class metadata, rights fields, edit lock state, and current capabilities
  - current implementation includes metadata editing plus document OCR page curation payloads for document objects
  - **auto-acquires an edit lock** for the requesting user (or extends an existing lock held by the same user)
- 200 response:
  - `object_id`
  - `revision` (current non-negative edit revision)
  - `media_type` (`document|image|audio|video|other`)
  - `curation_state`
  - `lock`:
    - `locked` (boolean)
    - `locked_by` (user id or `null`)
    - `locked_until` (ISO timestamp or `null`)
    - when another user holds the lock, `capabilities` are all `false`
  - `draft` (`null` before first user edit, otherwise `{ updated_at, updated_by }`)
    - `updated_by` is currently a user id or `null`
  - `metadata`:
    - `title`
    - `publication_date`
    - `date_precision` (`none|year|month|day`)
    - `date_approximate`
    - `language`
    - `tags[]`
    - `people[]`
    - `description`
  - `rights`:
    - `access_level`
    - `rights_note`
    - `sensitivity_note`
    - `access_level` is read-only in this contract
  - `capabilities`:
    - `can_edit_metadata`
    - `can_curate_text`
    - `can_submit_review`
  - `curation_payload`:
    - for `document`:
      - `kind = document`
      - `machine_ocr_artifact_id` (string or `null`; `null` when no OCR artifact exists)
      - `page_count` (integer or `null`; `null` when no valid page count in metadata)
      - `pages[]` (`page_number`, `label` (string or `null`), `machine_text`, `curated_text` (string or `null`), `status`)
    - for non-document types:
      - `kind` (`image|audio|video|other`)
- Example response:

```json
{
  "object_id": "OBJ-20260213-ABC123",
  "revision": 1,
  "media_type": "document",
  "curation_state": "needs_review",
  "lock": {
    "locked": true,
    "locked_by": "10000000-0000-0000-0000-000000000001",
    "locked_until": "2026-04-21T23:00:00.000Z"
  },
  "draft": {
    "updated_at": "2026-04-14T10:32:44.000Z",
    "updated_by": "10000000-0000-0000-0000-000000000001"
  },
  "metadata": {
    "title": "Edited Metadata Title",
    "publication_date": "1987-06-14",
    "date_precision": "day",
    "date_approximate": true,
    "language": "Tajik",
    "tags": ["migration", "oral history"],
    "people": ["Zarina T.", "M. Davlatov"],
    "description": "Updated description"
  },
  "rights": {
    "access_level": "private",
    "rights_note": "Updated rights note",
    "sensitivity_note": "Updated sensitivity note"
  },
  "capabilities": {
    "can_edit_metadata": true,
    "can_curate_text": true,
    "can_submit_review": true
  },
  "curation_payload": {
    "kind": "document",
    "machine_ocr_artifact_id": "60000000-0000-4000-8000-000000000881",
    "page_count": 2,
    "pages": [
      {
        "page_number": 1,
        "label": "1",
        "machine_text": "Machine OCR page 1...",
        "curated_text": null,
        "status": "machine"
      },
      {
        "page_number": 2,
        "label": "2",
        "machine_text": "Machine OCR page 2...",
        "curated_text": "Curated OCR page 2...",
        "status": "edited"
      }
    ]
  }
}
```
- Error behavior:
  - `400 BAD_REQUEST` for invalid `:object_id` format
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when object does not exist in tenant scope
  - when another user holds the edit lock, returns `200` with current lock information and all edit capabilities set to `false`

### PATCH `/api/objects/:object_id/metadata`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Purpose:
  - replaces the complete editor-managed metadata and rights-note field set
- Body:
  - `revision` (required non-negative integer from `GET /edit`)
  - all listed `metadata` and `rights` fields are required; unknown fields are rejected
  - `metadata`:
    - `title` (required non-empty string)
    - `publication_date` (required string; normalized to `""` when `date_precision = none`)
    - `date_precision` (`none|year|month|day`, required)
    - `date_approximate` (required boolean; normalized to `false` when `date_precision = none`)
    - `language` (nullable string)
    - `tags[]` (required array of non-empty strings; normalized lowercase, de-duplicated before normalization)
    - `people[]` (required array of non-empty strings; de-duplicated after trimming)
    - `description` (nullable string)
  - `rights`:
    - `rights_note` (nullable string; empty strings normalize to `null`)
    - `sensitivity_note` (nullable string; empty strings normalize to `null`)
- Example request:

```json
{
  "revision": 1,
  "metadata": {
    "title": "Edited Metadata Title",
    "publication_date": "1987-06-14",
    "date_precision": "day",
    "date_approximate": true,
    "language": "Tajik",
    "tags": ["Oral History", "Migration"],
    "people": ["Zarina T.", "M. Davlatov"],
    "description": "Updated description"
  },
  "rights": {
    "rights_note": "Updated rights note",
    "sensitivity_note": "Updated sensitivity note"
  }
}
```
- 200 response:
  - `object_id`
  - `revision`
  - `curation_state`
  - `updated_at`
- Example success response:

```json
{
  "object_id": "OBJ-20260213-ABC123",
  "revision": 2,
  "curation_state": "needs_review",
  "updated_at": "2026-04-14T10:38:52.000Z"
}
```
- Error behavior:
  - `400 BAD_REQUEST` for invalid `:object_id` format
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when object does not exist in tenant scope
  - `423 LOCKED` when another user holds the edit lock
  - `409 REVISION_CONFLICT` with `error.details.latest_revision` when request revision is stale
  - `422 VALIDATION_FAILED` for invalid metadata payload
- Example `423 LOCKED`:

```json
{
  "request_id": "uuid",
  "error": {
    "code": "LOCKED",
    "message": "Object is currently being edited by another user.",
    "details": {
      "locked_by": "10000000-0000-0000-0000-000000000002",
      "locked_until": "2026-04-21T23:00:00.000Z"
    }
  }
}
```
- Example `409 REVISION_CONFLICT`:

```json
{
  "request_id": "uuid",
  "error": {
    "code": "REVISION_CONFLICT",
    "message": "Object metadata revision is stale.",
    "details": {
      "latest_revision": 2
    }
  }
}
```
- Example `422 VALIDATION_FAILED`:

```json
{
  "request_id": "uuid",
  "error": {
    "code": "VALIDATION_FAILED",
    "message": "Validation failed.",
    "details": [
      {
        "path": "metadata.publication_date",
        "code": "INVALID"
      }
    ]
  }
}
```

### PUT `/api/objects/:object_id/curation/document`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Purpose:
  - saves curated OCR text page-by-page for one document object
- Body:
  - `revision` (required non-negative integer from `GET /edit`)
  - `pages[]` (required, non-empty):
    - `page_number` (required positive integer)
    - `curated_text` (required string; may be empty)
- Rules:
  - valid only for document objects
  - only submitted pages are updated
  - duplicate `page_number` values in one request are rejected
  - page numbers must exist in the current document page projection
- Example request:

```json
{
  "revision": 1,
  "pages": [
    {
      "page_number": 1,
      "curated_text": "Curated page 1 text..."
    },
    {
      "page_number": 2,
      "curated_text": "Curated page 2 text..."
    }
  ]
}
```
- 200 response:
  - `object_id`
  - `revision`
  - `updated_count` (integer >= 1)
  - `updated_at`
- Example success response:

```json
{
  "object_id": "OBJ-20260213-ABC123",
  "revision": 2,
  "updated_count": 2,
  "updated_at": "2026-04-14T11:00:00.000Z"
}
```
- Error behavior:
  - `400 BAD_REQUEST` for invalid `:object_id` format
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when object does not exist in tenant scope
  - `423 LOCKED` when another user holds the edit lock
  - `409 CONFLICT` when object is not a document
  - `409 REVISION_CONFLICT` with `error.details.latest_revision` when request revision is stale
  - `422 VALIDATION_FAILED` for invalid page payload or invalid page numbers
- Example `422 VALIDATION_FAILED`:

```json
{
  "request_id": "uuid",
  "error": {
    "code": "VALIDATION_FAILED",
    "message": "Validation failed.",
    "details": [
      {
        "path": "pages",
        "code": "INVALID_PAGE_NUMBER",
        "page_number": 999
      }
    ]
  }
}
```

### POST `/api/objects/:object_id/curation/submit`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Purpose:
  - submits current OCR curation state for archive-side apply
  - creates a `curation_apply` archive request with archive-compatible payload
- Body:
  - `revision` (required non-negative integer from `GET /edit`)
  - `review_note` (nullable string)
- Rules:
  - currently supported for document OCR curation only
  - backend assembles the current OCR document text and enqueues `curation_apply`
  - `dedupe_key` on the archive request uses stable `idempotency_key` format: `<object_id>:<curated_kind>:vpsrev-<revision_after>`
  - the revision-qualified `idempotency_key` is derived from the request revision plus one
  - the same publication revision is an exact retry in any status and returns the original request without another revision increment; `submitted_at` and `submitted_by` remain the original archive request's creation time and requester
  - at most one `PENDING|PROCESSING` `curation_apply` request exists per tenant and object; a different publication is rejected rather than deduplicated while that request is active
  - after the request reaches `COMPLETED|FAILED|CANCELED`, a newer publication may create a new request, including another write to the same archive day-file
  - the payload includes `publication_revision`, `target_version`, `size_bytes`, `checksum_sha256`, and a relative `request_source` URL; the worker resolves it against its VPS base URL and supplies its active lease token
- Example request:

```json
{
  "revision": 2,
  "review_note": "Ready for archive apply."
}
```
- 200 response:
  - `object_id`
  - `revision`
  - `curation_state`
  - `request` (`id`, `action_type`, `status`)
  - `submitted_at`
  - `submitted_by`
- Example success response:

```json
{
  "object_id": "OBJ-20260213-ABC123",
  "revision": 3,
  "curation_state": "review_in_progress",
  "request": {
    "id": "11111111-1111-4111-8111-111111111111",
    "action_type": "curation_apply",
    "status": "PENDING"
  },
  "submitted_at": "2026-04-14T11:15:00.000Z",
  "submitted_by": "10000000-0000-0000-0000-000000000001"
}
```
- Error behavior:
  - `400 BAD_REQUEST` for invalid `:object_id` format
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when object does not exist in tenant scope
  - `423 LOCKED` when another user holds the edit lock
  - `409 CONFLICT` when object is not a document or when document OCR projection is unavailable
  - `409 REVISION_CONFLICT` with `error.details.latest_revision` when request revision is stale
  - `409 PUBLICATION_ALREADY_ACTIVE` when a different `curation_apply` request is already `PENDING|PROCESSING`; `error.details` includes `existing_request_id` and `existing_request_status`
  - `422 VALIDATION_FAILED` for invalid payload

- Example active-publication response:

```json
{
  "request_id": "uuid",
  "error": {
    "code": "PUBLICATION_ALREADY_ACTIVE",
    "message": "A curation publication is already active for this object.",
    "details": {
      "existing_request_id": "11111111-1111-4111-8111-111111111111",
      "existing_request_status": "PROCESSING"
    }
  }
}
```

### GET `/api/objects/:object_id/curation-publication`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Returns the active (`PENDING|PROCESSING`) publication for the tenant/object when present; otherwise returns the latest publication, or `request: null`
- Response request includes raw `status`, `publication_revision`, `target_version`, failure reason, and lifecycle timestamps
- Clients must preserve unknown raw statuses and must not infer that an unknown status is active

### DELETE `/api/objects/:object_id/edit-lock`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Purpose:
  - explicitly releases the edit lock held by the current user
  - should be called when the client closes the editor
- 200 response:
  - `object_id`
  - `released` (boolean; `true` if lock was released, `false` if user did not hold the lock)
- Error behavior:
  - `400 BAD_REQUEST` for invalid `:object_id` format
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when object does not exist in tenant scope

### GET `/api/objects/:object_id/curation/history`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- Query params:
  - `limit` (optional int `1..200`, default `50`)
  - `cursor` (optional pagination cursor)
- 200 response:
  - `object_id`
  - `events[]`:
    - `id`
    - `type` (`METADATA_UPDATED|RIGHTS_UPDATED|DOCUMENT_PAGE_UPDATED|CURATION_SUBMITTED`)
    - `actor_user_id`
    - `at`
    - `revision_before` (integer or `null`)
    - `revision_after` (integer or `null`)
    - `payload`
  - `next_cursor` (`string|null`)
- Notes:
  - history describes backend edit actions, not archive artifact version history
  - `revision_before` and `revision_after` are `null` only for legacy history records without revision data
  - `CURATION_SUBMITTED` payload includes `request_id`, `review_note`, `archive_curated_kind`, `archive_target_version`, `archive_idempotency_key`
- Example response:

```json
{
  "object_id": "OBJ-20260213-ABC123",
  "events": [
    {
      "id": "11111111-1111-4111-8111-111111111111",
      "type": "RIGHTS_UPDATED",
      "actor_user_id": "10000000-0000-0000-0000-000000000001",
      "at": "2026-04-14T10:38:52.000Z",
      "revision_before": 1,
      "revision_after": 2,
      "payload": {
        "fields": ["rights_note", "sensitivity_note"]
      }
    },
    {
      "id": "33333333-3333-4333-8333-333333333333",
      "type": "DOCUMENT_PAGE_UPDATED",
      "actor_user_id": "10000000-0000-0000-0000-000000000001",
      "at": "2026-04-14T10:39:30.000Z",
      "revision_before": 2,
      "revision_after": 3,
      "payload": {
        "page_numbers": [1, 2]
      }
    },
    {
      "id": "22222222-2222-4222-8222-222222222222",
      "type": "METADATA_UPDATED",
      "actor_user_id": "10000000-0000-0000-0000-000000000001",
      "at": "2026-04-14T10:38:52.000Z",
      "revision_before": 1,
      "revision_after": 2,
      "payload": {
        "fields": [
          "title",
          "publication_date",
          "date_precision",
          "date_approximate",
          "language",
          "tags",
          "people",
          "description"
        ]
      }
    }
  ],
  "next_cursor": null
}
```
- Error behavior:
  - `400 BAD_REQUEST` for invalid path/query params
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when object does not exist in tenant scope

### GET `/api/objects/:object_id/artifacts`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- 200 response:
  - `object_id`
  - `artifacts[]` (`id`, `object_id`, `kind`, `variant`, `storage_key`, `content_type`, `size_bytes`, `created_at`)
- Notes:
  - `storage_key` is internal storage metadata, not a direct client URL
  - frontend must resolve delivery through `/view` or `/download` endpoints
  - this list may include artifacts that are not inline-viewable via `/view`
  - `kind` uses full backend enum (includes internal kinds such as `ocr`, `metadata`, `ingest_json`, `pipeline_json`, `catalog_json`)
- Error behavior:
  - `400 BAD_REQUEST` for invalid `:object_id` format
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when object does not exist in tenant scope

### GET `/api/objects/:object_id/available-files`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- 200 response:
  - `object_id`
  - `available_files[]` (`id`, `object_id`, `archive_file_key`, `artifact_kind`, `variant`, `display_name`, `content_type`, `size_bytes`, `checksum_sha256`, `metadata`, `is_available`, `synced_at`)
- Notes:
  - `artifact_kind` uses full backend enum, including internal kinds
  - `ocr` is backend/internal OCR artifact kind; `ocr_text` is the normalized OCR text artifact kind used by viewer contracts
- Error behavior:
  - `400 BAD_REQUEST` for invalid `:object_id` format
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when object does not exist in tenant scope

### POST `/api/objects/:object_id/download-requests`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- Body:
  - `available_file_id` (UUID from `GET /api/objects/:object_id/available-files`, required)
- Behavior:
  - If matching artifact already exists on backend (`object_id + artifact_kind + variant`), request is not queued and response returns `status = available` with artifact payload.
  - If artifact is missing, backend queues (or reuses existing active) generic archive request with `action_type = artifact_fetch` and returns `status = queued` with request payload.
- 200 response:
  - when artifact already exists: `status: "available"`, `object_id`, `artifact`
  - when an active request already exists: `status: "queued"`, `object_id`, `request`
- 201 response:
  - when a new queue request is created: `status: "queued"`, `object_id`, `request`
  - queued `request.available_file_id` is a required, non-null UUID matching the selected available-file record
- Error behavior:
  - `400 BAD_REQUEST` for invalid path/body
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when object or selected available file does not exist in tenant scope

### GET `/api/objects/:object_id/download-requests`

- Compatibility read endpoint; prefer `GET /api/archive-requests` for unified request visibility.

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- 200 response:
  - `object_id`
  - `requests[]` (`id`, `object_id`, `available_file_id`, `requested_by`, `artifact_kind`, `variant`, `status`, `failure_reason`, `failure_details`, `created_at`, `updated_at`, `completed_at`)
  - every `requests[].available_file_id` is a required, non-null UUID
  - request `status` values: `PENDING|PROCESSING|COMPLETED|FAILED|CANCELED`
- Error behavior:
  - `400 BAD_REQUEST` for invalid `:object_id` format
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when object does not exist in tenant scope

### POST `/api/objects/:object_id/resync`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Body:
  - optional JSON object
  - `action_payload` (optional object; defaults to `{}`)
- Behavior:
  - creates a generic archive request with:
    - `target_type = object`
    - `target_id = :object_id`
    - `action_type = object_resync`
  - request creation is idempotent for active requests (`PENDING|PROCESSING`) using dedupe key `object_resync:<object_id>`
- 201 response:
  - when new request is created: `status: "queued"`, `object_id`, `request`
- 200 response:
  - when an active request already exists: `status: "queued"`, `object_id`, `request`
- `request` fields:
   - `id`, `tenant_id`, `target_type`, `target_id`, `action_type`, `action_payload`, `requested_by`, `dedupe_key`, `status`, `failure_reason`, `failure_details`, `created_at`, `updated_at`, `completed_at`
   - `dedupe_key` is `string|null`; current object-resync requests generate a non-null key, but compatibility rows can contain `null`
- Error behavior:
  - `400 BAD_REQUEST` for invalid `:object_id` format or invalid JSON/body
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when object does not exist in tenant scope

### GET `/api/objects/:object_id/resync-requests`

- Compatibility read endpoint; prefer `GET /api/archive-requests` for unified request visibility.

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- 200 response:
  - `object_id`
   - `requests[]` for `object_resync` action only, each with:
     - `id`, `tenant_id`, `target_type`, `target_id`, `action_type`, `action_payload`, `requested_by`, `dedupe_key`, `status`, `failure_reason`, `failure_details`, `created_at`, `updated_at`, `completed_at`
     - `dedupe_key` is `string|null`
  - request `status` values: `PENDING|PROCESSING|COMPLETED|FAILED|CANCELED`
- Error behavior:
  - `400 BAD_REQUEST` for invalid `:object_id` format
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when object does not exist in tenant scope

Worker-only object endpoints are documented in `## Worker APIs`:

- `PUT /api/internal/objects/:object_id/available-files`
- `POST /api/archive-requests/lease`
- `POST /api/archive-requests/:id/lease/heartbeat`
- `POST /api/archive-requests/:id/lease/release`
- `POST /api/archive-requests/:id/complete`
- `POST /api/archive-requests/:id/fail`
- `POST /api/object-download-requests/lease` (deprecated compatibility route)
- `POST /api/object-download-requests/:id/lease/heartbeat` (deprecated compatibility route)
- `POST /api/object-download-requests/:id/lease/release` (deprecated compatibility route)
- `POST /api/object-download-requests/:id/artifacts/presign` (deprecated compatibility route)
- `PUT /api/archive-requests/uploads/:token`
- `PUT /api/object-download-requests/uploads/:token` (deprecated compatibility route)
- `POST /api/object-download-requests/:id/complete` (deprecated compatibility route)
- `POST /api/object-download-requests/:id/fail` (deprecated compatibility route)

### GET `/api/objects/:object_id/artifacts/:artifact_id/download`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- Access policy:
  - Endpoint authorization is role-aware and object access-policy aware.
  - A valid role alone does not guarantee download access.
  - `admin` can override access policy restrictions in the current implementation.
- Current behavior:
  - Returns `401 UNAUTHORIZED` for missing/invalid/expired session token.
  - Returns `403 FORBIDDEN` when authenticated role is not allowed.
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
  - current implementation returns full-file `200 OK` responses (no `Range`/`206 Partial Content` support yet)

### GET `/api/objects/:object_id/artifacts/:artifact_id/view`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- Purpose:
  - browser-consumable inline artifact delivery for object viewers
  - resolves viewer-selected `artifact_id` references into inline content
- Access policy:
  - endpoint is role-aware and object access-policy aware
  - artifact must be viewable under the object viewer contract
  - not every artifact in `GET /api/objects/:object_id/artifacts` is guaranteed to be viewable through this endpoint
  - frontend should use this endpoint for inline viewing/playback, not the download endpoint
- 200 response:
  - Binary file response
  - headers include `content-type`, `content-length`, `content-disposition: inline`, `accept-ranges: bytes`, `etag`, and `last-modified`
  - accepts a single `Range` header in `bytes=start-end`, `bytes=start-`, or `bytes=-suffixLength` form
  - valid ranges return `206 Partial Content` with `content-range`
  - multiple or malformed ranges are ignored and return the normal full-file `200 OK` response
  - `If-Range` permits a range only when its strong ETag or HTTP date matches the current artifact; otherwise the response is a full-file `200 OK`, including when the supplied range is otherwise unsatisfiable
  - unsatisfiable ranges return `416 Range Not Satisfiable` with `content-range: bytes */total`
- Browser-viewable MIME families:
  - `application/pdf`, `text/html`, `text/plain`, `image/*`, `audio/*`, `video/*`
- Error behavior:
  - `400 BAD_REQUEST` for invalid path params or when object exists but is not viewable in current access state
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when object or artifact does not exist in tenant scope
  - `409 CONFLICT` when artifact exists but is not viewable inline for browser use

### PATCH `/api/objects/:object_id/access-policy`

- Auth: Bearer token
- Roles: `admin`
- Body:
  - `access_level` (`private|family|public`, required)
  - `embargo_kind` (`none|timed|curation_state`, required)
  - `embargo_until` (ISO timestamp, required when `embargo_kind=timed`)
  - `embargo_curation_state` (`needs_review|review_in_progress|reviewed|curation_failed`, required when `embargo_kind=curation_state`)
  - `rights_note` (optional non-empty string when provided, nullable)
  - `sensitivity_note` (optional non-empty string when provided, nullable)
- 200 response:
  - `object` (Base Object Schema + `ingest_manifest` nullable)
  - includes resolved `thumbnail_artifact_id` (`null` when no thumbnail artifact exists)
- Error behavior:
  - `400 BAD_REQUEST` for invalid `:object_id` format or invalid body
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when object does not exist in tenant scope

### POST `/api/objects/:object_id/access-requests`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
- Body:
  - `requested_level` (`family|private`, required)
  - `reason` (optional non-empty string when provided)
- 201 response:
  - `request`:
    - `id`, `object_id`, `requester_user_id`, `requested_level`, `reason`, `status`, `created_at`, `updated_at`
    - `status` values: `PENDING|APPROVED|REJECTED|CANCELED`
- Conflict behavior:
  - returns `409` when the same user already has a `PENDING` request for that object
- Error behavior:
  - `400 BAD_REQUEST` for invalid `:object_id` format or invalid body
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when object does not exist in tenant scope
  - `409 CONFLICT` when a pending request already exists for requester+object

### GET `/api/objects/:object_id/access-requests`

- Auth: Bearer token
- Roles: `admin`
- 200 response:
  - `object_id`
  - `requests[]` (`id`, `requester_user_id`, `requested_level`, `reason`, `status`, `reviewed_by`, `reviewed_at`, `decision_note`, `created_at`, `updated_at`)
  - `status` values: `PENDING|APPROVED|REJECTED|CANCELED`
- Error behavior:
  - `400 BAD_REQUEST` for invalid `:object_id` format
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when object does not exist in tenant scope

### POST `/api/objects/:object_id/access-requests/:request_id/approve`

- Auth: Bearer token
- Roles: `admin`
- Body:
  - `decision_note` (optional non-empty string when provided)
  - Empty request body is allowed.
- 200 response:
  - `request` (status becomes `APPROVED`)
  - creates or updates assignment for requester
- Conflict behavior:
  - returns `409` if request status is not `PENDING`
- Error behavior:
  - `400 BAD_REQUEST` for invalid path/body
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when object or request does not exist in tenant scope
  - `409 CONFLICT` when request is already resolved

### POST `/api/objects/:object_id/access-requests/:request_id/reject`

- Auth: Bearer token
- Roles: `admin`
- Body:
  - `decision_note` (optional non-empty string when provided)
  - Empty request body is allowed.
- 200 response:
  - `request` (status becomes `REJECTED`)
- Conflict behavior:
  - returns `409` if request status is not `PENDING`
- Error behavior:
  - `400 BAD_REQUEST` for invalid path/body
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when object or request does not exist in tenant scope
  - `409 CONFLICT` when request is already resolved

### GET `/api/objects/:object_id/access-assignments`

- Auth: Bearer token
- Roles: `admin`
- 200 response:
  - `object_id`
  - `assignments[]` (`user_id`, `granted_level`, `created_by`, `created_at`)
- Error behavior:
  - `400 BAD_REQUEST` for invalid `:object_id` format
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when object does not exist in tenant scope

### PUT `/api/objects/:object_id/access-assignments`

- Auth: Bearer token
- Roles: `admin`
- Body:
  - `user_id` (UUID, required)
  - `granted_level` (`family|private`, required)
- 200 response:
  - `assignment`
- Error behavior:
  - `400 BAD_REQUEST` for invalid `:object_id` format or invalid body
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when object does not exist in tenant scope

### DELETE `/api/objects/:object_id/access-assignments/:user_id`

- Auth: Bearer token
- Roles: `admin`
- 200 response:
  - `status` (`ok`)
  - `object_id`
  - `user_id`
- Error behavior:
  - `400 BAD_REQUEST` for invalid path params
  - `401 UNAUTHORIZED` for missing/invalid/expired session token
  - `403 FORBIDDEN` when authenticated role is not allowed
  - `404 NOT_FOUND` when object does not exist or assignment is missing in tenant scope

---

## Worker APIs

Worker APIs are for archive/ingestion workers, not UI clients.

Integration guides for archive worker teams:

- `docs/archive-system-integration.md` (ingestion/event pipeline)
- `docs/archive-system-generic-requests.md` (generic archive request queue and worker lease flow)

### Worker Authentication

- Header: `x-worker-auth-token: <WORKER_AUTH_TOKEN>`
- Optional header: `x-worker-id: <worker-id>`
- Missing/invalid worker token: `401 UNAUTHORIZED`
- Missing server configuration for worker token: `500 CONFIGURATION_ERROR`
- Lease-protected ingestion endpoints require a valid body `lease_token` tied to the target ingestion.
- Lease-protected archive-request endpoints require a valid body `lease_token` tied to the target archive request.
- Worker request bodies must not provide `tenant_id`; tenant context is resolved by backend. `tenant_id` in worker lease responses is informational only.

### Ingestion Worker APIs

### POST `/api/ingestions/lease`

- Auth: `x-worker-auth-token` header
- Optional: `x-worker-id`
- Behavior:
  - returns next queued ingestion lease, or `lease: null` when no work is available
  - lease TTL is 5 minutes
  - `items[].files[]` includes ingestion files in `UPLOADED|VALIDATED` status only
  - payload validation completes before the lease and `PROCESSING` transition are persisted; an invalid payload leaves the ingestion queued without an active lease
- 200 response:
  - `lease: null` when no queued ingestion available
  - or
    - `lease { lease_id, lease_token, lease_expires_at, ingestion_id, batch_label, tenant_id, items[] }`
    - each `items[]` item: `{ ingestion_item_id, item_index, catalog_json, files[] }`
    - each `files[]` item: `{ file_id, filename, sort_order, storage_key, content_type, size_bytes, checksum_sha256, processing_overrides, download_url }`
- `catalog_json` is item-scoped and should be copied per future object
- ordering rule:
  - items: `item_index ASC`
  - files inside item: `sort_order ASC`, then `filename`, then `file_id`
- Error behavior:
  - `401 UNAUTHORIZED` for missing/invalid worker auth token
  - `409 CONFLICT` when queued ingestion metadata is not leasable (for example missing/invalid catalog metadata)
  - `500 CONFIGURATION_ERROR` when worker auth token is not configured server-side

### POST `/api/ingestions/:id/lease`

- Auth: `x-worker-auth-token` header
- Optional: `x-worker-id`
- Path:
  - `:id` ingestion UUID
- Behavior:
  - deterministic targeted lease acquire for a specific ingestion id
  - no active-lease takeover is allowed (reacquire/recovery path only)
  - payload validation completes before the lease and `PROCESSING` transition are persisted
- 200 response:
  - `lease { lease_id, lease_token, lease_expires_at, ingestion_id, batch_label, tenant_id, items[] }`
- Error behavior:
  - `400 BAD_REQUEST` for invalid `:id` format
  - `401 UNAUTHORIZED` for missing/invalid worker auth token
  - `404 NOT_FOUND` when ingestion does not exist
  - `409 CONFLICT` when ingestion exists but is not currently leasable (for example active lease or non-`QUEUED` status)
  - `500 CONFIGURATION_ERROR` when worker auth token is not configured server-side

### POST `/api/ingestions/:id/lease/heartbeat`

- Auth:
  - `x-worker-auth-token` header
  - body `lease_token` must be valid, unexpired, and match `:id`
- Body:
  - `lease_token` (required non-empty string)
- Behavior:
  - extends active lease and returns refreshed `lease_token`
- 200 response:
  - refreshed `lease { lease_id, lease_token, lease_expires_at, ingestion_id, batch_label, tenant_id, items[] }`
- Error behavior:
  - `400 BAD_REQUEST` for invalid `:id` format or invalid body
  - `401 UNAUTHORIZED` for missing/invalid worker auth token or invalid/expired lease token
  - `404 NOT_FOUND` when ingestion does not exist
  - `409 CONFLICT` when lease is no longer active
  - `500 CONFIGURATION_ERROR` when worker auth token is not configured server-side

### POST `/api/ingestions/:id/lease/release`

- Auth:
  - `x-worker-auth-token` header
  - body `lease_token` must be valid, unexpired, and match `:id`
- Body:
  - `lease_token` (required non-empty string)
- Behavior:
  - releases active lease
  - if ingestion is `PROCESSING`, it is moved back to `QUEUED` in the same transaction as lease release
- 200 response:
  - `status` (`ok`), `ingestion_id`, `lease_id`
- Error behavior:
  - `400 BAD_REQUEST` for invalid `:id` format or invalid body
  - `401 UNAUTHORIZED` for missing/invalid worker auth token or invalid/expired lease token
  - `404 NOT_FOUND` when ingestion does not exist
  - `409 CONFLICT` when lease is no longer active
  - `500 CONFIGURATION_ERROR` when worker auth token is not configured server-side

### Ingestion Worker Downloads and Events

### GET `/api/worker/downloads/:token`

- Auth: signed download token in path
- 200 response:
  - binary file response
  - headers: `content-type`, `content-length`, `accept-ranges`
- Error behavior:
  - `401 UNAUTHORIZED` for invalid/expired/malformed signed download token
  - `404 NOT_FOUND` when staged file does not exist

### POST `/api/ingestions/:id/events`

- Auth:
  - `x-worker-auth-token` header
  - body `lease_token` must be valid, unexpired, and match `:id`
- Body:
  - `lease_token` (required non-empty string)
  - `events[]` (array; may be empty). Each event includes:
    - `event_id` (UUID)
    - `event_type`:
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
      - `INGESTION_ITEM_CREATED`
      - `INGESTION_ITEM_UPDATED`
      - `INGESTION_ITEM_PROCESSING`
      - `INGESTION_ITEM_COMPLETED`
      - `INGESTION_ITEM_FAILED`
      - `OBJECT_CREATED`
      - `ARTIFACT_CREATED`
    - `timestamp` (ISO datetime string with offset)
    - `payload` (JSON object)
    - `ingestion_item_id` (required for `INGESTION_ITEM_*` events; not allowed for aggregate `INGESTION_COMPLETED`)
    - `object_id` (required for `INGESTION_ITEM_COMPLETED|OBJECT_CREATED|ARTIFACT_CREATED`; not allowed for aggregate `INGESTION_COMPLETED`; when present must match object id format)
- Behavior:
  - idempotent by `event_id`
  - an exact duplicate performs no additional projection writes; conflicting reuse of an `event_id` returns `409 CONFLICT`
  - each event and its database projections commit atomically; earlier events in a batch remain committed if a later event fails
  - out-of-order tolerant
  - `INGESTION_ITEM_COMPLETED` creates or resolves the object for its `ingestion_item_id`, updates its readiness state, and stores a valid `payload.ingest_json` manifest.
  - `INGESTION_COMPLETED` is an aggregate signal only. It creates no object and does not update an individual object projection.
  - state projection rules (current phase):
    - aggregate ingestion status becomes terminal only when all items are terminal
    - completed/skipped-only items yield `COMPLETED`; completed plus failed yields `COMPLETED_WITH_ERRORS`; failed-only or failed-plus-skipped yields `FAILED`
    - `curation_state` is not currently projected from worker event stream in this phase
    - other event types are stored for audit/activity and do not currently mutate object projection fields directly
  - item completion `payload.ingest_json` (when valid) updates that item's `objects.ingest_manifest` (last-write-wins)
- 200 response:
  - `status`, `ingestion_id`, `inserted_events`, `duplicate_events`, `object_ids` (`string[]`, item-scoped IDs represented by this request)
- Error behavior:
  - `400 BAD_REQUEST` for invalid `:id` format or invalid body/event schema
  - `401 UNAUTHORIZED` for missing/invalid worker auth token or invalid/expired lease token
  - `404 NOT_FOUND` when ingestion does not exist
  - `409 CONFLICT` for lease/object conflicts during event ingestion
  - `500 CONFIGURATION_ERROR` when worker auth token is not configured server-side

### Object Worker APIs

### POST `/api/archive-requests/lease`

- Auth: `x-worker-auth-token`
- Optional: `x-worker-id`
- Body:
  - optional JSON object
  - `action_type` (optional enum): `object_resync|artifact_fetch|curation_apply`
- `tenant_id` is not accepted in request body
- Behavior:
  - sweeps expired archive-request leases before selecting next request
  - lease TTL is 5 minutes
  - when `action_type` is provided, leasing is filtered to that action type only
- 200 response:
  - `request: null` when no pending work
   - otherwise `request` with:
     - `request_id`, `lease_id`, `lease_token`, `lease_expires_at`
     - `tenant_id`, `target_type`, `target_id`, `action_type`, `action_payload`
     - `requested_by`, `dedupe_key`
     - `dedupe_key` is `string|null`; workers must support `null`
- Error behavior:
  - `400 BAD_REQUEST` for invalid body
  - `401 UNAUTHORIZED` for missing/invalid worker auth token
  - `500 CONFIGURATION_ERROR` when worker auth token is not configured server-side

### POST `/api/archive-requests/:id/lease/heartbeat`

- Auth: `x-worker-auth-token`
- Body:
  - `lease_token` (required non-empty string)
- `tenant_id` is not accepted in request body
- 200 response:
  - `request` (`request_id`, `lease_id`, `lease_token`, `lease_expires_at`)
- Behavior:
  - returned `lease_token` is refreshed; use it for subsequent worker calls
- Error behavior:
  - `400 BAD_REQUEST` for invalid path/body
  - `401 UNAUTHORIZED` for missing/invalid worker auth token or invalid/expired lease token
  - `409 CONFLICT` when lease is no longer active
  - `500 CONFIGURATION_ERROR` when worker auth token is not configured server-side

### GET `/api/archive-requests/:id/source`

- Auth: `x-worker-auth-token`
- Headers:
  - `x-archive-request-lease-token` (required active lease token for this request)
  - `x-worker-id` (must match the worker identity embedded in the lease when both are present)
- Available only for `curation_apply` requests with an unpurged retained source
- Verifies the stored regular file length and SHA-256 checkpoint before returning bytes
- 200 response headers include `content-type`, `content-length`, `x-content-sha256`, and `cache-control: no-store`
- Error behavior:
  - `400 BAD_REQUEST` when the lease header is missing
  - `401 UNAUTHORIZED` for missing/invalid worker auth or lease token
  - `404 NOT_FOUND` when the source record or file is absent/purged
  - `409 CONFLICT` for the wrong action, mismatched worker, inactive lease, size mismatch, or checksum mismatch

### POST `/api/archive-requests/:id/lease/release`

- Auth: `x-worker-auth-token`
- Body:
  - `lease_token` (required non-empty string)
- `tenant_id` is not accepted in request body
- 200 response:
  - `status: "ok"`, `request_id`
- Error behavior:
  - `400 BAD_REQUEST` for invalid path/body
  - `401 UNAUTHORIZED` for missing/invalid worker auth token or invalid/expired lease token
  - `409 CONFLICT` when lease is no longer active
  - `500 CONFIGURATION_ERROR` when worker auth token is not configured server-side

### POST `/api/archive-requests/:id/complete`

- Auth: `x-worker-auth-token`
- Body:
  - `lease_token` (required non-empty string)
  - `upload_token` (optional non-empty string)
- `tenant_id` is not accepted in request body
- Behavior:
  - for `artifact_fetch`, `upload_token` is required
  - for non-`artifact_fetch` actions, `upload_token` is ignored when present
  - artifact uploads may already be finalized by the successful `PUT`; this call remains an idempotent worker acknowledgment
  - acknowledging a materialized upload does not read staged bytes, rewrite search projection/provenance, or repair a missing projection
  - finalization always uses the verified upload's exact immutable storage key, never another artifact inferred from kind/variant
- 200 response:
   - `status: "completed"`
   - `request` (`id`, `tenant_id`, `target_type`, `target_id`, `action_type`, `action_payload`, `requested_by`, `dedupe_key`, `status`, `failure_reason`, `failure_details`, `created_at`, `updated_at`, `completed_at`)
   - returned `request.dedupe_key` is `string|null`
- Error behavior:
  - `400 BAD_REQUEST` for invalid path/body
  - `401 UNAUTHORIZED` for missing/invalid worker auth token or invalid/expired lease token
  - `409 CONFLICT` when lease is no longer active
  - `500 CONFIGURATION_ERROR` when worker auth token is not configured server-side

### POST `/api/archive-requests/:id/artifacts/presign`

- Auth: `x-worker-auth-token`
- Body:
  - `lease_token` (required non-empty string)
  - `content_type` (required valid HTTP media type; the complete declared value is preserved)
  - `size_bytes` (required integer >= 0)
  - `extension` (required non-empty string)
- Behavior:
  - supported for `artifact_fetch` requests only
  - upload token TTL is 15 minutes
  - re-presigning invalidates only the prior unverified attempt; verified uploads cannot be replaced
  - a queued request remains fulfillable when its exact source row becomes inactive; the source checksum is captured immutably for the issued attempt
- 200 response:
  - `upload_token`
  - `upload_url` (for `PUT` upload)
  - `storage_key`
  - `expires_at`
  - `headers` (`content-type`, `content-length` where `content-length` is a JSON number)
- Error behavior:
  - `400 BAD_REQUEST` for invalid path/body
  - `401 UNAUTHORIZED` for missing/invalid worker auth token or invalid/expired lease token
  - `409 CONFLICT` when lease is no longer active or request action is not `artifact_fetch`
  - `409 CONFLICT` with `details.reason = artifact_source_missing` when the queued tenant/object/source identity no longer exists
  - `409 CONFLICT` with `details.reason = artifact_source_identity_changed` when the retained source kind or variant no longer matches the queued request
  - `409 CONFLICT` with `details.reason = artifact_source_checksum_invalid` when historical source metadata does not contain a valid SHA-256 checksum
  - `500 CONFIGURATION_ERROR` when worker auth token is not configured server-side

### POST `/api/archive-requests/:id/fail`

- Auth: `x-worker-auth-token`
- Body:
  - `lease_token` (required non-empty string)
  - `failure` object:
    - `code` (required non-empty string)
    - `message` (required non-empty string)
    - `retryable` (boolean)
    - `details` (object, optional)
- `tenant_id` is not accepted in request body
- Behavior:
  - when omitted, `failure.details` is persisted as `{}`
- 200 response:
  - `status: "failed"`, `request_id`, `retryable`
- Error behavior:
  - `400 BAD_REQUEST` for invalid path/body
  - `401 UNAUTHORIZED` for missing/invalid worker auth token or invalid/expired lease token
  - `409 CONFLICT` when lease is no longer active
  - `500 CONFIGURATION_ERROR` when worker auth token is not configured server-side

### PUT `/api/internal/objects/:object_id/available-files`

- Auth: `x-worker-auth-token`
- Path:
  - `:object_id` (format: `OBJ-YYYYMMDD-XXXXXX`)
- Body:
  - `files[]` full-replace snapshot entries (array may be empty):
    - `archive_file_key` (required string, non-empty)
    - `artifact_kind` (required enum):
      - `ingest_json|pipeline_json|catalog_json|original|preview|ocr|transcript|metadata|pdf|ocr_text|thumbnail|web_version|other`
      - `ocr` = backend/internal OCR artifact kind
      - `ocr_text` = normalized OCR text artifact kind used by viewer contracts and auto-request defaults
    - `variant` (optional nullable string, non-empty when present)
      - per-page OCR convention: `page_0001`, `page_0002`, etc.
      - combined OCR convention: `full_v1` or `null`
    - `display_name` (required string, non-empty)
    - `content_type` (optional nullable string, non-empty when present)
    - `size_bytes` (optional nullable number, integer >= 0)
    - `checksum_sha256` (optional nullable 64-character hexadecimal SHA-256; normalized to lowercase)
    - `metadata` (object, optional)
    - `is_available` (boolean, optional; defaults `true`)
- Behavior:
  - tenant is resolved internally from `object_id`
  - Replaces object snapshot by archive key: upserts provided entries and marks omitted entries unavailable.
  - Auto-request side effect: backend attempts to auto-queue `artifact_fetch` requests for configured default kinds (`thumbnail`, `ocr_text`, `web_version`) when corresponding available entries are present.
    - `web_version` auto-request currently applies only to `IMAGE` objects.
  - **Per-page OCR**: when `artifact_kind = "ocr_text"` and `variant` matches `page_0001`, `page_0002`, etc., backend auto-queues a separate `artifact_fetch` request for each page variant.
  - **Combined OCR**: when `artifact_kind = "ocr_text"` and `variant` is `null` or starts with `full_`, backend auto-queues one combined `artifact_fetch`. Selection priority: prefers `full_v1`, then `null`, then lexicographically lowest `archive_file_key`.
  - Auto-request suppression:
    - For combined OCR and non-OCR kinds: skip when any artifact of that kind already exists, or when any active request for that kind exists.
    - For per-page OCR: skip only when the specific `(kind, variant)` artifact already exists, or when an active request for that exact `(kind, variant)` exists.
  - Page metadata side effect: when per-page OCR entries include `metadata.page_number` and/or `metadata.label`, backend updates `objects.metadata.pages` to reflect page structure. If `metadata.pages` does not yet exist, it is created from the available files entries.
  - Artifact completion side effect: when a per-page OCR artifact download completes, backend updates the matching `metadata.pages[].ocr_text_artifact_id` by page number.
- 200 response:
  - `object_id`
  - `synced_files` (number)
- Error behavior:
  - `400 BAD_REQUEST` for invalid path/body
  - `401 UNAUTHORIZED` for missing/invalid worker auth token
  - `404 NOT_FOUND` when object does not exist
  - `500 CONFIGURATION_ERROR` when worker auth token is not configured server-side

### PUT `/api/internal/objects/:object_id/object-text-manifest`

- Auth: `x-worker-auth-token`
- Path:
  - `:object_id` (format: `OBJ-YYYYMMDD-XXXXXX`)
- Body:
  - `object_text_manifest` (required object):
    - `object_id` (required string; must match path `:object_id`)
    - `media_type` (required enum: `document|audio|video|photo|other`)
    - `projection_version` (required string; opaque object-level change token)
    - `generated_at` (required RFC3339 timestamp)
    - `text_artifacts[]` (required array, may be empty):
      - `kind` (required string)
      - `version` (required string)
      - `is_active` (required boolean)
      - `metadata` (optional object)
- Behavior:
  - tenant is resolved internally from `object_id`
  - replaces current object text manifest snapshot for that object
  - VPS should treat artifact identity as `kind + version`
  - at most one `text_artifacts[]` entry per `kind` may have `is_active = true`
  - archive-local file paths are not part of this public worker payload
- 200 response:
  - `object_id`
  - `status: "ok"`
- Error behavior:
  - `400 BAD_REQUEST` for invalid path/body
  - `401 UNAUTHORIZED` for missing/invalid worker auth token
  - `404 NOT_FOUND` when object does not exist
  - `500 CONFIGURATION_ERROR` when worker auth token is not configured server-side

### POST `/api/object-download-requests/lease`

Deprecated compatibility route. New archive workers should use `POST /api/archive-requests/lease` with `{"action_type":"artifact_fetch"}`.

- Auth: `x-worker-auth-token`
- Optional: `x-worker-id`
- Behavior:
  - sweeps expired download-request leases before selecting next request
  - lease TTL is 5 minutes
- 200 response:
  - `request: null` when no pending work
  - otherwise `request` with:
    - `request_id`, `lease_id`, `lease_token`, `lease_expires_at`
    - `object_id`, `tenant_id`, `available_file_id`, `artifact_kind`, `variant`
    - `available_file_id` is a required, non-null UUID when `request` is present
    - `available_file` (nullable object from available-files snapshot):
      - `id`, `object_id`, `archive_file_key`, `artifact_kind`, `variant`, `display_name`, `content_type`, `size_bytes`, `checksum_sha256`, `metadata`, `is_available`, `synced_at`
- Error behavior:
  - `401 UNAUTHORIZED` for missing/invalid worker auth token
  - `500 CONFIGURATION_ERROR` when worker auth token is not configured server-side

### POST `/api/object-download-requests/:id/lease/heartbeat`

Deprecated compatibility route. New archive workers should use `POST /api/archive-requests/:id/lease/heartbeat`.

- Auth: `x-worker-auth-token`
- Body:
  - `lease_token` (required non-empty string)
- 200 response:
  - `request` (`request_id`, `lease_id`, `lease_token`, `lease_expires_at`)
- Behavior:
  - returned `lease_token` is refreshed; use it for subsequent worker calls
- Error behavior:
  - `400 BAD_REQUEST` for invalid path/body
  - `401 UNAUTHORIZED` for missing/invalid worker auth token or invalid/expired lease token
  - `409 CONFLICT` when lease is no longer active
  - `500 CONFIGURATION_ERROR` when worker auth token is not configured server-side

### POST `/api/object-download-requests/:id/lease/release`

Deprecated compatibility route. New archive workers should use `POST /api/archive-requests/:id/lease/release`.

- Auth: `x-worker-auth-token`
- Body:
  - `lease_token` (required non-empty string)
- 200 response:
  - `status: "ok"`, `request_id`
- Error behavior:
  - `400 BAD_REQUEST` for invalid path/body
  - `401 UNAUTHORIZED` for missing/invalid worker auth token or invalid/expired lease token
  - `409 CONFLICT` when lease is no longer active
  - `500 CONFIGURATION_ERROR` when worker auth token is not configured server-side

### POST `/api/object-download-requests/:id/artifacts/presign`

Deprecated compatibility route. New archive workers should use `POST /api/archive-requests/:id/artifacts/presign`.

- Auth: `x-worker-auth-token`
- Body:
  - `lease_token` (required non-empty string)
  - `content_type` (required valid HTTP media type; the complete declared value is preserved)
  - `size_bytes` (required integer >= 0)
  - `extension` (required non-empty string)
- 200 response:
  - `upload_token`
  - `upload_url` (for `PUT` upload)
  - `storage_key`
  - `expires_at`
  - `headers` (`content-type`, `content-length` where `content-length` is a JSON number)
- Behavior:
  - upload token TTL is 15 minutes
- Error behavior:
  - `400 BAD_REQUEST` for invalid path/body
  - `401 UNAUTHORIZED` for missing/invalid worker auth token or invalid/expired lease token
  - `409 CONFLICT` when lease is no longer active
  - `500 CONFIGURATION_ERROR` when worker auth token is not configured server-side

### PUT `/api/archive-requests/uploads/:token`

- Auth: none (signed token in path)
- Required headers:
  - `content-type` must have the same case-insensitive base media type as the signed value; parameters are ignored and both values must be valid media types
  - `content-length` is required and must exactly match signed token constraints
- Body: raw file bytes; byte length must exactly match signed token constraints
- 200 response:
  - `status: "ok"`, `request_id`, `size_bytes` (number)
- Behavior:
  - `200` means temporary bytes and their immutable directory entry were synchronized, byte length and checksum were verified while the issuing lease was active, and ownership transferred to the backend
  - the backend attempts finalization immediately and retries verified attempts in the background; verified requests are never leased to another worker
  - finalization revalidates the current regular file size and SHA-256 before creating any artifact, projection, provenance, metadata, or completion side effect
  - identical retries preserve the immutable file and may restore a missing path with the exact accepted bytes; different bytes for the accepted checkpoint return `409 CONFLICT`
  - bytes that differ from a known source checksum are rejected before immutable publication and create no checkpoint; corrected bytes may retry the same URL while its token and lease remain active
- Error behavior:
  - `400 BAD_REQUEST` for header/body constraint mismatch
  - `401 UNAUTHORIZED` for invalid/expired signed token
  - `409 CONFLICT` with `details.reason = expected_checksum_mismatch`, expected/actual SHA-256, and `retry_action = retry_same_upload_url` when bytes differ from the known source checksum
  - `409 CONFLICT` with `details.reason = accepted_checkpoint_mismatch` and `retry_action = retry_exact_accepted_bytes_only` when bytes differ from an already accepted artifact checkpoint
  - `409 CONFLICT` with `details.reason = accepted_checkpoint_storage_conflict` and `retry_action = operator_repair_required` when exact accepted bytes encounter different immutable destination content
  - `409 CONFLICT` without checksum retry guidance when the upload attempt or issuing lease is no longer active

### PUT `/api/object-download-requests/uploads/:token`

Deprecated compatibility route. New archive workers should use `PUT /api/archive-requests/uploads/:token`.

### POST `/api/object-download-requests/:id/complete`

Deprecated compatibility route. New archive workers should use `POST /api/archive-requests/:id/complete` with both `lease_token` and `upload_token` for `artifact_fetch`.

- Auth: `x-worker-auth-token`
- Body:
  - `lease_token` (required non-empty string)
  - `upload_token` (required non-empty string)
- Behavior:
  - idempotent success for the exact verified upload attempt
  - upload token and lease context must match (`request_id`, `object_id`, `tenant_id`, `artifact_kind`, `variant`)
  - the successful upload may already have completed the request; workers should still call this endpoint for compatibility
- 200 response:
  - `status: "completed"`, `request_id`, `object_id`, `artifact`
- Error behavior:
  - `400 BAD_REQUEST` for invalid path/body or lease/upload token context mismatch
  - `401 UNAUTHORIZED` for missing/invalid worker auth token or invalid/expired lease/upload token
  - `404 NOT_FOUND` when request or uploaded artifact file is not found
  - `409 CONFLICT` when lease is no longer active for completion transition
  - `500 CONFIGURATION_ERROR` when worker auth token is not configured server-side

### POST `/api/object-download-requests/:id/fail`

- Auth: `x-worker-auth-token`
- Body:
  - `lease_token` (required non-empty string)
  - `failure` object:
    - `code` (required non-empty string)
    - `message` (required non-empty string)
    - `retryable` (boolean)
    - `details` (object, optional)
- Behavior:
  - when omitted, `failure.details` is persisted as `{}`
- 200 response:
  - `status: "failed"`, `request_id`, `retryable`
- Error behavior:
  - `400 BAD_REQUEST` for invalid path/body
  - `401 UNAUTHORIZED` for missing/invalid worker auth token or invalid/expired lease token
  - `409 CONFLICT` when lease is no longer active
  - `500 CONFIGURATION_ERROR` when worker auth token is not configured server-side

---

## Notes for UI Integrators

- Use client Bearer APIs only; do not call worker endpoints from UI.
- `ingest_manifest` is available on object detail only (`GET /api/objects/:object_id`).
- List endpoints return `next_cursor`; pass it back as `cursor` for pagination.
- There is no dedicated `recents` endpoint; use `GET /api/dashboard/activity` for recents/activity UX.
- Do not infer rendering behavior from worker/internal artifact-kind taxonomy; use `viewer.primary_source` + `viewer_payload` and resolve artifact bytes via `/view`.
