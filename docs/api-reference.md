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

### POST `/api/ingestions/:id/files/presign`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Body (new file):
  - `filename`, `content_type`, `size_bytes`
- Body (re-presign existing):
  - `file_id`
- 201 response:
  - `file_id`, `storage_key`, `upload_url`, `expires_at`, `headers { content-type, content-length }`

### POST `/api/ingestions/:id/files/commit`

- Auth: Bearer token
- Roles: `archiver`, `admin`
- Body:
  - `file_id`, `checksum_sha256`
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
- 200 response:
  - `ingestion`

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
  - `curation_state`
  - `availability_state`
  - `access_level`
  - `type`
  - `tenant_id`
  - `source_ingestion_id` (`null` allowed)
  - `source_batch_label` (`null` allowed)
  - `metadata`
  - `created_at`
  - `updated_at`
  - `embargo_until` (`null` allowed)
  - `embargo_kind`
  - `embargo_curation_state` (`null` allowed)
  - `rights_note` (`null` allowed)
  - `sensitivity_note` (`null` allowed)
  - `can_download`
  - `access_reason_code` (`OK`, `FORBIDDEN_POLICY`, `EMBARGO_ACTIVE`, `RESTORE_REQUIRED`, `RESTORE_IN_PROGRESS`, `TEMP_UNAVAILABLE`)
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
      "embargo_until": null,
      "rights_note": null,
      "sensitivity_note": null,
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

### GET `/api/objects/:object_id/artifacts/:artifact_id/download`

- Auth: Bearer token
- Roles: `viewer`, `archiver`, `admin`
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

## Lease lifecycle

### POST `/api/ingestions/lease`

- Auth: `x-worker-auth-token` header
- Optional: `x-worker-id`
- 200 response:
  - `lease: null` when no queued ingestion available
  - or `lease { lease_id, lease_token, lease_expires_at, ingestion_id, batch_label, tenant_id, download_urls[] }`

### POST `/api/ingestions/:id/lease/heartbeat`

- Auth:
  - `x-worker-auth-token` header
  - body `lease_token` must be valid and match `:id`
- Body:
  - `lease_token`
- 200 response:
  - refreshed `lease { ... }` including a refreshed `lease_token`

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
    - `event_type` (supported ingestion/object pipeline event types)
    - `timestamp` (ISO datetime)
    - `payload` (object)
    - `object_id` (required for completion/object/artifact event types)
- Behavior:
  - idempotent by `event_id`
  - out-of-order tolerant
  - completion event creates or resolves object by source ingestion
  - `payload.ingest_json` (when provided) updates `objects.ingest_manifest` (last-write-wins)
- 200 response:
  - `status`, `ingestion_id`, `inserted_events`, `duplicate_events`, `object_id`

---

## Notes for UI Integrators

- Use client Bearer APIs only; do not call worker lease/event endpoints from UI.
- `ingest_manifest` is available on object detail only (`GET /api/objects/:object_id`).
- List endpoints return `next_cursor`; pass it back as `cursor` for pagination.
