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
- Roles: `viewer`, `operator`, `admin`
- 200 response:
  - `summary { total_ingestions, total_objects, processed_today, processed_week, failed_count }`

### GET `/api/dashboard/activity`

- Auth: Bearer token
- Roles: `viewer`, `operator`, `admin`
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
- Roles: `operator`, `admin`
- Body:
  - `batch_label` (string)
- 201 response:
  - `ingestion` (draft ingestion object)

### GET `/api/ingestions`

- Auth: Bearer token
- Roles: `viewer`, `operator`, `admin`
- Query params:
  - `limit` (optional)
  - `cursor` (optional)
- 200 response:
  - `ingestions[]`
  - `next_cursor` (string or `null`)

### GET `/api/ingestions/:id`

- Auth: Bearer token
- Roles: `viewer`, `operator`, `admin`
- 200 response:
  - `ingestion`
  - `files[]`

### POST `/api/ingestions/:id/files/presign`

- Auth: Bearer token
- Roles: `operator`, `admin`
- Body (new file):
  - `filename`, `content_type`, `size_bytes`
- Body (re-presign existing):
  - `file_id`
- 201 response:
  - `file_id`, `storage_key`, `upload_url`, `expires_at`, `headers { content-type, content-length }`

### POST `/api/ingestions/:id/files/commit`

- Auth: Bearer token
- Roles: `operator`, `admin`
- Body:
  - `file_id`, `checksum_sha256`
- 200 response:
  - `file` (updated ingestion file record)

### POST `/api/ingestions/:id/submit`

- Auth: Bearer token
- Roles: `operator`, `admin`
- Preconditions:
  - at least one file exists
  - at least one file is committed (`UPLOADED` or `VALIDATED`)
- 200 response:
  - `ingestion` (status transitions to queued flow)

### POST `/api/ingestions/:id/cancel`

- Auth: Bearer token
- Roles: `operator`, `admin`
- 200 response:
  - `ingestion`

### POST `/api/ingestions/:id/retry`

- Auth: Bearer token
- Roles: `operator`, `admin`
- 200 response:
  - `ingestion`

## Objects

### GET `/api/objects`

- Auth: Bearer token
- Roles: `viewer`, `operator`, `admin`
- Query params:
  - `limit` (optional)
  - `cursor` (optional)
  - `type` (`GENERIC|IMAGE|AUDIO|VIDEO|DOCUMENT`, optional)
  - `from` (ISO timestamp, optional)
  - `to` (ISO timestamp, optional)
  - `tag` (optional)
- 200 response:
  - `objects[]` (does **not** include `ingest_manifest`)
  - `next_cursor` (string or `null`)

### GET `/api/objects/:object_id`

- Auth: Bearer token
- Roles: `viewer`, `operator`, `admin`
- 200 response:
  - `object` including `ingest_manifest` (or `null`)

### PATCH `/api/objects/:object_id`

- Auth: Bearer token
- Roles: `operator`, `admin`
- Body:
  - `title` (required string, non-empty)
- Notes:
  - `metadata` patching is intentionally not supported in this phase.
- 200 response:
  - `object` (updated)

### GET `/api/objects/:object_id/artifacts`

- Auth: Bearer token
- Roles: `viewer`, `operator`, `admin`
- 200 response:
  - `object_id`
  - `artifacts[]` (`id`, `kind`, `storage_key`, `content_type`, `size_bytes`, `created_at`)

### GET `/api/objects/:object_id/artifacts/:artifact_id/download`

- Auth: Bearer token
- Roles: `viewer`, `operator`, `admin`
- 200 response:
  - Binary file response
  - headers include `content-type`, `content-length`, `content-disposition`

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
