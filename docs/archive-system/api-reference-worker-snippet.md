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
- 200 response:
  - `status: "completed"`
  - `request` (`id`, `tenant_id`, `target_type`, `target_id`, `action_type`, `action_payload`, `requested_by`, `dedupe_key`, `status`, `failure_reason`, `failure_details`, `created_at`, `updated_at`, `completed_at`)
- Error behavior:
  - `400 BAD_REQUEST` for invalid path/body
  - `401 UNAUTHORIZED` for missing/invalid worker auth token or invalid/expired lease token
  - `409 CONFLICT` when lease is no longer active
  - `500 CONFIGURATION_ERROR` when worker auth token is not configured server-side

### POST `/api/archive-requests/:id/artifacts/presign`

- Auth: `x-worker-auth-token`
- Body:
  - `lease_token` (required non-empty string)
  - `content_type` (required non-empty string)
  - `size_bytes` (required integer >= 0)
  - `extension` (required non-empty string)
- Behavior:
  - supported for `artifact_fetch` requests only
  - upload token TTL is 15 minutes
- 200 response:
  - `upload_token`
  - `upload_url` (for `PUT` upload)
  - `storage_key`
  - `expires_at`
  - `headers` (`content-type`, `content-length` where `content-length` is a JSON number)

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

### `curation_apply` action payload

- `target_type = object`
- `target_id = OBJ-...`
- `action_type = curation_apply`
- `action_payload` object:
  - `object_id` (required string; must match `target_id`)
  - `curated_kind` (required enum): `transcript_curated|ocr_curated`
  - `target_version` (required UTC date string `YYYYMMDD`)
  - `source_ref` (required object)
    - `type` (required string): `signed_download_url`
    - `url` (required string)
  - `content_type` (required string): `text/plain`
  - `idempotency_key` (required string)

Behavior notes:

- archive worker downloads the full replacement file from `source_ref.url`
- archive writes full-file replacement only; no partial patch semantics
- archive updates `meta/object_text_manifest.json` and enqueues `object_text_manifest_snapshot`
- apply success boundary is local archive state: once the curated file and local manifest are updated, the apply is successful even if downstream VPS republish must retry separately
- file naming in v1:
  - `transcript/transcript_curated_YYYYMMDD.txt`
  - `ocr/ocr_curated_YYYYMMDD.txt`

### PUT `/api/internal/objects/:object_id/available-files`

- Auth: `x-worker-auth-token`
- Path:
  - `:object_id` (format: `OBJ-YYYYMMDD-XXXXXX`)
- Body:
  - `files[]` full-replace snapshot entries (array may be empty):
    - `archive_file_key` (required string, non-empty)
    - `artifact_kind` (required enum):
      - `ingest_json|pipeline_json|catalog_json|original|preview|ocr|transcript|metadata|pdf|ocr_text|thumbnail|web_version|other`
    - `variant` (optional nullable string, non-empty when present)
    - `display_name` (required string, non-empty)
    - `content_type` (optional nullable string, non-empty when present)
    - `size_bytes` (optional nullable number, integer >= 0)
    - `checksum_sha256` (optional nullable string, non-empty when present)
    - `metadata` (object, optional)
    - `is_available` (boolean, optional; defaults `true`)
- Behavior:
  - tenant is resolved internally from `object_id`
  - Replaces object snapshot by archive key: upserts provided entries and marks omitted entries unavailable.
  - Auto-request side effect: backend attempts to auto-queue one `artifact_fetch` request per configured default kind (`thumbnail`, `ocr_text`) when corresponding available entries are present.
  - Auto-request suppression: per kind, backend skips auto-queue when artifact already exists or an active request (`PENDING`/`PROCESSING`) already exists.
  - Auto-request selection priority: for each kind, prefer `variant = null`; otherwise choose lexicographically lowest `archive_file_key`.
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
  - `object_text_manifest` (required object)
    - `object_id` (required string)
    - `media_type` (required enum: `document|audio|video|photo|other`)
    - `projection_version` (required string; opaque object-level change token)
    - `generated_at` (required RFC3339 timestamp)
    - `text_artifacts[]` (required array, may be empty)
      - `kind` (required string)
      - `version` (required string)
      - `is_active` (required boolean)
      - `metadata` (optional object)
- Behavior:
  - tenant is resolved internally from `object_id`
  - replaces current object text manifest snapshot for that object
  - VPS should treat artifact identity as `kind + version`
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
  - `content_type` (required non-empty string)
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
  - `content-type` must match signed token constraints by media type (parameters are ignored)
  - `content-length` is required and must exactly match signed token constraints
- Body: raw file bytes; byte length must exactly match signed token constraints
- 200 response:
  - `status: "ok"`, `request_id`, `size_bytes` (number)
- Error behavior:
  - `400 BAD_REQUEST` for header/body constraint mismatch
  - `401 UNAUTHORIZED` for invalid/expired signed token

### PUT `/api/object-download-requests/uploads/:token`

Deprecated compatibility route. New archive workers should use `PUT /api/archive-requests/uploads/:token`.

### POST `/api/object-download-requests/:id/complete`

Deprecated compatibility route. New archive workers should use `POST /api/archive-requests/:id/complete` with both `lease_token` and `upload_token` for `artifact_fetch`.

- Auth: `x-worker-auth-token`
- Body:
  - `lease_token` (required non-empty string)
  - `upload_token` (required non-empty string)
- Behavior:
  - idempotent success when matching artifact already exists
  - upload token and lease context must match (`request_id`, `object_id`, `tenant_id`, `artifact_kind`, `variant`)
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
