# Testing Specification

This document defines the required tests for the Osimi backend control plane (VPS).

## Scope

- Auth and tenant scoping
- Ingestion lifecycle + state transitions
- File upload flow (presign + commit)
- Worker lease + event ingestion
- Objects and artifacts
- Dashboard summary + activity feed

## Test Types

- Unit tests: state transitions, token validation, checksum logic
- Integration tests: API + DB + filesystem staging
- Contract tests: worker lease and event flows

## Required Scenarios

- Auth login success with valid credentials and tenant membership
- Auth login failure with invalid credentials
- Auth session persistence across app instance restarts
- Auth session revocation on logout
- Auth session expiry rejection
- Auth token storage validation (hash-only, no raw token persistence)
- Auth audit events for login/logout/session rejection
- Liveness remains available when readiness dependencies or probe authentication headers are invalid
- Readiness reports healthy lifecycle/configuration/database/exact migration state, and reports unavailable DB, timeout, migration drift, and draining as `503`
- Graceful shutdown marks readiness false, rejects new work, drains admitted requests, and exits within the configured deadline; deadline expiry and a second signal force shutdown
- Lease exclusivity: only one worker can lease a batch at a time
- Submission freezes the manifest atomically: queued or actively leased ingestions reject metadata, file, item, link, and ordering mutations
- Submission rejects pending or failed files, items without files, and committed files without item links
- Submission and manifest writes are serialized so a queued ingestion cannot contain a post-submit mutation
- Lease payload validation failure: both next and targeted claims leave the ingestion `QUEUED` with no active lease
- Lease release/requeue is atomic: a successful release has no observable state with a released lease and `PROCESSING` ingestion
- Lease expiry: expired leases re-queue and can be leased again
- Redundancy sweep: expired leases are re-queued even if automatic requeue fails
- Targeted lease reacquire: `POST /api/ingestions/:id/lease` leases only the requested queued ingestion
- Targeted lease conflict: requesting `POST /api/ingestions/:id/lease` for an actively leased ingestion returns conflict (no takeover)
- Signed URL constraints: method/TTL/content-type/content-length enforced
- Upload commit checksum validation (SHA-256)
- Ingestion create/update rejects incompatible `classification_type` and `item_kind` combinations
- File commit rejects media kinds incompatible with ingestion `item_kind`
- Item create/update rejects incompatible `classification_type` and effective `item_kind` combinations
- File-to-item linking rejects media kinds incompatible with the item's effective `item_kind`
- Committed image/video uploads expose `preview.status = pending` until a preview is ready
- Unsupported upload media expose `preview.status = unsupported`
- Worker-generated ingestion previews reject unsupported thumbnail output MIME types, oversized files, and missing dimensions
- Ingestion preview fetch serves staged preview bytes only when preview status is ready and tenant auth passes
- Worker preview claim/report flow is duplicate-safe enough for a single claimed job at a time and can recover claimed preview jobs after timeout
- Worker download checksum mismatch emits `FILE_FAILED`
- Event ingestion idempotency by `event_id`
- Ingestion request idempotency: sequential and concurrent identical requests replay one successful response; a mismatched key reuse returns conflict
- Event projection transactions roll back the event reservation and all derived writes on failure
- Conflicting reuse of an `event_id` is rejected without applying another projection
- Batched event delivery commits successfully projected earlier events while allowing a failed later event to be retried
- Event ordering tolerance (out-of-order delivery)
- Item completion materializes one object per item and returns all item-scoped `object_ids`
- Aggregate `INGESTION_COMPLETED` rejects object/item identity and creates no object
- Aggregate completion cannot terminalize an ingestion while items remain non-terminal
- Terminal item outcomes derive `COMPLETED`, `COMPLETED_WITH_ERRORS`, or `FAILED`, including skipped-item combinations
- Staging retention rules by ingestion state
- Staging retention uses bounded exclusive purge claims, converges after missing paths or a crash, and treats `COMPLETED_WITH_ERRORS` as completed retention
- Staging retention removes temporary preview derivatives alongside original staged files
- Purge intent rejects retry, restore, mutable staging actions, and new preview work
- Object editing enforces public/family/private assignment policy with an admin override across every edit operation
- Stuck attention for `UPLOADING` and `PROCESSING`
- Tenant scoping on all endpoints

Planned future scenarios (file ordering contract):

- Lease file ordering honors `source_order` when provided.
- Lease file ordering falls back deterministically when `source_order` is absent.
- Lease payload includes `filename` and `source_order` for each `download_urls[]` item.
- `storage_key` lexical order is not used as a semantic ordering source.

## Fixtures

- Seed tenants, users, ingestions, files, objects
- Fake worker identity + lease token
- Staging files with known checksums
- Make sure to use UUID that is RFC 9562/4122 specification conformant. 
- Auth fixtures: users, tenant memberships, hashed passwords, active/revoked/expired sessions

## Test Environment

- Use `bun run test:unit` for database-free unit tests.
- Use `bun run test:integration` for PostgreSQL-backed integration tests.
- Use `bun run test:release` for the required type-check, unit, and integration release gate.
- Integration tests require `TEST_DATABASE_URL` and never use `DATABASE_URL`.
- The test database name must contain `test`; use `ALLOW_UNSAFE_TEST_DATABASE_NAME=true` only for a known disposable database.
- Integration tests must not be conditionally skipped because the database is unavailable.
- Use local filesystem staging directory
