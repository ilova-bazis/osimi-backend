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
- Lease exclusivity: only one worker can lease a batch at a time
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
- Event ordering tolerance (out-of-order delivery)
- Staging retention rules by ingestion state
- Staging retention removes temporary preview derivatives alongside original staged files
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
