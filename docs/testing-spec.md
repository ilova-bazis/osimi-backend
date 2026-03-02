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
- Worker download checksum mismatch emits `FILE_FAILED`
- Event ingestion idempotency by `event_id`
- Event ordering tolerance (out-of-order delivery)
- Staging retention rules by ingestion state
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

- Use `bun test`
- Use PostgreSQL test database
- Use local filesystem staging directory
