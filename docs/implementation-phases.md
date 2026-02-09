# Implementation Phases

This document turns the backend requirements into a phased implementation plan for the Osimi control plane.

## Delivery Strategy

- Build in vertical slices: route -> service -> repository -> tests -> observability.
- Lock invariants early: auth, tenant scoping, state machine, idempotency, error model.
- Use mock worker behavior first, then enforce the full lease and event contract.
- Treat `docs/project-requirements.md` as the API contract and `docs/testing-spec.md` as release gates.

## Phase 1: Foundation

Goal: establish project structure, shared primitives, and schema.

- Create module boundaries: `routes`, `services`, `repos`, `domain`, `jobs`, `storage`, `auth`.
- Add request context (`request_id`, `tenant_id`, `user_id`, `role`) and structured error responses.
- Add cursor pagination and idempotency helpers.
- Add initial migrations for:
  - `ingestions`
  - `ingestion_files`
  - `ingestion_leases`
  - `objects`
  - `object_artifacts`
  - `object_events`
- Implement ingestion state machine with strict transition rules.

Exit criteria:

- App boots with shared middleware and domain primitives.
- Schema is migrated and test database can be initialized repeatedly.

## Phase 2: Auth and Tenant Guardrails

Goal: enforce identity and authorization before feature endpoints expand.

- Implement:
  - `POST /api/auth/login`
  - `POST /api/auth/logout`
  - `GET /api/auth/me`
- Add authn/authz middleware with tenant awareness and role checks (`viewer`, `operator`, `admin`).
- Enforce tenant scoping on all queries and mutations.
- Implement persistent auth storage (no in-memory-only sessions):
  - `tenants`
  - `users`
  - `tenant_memberships`
  - `auth_sessions`
- Store session tokens as hashes, enforce session expiry/revocation, and persist auth audit events.

Exit criteria:

- Auth endpoints pass integration tests.
- Tenant isolation and role rules are covered by tests.
- Session persistence survives process restarts and supports logout revocation semantics.

## Phase 3: Ingestion CRUD and Upload Flow

Goal: make ingestion creation and file upload reliable and auditable.

- Implement:
  - `POST /api/ingestions`
  - `GET /api/ingestions`
  - `GET /api/ingestions/:id`
  - `POST /api/ingestions/:id/files/presign`
  - `POST /api/ingestions/:id/files/commit`
  - `POST /api/ingestions/:id/submit`
  - `POST /api/ingestions/:id/cancel`
  - `POST /api/ingestions/:id/retry`
- Enforce signed URL constraints (method, TTL, content-type, content-length).
- Verify SHA-256 at upload commit.
- Add idempotency on create/submit/retry paths.

Exit criteria:

- Upload flow works end-to-end against local staging storage.
- Checksum mismatch handling is tested and deterministic.

## Phase 4: Worker Lease Protocol

Goal: support outbound-only worker processing without race conditions.

- Implement:
  - `POST /api/ingestions/lease`
  - `POST /api/ingestions/:id/lease/heartbeat`
  - `POST /api/ingestions/:id/lease/release`
- Lease grant must be atomic in one transaction to guarantee exclusivity.
- Issue signed `lease_token`; validate ownership on heartbeat/release.
- Issue short-lived worker download URLs on lease and refresh on heartbeat.
- Add periodic redundancy sweep to re-queue expired leases.

Exit criteria:

- Lease exclusivity and lease expiry re-queue tests pass.
- Redundancy sweep behavior is covered by tests.

## Phase 5: Event Ingestion and Completion Path

Goal: persist worker progress, support idempotent event ingestion, and finalize objects.

- Implement:
  - `POST /api/ingestions/:id/events`
- Enforce valid lease token and deduplicate by `event_id`.
- Accept out-of-order events safely (best-effort ordering semantics).
- Persist all events in `object_events` for feed and audit.
- On successful completion:
  - generate immutable `object_id` (`OBJ-YYYYMMDD-XXXXXX`)
  - create object and artifact records
  - store `ingest.json` under object artifact layout

Exit criteria:

- Event idempotency and ordering tolerance tests pass.
- Object creation on completion is stable and reproducible.

## Phase 6: Objects and Dashboard APIs

Goal: provide read/write surfaces used by UI for discovery and monitoring.

- Implement Objects endpoints:
  - `GET /api/objects`
  - `GET /api/objects/:object_id`
  - `PATCH /api/objects/:object_id`
  - `GET /api/objects/:object_id/artifacts`
  - `GET /api/objects/:object_id/artifacts/:artifact_id/download`
- Implement Dashboard endpoints:
  - `GET /api/dashboard/summary`
  - `GET /api/dashboard/activity?limit=&cursor=`
- Ensure cursor pagination and tenant filters are applied consistently.

Exit criteria:

- Dashboard and object flows match frontend contract.
- Activity feed is sourced from persisted events.

## Phase 7: Retention, Stuck Detection, and Operations

Goal: add lifecycle hygiene and operational confidence.

- Implement staging cleanup job:
  - `COMPLETED` retention: 7 days
  - `FAILED` and `CANCELED` retention: 14 days
  - active states retained indefinitely
- Implement stuck attention detection for `UPLOADING` and `PROCESSING` with configurable threshold.
- Add structured logs and metrics:
  - throughput/failure rates
  - lease grant/renew/expire counts
  - checksum mismatch counts
  - event dedupe counts

Exit criteria:

- Cleanup and stuck detection jobs run safely in test and staging environments.
- Operational signals are available for debugging and alerting.

## Testing Gate (Definition of Done)

Before MVP completion, all required scenarios in `docs/testing-spec.md` must pass with `bun test`.

Mandatory coverage areas:

- Lease exclusivity under concurrent workers.
- Lease expiry and redundancy re-queue behavior.
- Signed URL policy enforcement.
- Checksum verification at upload commit and worker download.
- Event ingestion idempotency by `event_id` and out-of-order tolerance.
- Retention and stuck-attention behavior.
- Tenant scoping and role enforcement across all endpoints.

## Recommended PR Sequence

1. Foundation + Auth + tenant tests.
2. Ingestion CRUD + upload presign/commit + checksum tests.
3. Lease protocol + heartbeat + redundancy sweep.
4. Event ingestion + completion + `ingest.json` artifact flow.
5. Objects + dashboard + retention/stuck jobs + observability.
