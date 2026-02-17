# Typed Worker Validation and Middleware Plan

This document captures the implementation plan for moving worker route handling to a fully typed pipeline.

Goal: `unknown` should exist only at the HTTP boundary. After middleware parsing and authorization, handlers and services should operate on typed DTOs.

## Scope

- Worker ingestion events endpoint: `POST /api/ingestions/:id/events`
- Worker lease endpoints:
  - `POST /api/ingestions/:id/lease/heartbeat`
  - `POST /api/ingestions/:id/lease/release`

## Design Principles

- Validation is centralized in `src/validation/*` (Zod-based).
- Authorization policy is centralized in `src/auth/*`.
- Route middleware composes parsing + auth + authorization.
- Services accept typed inputs only and perform business logic only.
- Keep `401` behavior for lease token ingestion-id mismatch.
- For event payload schemas, start with typed known fields plus `.passthrough()` for forward compatibility.

## Current Baseline

- Zod validation layer exists (`src/validation/*`).
- Lease authorization helper exists (`src/auth/worker-lease.ts`).
- Worker auth wrapper exists (`withWorkerAuth` in `src/routes/middleware.ts`).
- Services still accept some `body: unknown` inputs and do parsing internally.
- Event payload typing is partially permissive (`Record<string, unknown>` in service flow).

## Step 1: Typed Middleware Pipeline + Typed Service Inputs

### 1. Add explicit worker types

Create `src/types/worker-events.ts`:

- `IngestWorkerEventsBody` (derived from validation)
- `IngestWorkerEventsInput` (service input)
- `IngestWorkerEventsResponse` (service output)

Create `src/types/lease.ts`:

- `LeaseTokenBody`
- `HeartbeatLeaseInput`
- `HeartbeatLeaseResponse`
- `ReleaseLeaseInput`
- `ReleaseLeaseResponse`

### 2. Expand route middleware composition

Extend `src/routes/middleware.ts` with composable wrappers:

- `withParsedJsonBody(parser)`
- `withPathParam(pattern, name)` (or endpoint-specific wrappers)
- `withAuthorizedWorkerLease(getIngestionId, getLeaseToken)`

Expected middleware output for handlers should include typed fields (e.g. `ingestionId`, `body`, `authorizedLease`).

### 3. Refactor routes to use typed middleware outputs

Update `src/routes/lease.ts`:

- Worker endpoints compose wrappers instead of manually parsing body/path each time.
- Call services using fully typed inputs.

### 4. Refactor service signatures

Update `src/services/event-service.ts`:

- Replace `{ ingestionId: string; body: unknown }` input with typed input DTO.
- Remove internal parse/authz calls from service entry.

Update `src/services/lease-service.ts`:

- `heartbeatLease` and `releaseActiveLease` should accept typed inputs (including authorized lease data), not raw bodies.

### 5. Step 1 tests

- Add middleware unit tests for parser/authz wrappers.
- Keep integration route tests passing for event/lease endpoints.

## Step 2: Strict Per-Event Payload Typing

### 1. Define per-event payload schemas

In `src/validation/event.ts`, convert payload typing from generic object to per-event schemas keyed by `event_type`.

Examples:

- `INGESTION_COMPLETED` payload schema includes known fields (e.g. `title`, `ingest_json`) with `.passthrough()`.
- `PIPELINE_STEP_STARTED`, `FILE_VALIDATED`, etc., each get event-specific payload shape.

### 2. Export typed event payloads

Expose inferred types for each event branch from validation module (or via `src/types/event-payloads.ts` if needed).

### 3. Refactor event service logic

- Remove `typeof` payload guards for known fields.
- Use discriminated union narrowing on `event_type` and typed payload access.
- Remove `as Record<string, unknown>` casts where schema already proves type.

### 4. Step 2 tests

- Add validation unit tests for each event type payload branch.
- Update/add integration tests for invalid payloads per event type.

## Completion Criteria

- Worker services in scope do not accept `body: unknown`.
- Route handlers pass typed DTOs produced by middleware.
- Event payload handling is typed by `event_type` branch.
- Authorization and validation logic are centralized and not duplicated in services.

## Suggested Execution Order

1. Implement Step 1 end-to-end for worker events endpoint.
2. Apply Step 1 pattern to lease heartbeat/release.
3. Implement Step 2 payload typing per event type.
4. Run focused tests after each sub-step.
