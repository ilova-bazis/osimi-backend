# Event Service Review Checklist (Monday)

Target file: `src/services/event-service.ts`

## Why this review matters

- We changed object identity authority to Archive System.
- `INGESTION_COMPLETED` now depends on worker-provided `object_id`.
- This path controls object materialization, idempotency, and ingestion terminal transitions.

## Review Goals

- Confirm correctness under out-of-order and duplicate events.
- Confirm no backend-generated `object_id` logic remains.
- Confirm conflict behavior is strict when object identity is inconsistent.
- Confirm event validation rules match docs and tests.

## Step-by-step Checklist

### 1) Event schema validation

- [ ] Verify `object_id` is required for:
  - `INGESTION_COMPLETED`
  - `OBJECT_CREATED`
  - `ARTIFACT_CREATED`
- [ ] Verify `object_id` format validation (`OBJ-YYYYMMDD-XXXXXX`) is enforced.
- [ ] Verify non-object events can still omit `object_id`.
- [ ] Verify error messages are clear and field-specific.

### 2) Lease and ingestion integrity gates

- [ ] Confirm lease token parsing happens before event processing.
- [ ] Confirm token ingestion id must match route ingestion id.
- [ ] Confirm active lease check is mandatory.
- [ ] Confirm ingestion existence check is mandatory before event loop.

### 3) Event dedupe behavior

- [ ] Confirm `insertObjectEvent(... ON CONFLICT DO NOTHING)` controls dedupe.
- [ ] Confirm duplicate events do not re-run side effects.
- [ ] Confirm `inserted_events` and `duplicate_events` counters are accurate.

### 4) Status transition logic

- [ ] Confirm `currentStatus` is used (not stale ingestion snapshot).
- [ ] Confirm transitions use CAS helper (`applyStatusTransition`).
- [ ] Confirm CAS miss fallback behavior is acceptable for best-effort ordering.
- [ ] Confirm terminal state behavior remains stable under out-of-order events.

### 5) Object creation/materialization

- [ ] Confirm completion path uses `event.object_id` (archive authority).
- [ ] Confirm no backend object id generator exists in the file.
- [ ] Confirm atomic create/get repo method is used.
- [ ] Confirm mismatch (`same ingestion`, `different object_id`) throws `409 Conflict`.

### 6) Artifact handling

- [ ] Confirm `ingest_json` is persisted into `objects.ingest_manifest`.
- [ ] Confirm repeated completion events follow last-write-wins for `objects.ingest_manifest`.

### 7) Response contract

- [ ] Confirm returned `object_id` in response reflects the completed object when present.
- [ ] Confirm response stays stable for duplicate-only event batches.

## Test follow-up to verify

Target file: `tests/integration/http/event-routes.test.ts`

- [ ] Add/confirm test: missing `object_id` on `INGESTION_COMPLETED` -> `400`.
- [ ] Add/confirm test: invalid `object_id` format -> `400`.
- [ ] Add/confirm test: same ingestion, different `object_id` on repeated completion -> `409`.
- [ ] Confirm existing tests still pass for:
  - out-of-order events
  - duplicate completion events
  - concurrent completion events

## Docs alignment check

- [ ] `docs/project-requirements.md` event schema and authority wording still match behavior.
- [ ] `docs/implementation-phases.md` completion bullet still matches behavior.
- [ ] `docs/architecture.md` ownership statement still matches behavior.

## Nice-to-have cleanup (if time)

- [ ] Remove dead repo methods no longer used by services.
- [ ] Reduce repeated path-param/body parsing helpers across route files.
