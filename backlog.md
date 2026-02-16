# Backlog

## High Priority

- [x] Resolve object materialization race on ingestion completion.
  - Replaced check-then-insert with atomic `createOrGetObjectBySourceIngestion` path.
  - Added DB uniqueness guard on `objects.source_ingestion_id` (non-null).

- [x] Decide and codify ingestion-to-object cardinality.
  - Decision: `1 ingestion -> 1 object` (MVP).
  - Applied in schema/service/tests; requirement wording updated accordingly.

- [x] Align object ID authority model across code and docs.
  - `INGESTION_COMPLETED`, `OBJECT_CREATED`, and `ARTIFACT_CREATED` now require `object_id`.
  - Backend event flow no longer generates `object_id`; it uses archive-supplied identity.
  - Completion path rejects conflicting object identity for the same ingestion.
  - Contract/docs aligned in requirements, architecture, and implementation phases.

## Medium Priority

- [ ] Replace remaining test setup `sql.unsafe(...)` usage in integration tests.
  - Prefer tagged SQL and safe fragments/explicit `search_path`.
  - Use `sql``.simple()` only where static multi-statement queries are truly needed.

- [x] Consolidate repo SQL helper usage.
  - Standardize on one schema-scoped helper import pattern (`withSchemaClient`) across repos.
  - Remove any leftover dead imports from previous refactors.

- [x] Add concurrency-focused tests for object completion idempotency.
  - Added concurrent completion test for same ingestion.
  - Asserts single object row under the `1 ingestion -> 1 object` decision.

## Documentation

- [x] Update docs to reflect final object authority + cardinality decisions.
  - `docs/project-requirements.md`
  - `docs/architecture.md`
  - `docs/implementation-phases.md`

- [ ] Add migration tooling decision record.
  - Compare current custom runner vs external tool (dbmate / Atlas / Drizzle).
  - Record chosen direction and rollout plan.
