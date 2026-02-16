# Batch Label Rename Plan

## Goal

Rename ingestion user label naming to remove identifier ambiguity:

- `upload_id` -> `batch_label`
- `batch_id` -> `batch_label`

This is a pre-release codebase, so no backward-compatibility layer is planned.

## Why

- `ingestion_id` is the true unique workflow identifier.
- `upload_id` and `batch_id` are user-provided, non-unique labels.
- The `_id` suffix implies uniqueness and causes design confusion.

## Scope

- Database schema (initial migration file)
- Repositories
- Services
- Routes
- Integration tests
- Documentation

## Execution Steps

1. Update schema definitions in `src/db/migrations/0001_init.sql`:
   - Rename `ingestions.upload_id` to `ingestions.batch_label`.
   - Rename index `ingestions_tenant_upload_idx` to `ingestions_tenant_batch_label_idx`.

2. Update ingestion repository in `src/repos/ingestion-repo.ts`:
   - Row/model fields: `upload_id`/`uploadId` -> `batch_label`/`batchLabel`.
   - SQL query column lists and mapper fields.
   - Create params: `uploadId` -> `batchLabel`.

3. Update lease repository in `src/repos/lease-repo.ts`:
   - Row/model fields and query projections to `batch_label`/`batchLabel`.

4. Update services:
   - `src/services/ingestion-service.ts`:
     - input + validation names to `batch_label`/`batchLabel`
     - serializer key `upload_id` -> `batch_label`
   - `src/services/lease-service.ts`:
     - response field `batch_id` -> `batch_label`
     - source property `uploadId` -> `batchLabel`

5. Update routes in `src/routes/ingestions.ts`:
   - Request body field `upload_id` -> `batch_label`
   - Validation messages updated accordingly

6. Update tests:
   - `tests/integration/http/ingestion-routes.test.ts`
   - `tests/integration/http/lease-routes.test.ts`
   - `tests/integration/http/event-routes.test.ts`
   - `tests/integration/http/dashboard-routes.test.ts`
   - Replace request payload fields and SQL fixture column names.

7. Update docs:
   - `docs/project-requirements.md`
   - `docs/implementation-phases.md` (if referenced)
   - `docs/architecture.md` (if referenced)

8. Verification:
   - Repo-wide search confirms no remaining `upload_id`, `uploadId`, `batch_id`, `batchId` in source/tests/docs.
   - `bun test` passes.

## Out of Scope (for this rename pass)

- Object authority model changes (`object_id` generation ownership)
- Source ingestion cardinality decision (`1:1` vs `1:N`)
- Atomic object materialization redesign
