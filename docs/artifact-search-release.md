# Artifact Search Release

Artifact search adds migration `0013_object_artifact_search_documents.sql` and an idempotent backfill for materialized OCR/transcript text.

## Rollout

1. Back up PostgreSQL and confirm VPS staging storage is mounted.
2. Set `MAX_ARTIFACT_SEARCH_TEXT_BYTES` to a positive byte limit or retain the 10 MiB default.
3. Run `bun run migrate` before promoting the new backend.
4. Start the backend and require `GET /readyz` to return `200`.
5. Confirm all backend instances run the new upload-attempt code before workers create verified checkpoints; do not mix old lease sweepers with the new verified-upload lifecycle.
6. Run `bun run backfill-artifact-search-text --batch-size 100` with the production `DATABASE_URL`, `DB_SCHEMA`, and staging root.
7. Review the content-free summary. Investigate any `failures`; missing, malformed, empty, unsupported, and over-limit files are reported as explicit skipped counts.
8. Deploy the UI and smoke-test title, object ID, indexed OCR/transcript, and curated OCR searches.
9. Verify a restricted term does not match for an unassigned viewer and does match for an authorized assignment or administrator.

The backfill is safe to rerun. It skips artifacts that already have indexed text and retries rows that remain unindexed.

## Rollback

Switch traffic back to the previous complete backend and UI artifacts. Leave migration `0013` and its data in place: the schema is additive, and the previous backend does not read it. Do not drop the table during an incident rollback. A later forward deployment can rerun the backfill to index artifacts materialized while the older backend was active.

## Limits

- Search uses case-insensitive literal substring matching without ranking or snippets.
- Arbitrary PDF, image, audio, and video bytes are not extracted by the API. They require a materialized OCR/transcript text artifact.
- Existing text whose staging file is missing cannot be backfilled until it is materialized again.
- Correlated substring searches may require query-plan and index tuning as the catalog grows.
