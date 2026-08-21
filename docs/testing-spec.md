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
- Ingestion list/detail expose role-, lease-, and purge-aware action capabilities; clients cannot infer actions from status or previews
- Retained failed ingestions can retry only; purge-pending and purged failed/canceled ingestions expose no mutable capabilities and reject retry/restore/delete
- Configured CORS origins receive direct-upload `OPTIONS` and `PUT` headers; unapproved origins receive no CORS grant
- CORS origin configuration rejects malformed values and browser-visible relative upload URLs resolve through the public API base
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
- Curation publication source cleanup retains completed/canceled sources for 24 hours and failed sources for 7 days, converges when files are missing, preserves active sources, and removes only old untracked source files
- Purge intent rejects retry, restore, mutable staging actions, and new preview work
- Object editing enforces public/family/private assignment policy with an admin override across every edit operation
- Curation publication migration fencing waits for in-flight archive-request writers, fails closed on multiple processing rows, and creates the one-active-request invariant without a writer race
- Exact curation publication retries replay the original request in active and terminal states; concurrent exact submits create one request and one retained source
- Curation source download requires worker auth, the matching worker identity when present, and the active request lease; bytes, size, and SHA-256 match the persisted checkpoint
- Object search trims `q`, treats an empty trimmed query as omitted, rejects more than 256 trimmed characters, and matches literal case-insensitive substrings with `%`, `_`, and `\` escaped
- Object search retains tenant-catalog `title` and `object_id` matches and preserves the requested sort without ranking or snippets
- Object search matches materialized artifact `id`, `kind`, `variant`, and `content_type`, and available-file `display_name` and `archive_file_key` only through authoritative persisted materialization provenance, never inferred `kind`/`variant`; unmaterialized available-file inventory does not match
- Artifact metadata, indexed OCR/transcript bodies, and curated document page text contribute matches only while the requester passes role/access assignment authorization, the embargo is inactive, and availability is `AVAILABLE`; access or embargo changes take effect without stale-index disclosure
- Object search matches indexed, eligible materialized OCR/transcript text and `object_curated_document_pages.curated_text`, but never extracts arbitrary PDF/image/audio/video bytes during a request
- Artifact indexing skips empty, malformed/non-text, unavailable, and over-limit bodies and enforces the configured per-artifact text-size limit (10 MiB recommended default)
- Artifact presign attempts are lease-bound; release, failure, expiry, and re-presign invalidate unfinished attempts, and a released/reassigned lease cannot verify or finalize its former upload
- Stale or mismatched artifact-upload leases are rejected before the request body is read or staged, proven by focused coverage asserting an untouched body and no staging residue while valid and concurrent uploads remain correct
- A verified artifact upload transfers ownership to the backend, is never leased for re-upload, and is finalized synchronously or by a fenced multi-instance background claim
- Artifact upload durability tests prove temporary-file sync precedes immutable publication, directory sync precedes `VERIFIED`, and sync failures leave worker ownership active
- Exact `VERIFIED` and `MATERIALIZED` retries are idempotent and may restore a missing path, while different bytes cannot replace the accepted size/SHA-256 checkpoint
- A known source-checksum mismatch returns deterministic retry guidance, publishes and indexes no rejected bytes, leaves the attempt `AUTHORIZED`, and accepts corrected bytes through the same active URL
- Artifact presign preserves the checksum of an exact source that became inactive after queueing, rejects missing, kind/variant-changed, or malformed-checksum sources before authorization without invalidating a prior attempt, and keeps the captured expected checksum immutable across later source updates
- Artifact presign and PUT share valid media-type parsing: parameterized declared values are preserved through materialization and indexing, case-insensitive base-type matches succeed, and malformed or different base types fail before staging
- Concurrent rejected and corrected uploads use isolated temporary files; cleanup cannot remove or replace the corrected winner, and exactly one checkpoint/artifact/projection completes
- Finalization rejects missing, non-regular, truncated, changed, oversized, and checksum-mismatched verified storage without artifact, search, metadata, attempt-materialization, or request-completion side effects
- Artifact finalization uses the verified attempt's exact storage key and atomically commits artifact, search projection/provenance, page metadata, attempt state, and request completion; injected intermediate failure leaves no partial projection and remains retryable
- Generic and legacy completion acknowledgments for a materialized artifact are read-only: retries do not inspect staged bytes, rewrite projection timestamps/text/provenance, or recreate a missing projection
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
