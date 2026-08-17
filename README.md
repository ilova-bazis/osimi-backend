# osimi-backend

Backend control plane for Osimi Digital Library.

## Install

```bash
bun install
```

## Run (development)

```bash
bun run --watch index.ts
```

## Run (production)

```bash
bun run index.ts
```

## Required Security Configuration

The server refuses to start unless both signing secrets are set to at least 32
non-whitespace characters:

- `UPLOAD_SIGNING_SECRET`: signs staged-file and artifact upload/download URLs.
- `LEASE_SIGNING_SECRET`: signs ingestion and archive-request worker lease tokens.

Generate distinct, deployment-specific values and store them in your secret
manager. Rotating either value invalidates outstanding tokens signed with the
previous value, so rotate only after their maximum expiry window has passed or
coordinate the worker/client rollout.

## Upload Configuration

- `MAX_UPLOAD_SIZE_BYTES`: maximum signed upload size and streamed request body
  size. Defaults to `2147483648` (2 GiB) and must be a positive safe integer.
- `MAX_ARTIFACT_SEARCH_TEXT_BYTES`: maximum materialized OCR/transcript text
  body read for indexing. Defaults to `10485760` (10 MiB) and must be a
  positive safe integer. Larger artifacts remain materialized but are skipped
  by text indexing.

Every presign receives a unique immutable staging key. Re-presigning a pending
file or artifact invalidates the earlier URL; uploads stream to a temporary
file, synchronize its bytes, publish it without replacing an existing object,
and synchronize the containing directory entry. Artifact uploads are durably
checkpointed only after the active lease, exact size, and checksum are verified.
At that point ownership transfers to the backend: artifact creation, search
projection, page metadata, and request completion are finalized atomically and
retried by a background reconciler without asking another worker to re-upload.
Finalization revalidates the stored regular file against the checkpoint. These
guarantees require POSIX-compatible same-filesystem hard links plus file and
directory `fsync`; unsupported storage fails closed rather than acknowledging a
non-durable checkpoint. The backend service identity must be the only writer
with permission to create, replace, or unlink paths below the staging root.

## CORS and UI Deployment

`CORS_ALLOWED_ORIGINS` is a comma-separated allowlist of exact browser UI
origins that may call the API and direct-upload endpoints. Origins include the
scheme, hostname, and optional port, for example:

```bash
CORS_ALLOWED_ORIGINS=https://archive.example,https://admin.archive.example
```

When unset, it preserves the local development origins
`http://localhost:4444` and `http://localhost:5173`. Set it to an empty value
to deny all cross-origin callers. Wildcards, credentials, paths, query strings,
fragments, and embedded user credentials are rejected. Production origins
should use HTTPS.

Direct upload `PUT` requests use their signed path token and do not use cookies
or bearer credentials. A same-origin reverse proxy needs no CORS entry. For a
separately deployed UI, add its exact origin to `CORS_ALLOWED_ORIGINS` and make
the browser-visible API base publicly reachable.

## Tests

Run fast unit tests without PostgreSQL:

```bash
bun run test:unit
```

Run the database-backed integration suite:

```bash
TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/osimi_test bun run test:integration
```

Run the release gate, including type-checking, unit tests, and integration tests:

```bash
TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/osimi_test bun run test:release
```

`bun run test` runs the release gate. Integration tests require
`TEST_DATABASE_URL`; they never fall back to `DATABASE_URL`. The database name
must contain `test` because the suite creates and drops isolated schemas with
`CASCADE`. For a known disposable database with a different name, set
`ALLOW_UNSAFE_TEST_DATABASE_NAME=true` explicitly. Never point integration
tests at a production database.

## Database Migrations

```bash
bun run migrate
```

Optional flags:

- `--database-url=postgres://...`
- `--schema=public`
- `--migrations-dir=src/db/migrations`
- `--dry-run`

Migration schema selection is, in order: explicit `--schema`, `DB_SCHEMA`,
then `public`. An explicit schema that differs from `DB_SCHEMA` wins and emits
a warning. `--dry-run` is read-only: it does not create the target schema,
tracking table, or migration objects. Migration runners take a database-scoped
advisory lock so concurrent deployments cannot apply the same migration twice.

## Health Endpoint

- `GET /healthz` is a process liveness probe. It does not require a database or
  configuration dependency check.
- `GET /readyz` is a deployment readiness probe. It returns `200` only when
  the process is accepting traffic, required configuration is valid, PostgreSQL
  is reachable, and the configured schema's migration history exactly matches
  the application migrations. It returns `503` with non-sensitive check codes
  otherwise.
- `READINESS_TIMEOUT_MS` bounds each readiness database check. It defaults to
  `1000` and must be a positive integer.

Deploy migrations before starting or promoting an application instance. A
database with pending, changed, or unknown migration records is intentionally
not ready. During shutdown the process marks readiness false before draining;
the listener rejects new connections while active requests and jobs drain.
`SHUTDOWN_GRACE_PERIOD_MS` defaults to `60000`. A first `SIGINT` or `SIGTERM`
drains and exits `0`; a deadline expiry or second signal force-closes resources
and exits `1`.

After deploying the artifact search-document migration, index existing
materialized OCR/transcript text from the configured `STAGING_ROOT`:

```bash
DATABASE_URL=postgres://... bun run backfill-artifact-search-text --batch-size 100
```

The operation scans all tenants, is safe to rerun, and does not rewrite rows
whose text is already indexed. Missing, unsupported, malformed, empty, and
over-limit artifacts remain retryable on a later run. It exits nonzero if any
artifact failed unexpectedly; review the content-free failure identifiers and
rerun after correcting the cause.

## Auth Endpoints

- `POST /api/auth/login`
- `POST /api/auth/logout`
- `GET /api/auth/me`

Authentication is DB-backed and tenant-aware. The project currently does not ship seed users by default.
Provision users/tenants/memberships in your target database before calling auth endpoints.

## HTTP Access Logs

Request logging is enabled by default and emits single-line, human-readable logs.

Optional environment variables:

- `HTTP_ACCESS_LOGS=false` to disable access logs
- `LOG_FORMAT=pretty|json` (`pretty` default)
- `LOG_COLOR=true|false` to force color output in `pretty` mode
- `NO_COLOR=1` to disable ANSI colors

## Background Jobs

By default, the server runs background operations for staging retention and stuck-ingestion attention.

Optional environment variables:

- `BACKGROUND_JOBS_ENABLED=true|false` (default: `true`)
- `COMPLETED_STAGING_RETENTION_DAYS` (default: `7`)
- `FAILED_CANCELED_STAGING_RETENTION_DAYS` (default: `14`)
- `STAGING_RETENTION_SWEEP_INTERVAL_SECONDS` (default: `300`)
- `STUCK_ATTENTION_THRESHOLD_MINUTES` (default: `60`)
- `STUCK_ATTENTION_INTERVAL_SECONDS` (default: `120`)
- `STAGING_RETENTION_BATCH_SIZE` (default: `25`)
- `STAGING_RETENTION_CLAIM_TIMEOUT_SECONDS` (default: `900`)
- `ARTIFACT_FINALIZATION_INTERVAL_SECONDS` (default: `30`)
- `ARTIFACT_FINALIZATION_BATCH_SIZE` (default: `25`)
- `ARTIFACT_FINALIZATION_CLAIM_TIMEOUT_SECONDS` (default: `300`)
