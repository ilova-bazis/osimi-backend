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
bun run src/db/migrate.ts
```

Optional flags:

- `--database-url=postgres://...`
- `--schema=public`
- `--migrations-dir=src/db/migrations`
- `--dry-run`

## Health Endpoint

- `GET /healthz`

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
