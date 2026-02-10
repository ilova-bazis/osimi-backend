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

## Tests

```bash
bun test
```

Integration migration tests require either `TEST_DATABASE_URL` or `DATABASE_URL`.

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
