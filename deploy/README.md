# VPS Deployment

This Compose project builds and runs the Osimi UI and API. It does not create nginx, PostgreSQL, or the archive worker. No container port is published to the host.

## Prerequisites

- Docker Engine with the Compose plugin.
- Existing nginx and PostgreSQL Docker networks.
- Existing PostgreSQL database and role.
- Existing worker configured with the same worker token as this deployment.
- Both repositories checked out as sibling directories named `osimi-backend` and `osimi-archive-ui`.

## Configure

1. Copy `deploy/.env.example` to `deploy/.env` and replace the domain, external network names, staging path, image tag, and build ID.
2. Create the staging host directory with the configured path, for example `sudo install -d -o 10001 -g 10001 -m 0750 /srv/osimi-archive/staging`. Preserve and back it up during deployments.
3. Create the four files documented in `deploy/secrets/README.md`, then set their host ownership to `10001:10001` and mode to `0400` as shown there. Do not put secret values in `deploy/.env`.
4. Attach the existing nginx container to `PROXY_NETWORK` if it is not attached already.
5. Adapt `deploy/nginx.conf.example` to the public domain and existing TLS certificate paths, validate nginx configuration, and reload the existing nginx service.

Keep `PUBLIC_ORIGIN` as the exact browser origin with no trailing path, for example `https://archive.example.com`. The API receives it as its CORS allowlist and the UI uses the same origin for browser API requests. Server-side UI requests use the private `http://api:3000` stack address.

## Build And Migrate

Run from the backend repository:

```bash
docker compose --env-file deploy/.env -f deploy/compose.yaml build
docker compose --env-file deploy/.env -f deploy/compose.yaml --profile tools run --rm migrate
```

Migrations are never run by `docker compose up`. The migration runner uses PostgreSQL advisory locking and migration checksums, but should still be invoked once as an explicit release step before the application rollout.

## Start Or Upgrade

```bash
docker compose --env-file deploy/.env -f deploy/compose.yaml up -d --remove-orphans
docker compose --env-file deploy/.env -f deploy/compose.yaml ps
```

Compose waits for API readiness before starting the UI. The API joins the existing PostgreSQL and nginx networks; the UI joins nginx and the private stack network. `/healthz` and `/readyz` are intentionally not routed by the nginx example and remain available only inside Docker networks.

The default CPU, memory, PID, and JSON log rotation limits are conservative VPS safeguards and can be tuned in `deploy/.env`. Docker records failed health checks but does not restart an otherwise running unhealthy container; monitor `docker compose ps` or Docker health events. Database readiness can recover automatically after a transient PostgreSQL outage.

## Verify

```bash
docker compose --env-file deploy/.env -f deploy/compose.yaml exec api bun -e "const r=await fetch('http://127.0.0.1:3000/healthz'); console.log(r.status, await r.text())"
docker compose --env-file deploy/.env -f deploy/compose.yaml exec api bun -e "const r=await fetch('http://127.0.0.1:3000/readyz'); console.log(r.status, await r.text())"
curl --fail --show-error --silent https://archive.example.com/login
```

Also log in through the browser, upload a disposable file, confirm the worker can authenticate and process it, and verify the file exists below `STAGING_HOST_PATH`.

## Rate Limiting

The API applies bounded in-memory rate limits for repeated failed logins
(per normalized username) and repeated missing/invalid worker credentials.
Blocked requests receive JSON `429 RATE_LIMITED` with a `Retry-After` header.
Login blocks after five failures in a 10-minute window and allows attempts
again after a 10-minute cooldown.
The login candidate lookup has a bounded database deadline and returns
`503 DEPENDENCY_TIMEOUT` without counting a credential failure on timeout.
Database pool connection waits are capped globally. State is process-local
and resets on container restart; it does not require a database migration.
Limits are fixed defaults in the API image.

This is intentionally a single-instance design. Production deploys exactly one
`api` service replica, so rate-limit counters are not synchronized through
PostgreSQL, Redis, or another centralized store. Do not scale the API above one
replica without first replacing or coordinating the in-memory counters. A
restart clears those counters; the independent NPM per-IP limit remains the
public edge control during an API restart.

The public proxy must additionally apply per-IP limits to public login
submissions. Configure method-aware limits for both `POST /login` (forwarded to
`osimi-ui:3000`) and `POST /api/auth/login` (forwarded to `osimi-api:3000`).
Keep ordinary `GET /login` page views unaffected. Derive the rate-limit key
from the proxy's trusted client address; do not trust a caller-supplied
`X-Forwarded-For` header. Where possible, also bound publicly reachable worker
control paths without throttling signed upload/download transfers. Internal
worker traffic that bypasses the proxy must remain usable.

Validate the generated proxy configuration before reloading and test POST,
GET, trailing-slash, query-string, and spoofed-forwarding-header cases. The
backend account limit and the proxy per-IP limit have different scopes and are
both required.

Before the production rate-limiting rollout, record the live verification in
`deploy/npm-rate-limit-verification.md`. That record must contain the active
rate and burst, trusted client-IP source, generated-config validation result,
and observed responses for both login paths.

## Backup

Before migrations or upgrades, back up PostgreSQL with the existing database operator workflow and snapshot or copy `STAGING_HOST_PATH`. The Compose project owns neither PostgreSQL data nor worker/archive storage.

## Rollback

1. Set `IMAGE_TAG` to the previously built tag in `deploy/.env`.
2. Run `docker compose --env-file deploy/.env -f deploy/compose.yaml up -d --no-build`.
3. Recheck API readiness and the login route.

Database migrations are forward-only. If a release requires database rollback, restore the pre-release PostgreSQL backup together with the matching application image and staging snapshot rather than editing `schema_migrations` manually.
