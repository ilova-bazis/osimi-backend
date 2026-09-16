# Deployment Secrets

Create these extensionless files on the VPS in this directory, or set `SECRETS_DIR` in `deploy/.env` to another absolute directory:

- `database_url`: PostgreSQL URL using the database container/service hostname reachable on `POSTGRES_NETWORK`.
- `upload_signing_secret`: Random value of at least 32 characters.
- `lease_signing_secret`: Independent random value of at least 32 characters.
- `worker_auth_token`: The exact token configured on the existing external worker.

The files are ignored by Git. Local Docker Compose bind-mounts file secrets without changing host ownership, so each file must be owned by the runtime UID/GID and not be readable by other users:

```bash
sudo chown 10001:10001 deploy/secrets/database_url deploy/secrets/upload_signing_secret deploy/secrets/lease_signing_secret deploy/secrets/worker_auth_token
sudo chmod 0400 deploy/secrets/database_url deploy/secrets/upload_signing_secret deploy/secrets/lease_signing_secret deploy/secrets/worker_auth_token
```

Docker Compose mounts them read-only under `/run/secrets`; the backend entrypoint exports them only inside the API or migration process. If `SECRETS_DIR` points elsewhere, apply the same ownership and modes there.
