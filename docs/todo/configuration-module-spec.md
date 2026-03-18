# Configuration Module Spec (v1)

## Purpose

Define a centralized configuration module for runtime-adjustable system behavior, starting with artifact retention and cache lifecycle policies.

This module must support:

- Safe defaults
- Strict validation
- Admin-only updates
- Auditable changes
- Runtime reads by services/jobs without code edits

## Scope (v1)

### Included

1. Artifact retention policy configuration
2. Admin API to read/update configuration
3. Persistent storage for effective config values
4. Runtime accessor used by services/jobs
5. Validation and guardrails for config updates

### Not Included (v1)

1. Per-tenant overrides (global config only)
2. Scheduled UI management (API only)
3. Full historical versioning beyond audit log
4. Worker-side config push (workers read behavior through backend APIs)

---

## Functional Requirements

## 1) Config Domains

### 1.1 Artifact Retention

- Derivative artifacts are cacheable and regenerable.
- Original bundled artifact is cacheable.
- System metadata artifacts are permanent.

### 1.2 Defaults (global)

- `derivatives_ttl_days`: `14`
- `original_ttl_days`: `30`
- `extend_ttl_on_access`: `true`
- `sweeper_interval_minutes`: `60`
- `permanent_artifact_kinds`:
  - `ingest_json`
  - `pipeline_json`
  - `catalog_json`

### 1.3 Matching Identity

Artifact presence check identity is:

- `object_id`
- `artifact_kind`
- `variant` (future, nullable for now)

---

## 2) Behavioral Rules

1. On file request:
   - Check `object_artifacts` for matching artifact identity.
   - If present and not expired => return available (no queue).
   - If missing/expired => queue retrieval request.

2. On artifact access/download:
   - If `extend_ttl_on_access = true`, extend `expires_at` by policy TTL from access time.

3. Sweeper:
   - Deletes only expired, non-permanent artifacts.
   - Never deletes permanent artifacts.
   - Must be idempotent and safe for repeated runs.

4. Regenerable policy:
   - All derivatives are regenerable.
   - Original bundle is treated as cacheable (not permanent) unless explicitly pinned in future phases.

---

## 3) Configuration API (Admin Only)

All endpoints require authenticated `admin` role.

### 3.1 Get Effective Config

`GET /api/admin/config`

Response shape:

```json
{
  "config": {
    "artifact_retention": {
      "derivatives_ttl_days": 14,
      "original_ttl_days": 30,
      "extend_ttl_on_access": true,
      "sweeper_interval_minutes": 60,
      "permanent_artifact_kinds": ["ingest_json", "pipeline_json", "catalog_json"]
    }
  }
}
```

### 3.2 Update Config (Partial)

`PATCH /api/admin/config`

Request shape (partial):

```json
{
  "artifact_retention": {
    "derivatives_ttl_days": 21,
    "original_ttl_days": 45,
    "extend_ttl_on_access": true,
    "sweeper_interval_minutes": 30
  }
}
```

Response: same as GET (effective config after validation + persist).

### 3.3 Validation Constraints

- `derivatives_ttl_days` integer, `>= 1`, `<= 3650`
- `original_ttl_days` integer, `>= 1`, `<= 3650`
- `sweeper_interval_minutes` integer, `>= 5`, `<= 1440`
- `extend_ttl_on_access` boolean
- `permanent_artifact_kinds` array of allowed artifact enum values
- Reject unknown keys (strict schema)
- Reject invalid enum values
- Optional guardrail: `original_ttl_days >= derivatives_ttl_days`

---

## 4) Storage Model

Introduce persistent config table for runtime settings.

Suggested table:

- `system_config`
  - `key` (text PK)
  - `value_json` (jsonb)
  - `updated_by` (uuid)
  - `updated_at` (timestamptz)

Audit table (recommended v1):

- `system_config_audit`
  - `id` (uuid PK)
  - `key`
  - `old_value_json`
  - `new_value_json`
  - `changed_by`
  - `changed_at`

Suggested key for v1:

- `artifact_retention`

---

## 5) Runtime Access Pattern

1. Read-on-demand accessor from config service/repo.
2. In-memory short TTL cache allowed (e.g. 30-60s) to reduce DB load.
3. Cache must invalidate on successful PATCH in same process.
4. If config read fails, fallback to safe defaults and emit warning.

---

## 6) Error Handling

- `400` for invalid payload/validation errors
- `401` unauthorized
- `403` non-admin caller
- `404` only if endpoint path invalid (not for config keys in v1 global model)
- `409` for conflicting update semantics (if optimistic locking added later)
- `500` for unexpected failures

Error responses follow existing typed error contract.

---

## 7) Security and Governance

- Admin-only write access.
- Read access:
  - v1: admin only (recommended)
  - optional: read-only exposure to internal services via runtime layer, not public API.
- Log all changes in audit table.
- Include request id/user id in structured logs.

---

## 8) Integration Points

1. Download request flow checks retention config when deciding availability/queue.
2. Artifact download endpoint updates `last_accessed_at` and optionally extends TTL.
3. Retention sweeper job uses configured TTL values.
4. API docs must reflect configurable behavior and default values.

---

## 9) Open Questions

1. Should `GET /api/admin/config` include only editable keys or full resolved config with defaults?
2. Should optimistic concurrency be required for PATCH (`version`/`etag`)?
3. Should `permanent_artifact_kinds` be editable in v1 or fixed constant list?

---

## 10) Acceptance Criteria

1. Admin can read and patch retention config successfully.
2. Invalid config payloads are rejected with clear field-level errors.
3. Config values persist across restarts.
4. Artifact access extends TTL when enabled.
5. Sweeper honors configured TTLs and never deletes permanent artifact kinds.
6. Audit trail records each config update with actor + before/after values.
