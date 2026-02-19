# Access Level Model

This document defines object access policy and download authorization behavior.

## Scope

- Access policy is **object-only**.
- Ingestion records do **not** own or define policy.
- There is no owner/creator shortcut for object access.

## Policy Fields (Object)

- `access_level`: `private | family | public`
- `embargo_kind`: `none | timed | curation_state`
- `embargo_until`: nullable datetime (used when `embargo_kind = timed`)
- `embargo_curation_state`: nullable curation enum (used when `embargo_kind = curation_state`)
- `rights_note`: nullable text
- `sensitivity_note`: nullable text

## Access Level Semantics

- `public`
  - any authenticated tenant member is authorized.
- `family`
  - only users explicitly assigned to the object with sufficient assignment level are authorized.
- `private`
  - only users explicitly assigned to the object with `private` level are authorized.

## Explicit Assignment Model

Object access assignments are explicit; access is not inferred from object creation.

Recommended assignment structure:

- `object_id`
- `tenant_id`
- `user_id`
- `granted_level` (`family | private`)
- `created_at`
- `created_by`

Uniqueness:

- one assignment per `(object_id, user_id)`.

## Admin Override

- `admin` role can access everything regardless of assignment or `access_level`.

## Embargo Semantics

- `none`: no embargo gate.
- `timed`: embargo is active while `now < embargo_until`.
- `curation_state`: embargo is active while object curation state has not reached the configured required state.

## Authorization and Download Decision

Computed fields:

- `is_authorized`
- `is_deliverable`
- `can_download`
- `access_reason_code`

Decision formula:

`can_download = authorized(access_level, membership, assignment, role) AND not embargo_active AND artifact_available`

Where:

- `artifact_available` means object `availability_state === AVAILABLE`.

Reason codes:

- `OK`
- `FORBIDDEN_POLICY`
- `EMBARGO_ACTIVE`
- `RESTORE_REQUIRED`
- `RESTORE_IN_PROGRESS`
- `TEMP_UNAVAILABLE`

## API Direction

### Object detail (`GET /api/objects/:object_id`)

Return:

- policy fields (`access_level`, embargo fields, notes)
- decision fields (`is_authorized`, `is_deliverable`, `can_download`, `access_reason_code`)

### Object list (`GET /api/objects`)

Support filter by:

- `access_level`

`access_reason_code` is optional on list rows (detail/download are primary).

### Object policy updates

Object policy can be updated at any time by authorized roles.

### Object assignment management

Provide endpoints to:

- add/update object assignment
- remove object assignment
- list assignments for object

## Validation Rules

- `access_level` must be one of: `private`, `family`, `public`.
- `embargo_kind` must be one of: `none`, `timed`, `curation_state`.
- `embargo_kind = timed` requires `embargo_until`.
- `embargo_kind = curation_state` requires `embargo_curation_state`.
- `rights_note` and `sensitivity_note`, when present, must be non-empty trimmed strings.

## Non-goals (Current Phase)

- Unauthenticated public access.
- Per-artifact policy overrides.
- Ownership-derived access.
