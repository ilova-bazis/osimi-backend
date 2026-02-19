# Objects Access and Availability Model

This document captures decisions and direction for object status modeling across:

- Archive System (private, authoritative object processing state)
- VPS Control Plane/API (public-facing integration layer)
- UI (user-facing list/detail/download behavior)

## Why This Exists

The previous single `status` approach mixed different concerns:

- Processing progress (`ingesting`, `ocr_done`, etc.)
- Human curation state (`needs_review`, `reviewed`, etc.)
- VPS availability (whether artifacts are currently servable)
- Access policy (who is allowed to access the object)

These are separate dimensions and should not be overloaded into one enum.

## Source of Truth by Dimension

1) Processing state (Archive System)
- Source: `objects.processing_state`
- Describes pipeline progression in archive system.

2) Curation state (Archive System)
- Source: `objects.curation_state`
- Describes human review workflow state.

3) Availability state (VPS)
- Source: VPS object projection/runtime state
- Describes whether artifacts are currently available to serve from VPS.

4) Access policy (VPS/domain policy)
- Source: object access policy fields (`access_level`, embargo, etc.)
- Describes who is allowed to access object content.

## Archive-System States (Existing)

### processing_state

- `queued`
- `ingesting`
- `ingested`
- `derivatives_running`
- `derivatives_done`
- `ocr_running`
- `ocr_done`
- `index_running`
- `index_done`
- `processing_failed`
- `processing_skipped`

### curation_state

- `needs_review`
- `review_in_progress`
- `reviewed`
- `curation_failed`

## VPS Availability State (Proposed)

- `AVAILABLE`
  - VPS can serve requested artifacts now.
- `ARCHIVED`
  - Intentionally not served from VPS (policy/intended state), data retained in archive system.
- `RESTORE_PENDING`
  - User requested availability; queued for retrieval/sync.
- `RESTORING`
  - Retrieval/sync from archive system in progress.
- `UNAVAILABLE`
  - Expected to be available but currently not servable due to operational issue.

Notes:
- `ARCHIVED` is intentional policy state.
- `UNAVAILABLE` is operational/problem state.
- `RESTORE_PENDING`/`RESTORING` provide user-visible transition when requesting access.

## Access Policy (Proposed)

Recommended policy field:

- `access_level`: `private | family | public`

Optional policy fields:

- `embargo_kind` (`none | timed | curation_state`)
- `embargo_until` (nullable datetime, for `timed`)
- `embargo_curation_state` (nullable curation state, for `curation_state`)
- `rights_note` (nullable)
- `sensitivity_note` (nullable)

## Access Decision Model

Do not use a single boolean-only outcome.

Compute and expose:

- `is_authorized`: policy/role/tenant/embargo check result
- `is_deliverable`: availability check result (`availability_state === AVAILABLE`)
- `can_download`: `is_authorized && is_deliverable`

And include a reason code.

### access_reason_code (Proposed)

- `OK`
- `FORBIDDEN_POLICY`
- `EMBARGO_ACTIVE`
- `RESTORE_REQUIRED`
- `RESTORE_IN_PROGRESS`
- `TEMP_UNAVAILABLE`

This allows UI to distinguish:

- "Not allowed" vs
- "Allowed but restore required" vs
- "Allowed but temporarily unavailable".

## API Contract Direction

For object list/detail payloads, expose separate fields instead of one overloaded status.

Suggested list/detail fields:

- `processing_state`
- `curation_state`
- `availability_state`
- `access_level`
- `embargo_until` (optional)
- `access_reason_code` (optional, usually computed per requester)

Keep list row guarantees from `docs/api-reference.md` and extend with the fields above.

## UI Behavior Direction

Objects list/detail should present separate indicators:

- Processing badge (`processing_state`)
- Curation badge (`curation_state`)
- Availability badge (`availability_state`)
- Access badge (`access_level`)

Action behavior examples:

- `RESTORE_REQUIRED` => show "Request access" / "Restore" action
- `RESTORE_IN_PROGRESS` => show progress/pending label
- `FORBIDDEN_POLICY` => disable access actions with policy message

## Transition Examples

1) Archived object requested by authorized user
- `ARCHIVED` -> `RESTORE_PENDING` -> `RESTORING` -> `AVAILABLE`

2) Restore fails
- `RESTORING` -> `UNAVAILABLE`

3) Policy archive action
- `AVAILABLE` -> `ARCHIVED`

## Implementation Notes

- Keep archive-system processing/curation enums as-is.
- Introduce VPS availability and access fields in API projection layer.
- Do not coerce availability into processing or curation states.
- Keep authorization and deliverability checks explicit in API responses where feasible.

## Open Questions

1) Keep both `RESTORE_PENDING` and `RESTORING`, or collapse into one transitional state?
2) Should `access_reason_code` be returned on all object list rows or detail/download endpoints only?

Resolved for this phase:

- `public` means authenticated tenant members only.
- `family` and `private` require explicit object assignment.
- `admin` has override access to all objects.
