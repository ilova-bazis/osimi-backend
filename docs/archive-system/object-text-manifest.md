# Object Text Manifest Contract (v1)

This document defines the `object_text_manifest` contract used to publish archive-origin text state to VPS.

## Purpose

- Give VPS a canonical, object-level view of text artifacts available for editing and display.
- Keep integration aligned with outbound-only worker architecture.
- Keep archive persistence file-native while publishing a stable VPS-facing contract.

## Architecture Alignment

- Archive worker is outbound-only; there are no inbound calls into archive.
- Worker publishes manifest data to VPS asynchronously.
- VPS serves UI/backend reads from VPS-held state.
- Archive canonical edit unit remains the versioned file.

## Scope

This v1 contract covers:

- object-level text artifact inventory
- active/current version per artifact kind
- object-level projection change token

This v1 contract does not cover:

- direct VPS read of archive filesystem
- partial/patch edit semantics
- stable segment IDs for transcript/captions
- caption pipeline guarantees
- inline large derived page/segment bodies

## Canonical Terms

- `object_text_manifest`: the published object-level text state contract.
- `projection_version`: opaque object-level change token; VPS treats as non-semantic.
- `kind`: semantic artifact type.
- `version`: archive-defined version string for that `kind`.
- `is_active`: whether this `kind + version` is currently active.

## VPS-Facing Payload Schema (Worker -> VPS)

```json
{
  "object_id": "OBJ-20260414-000001",
  "media_type": "document",
  "projection_version": "2026-04-14T12:45:10Z",
  "generated_at": "2026-04-14T12:45:10Z",
  "text_artifacts": [
    {
      "kind": "ocr_machine",
      "version": "v1",
      "is_active": true,
      "metadata": {
        "source": "machine",
        "content_type": "text/plain",
        "page_count": 12
      }
    }
  ]
}
```

### Top-level fields

- `object_id` (required, string)
  - format: archive object id (`OBJ-YYYYMMDD-XXXXXX`)
- `media_type` (required, string)
  - enum: `document|audio|video|photo|other`
- `projection_version` (required, string)
  - opaque object-level change token
  - must change when text artifact state relevant to VPS changes
- `generated_at` (required, string)
  - RFC3339 UTC timestamp
- `text_artifacts` (required, array, may be empty)

### `text_artifacts[]` fields

- `kind` (required, string)
- `version` (required, string)
- `is_active` (required, boolean)
- `metadata` (optional, object)

`metadata` is for small descriptive fields only (for example: `source`, `content_type`, counts, flags).

### `kind` enum

- implemented now:
  - `ocr_machine`
  - `ocr_curated`
  - `transcript_machine`
  - `transcript_curated`
- reserved for future phases:
  - `captions_machine`
  - `captions_curated`
  - `description_curated`
  - `translation_machine`
  - `translation_curated`

Note: archive may publish only kinds currently supported by implementation.

## Artifact Identity Rule

For VPS integration, artifact identity is:

- `kind + version`

No filesystem path is required in VPS-facing payload.

## Active Version Rule

- Manifest lists all known versions for a `kind`.
- At most one artifact for the same `kind` may have `is_active = true`.

## Archive-Local Representation

Archive maintains `meta/object_text_manifest.json` as the local source of truth for known text artifact versions and active markers.

It may include additional internal fields (for example local relative file paths) for worker file resolution.

Those archive-local fields are not part of the VPS-facing field-level contract.

## Example: Audio with machine + curated transcript

```json
{
  "object_id": "OBJ-20260414-000002",
  "media_type": "audio",
  "projection_version": "2026-04-14T14:00:00Z",
  "generated_at": "2026-04-14T14:00:00Z",
  "text_artifacts": [
    {
      "kind": "transcript_machine",
      "version": "v1",
      "is_active": false,
      "metadata": {
        "source": "machine",
        "content_type": "text/plain"
      }
    },
    {
      "kind": "transcript_curated",
      "version": "20260414",
      "is_active": true,
      "metadata": {
        "source": "curated",
        "content_type": "text/plain"
      }
    }
  ]
}
```

## Publication Triggers

Worker should (re)publish `object_text_manifest` when text state changes, including:

- OCR completion
- transcript completion
- curated apply completion
- active curated version change

## Validation Expectations

- Unknown fields should be ignored by VPS for forward compatibility.
- Unknown `kind` values may be accepted and stored, but not required to be rendered by VPS UI.
- Payload should be idempotent at object level by replacing current manifest state for `object_id`.

## Versioning and Compatibility

- This document defines v1 contract behavior.
- Future extensions (for example derived segment/page convenience projections) should be additive.
