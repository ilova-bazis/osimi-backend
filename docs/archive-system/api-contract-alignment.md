# VPS Event Contract Alignment Draft

This document proposes how archive-system and VPS should align the existing worker event contracts so object editing can be supported without overloading the event stream.

This is a review draft for confirmation with the VPS backend team.

## Purpose

- keep the current outbound-only worker architecture intact
- enrich existing worker events with small, useful object/text summary fields
- keep full text artifact state in `object_text_manifest`
- avoid turning event payloads into large editing payloads

## Core Split Of Responsibilities

Archive -> VPS worker communication should use three distinct contract types:

1. Event stream
- lifecycle and summary information
- small payloads
- out-of-order tolerant
- suitable for activity, readiness, and projection refresh signals

2. Object text manifest
- canonical VPS-facing text artifact inventory
- machine + curated text versions
- active version state
- editing-related text metadata

3. Available-files snapshot
- downloadable artifact inventory
- artifact fetch behavior
- not the canonical editing read model

## Alignment Principle

Events should answer:

- what changed?
- what completed?
- should VPS refresh object state?

`object_text_manifest` should answer:

- what text artifacts exist for this object?
- which versions are active?
- what machine/curated text state is currently available for editing?

## Current System Behavior

Today, current documented behavior is approximately:

- `INGESTION_COMPLETED`
  - drives object projection readiness (`processing_state = index_done`, `availability_state = AVAILABLE`)
  - can persist `payload.ingest_json`
- `OBJECT_CREATED`
  - persisted for audit/activity
- `PIPELINE_STEP_*`
  - persisted for audit/activity
- `ARTIFACT_CREATED`
  - persisted for audit/activity
- `object_text_manifest`
  - now exists as the richer text artifact publish contract

This draft proposes how to enrich the existing event contracts without duplicating the full manifest payload.

## Recommended Event Contract Split

### 1. `OBJECT_CREATED`

Role:

- lightweight object bootstrap event
- gives VPS enough object identity/classification context early

Recommended payload fields:

```json
{
  "object_id": "OBJ-20260417-000001",
  "media_type": "document",
  "item_kind": "scanned_document",
  "has_object_text_manifest": false,
  "has_machine_text": false,
  "has_curated_text": false
}
```

Field notes:

- `object_id`
  - required
- `media_type`
  - recommended
  - enum: `document|audio|video|photo|other`
- `item_kind`
  - recommended
  - preserves archive-side object classification details
- `has_object_text_manifest`
  - recommended
  - whether archive expects text artifact state to be published separately
- `has_machine_text`
  - recommended boolean summary
- `has_curated_text`
  - recommended boolean summary

What should not go here:

- OCR pages
- transcript segments
- full text artifact list
- file paths

### 2. `PIPELINE_STEP_COMPLETED`

Role:

- pipeline-specific completion summary
- best place for OCR/transcript summary metadata

#### OCR step payload

```json
{
  "step": "ocr",
  "object_id": "OBJ-20260417-000001",
  "media_type": "document",
  "version": "v1",
  "page_count": 12,
  "has_machine_text": true,
  "projection_version": "2026-04-17T12:00:00Z"
}
```

Recommended OCR fields:

- `step = "ocr"`
- `object_id`
- `media_type`
- `version`
- `page_count`
- `has_machine_text`
- `projection_version`

#### Transcript step payload

```json
{
  "step": "transcript",
  "object_id": "OBJ-20260417-000002",
  "media_type": "audio",
  "source_kind": "audio",
  "version": "v1",
  "segment_count": 42,
  "no_speech_detected": false,
  "has_machine_text": true,
  "projection_version": "2026-04-17T12:05:00Z"
}
```

Recommended transcript fields:

- `step = "transcript"`
- `object_id`
- `media_type`
- `source_kind`
  - `audio|video`
- `version`
- `segment_count`
- `no_speech_detected`
- `has_machine_text`
- `projection_version`

What should not go here:

- full transcript text
- transcript segment arrays
- page arrays
- curated file content

### 3. `INGESTION_COMPLETED`

Role:

- final object-ready summary event
- tells VPS object is available and whether richer text state should already exist

Recommended payload:

```json
{
  "step": "ingest",
  "item_count": 1,
  "successful_item_count": 1,
  "object_id": "OBJ-20260417-000002",
  "media_type": "audio",
  "has_object_text_manifest": true,
  "has_ocr_machine": false,
  "has_transcript_machine": true,
  "has_ocr_curated": false,
  "has_transcript_curated": false,
  "projection_version": "2026-04-17T12:05:00Z",
  "ingest_json": {}
}
```

Recommended fields:

- existing fields retained:
  - `step`
  - `item_count`
  - `successful_item_count`
  - `ingest_json`
- new summary fields:
  - `object_id`
  - `media_type`
  - `has_object_text_manifest`
  - `has_ocr_machine`
  - `has_transcript_machine`
  - `has_ocr_curated`
  - `has_transcript_curated`
  - `projection_version`

What should not go here:

- full `object_text_manifest`
- OCR/transcript full text
- full artifact version history

### 4. `ARTIFACT_CREATED`

Role:

- audit/activity signal for artifact creation
- optional support for history/feed style UI

Recommended payload:

```json
{
  "object_id": "OBJ-20260417-000002",
  "artifact_kind": "transcript_curated",
  "version": "20260417",
  "is_active": true,
  "source": "curated"
}
```

This event is optional for the editing workflow and should not replace `object_text_manifest`.

## What Must Stay Out Of Event Payloads

The following should not be added to the normal worker event stream:

- full OCR page text
- full transcript text
- transcript/caption segment arrays
- archive filesystem paths
- full `text_artifacts[]` inventories when they may grow over time
- curated file bodies
- segment IDs or per-segment editing references

Those belong either in:

- `object_text_manifest`
- or future optional derived convenience projections

## Relationship To `object_text_manifest`

VPS should treat `object_text_manifest` as the authoritative object-level text artifact read model.

That means:

- events provide refresh/readiness signals
- `object_text_manifest` provides current text artifact state

Current supported manifest kinds:

- `ocr_machine`
- `ocr_curated`
- `transcript_machine`
- `transcript_curated`

## Recommended VPS Handling

### On `OBJECT_CREATED`

- create/bootstrap lightweight object state
- persist media classification summary

### On `PIPELINE_STEP_COMPLETED`

- update activity timeline
- store pipeline completion summary
- optionally mark text state as refresh-needed

### On `INGESTION_COMPLETED`

- mark object available
- update top-level readiness state
- persist `ingest_json`
- trust that `object_text_manifest` will be or has been published separately

### On `object_text_manifest`

- replace current object text artifact view for that object
- drive editing/read model from this payload

## Proposed Minimal Confirmation Questions For VPS

Please confirm the following alignment points:

1. Event payloads should remain summary-oriented, not full text payloads.
2. `object_text_manifest` should be the authoritative text/editing artifact read model.
3. `OBJECT_CREATED` should be enriched with object/media bootstrap summary fields.
4. `PIPELINE_STEP_COMPLETED` should be enriched with OCR/transcript summary fields.
5. `INGESTION_COMPLETED` should include top-level text readiness flags and `projection_version`, but not full text data.
6. VPS should not expect segment identities or segment-level apply semantics from archive.

## Proposed Rollout Strategy

### Phase A

- confirm this contract split
- agree on exact event payload fields

### Phase B

- enrich `OBJECT_CREATED`
- enrich OCR/transcript `PIPELINE_STEP_COMPLETED`
- enrich `INGESTION_COMPLETED`

### Phase C

- VPS consumes enriched summaries and uses `object_text_manifest` as the editing read model

## Summary

The intended model is:

- events = lifecycle + small summary payloads
- `object_text_manifest` = text artifact read model
- curated apply = async full-file replacement

This keeps archive-system aligned with its architecture while giving VPS the richer state it needs for editing workflows.
