# Object View Backend Contract

This document defines the proposed backend response contract for the media-first object view.

It is intended for frontend/backend review before implementation.

## Goal

The object detail response should let the frontend answer, without heuristics:

- what media type the object is
- what the primary media source is
- whether that source is available now, requestable, pending, restricted, or temporarily unavailable
- which active request applies to that primary source
- which preview artifacts are available independently of the primary source
- what structured payload should drive the viewer

## Endpoint Shape

Recommended approach: extend `GET /api/objects/:object_id` with a new additive `viewer` block while preserving the existing `object` payload.

This contract also assumes a dedicated browser-delivery endpoint for viewable artifacts:

- `GET /api/objects/:object_id/artifacts/:artifact_id/view`

The object-view response provides stable artifact identity. The `/view` endpoint turns that identity into a browser-consumable inline resource.

## Final JSON Shape

```json
{
  "object": {
    "id": "OBJ-20260213-ABC123",
    "object_id": "OBJ-20260213-ABC123",
    "thumbnail_artifact_id": "60000000-0000-4000-8000-000000000777",
    "tenant_id": "00000000-0000-0000-0000-000000000001",
    "type": "DOCUMENT",
    "title": "Document title",
    "language": "en",
    "tags": ["source:family_archive"],
    "metadata": {},
    "source_ingestion_id": "13dd3927-17be-4211-9a77-fdea3104a028",
    "source_batch_label": "batch-2026-02-13-001",
    "processing_state": "index_done",
    "curation_state": "reviewed",
    "availability_state": "AVAILABLE",
    "access_level": "private",
    "embargo_kind": "none",
    "embargo_until": null,
    "embargo_curation_state": null,
    "rights_note": null,
    "sensitivity_note": null,
    "created_at": "2026-02-13T20:22:29.993Z",
    "updated_at": "2026-02-14T08:01:00.000Z",
    "ingest_manifest": {
      "schema_version": "1.0"
    },
    "is_authorized": true,
    "is_deliverable": true,
    "can_download": true,
    "access_reason_code": "OK"
  },
  "viewer": {
    "media_type": "document",
    "primary_source": {
      "source_type": "access_copy",
      "artifact_kind": "pdf",
      "variant": null,
      "status": "available",
      "available_file_id": "70000000-0000-4000-8000-000000000001",
      "artifact_id": "60000000-0000-4000-8000-000000000099",
      "display_name": "Reading PDF",
      "content_type": "application/pdf",
      "size_bytes": 12345,
      "access_reason_code": "OK"
    },
    "active_request": null,
    "preview_artifacts": {
      "thumbnail": {
        "available": true,
        "artifact_id": "60000000-0000-4000-8000-000000000777",
        "content_type": "image/jpeg",
        "display_name": "Thumbnail",
        "metadata": {}
      },
      "poster": null,
      "ocr_text": {
        "available": true,
        "artifact_id": "60000000-0000-4000-8000-000000000778",
        "content_type": "text/plain",
        "display_name": "OCR Text",
        "metadata": {
          "page_count": 12
        }
      },
      "transcript": null,
      "captions": null
    },
    "viewer_payload": {
      "kind": "document",
      "artifact_id": "60000000-0000-4000-8000-000000000099",
      "content_type": "application/pdf",
      "ocr_text_artifact_id": "60000000-0000-4000-8000-000000000778",
      "page_count": 12
    }
  }
}
```

## Type Schema

```ts
type AccessReasonCode =
  | "OK"
  | "FORBIDDEN_POLICY"
  | "EMBARGO_ACTIVE"
  | "RESTORE_REQUIRED"
  | "RESTORE_IN_PROGRESS"
  | "TEMP_UNAVAILABLE";

type ObjectViewerMediaType = "document" | "image" | "audio" | "video";

type ObjectViewerSourceType =
  | "original"
  | "access_copy"
  | "stream"
  | "preview"
  | "other";

type ObjectViewerPrimarySourceStatus =
  | "available"
  | "request_required"
  | "request_pending"
  | "restricted"
  | "temporarily_unavailable";

type ObjectViewerActiveRequest = {
  id: string;
  action_type: "artifact_fetch";
  status: "PENDING" | "PROCESSING";
  created_at: string;
  updated_at: string;
};

type ObjectViewerArtifactRef = {
  available: true;
  artifact_id: string;
  content_type: string | null;
  display_name: string | null;
  metadata: Record<string, unknown>;
};

type ObjectViewerPreviewArtifacts = {
  thumbnail: ObjectViewerArtifactRef | null;
  poster: ObjectViewerArtifactRef | null;
  ocr_text: ObjectViewerArtifactRef | null;
  transcript: ObjectViewerArtifactRef | null;
  captions: ObjectViewerArtifactRef | null;
};

type ObjectViewerPrimarySource = {
  source_type: ObjectViewerSourceType;
  artifact_kind:
    | "original"
    | "preview"
    | "pdf"
    | "ocr_text"
    | "thumbnail"
    | "web_version"
    | "transcript"
    | "other";
  variant: string | null;
  status: ObjectViewerPrimarySourceStatus;
  available_file_id: string | null;
  artifact_id: string | null;
  display_name: string | null;
  content_type: string | null;
  size_bytes: number | null;
  access_reason_code: AccessReasonCode;
};

type DocumentViewerPage = {
  page_number: number;
  label: string | null;
  image_artifact_id: string | null;
  ocr_text_artifact_id: string | null;
};

type DocumentViewerPayload = {
  kind: "document";
  artifact_id: string | null;
  content_type: string | null;
  ocr_text_artifact_id: string | null;
  page_count: number | null;
  pages?: DocumentViewerPage[];
};

type ImageViewerPayload = {
  kind: "image";
  artifact_id: string | null;
  content_type: string | null;
  width: number | null;
  height: number | null;
};

type AudioViewerPayload = {
  kind: "audio";
  artifact_id: string | null;
  content_type: string | null;
  transcript_artifact_id: string | null;
  duration_seconds: number | null;
};

type VideoViewerPayload = {
  kind: "video";
  artifact_id: string | null;
  content_type: string | null;
  poster_artifact_id: string | null;
  transcript_artifact_id: string | null;
  captions_artifact_id: string | null;
  duration_seconds: number | null;
};

type ObjectViewerPayload =
  | DocumentViewerPayload
  | ImageViewerPayload
  | AudioViewerPayload
  | VideoViewerPayload;

type ObjectViewer = {
  media_type: ObjectViewerMediaType;
  primary_source: ObjectViewerPrimarySource;
  active_request: ObjectViewerActiveRequest | null;
  preview_artifacts: ObjectViewerPreviewArtifacts;
  viewer_payload: ObjectViewerPayload;
};
```

## Field Semantics

### `viewer.media_type`

Canonical viewer media type.

Allowed values:

- `document`
- `image`
- `audio`
- `video`

Recommended mapping from current backend object type:

- `DOCUMENT -> document`
- `IMAGE -> image`
- `AUDIO -> audio`
- `VIDEO -> video`

`GENERIC` should not use this contract until a viewer strategy is defined.

### `viewer.primary_source`

The single canonical source the frontend should use for the main media canvas.

This is the key field that removes frontend heuristics.

#### `source_type`

Viewer-facing abstraction for the selected source.

- `original`
- `access_copy`
- `stream`
- `preview`
- `other`

#### `artifact_kind`

Backend-facing concrete artifact identity.

This preserves alignment with current backend/file infrastructure while allowing `source_type` to stay product-oriented.

Frontend behavior must key off `source_type + status`, not `artifact_kind`.

`artifact_kind` exists so backend and worker systems can preserve concrete storage and fetch identity. It must not be treated as the primary UI decision field.

#### `status`

- `available`
  - the primary source is ready to render now
- `request_required`
  - the user is allowed to access the media, but the primary source is not materialized yet and must be requested
- `request_pending`
  - a relevant active request already exists for this primary source
- `restricted`
  - the user cannot access the primary source because of policy or embargo
- `temporarily_unavailable`
  - the source is expected but not currently deliverable for operational reasons

#### `access_reason_code`

Must reuse the existing backend access reason enum:

- `OK`
- `FORBIDDEN_POLICY`
- `EMBARGO_ACTIVE`
- `RESTORE_REQUIRED`
- `RESTORE_IN_PROGRESS`
- `TEMP_UNAVAILABLE`

Recommended mapping:

- `available -> OK`
- `request_required -> RESTORE_REQUIRED`
- `request_pending -> RESTORE_IN_PROGRESS`
- `restricted -> FORBIDDEN_POLICY | EMBARGO_ACTIVE`
- `temporarily_unavailable -> TEMP_UNAVAILABLE`

### `viewer.active_request`

Relevant active request for `viewer.primary_source`.

Rules:

- return `null` when no relevant active request exists
- only include active request states
- do not include completed or failed history here
- request history, if needed, should stay in request list endpoints

### `viewer.preview_artifacts`

Preview/supporting artifacts that may be shown even when the primary source is not available.

Each field is either:

- `null` when absent
- an artifact reference object when present

Current fields:

- `thumbnail`
- `poster`
- `ocr_text`
- `transcript`
- `captions`

Notes:

- `thumbnail` is the generic small visual preview
- `poster` is the larger visual poster for video or media shells
- `ocr_text` is structured as an artifact reference; richer OCR content belongs in `viewer_payload`
- `transcript` is a transcript artifact reference
- `captions` is a caption/subtitle artifact reference; cue data should not be inlined in object detail unless intentionally small and normalized

### `viewer.viewer_payload`

Render-oriented payload for the current media type.

This should contain the minimum structured data needed to power the viewer without another heuristic join.

## Artifact Resolution

`artifact_id` in this contract is a stable backend identifier, not a browser-consumable URL.

The resolution model is:

1. object detail returns `artifact_id` references in `viewer.primary_source`, `viewer.preview_artifacts`, and `viewer.viewer_payload`
2. frontend decides what to render from `source_type + status`
3. when the frontend needs the actual browser-consumable bytes for an available artifact, it resolves that artifact through the artifact delivery endpoint

Recommended endpoint:

- `GET /api/objects/:object_id/artifacts/:artifact_id/view`

Existing related endpoint:

- `GET /api/objects/:object_id/artifacts/:artifact_id/download`

Rules:

- use `/view` for inline document viewing, image rendering, audio playback, and video playback
- use `/download` only for explicit file download behavior
- frontend must not derive storage paths or direct URLs from `artifact_id`
- if `viewer.primary_source.status = available`, the referenced primary artifact must be resolvable through `/view`
- if a preview artifact is present and marked available, it should also be resolvable through `/view`

Artifact resolution flow is:

1. backend determines `viewer.media_type`
2. backend selects the canonical `viewer.primary_source`
3. frontend decides canvas behavior from `viewer.primary_source.source_type` and `viewer.primary_source.status`
4. if `viewer.primary_source.status = available`, frontend uses `viewer.viewer_payload` as the render contract for the main viewer and resolves renderable artifacts through `GET /api/objects/:object_id/artifacts/:artifact_id/view`
5. if `viewer.primary_source.status = request_required` or `request_pending`, frontend shows the request gate in the media canvas and may still render any `viewer.preview_artifacts`
6. `artifact_kind` may be used for diagnostics or backend-aware UI text, but not as the source-selection switch

`artifact_id` is not itself a browser resource. It is a stable backend identity that must be resolved through the `/view` endpoint for inline viewing, listening, or watching.

## Media-Specific Payload Rules

### Document

`viewer_payload.document` must include page-level data when the backend can resolve a multi-page representation. This is required for a document viewer that supports page navigation without frontend heuristics.

Minimum rule:

- include `page_count` whenever known
- include `pages` when page-level rendering or sequencing is known
- omit `pages` only when the backend truly has no page-level structure yet

Recommended shape:

```json
{
  "kind": "document",
  "artifact_id": "artifact-id-or-null",
  "content_type": "application/pdf",
  "ocr_text_artifact_id": "artifact-id-or-null",
  "page_count": 12,
  "pages": [
    {
      "page_number": 1,
      "label": "1",
      "image_artifact_id": null,
      "ocr_text_artifact_id": null
    }
  ]
}
```

### Image

```json
{
  "kind": "image",
  "artifact_id": "artifact-id-or-null",
  "content_type": "image/jpeg",
  "width": 2400,
  "height": 1600
}
```

### Audio

For audio, `artifact_id` in `viewer_payload` must represent a browser-playable source when `viewer.primary_source.status = available`.

That means:

- the referenced artifact should be suitable for direct browser playback
- if only a preservation/original file exists and it is not reliably browser-playable, backend should not present it as an available playable source
- in that case backend should choose a different source, require a request, or mark the source temporarily unavailable
- the frontend should fetch or assign the media element source via `GET /api/objects/:object_id/artifacts/:artifact_id/view`

```json
{
  "kind": "audio",
  "artifact_id": "artifact-id-or-null",
  "content_type": "audio/mpeg",
  "transcript_artifact_id": "artifact-id-or-null",
  "duration_seconds": 321
}
```

### Video

For video, `artifact_id` in `viewer_payload` must represent a browser-playable source when `viewer.primary_source.status = available`.

That means:

- the referenced artifact should be suitable for direct browser playback
- if only a preservation/original file exists and it is not reliably browser-playable, backend should not present it as an available playable source
- in that case backend should choose a different source, require a request, or mark the source temporarily unavailable
- the frontend should fetch or assign the media element source via `GET /api/objects/:object_id/artifacts/:artifact_id/view`

```json
{
  "kind": "video",
  "artifact_id": "artifact-id-or-null",
  "content_type": "video/mp4",
  "poster_artifact_id": "artifact-id-or-null",
  "transcript_artifact_id": "artifact-id-or-null",
  "captions_artifact_id": "artifact-id-or-null",
  "duration_seconds": 642
}
```

## Selection Rules

These rules define the backend default for choosing the canonical primary source.

### Document

Preferred order:

1. `pdf`
2. `web_version`
3. `original`
4. `preview`
5. `other`

Recommended `source_type` mapping:

- `pdf -> access_copy`
- `web_version -> access_copy`
- `original -> original`
- `preview -> preview`

### Image

Preferred order:

1. `web_version`
2. `preview`
3. `original`
4. `other`

Recommended `source_type` mapping:

- `web_version -> access_copy`
- `preview -> preview`
- `original -> original`

### Audio

Preferred order:

1. `web_version`
2. `original`
3. `other`

Recommended `source_type` mapping:

- `web_version -> stream`
- `original -> original`

### Video

Preferred order:

1. `web_version`
2. `original`
3. `preview`
4. `other`

Recommended `source_type` mapping:

- `web_version -> stream`
- `original -> original`
- `preview -> preview`

## Resolution Rules

The backend must resolve viewer state in the following order:

1. compute object-level authorization and deliverability using existing access-policy logic
2. derive `viewer.media_type`
3. select the canonical primary source candidate by media-type selection rules
4. look for an already materialized artifact matching that primary source candidate
5. if no artifact exists, look for a matching requestable available-file candidate
6. if no artifact exists, look for an active matching `artifact_fetch` request
7. derive `viewer.primary_source.status`
8. populate `viewer.active_request` only when step 6 finds an active relevant request
9. populate `viewer.preview_artifacts` independently of whether the primary source is available
10. populate `viewer.viewer_payload` with the render contract for the selected media type

Matching rules:

- artifact match identity is `(object_id, artifact_kind, variant)`
- active request match identity is the same primary-source identity for `artifact_fetch`
- available-file match should resolve to the same canonical primary-source identity whenever possible

## View Artifact Delivery Endpoint

### Endpoint

`GET /api/objects/:object_id/artifacts/:artifact_id/view`

### Purpose

Return a browser-consumable inline representation of an object artifact.

This endpoint exists because `artifact_id` is a stable backend identifier, not a browser URL. The frontend should use this endpoint whenever the viewer needs to render an available artifact inline.

### Relationship To Existing Download Endpoint

- `/view` is for browser viewing, listening, and watching
- `/download` is for explicit file download
- the same artifact may support both endpoints
- `viewer_payload` artifact references are expected to resolve through `/view` when the corresponding source is `available`

### Access Rules

- auth and tenant scoping should match existing artifact download behavior
- object access policy still applies
- `viewer.primary_source.status = available` implies the referenced primary artifact is eligible for `/view`
- preview artifacts included in `viewer.preview_artifacts` should also be eligible for `/view`

### Success Behavior

- return `200 OK` with the artifact body
- return `Content-Type` based on artifact content type
- return `Content-Length` when known
- return `Content-Disposition: inline`
- audio and video responses should support byte-range delivery when runtime/storage support is available

### Viewability Rules

An artifact is viewable only when it is suitable for direct browser consumption for its intended use.

Examples:

- documents: inline PDF or page images
- images: browser-renderable image content types
- audio: browser-playable audio formats/codecs
- video: browser-playable video formats/codecs

If an artifact exists but is not suitable for direct browser consumption, it must not be exposed as an `available` viewer source.

### Error Behavior

- `404 NOT_FOUND` when object or artifact does not exist in tenant scope
- `400 BAD_REQUEST` for invalid path params
- `401 UNAUTHORIZED` for missing or invalid auth
- `403 FORBIDDEN` when authenticated role is not allowed
- `409 CONFLICT` when the artifact exists but is not viewable inline for browser use

### Frontend Rule

- frontend should treat `artifact_id` as stable identity only
- frontend should obtain browser-consumable content through `/view`
- frontend should not derive storage URLs or guess direct file paths

### When primary source is materialized as an artifact

- set `artifact_id`
- set `status = available`
- set `access_reason_code = OK`
- ensure the artifact is resolvable through `/view`

### When source is not materialized but can be fetched from available files

- set `available_file_id`
- set `artifact_id = null`
- set `status = request_required` when no active request exists
- set `status = request_pending` when relevant active request exists

### When user is not allowed to access the source

- set `status = restricted`
- set `access_reason_code = FORBIDDEN_POLICY` or `EMBARGO_ACTIVE`

### When object is operationally unavailable

- set `status = temporarily_unavailable`
- set `access_reason_code = TEMP_UNAVAILABLE`

## Nullability Rules

- `viewer.active_request` is `null` when no relevant active request exists
- `viewer.primary_source.available_file_id` is `null` when no requestable available-file source exists
- `viewer.primary_source.artifact_id` is `null` when no materialized artifact exists
- each `viewer.preview_artifacts.*` field is `null` when that artifact is absent
- `viewer.viewer_payload` must still be present even when the primary source is not yet available; in that case it should carry nullable references

## Non-Goals For This Contract Revision

- exposing direct signed URLs in object detail
- embedding large OCR bodies or caption cue arrays inline by default
- exposing request history inside `viewer`
- defining a viewer contract for `GENERIC` objects

## Open Follow-Ups

- decide whether caption/subtitle data needs a dedicated backend artifact kind beyond `transcript`
- decide whether `source_ingestion_item_id` or equivalent sequencing metadata should be exposed to help multi-page/multi-side rendering

## Archive-System Dependency

To support full document viewer payloads without frontend heuristics, the archive-system should provide page structure metadata for viewer-capable document sources.

Requested fields:

- `page_count`
- `pages[]` in canonical order

Each `pages[]` entry should include:

- `page_number`
- `label` when available
- `image_artifact_id` when a page-level renderable artifact exists
- `ocr_text_artifact_id` when page-level OCR mapping exists

Recommended payload shape:

```json
{
  "page_count": 12,
  "pages": [
    {
      "page_number": 1,
      "label": "1",
      "image_artifact_id": null,
      "ocr_text_artifact_id": null
    }
  ]
}
```

Requirements:

- page order must be explicit and stable
- page numbering must not require filename-based inference
- OCR-to-page association must be explicit when available
- `page_count` should be provided even when only the top-level document artifact is renderable
