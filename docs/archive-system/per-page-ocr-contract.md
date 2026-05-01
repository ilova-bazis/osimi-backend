# Archive System Hand-Off: Per-Page OCR Artifact Contract

> **Document purpose:** This is a hand-off document for the archive-system development team. It defines the new data shapes and contracts the backend expects when the archive system publishes per-page OCR text artifacts for document objects.
>
> **Companion documents:**
> - `docs/api-reference.md` — full HTTP API reference for worker endpoints
> - `docs/archive-system/generic-requests.md` — archive request queue worker guide
> - `docs/archive-system/object-text-manifest.md` — object text manifest contract
>
> **Last updated:** 2026-04-21

---

## 1. What Changed

The backend now supports **per-page OCR text artifacts** for document editing. This means:

- The archive system can publish individual OCR text files per page (e.g., one `.txt` file per page).
- The backend will auto-fetch each page as a separate artifact.
- The edit view (`GET /api/objects/:id/edit`) will display `machine_text` per page, loaded from the corresponding per-page artifact.
- The existing **combined full-object OCR** artifact remains supported for backward compatibility.

### High-level flow

```
Archive OCR pipeline completes
    |
    v
Archive publishes available_files snapshot
    |
    v
Backend auto-queues artifact_fetch for:
    - each per-page OCR variant (page_0001, page_0002, ...)
    - one combined OCR variant (full_v1 or null)
    |
    v
Archive worker downloads each file from archive storage
    |
    v
Archive worker uploads each file to backend staging via presigned URL
    |
    v
Backend links completed artifacts to metadata.pages[]
```

---

## 2. Archive Contract: `available_files` Snapshot

### Endpoint

`PUT /api/internal/objects/:object_id/available-files`

Auth: `x-worker-auth-token`

### Per-page OCR entry shape

For each page, include a separate entry in the `files` array:

```json
{
  "archive_file_key": "objects/OBJ-20260421-000001/ocr/page_0001.txt",
  "artifact_kind": "ocr_text",
  "variant": "page_0001",
  "display_name": "OCR Text Page 1",
  "content_type": "text/plain",
  "size_bytes": 1234,
  "checksum_sha256": "a1b2c3...",
  "metadata": {
    "page_number": 1,
    "label": "1"
  },
  "is_available": true
}
```

#### Field rules

| Field | Required | Description |
|-------|----------|-------------|
| `archive_file_key` | Yes | Unique archive storage path for this page file |
| `artifact_kind` | Yes | Must be `"ocr_text"` |
| `variant` | Yes | Must match `page_0001`, `page_0002`, etc. Zero-padded to 4 digits |
| `display_name` | Yes | Human-readable name (e.g., `"OCR Text Page 1"`) |
| `content_type` | Recommended | Should be `"text/plain"` |
| `size_bytes` | Recommended | File size in bytes |
| `checksum_sha256` | Recommended | File checksum |
| `metadata.page_number` | Recommended | 1-based page number. Backend uses this as the preferred source of page identity |
| `metadata.label` | Optional | Page label string (e.g., `"i"`, `"Cover"`). If omitted, backend falls back to `String(page_number)` |
| `is_available` | Optional | Defaults to `true` |

### Combined full-object OCR entry shape

Publish a combined artifact **alongside** per-page entries (recommended for backward compatibility):

```json
{
  "archive_file_key": "objects/OBJ-20260421-000001/ocr/full_v1.txt",
  "artifact_kind": "ocr_text",
  "variant": "full_v1",
  "display_name": "Full OCR Text",
  "content_type": "text/plain",
  "size_bytes": 15000,
  "checksum_sha256": "d4e5f6...",
  "metadata": {
    "page_count": 12,
    "scope": "full"
  },
  "is_available": true
}
```

#### Field rules

| Field | Required | Description |
|-------|----------|-------------|
| `variant` | Yes | Use `"full_v1"` (recommended) or `null` |
| `metadata.page_count` | Recommended | Total page count. Backend uses this as a fallback if page count is not yet known |
| `metadata.scope` | Optional | `"full"` for documentation |

### Complete snapshot example

```json
{
  "files": [
    {
      "archive_file_key": "objects/OBJ-20260421-000001/ocr/page_0001.txt",
      "artifact_kind": "ocr_text",
      "variant": "page_0001",
      "display_name": "OCR Text Page 1",
      "content_type": "text/plain",
      "size_bytes": 1234,
      "metadata": {
        "page_number": 1,
        "label": "1"
      }
    },
    {
      "archive_file_key": "objects/OBJ-20260421-000001/ocr/page_0002.txt",
      "artifact_kind": "ocr_text",
      "variant": "page_0002",
      "display_name": "OCR Text Page 2",
      "content_type": "text/plain",
      "size_bytes": 1567,
      "metadata": {
        "page_number": 2,
        "label": "2"
      }
    },
    {
      "archive_file_key": "objects/OBJ-20260421-000001/ocr/page_0003.txt",
      "artifact_kind": "ocr_text",
      "variant": "page_0003",
      "display_name": "OCR Text Page 3",
      "content_type": "text/plain",
      "size_bytes": 890,
      "metadata": {
        "page_number": 3,
        "label": "3"
      }
    },
    {
      "archive_file_key": "objects/OBJ-20260421-000001/ocr/full_v1.txt",
      "artifact_kind": "ocr_text",
      "variant": "full_v1",
      "display_name": "Full OCR Text",
      "content_type": "text/plain",
      "size_bytes": 3691,
      "metadata": {
        "page_count": 3,
        "scope": "full"
      }
    },
    {
      "archive_file_key": "objects/OBJ-20260421-000001/thumbnails/thumb_v1.jpg",
      "artifact_kind": "thumbnail",
      "variant": "v1",
      "display_name": "Thumbnail",
      "content_type": "image/jpeg",
      "size_bytes": 45000
    }
  ]
}
```

---

## 3. What the Backend Does Automatically

### 3.1 Auto-request `artifact_fetch` per page

When the snapshot arrives, the backend:

1. **Upserts** all `available_files` entries.
2. **Auto-queues** `artifact_fetch` archive requests:
   - One request **per page variant** (`page_0001`, `page_0002`, ...).
   - One request for the **combined variant** (`full_v1` or `null`).
3. **Skips** auto-queue when:
   - For per-page: the specific `(ocr_text, page_0001)` artifact already exists, or an active request for that exact variant is already pending.
   - For combined: any combined OCR artifact already exists, or any active combined OCR request is pending.

**No new archive request type is needed.** Per-page OCR reuses the existing `artifact_fetch` action.

### 3.2 Update `metadata.pages` from snapshot

After syncing available files, the backend updates `objects.metadata.pages`:

- Creates page entries for any new `page_*` variants.
- Updates `label` if `metadata.label` changed.
- Preserves existing `image_artifact_id` and `ocr_text_artifact_id` values.
- Sets `metadata.page_count` to the number of pages.

### 3.3 Link artifacts on download completion

When the archive worker completes uploading a per-page artifact:

- Backend parses page number from `variant`.
- Updates `metadata.pages[n].ocr_text_artifact_id` to the newly created artifact ID.
- Creates the page entry defensively if it does not yet exist.

---

## 4. Archive Worker Processing

### No changes to `artifact_fetch` processing

The archive worker handles per-page `artifact_fetch` requests **exactly the same** as any other artifact fetch:

1. **Lease** the request from `POST /api/archive-requests/lease`.
2. **Download** the file from archive storage using `archive_file_key` (or internal path).
3. **Presign** upload via `POST /api/archive-requests/:id/artifacts/presign`.
4. **Upload** file bytes via `PUT /api/archive-requests/uploads/:token`.
5. **Complete** the request via `POST /api/archive-requests/:id/complete` with both `lease_token` and `upload_token`.

The `action_payload` for per-page requests looks like:

```json
{
  "available_file_id": "uuid-of-object_available_files-row",
  "artifact_kind": "ocr_text",
  "variant": "page_0001"
}
```

This is identical to existing `artifact_fetch` payloads except `variant` is a concrete page identifier instead of `null`.

---

## 5. Object Creation and Page Skeleton

### Backend creates page skeleton at ingestion time

When a `DOCUMENT` object is created from ingestion completion:

- Backend reads `ingestion_item_files` to find files with `page_number`.
- Pre-populates `metadata.pages` with:
  - `page_number`
  - `label` (from `logical_label` or `String(page_number)`)
  - `image_artifact_id: null`
  - `ocr_text_artifact_id: null`

**Archive system does not need to do anything special here.** The archive system only needs to publish the `available_files` snapshot with per-page OCR entries when OCR is ready. The backend will match them to the existing page skeleton by `page_number`.

---

## 6. Variant Naming Convention

### Per-page variants

- Format: `page_` + zero-padded 4-digit page number
- Examples: `page_0001`, `page_0002`, `page_0012`, `page_1234`
- Backend regex: `/^page_\d{4}$/`

### Combined variants

- Preferred: `full_v1`
- Legacy: `null` (still supported but `full_v1` is recommended for explicitness)
- Backend selection priority: `full_v1` > `null` > lexicographically lowest other `full_*`

---

## 7. Backward Compatibility

### What continues to work unchanged

- Publishing a **single** `ocr_text` entry with `variant = null` or `variant = "full_v1"` works exactly as before.
- The backend will still auto-fetch it and use it for the combined `preview_artifacts.ocr_text` and `viewer_payload.ocr_text_artifact_id`.
- All other artifact kinds (`thumbnail`, `web_version`, `original`, etc.) are unaffected.
- The `object_text_manifest` contract is unaffected.

### Migration path

1. **Phase 1 (now):** Archive system can start publishing per-page OCR entries alongside the existing combined entry. Backend will begin auto-fetching them.
2. **Phase 2 (later):** Once per-page artifacts are fully operational, archive system can optionally stop publishing the combined entry if desired. The backend viewer and curation submit logic may need adjustment if combined OCR is removed entirely.

---

## 8. Recommended Archive Implementation Checklist

- [ ] OCR pipeline generates one `.txt` file per page under `ocr/page_0001.txt`, `ocr/page_0002.txt`, etc.
- [ ] OCR pipeline also generates one combined file under `ocr/full_v1.txt` (recommended).
- [ ] Available-files snapshot includes per-page entries with `artifact_kind: "ocr_text"` and `variant: "page_0001"`, etc.
- [ ] Per-page entries include `metadata.page_number` (preferred) and optionally `metadata.label`.
- [ ] Available-files snapshot includes combined entry with `variant: "full_v1"` (or `null`).
- [ ] Archive worker `artifact_fetch` handler processes per-page requests identically to other artifact fetches.
- [ ] Archive worker downloads the per-page file and uploads it to backend staging.
- [ ] Archive worker completes the request normally.
- [ ] No changes needed to `object_text_manifest`, event payloads, or archive request types.

---

## 9. Questions for Archive Team

Please confirm the following:

1. Can the OCR pipeline produce per-page `.txt` files with the naming convention `page_0001.txt`, `page_0002.txt`, etc.?
2. Can the available-files snapshot include `metadata.page_number` and `metadata.label` for each per-page entry?
3. Will the archive system publish per-page and combined OCR in the same snapshot, or in separate snapshots?
4. Does the archive system need the backend to expose a page skeleton (e.g., page count/labels) before publishing OCR, or can the archive system derive page structure independently?

---

## 10. Example: Minimal Per-Page Snapshot

```json
{
  "files": [
    {
      "archive_file_key": "objects/OBJ-20260421-000001/ocr/page_0001.txt",
      "artifact_kind": "ocr_text",
      "variant": "page_0001",
      "display_name": "OCR Page 1",
      "content_type": "text/plain",
      "metadata": {
        "page_number": 1
      }
    },
    {
      "archive_file_key": "objects/OBJ-20260421-000001/ocr/page_0002.txt",
      "artifact_kind": "ocr_text",
      "variant": "page_0002",
      "display_name": "OCR Page 2",
      "content_type": "text/plain",
      "metadata": {
        "page_number": 2
      }
    },
    {
      "archive_file_key": "objects/OBJ-20260421-000001/ocr/full_v1.txt",
      "artifact_kind": "ocr_text",
      "variant": "full_v1",
      "display_name": "Full OCR",
      "content_type": "text/plain",
      "metadata": {
        "page_count": 2
      }
    }
  ]
}
```

This is the minimum the archive system needs to publish to enable per-page OCR editing in the backend.
