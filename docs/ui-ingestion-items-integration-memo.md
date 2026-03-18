# UI Integration Memo: Ingestion Items, Grouping, and Ordering

Audience: frontend/UI team integrating ingestion workflows.

This memo summarizes new ingestion APIs and expected UI behavior for:

- grouping files into logical archival items
- ordering items within an ingestion
- ordering files within each item
- editing item-level metadata (including dates)

---

## 1) Model Update

Backend now supports this structure:

```text
ingestion
  -> ingestion_items (ordered by item_index)
    -> ingestion_item_files (ordered by sort_order)
```

- One ingestion can have many items.
- Each item can have many files.
- Ordering is explicit and server-authoritative:
  - item order: `item_index`
  - file order (inside item): `sort_order`

---

## 2) New/Updated Endpoints for UI

All endpoints below use Bearer auth and tenant scoping from session.

### Item CRUD-ish

- `POST /api/ingestions/:id/items`
  - create an item
- `GET /api/ingestions/:id/items`
  - list items ordered by `item_index`
- `PATCH /api/ingestions/:id/items/:itemId`
  - merge-update item metadata

### Item order

- `PATCH /api/ingestions/:id/items/order`
  - full-set reorder of item positions

### File grouping + file order per item

- `POST /api/ingestions/:id/items/:itemId/files`
  - attach ingestion file to item with `sort_order`
- `GET /api/ingestions/:id/items/:itemId/files`
  - list item files ordered by `sort_order`
- `PATCH /api/ingestions/:id/items/:itemId/files/order`
  - full-set reorder for that item's files

Existing upload flow remains:

- `POST /api/ingestions/:id/files/presign`
- `PUT /api/uploads/:token`
- `POST /api/ingestions/:id/files/commit`

Note: file ordering is no longer a presign concern; it is managed via item-file APIs.

---

## 3) Required UI Behavior

### 3.1 Build and persist item grouping

After files exist in ingestion, UI should:

1. create item(s)
2. attach each file to exactly one item (recommended UX rule)
3. assign `sort_order` during attach

### 3.2 Reordering semantics

Both reorder endpoints use full-set semantics:

- item reorder payload must include exactly all current item IDs
- item-file reorder payload must include exactly all linked file IDs for that item

If list is partial/mismatched, backend returns conflict.

### 3.3 Mutability window

Item/file grouping and ordering updates are allowed only when ingestion is mutable:

- `DRAFT`, `UPLOADING`, `CANCELED`
- and no active lease

UI should disable edit/reorder controls once ingestion becomes non-mutable.

---

## 4) Metadata Patch Contract (`PATCH /items/:itemId`)

Patch uses merge semantics:

- only provided fields change
- unspecified fields remain as-is
- nested `summary` metadata is merged, not fully replaced

Supported patch fields:

- `classification_type`
- `item_kind`
- `language_code`
- `title` (string or `null`)
- `description` -> stored at `summary.classification.summary`
- `tags` -> stored at `summary.classification.tags` (normalized + deduped)
- `dates` -> stored at `summary.dates`

Date shape for both `published` and `created`:

```json
{
  "value": "YYYY | YYYY-MM | YYYY-MM-DD | null",
  "approximate": true,
  "confidence": "low | medium | high",
  "note": "string | null"
}
```

---

## 5) Recommended UI Sequence (Happy Path)

1. Create ingestion (`POST /api/ingestions`)
2. Presign/upload/commit files
3. Create items (`POST /api/ingestions/:id/items`)
4. Attach files to items with initial `sort_order`
5. Allow drag-drop reorder:
   - item reorder -> `PATCH /items/order`
   - page reorder in item -> `PATCH /items/:itemId/files/order`
6. Edit item metadata (`PATCH /items/:itemId`)
7. Submit ingestion (`POST /api/ingestions/:id/submit`)

---

## 6) Worker Lease Data Relevant to UI Expectations

Lease payload now includes, per downloadable file:

- `filename`
- `ingestion_item_id`
- `item_index`
- `sort_order`

Worker ordering rule is deterministic:

1. `item_index ASC NULLS LAST`
2. `sort_order ASC NULLS LAST`
3. `filename ASC`
4. `file_id ASC`

This means UI-configured grouping/order is propagated to worker processing.

---

## 7) Quick Payload Examples

Create item:

```json
{
  "item_index": 1,
  "title": "Letter from 1952",
  "classification_type": "letter",
  "item_kind": "scanned_document"
}
```

Attach file to item:

```json
{
  "ingestion_file_id": "<uuid>",
  "role": "page",
  "sort_order": 1,
  "page_number": 1,
  "is_primary": true
}
```

Reorder items:

```json
{
  "items": [
    { "ingestion_item_id": "<item-a>", "item_index": 1 },
    { "ingestion_item_id": "<item-b>", "item_index": 2 }
  ]
}
```

Reorder files in item:

```json
{
  "files": [
    { "ingestion_file_id": "<file-1>", "sort_order": 1 },
    { "ingestion_file_id": "<file-2>", "sort_order": 2 }
  ]
}
```

Metadata patch (merge):

```json
{
  "title": "Updated title",
  "description": "Updated description",
  "tags": ["magazine", "history"],
  "dates": {
    "published": {
      "value": "1960-05",
      "approximate": false,
      "confidence": "high",
      "note": null
    }
  }
}
```

---

## 8) Implementation Notes for UI Team

- Prefer optimistic UI for drag-drop, but always reconcile from server response.
- Treat `item_index` and `sort_order` as server-owned canonical order after each reorder call.
- Keep local validation for duplicate positions before submitting reorder payload.
- Surface conflict errors as "list changed, please refresh" when full-set reorder fails.
