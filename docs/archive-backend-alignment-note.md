# Backend Alignment Note: Object Editing Integration with Archive System

**Date:** 2026-04-13  
**Status:** Accepted Direction  
**Scope:** Object editing backend implementation and archive-system integration

---

## Context

The UI team (`docs/object-editing-change-requirement.md`) requested a set of object editing endpoints covering metadata, OCR, transcript, and caption curation. Before committing to that contract, we audited the archive-system to understand what is actually feasible and what assumptions in the UI requirements do not match the archive architecture.

This document records the agreed integration direction between backend and archive-system, and defines what backend must still obtain from archive before specific curation features can be implemented.

This note is intentionally architecture-first. It does not define the final public API for object editing. Instead, it captures the non-negotiable archive-side constraints that future backend design must respect.

---

## Archive System Integration Model

Archive-system is a private, outbound-only, worker-driven system. It is **not** a directly callable read/write service for editing workflows.

The correct integration shape is:

- Archive worker **publishes** archive-origin data to VPS (via worker event flow)
- VPS **serves** the UI/editor from its own state
- VPS **enqueues** async work for archive when archive-side materialization is needed
- Archive worker **polls** and **processes** that work asynchronously

```
Worker -> [events/push] -> VPS (machine projections, availability)
VPS    -> [queue]       -> Worker (curation apply, async work)
Worker -> [result]      -> VPS (apply completion/failure)
```

This is consistent with `docs/architecture.md` and `docs/archive-system-integration.md`.

---

## Key Archive-Side Principles

### 1. Canonical editable unit is the file

From archive's perspective:
- Edits are **not** canonicalized as partial patches, page patches, or segment patches.
- The canonical persisted unit is the **versioned curated file**.
- Human edits should produce curated text files versioned over time.
- A practical model is **date-based version bumping** for curated outputs.

### 2. Page/segment structure is a derived editing view

- For scanned documents, page structure is useful for editing UX.
- For transcripts/captions, segment structure is useful for editing UX.
- These are **editor-facing projections**, not archive's canonical persistence model.

### 3. Archive stores curated outputs alongside machine-generated outputs

Example:
- `transcript.v1.txt` — machine-generated transcript (preserved)
- `transcript.curated.v2.txt` — human-curated transcript (versioned artifact)

Same pattern applies to OCR-derived text and captions.

### 4. Full file replacement is preferred over partial patch apply

- Archive does **not** want patch/merge semantics on partial file fragments.
- Apply model should **replace the full curated file content** for the active dated version.
- This avoids version conflict complexity.

---

## What This Means for Integration Design

### Still valid to ask archive

- What machine-derived text outputs does archive produce?
- What editor-facing projection shape can archive publish to VPS?
- What version/revision signal does archive publish with those projections?
- What curated file versioning rules does archive support?
- What async apply workflow can archive worker support?
- What happens when reprocessing occurs after human curation?

### Does not fit archive

- Direct VPS → archive read endpoints for machine projections
- Direct VPS → archive write/apply endpoints for curation
- Requirement that every editable caption/transcript unit must be a first-class archive DB record
- Requirement for archive to behave like a row-level collaborative editing datastore

---

## Specific Correction on Segment IDs

A blanket requirement for durable `caption_segment_id` or `transcript_segment_id` is **not aligned** with archive's current model.

For captions especially, archive is better modeled as:
- versioned caption files
- active/current caption version
- optional derived segment projection for editor use

If VPS needs segment-level editing UX, that segment structure can be part of the **projection/read model in VPS** without requiring archive to treat segment records as the canonical persisted storage model.

---

## Recommended Integration Direction

### 1. VPS serves editing UI from VPS-held data

VPS must serve the editor from VPS-held data. Archive-origin machine text, curated file metadata, and projection/version signals must be **pushed to VPS proactively** through worker flows, not fetched on demand from archive.

### 2. Archive publishes what exists

Archive should tell VPS what editable machine and curated outputs exist for an object. VPS can then ensure the minimal required editing resources are available on its side.

### 3. Archive persists curation as versioned curated files

Not as patch sets. Not as mutable segment rows.

### 4. Any archive-side apply is async

- VPS queues work
- Archive worker polls and applies it
- Archive reports completion/failure back

---

## Critical Separation

We must separate:

| Archive Persistence Model | VPS Editing/View Model |
|---|---|
| Versioned curated files | Page/segment/file projections used by the editor |
| File-based, versioned | Derived structures for editing UX |
| Source of truth for curated outputs | Source of truth for editor read model and optimistic revisioning |

This separation is critical. Without it, the integration starts assuming archive has internal persistence semantics it does not actually have.

---

## Implied Backend Responsibilities

Given the above, backend (VPS) is responsible for:

1. **Owning the editor-serving read model** — projecting machine/curated text into page/segment structures for the UI, derived from archive-published artifacts and VPS-stored curation state.

2. **Maintaining revision and conflict semantics** — optimistic concurrency, stale detection, and merge UX decisions live in VPS, not archive.

3. **Serving the curation workflow** — metadata editing, transcript/OCR curation writes, history/audit, and submit-for-review all operate against VPS-held state.

4. **Enqueuing async apply work** — when curated content needs to be materialized in archive, VPS enqueues the appropriate work request for the archive worker to process.

5. **Tracking stale state after reprocessing** — when archive publishes new machine projections, VPS must detect divergence from curated state and surface that to the UI appropriately.

---

## MVP Scope

Based on archive-system's current capabilities:

**In scope for v1:**
- Metadata editing (title, publication date, date precision, description, people, tags, rights notes)
- Optimistic concurrency and revision tracking
- Curation history/audit endpoint
- Submit-for-review workflow for VPS-managed editing state

**Deferred until archive contract confirmed:**
- Video caption curation (no caption pipeline/model exists in archive today)
- Full transcript curation (depends on stable projection publish + apply contract)
- Document OCR curation (depends on projection publish + apply contract)

---

## Required Archive Contracts (Before Full Curation Editing)

Backend cannot implement transcript/document/caption curation endpoints until archive provides the following. Each item is a **blocking requirement**.

The payloads below are **illustrative contract sketches**, not final route or event definitions. Their purpose is to make the required data shape concrete enough for backend and archive discussions.

### 1. Projection Publish Contract

Archive worker must publish to VPS (via event flow) for each object:

```json
{
  "object_id": "OBJ-...",
  "media_type": "audio",
  "projection_version": "v3",
  "generated_at": "2026-04-13T00:00:00Z",
  "artifacts": [
    {
      "kind": "transcript",
      "artifact_id": "...",
      "storage_key": "...",
      "content_type": "text/plain",
      "version": "v1",
      "is_curated": false
    },
    {
      "kind": "transcript_curated",
      "artifact_id": "...",
      "storage_key": "...",
      "content_type": "text/plain",
      "version": "v2",
      "is_curated": true
    }
  ]
}
```

Acceptance criteria:
- VPS receives this payload when archive produces or updates machine/curated outputs.
- `projection_version` changes whenever machine outputs are regenerated.
- VPS can build or refresh its editor-facing read model from the published data without requiring direct archive access.

### 2. Segment Projection Shape (Editor-Facing)

For transcript editing UX, archive does not need to publish segment records. Instead:

- Archive publishes transcript artifact files (machine + curated versions).
- VPS derives and maintains segment projection internally from those artifacts.
- VPS owns segment identity for editing purposes.
- Archive does **not** need to emit stable `segment_id` values.

If archive can additionally publish a derived segment projection (`start_ms`, `end_ms`, `speaker`, `text`) as a **convenience metadata artifact**, backend will use it. This is helpful, but not required for the archive persistence model.

### 3. Curation Apply Contract

Archive worker must support a new queue action:

**Request (VPS → worker queue):**

```json
{
  "action_type": "curation_apply",
  "target_type": "object",
  "target_id": "OBJ-...",
  "action_payload": {
    "curated_artifact_kind": "transcript_curated",
    "target_version": "v3",
    "curated_file_ref": "backend-defined reference to the curated file content to materialize"
  }
}
```

**Apply rules:**
- Full file replacement — worker materializes the requested curated file version as the active curated output.
- Idempotent by `(target_id, curated_artifact_kind, target_version)`.
- Worker does not need row-level patch or merge semantics.

**Result (worker → VPS):**

```json
{
  "status": "COMPLETED",
  "object_id": "OBJ-...",
  "applied_version": "v3",
  "artifact_id": "...",
  "storage_key": "..."
}
```

Or on failure:

```json
{
  "status": "FAILED",
  "object_id": "OBJ-...",
  "failure_reason": "...",
  "retryable": false
}
```

### 4. Reprocessing After Curation

Archive must define what happens when machine outputs are regenerated after human curation exists:

Options (archive chooses one):
- **Keep curated active** — new machine projection does not overwrite curated file; curator notified of divergence.
- **Auto-bump curated version** — curated file is preserved as historical version; new machine version becomes new base; curator reviews.
- **Mark stale** — curated file remains active but VPS marks it as potentially stale; explicit curator action required.

Backend needs one confirmed policy to implement correct stale-detection and notification UX.

At minimum, archive must publish enough version/change information for VPS to detect that machine-generated source material changed after a curated version was created.

### 5. Video Caption Readiness

Archive must explicitly declare:
- Whether caption pipeline exists today
- If not, expected availability date
- Data model if separate from transcript (file-based or structured)

---

## Open Questions for Archive Team

1. What machine-derived text artifact kinds does archive produce today? (transcript, OCR, caption — confirm each)
2. What is the naming/versioning convention for curated output files?
3. Does archive support `curation_apply` as a new action type in its queue processing?
4. What is the reprocessing-after-curation policy?
5. What is the timeline for video caption support?
6. What is the maximum artifact file size for curated text files?
7. Does archive publish any segment/projection metadata as a convenience artifact, or does VPS derive all segment structure internally?

---

## Practical Implications for Backend Work

Future assistants working in this repository should use this note as the baseline when planning object editing work.

- Do not assume direct VPS -> archive API calls are possible.
- Do not design transcript/caption editing around archive-owned row-level segment records unless archive explicitly adds that model later.
- Treat archive curated outputs as file-based and versioned.
- Treat VPS as the owner of the editor read model, optimistic revisioning, and user-facing conflict behavior.
- When proposing archive-side changes, ask for worker-published payloads and async queue actions, not synchronous archive endpoints.

---

## References

- `docs/architecture.md` — overall system interaction architecture
- `docs/archive-system-integration.md` — worker integration protocol
- `docs/object-editing-change-requirement.md` — UI requirements (target state, needs revision against this note)
- `docs/api-reference.md` — current VPS API surface
- `src/routes/objects.ts` — current object route implementations
- `src/services/object-service.ts` — current object service layer
- `src/repos/object-repo.ts` — current object persistence layer
