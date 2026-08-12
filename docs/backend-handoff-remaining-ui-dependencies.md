# Backend Handoff: Remaining UI Dependencies

## Purpose

This document tracks the remaining backend change blocking closure of:

- `UM-91`: ingestion-item people PATCH support

Ranged inline-media delivery is complete in root `UM-4`; the backend and UI proxy now preserve the range contract.

## 1. Ranged Artifact Viewing (`UM-86`, Complete)

### Current State

`GET /api/objects/:object_id/artifacts/:artifact_id/view` supports full-file and single byte-range responses from:

- `src/routes/objects.ts`
- `src/services/object-service.ts` (`viewObjectArtifactForTenant`)

The service streams full files or `Bun.file(...).slice(...)` range responses, honors `If-Range`, and returns strong ETag and Last-Modified validators.

### Implemented Contract

Support byte ranges on the existing inline-view endpoint without changing access control or artifact eligibility.

Request headers:

- `Range`
- `If-Range`

Response behavior:

| Request | Response |
| --- | --- |
| No `Range` | `200 OK`, full stream |
| Valid single byte range | `206 Partial Content`, selected stream |
| Unsatisfiable range | `416 Range Not Satisfiable` |
| `If-Range` does not match the current validator | `200 OK`, full stream, including an otherwise unsatisfiable range |

Support one range only:

- `bytes=start-end`
- `bytes=start-`
- `bytes=-suffixLength`

For multiple ranges, ignore `Range` and return the normal `200` full response. Multipart range responses are out of scope.

### Required Headers

For successful `200` and `206` responses:

- `Content-Type`
- `Content-Disposition: inline`
- `Content-Length`
- `Accept-Ranges: bytes`
- `ETag`
- `Last-Modified`

For `206` additionally:

- `Content-Range: bytes start-end/total`

For `416`:

- `Content-Range: bytes */total`
- `Accept-Ranges: bytes`
- current validators where available

Use the immutable artifact identity for a stable strong ETag, for example an artifact-ID-derived quoted value. Use `object_artifacts.created_at` as `Last-Modified`.

### Implementation Notes

- Keep current authorization, access projection, viewable-artifact checks, and MIME restrictions unchanged.
- Parse ranges after artifact lookup so total size comes from `artifact.sizeBytes`.
- Use `Bun.file(...).slice(start, endExclusive)` or equivalent streamed storage slicing. Do not buffer a file into memory.
- Preserve the current full-response behavior for non-range requests.
- Update backend API documentation for this endpoint.

### Tests

Extend `tests/integration/http/object-routes.test.ts` to cover:

- full `200` response with range capability and validators
- `Range: bytes=0-2` returns `206`, `Content-Range`, correct length and body
- open-ended and suffix ranges
- unsatisfiable range returns `416` with `bytes */total`
- matching `If-Range` returns `206`
- stale `If-Range` returns full `200`
- authorization and non-viewable-artifact behavior remain unchanged

### UI Completion

`src/routes/objects/[objectId]/artifacts/[artifactId]/view/+server.ts` forwards `Range` and `If-Range`, preserves `200`, `206`, and `416`, and relays range and validator headers while retaining authenticated `private, no-store` behavior.

## 2. Ingestion Item People PATCH (`UM-91`)

### Current State

The UI already accepts and conditionally sends:

```json
{ "people": ["Ada Lovelace"] }
```

The backend accepts `people` and maps it to `summary.people.mentioned` while the existing metadata merger preserves sibling fields.

Relevant files:

- `src/validation/ingestion.ts`
- `src/services/ingestion-item-service.ts`
- `tests/integration/http/ingestion-routes.test.ts`

### Required Contract

Extend:

`PATCH /api/ingestions/:id/items/:itemId`

with an optional `people` field:

```ts
people?: string[]
```

Semantics:

- Omitted `people`: preserve `summary.people` unchanged.
- Present non-empty array: replace only `summary.people.mentioned`.
- Present empty array: clear `summary.people.mentioned`.
- Preserve other people fields, including `summary.people.authors`, `contributors`, and `subjects`.
- Preserve unrelated `summary` fields.

Canonical storage mapping:

```ts
summary.people.mentioned = normalizedPeople
```

Normalization:

- trim entries
- reject blank entries
- remove duplicates while preserving first-seen order

### Implementation Notes

- Add `people` to `updateIngestionItemBodySchema` as an optional array of trimmed, non-empty strings.
- Update `buildSummaryPatch` in `src/services/ingestion-item-service.ts` to produce:

```ts
patch.people = { mentioned: normalizedPeople };
```

- Continue using `mergeJsonObjects`, so sibling people and unrelated summary fields survive.
- No repository schema or route change is required; `updateIngestionItem` already persists a merged `summary`.
- Update API documentation for the existing PATCH endpoint.

### Tests

Extend the metadata PATCH coverage in `tests/integration/http/ingestion-routes.test.ts`:

- people-only PATCH writes `summary.people.mentioned`
- people PATCH preserves existing `authors`, `contributors`, and `subjects`
- empty array clears `mentioned`
- omitted `people` preserves existing `mentioned`
- combined title, description, tags, date, and people patch merges all values
- duplicate names normalize deterministically
- blank names and unknown fields are rejected
- immutable ingestion and authorization behavior remain unchanged

### UI Follow-Up

After backend deployment:

- enable `peopleEditable` in both `ObjectMetadataPanel` usages in `src/routes/ingestion/[batchId]/setup/+page.svelte`
- add an end-to-end browser test for people editing and persistence
- resume and close `UM-91`

## Deployment Order

1. Deploy backend people PATCH support.
2. Deploy backend ranged artifact viewing.
3. Notify the UI team with endpoint version or commit and test evidence.
4. UI resumes `UM-91`, then `UM-86`.
5. Run UI integration tests against the deployed backend and close parents `UM-87` and `UM-78`.
