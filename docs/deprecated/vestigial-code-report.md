# Vestigial & Dead Code Report

Generated: 2026-05-22

This document catalogs all methods, exports, modules, and code paths that are no longer used (or are deprecated but unmarked) in the codebase. Each entry includes the precise file path and line number for review.

---

## 1. Entire Dead Module

The following file is **never imported** by any other module in the codebase. All 11 exports are dead code.

### `src/repos/object-download-request-repo.ts`

Superseded by `src/repos/archive-request-repo.ts`. The service layer was migrated to use archive-request-repo exclusively.

[OpenCode fact-check] Confirmed. I found no imports of `src/repos/object-download-request-repo.ts`; the listed symbols only appear in this module. This looks like a valid candidate for deletion after a final migration/test pass.

| Export | Line |
|--------|------|
| `findActiveObjectDownloadRequest` | 79 |
| `createObjectDownloadRequest` | 108 |
| `listObjectDownloadRequestsByObjectId` | 148 |
| `findObjectDownloadRequestById` | 170 |
| `leaseNextPendingObjectDownloadRequest` | 189 |
| `extendObjectDownloadRequestLease` | 252 |
| `releaseObjectDownloadRequestLease` | 289 |
| `completeObjectDownloadRequest` | 317 |
| `failObjectDownloadRequest` | 347 |
| `findActiveObjectDownloadRequestLeaseByToken` | 378 |
| `sweepExpiredObjectDownloadRequestLeases` | 404 |

---

## 2. Individually Unused Exports

Defined and exported, but never imported by any other file in `src/` or `tests/`.

### Repos

| Symbol | File | Line |
|--------|------|------|
| `listObjectsBySourceIngestion` | `src/repos/object-repo.ts` | 310 |
| `listArchiveRequestsByAction` | `src/repos/archive-request-repo.ts` | 249 |
| `updateIngestionItemStatus` | `src/repos/ingestion-item-repo.ts` | 427 |
| `findObjectTextManifestByObjectId` | `src/repos/object-text-manifest-repo.ts` | 43 |
| `deleteObjectTextManifest` | `src/repos/object-text-manifest-repo.ts` | 93 |

[OpenCode fact-check] Confirmed for the listed symbols. Note that `src/repos/object-text-manifest-repo.ts` as a module is not dead because `upsertObjectTextManifest` is imported by `src/services/object-text-manifest-service.ts`; only the two listed exports appear unused.

### DB

| Symbol | File | Line |
|--------|------|------|
| `qualifiedTableName` | `src/db/runtime.ts` | 30 |

[OpenCode fact-check] Confirmed. `qualifiedTableName` appears unused outside its own definition. Its local helper `validateIdentifier` is still used by it only, so removing `qualifiedTableName` may also make `validateIdentifier` removable unless another caller is added.

### HTTP

| Symbol | File | Line |
|--------|------|------|
| `AppError` (class) | `src/http/errors.ts` | 21 |

> `AppError` is the base class for all error types. It is never imported by name elsewhere, only referenced implicitly through `isAppError` and subclass imports.

[OpenCode fact-check] Counterpoint: `AppError` is not dead code. It is the runtime base class for all app error subclasses and is directly referenced by `isAppError()` and `createErrorBody()` inside `src/http/errors.ts`. At most, consider making the class non-exported if no external module needs to import the class/type by name.

### Auth

| Symbol | File | Line |
|--------|------|------|
| `requireTenantScope` | `src/auth/guards.ts` | 40 |

[OpenCode fact-check] Confirmed. `requireTenantScope` appears unused outside its own definition. It is safe to evaluate for removal if no near-term route code needs a tenant-only guard helper.

### CLI (entry points never imported)

| Symbol | File | Line |
|--------|------|------|
| `runUserManagement` | `src/cli/create-user.ts` | 356 |
| `runTenantManagement` | `src/cli/create-tenant.ts` | 224 |

> Both are only invoked via `import.meta.main` in their own files when run directly as CLI scripts.

[OpenCode fact-check] Counterpoint: these functions are not dead because package scripts invoke their files directly: `bun run create-user` and `bun run create-tenant`. They can be unexported if no tests/importers need them, but the functions themselves are active CLI entrypoint implementations.

---

## 3. Exported-but-Should-Be-Private Helpers

These functions are exported and used, but only as internal helpers within their own defining file. No external file imports them. They should be unexported or inlined.

| Symbol | File | Line | Used internally by |
|--------|------|------|--------------------|
| `parseLeaseToken` | `src/auth/worker-lease.ts` | 104 | `authorizeWorkerLeaseForIngestion` |
| `parseDownloadRequestLeaseToken` | `src/auth/worker-download-request.ts` | 119 | `authorizeWorkerLeaseForDownloadRequest` |
| `parseArchiveRequestLeaseToken` | `src/auth/worker-archive-request.ts` | 122 | `authorizeWorkerLeaseForArchiveRequest` |
| `resolveRequestId` | `src/http/context.ts` | 37 | `withRequestContext` |
| `stagingRootPath` | `src/storage/staging.ts` | 63 | `resolveStagingPath` |
| `withWorkerIngestionJsonBody` | `src/routes/middleware.ts` | 40 | `withWorkerAuthorizedLease` |
| `parseDashboardCursorPayload` | `src/validation/dashboard.ts` | 79 | `parseDashboardActivityQuery` |
| `authorizeArtifactFetchArchiveRequestLease` | `src/services/object-service.ts` | 2044 | Internal calls (lines 2062, 2274) |
| `INGESTION_STATUSES` | `src/domain/ingestions/state-machine.ts` | 1 | Type derivation only |

[OpenCode fact-check] Mostly confirmed. These are used only within their defining files based on current references. `INGESTION_STATUSES` is also used at runtime by `isIngestionStatus`, not just for type derivation; however, both are in the same module, so the "exported-but-should-be-private" conclusion still holds if no external consumer needs the constant.

---

## 4. Exports Used Only in Tests

These production exports have no consumers in `src/` — only test files reference them.

| Symbol | File | Line |
|--------|------|------|
| `requireNonEmptyStringField` | `src/validation/common.ts` | 56 |
| `requirePositiveIntField` | `src/validation/common.ts` | 69 |
| `requireUuid` | `src/validation/common.ts` | 82 |
| `DEFAULT_PAGE_LIMIT` | `src/http/pagination.ts` | 3 |
| `MAX_PAGE_LIMIT` | `src/http/pagination.ts` | 4 |
| `canTransitionIngestionStatus` | `src/domain/ingestions/state-machine.ts` | 48 |
| `isTerminalIngestionStatus` | `src/domain/ingestions/state-machine.ts` | 62 |
| `isIngestionStatus` | `src/domain/ingestions/state-machine.ts` | 44 |

[OpenCode fact-check] Needs correction. `DEFAULT_PAGE_LIMIT` and `MAX_PAGE_LIMIT` are production-used inside `src/http/pagination.ts`; only their exported status is test-only. `canTransitionIngestionStatus` is production-used by `assertIngestionStatusTransition`, so it is not test-only. `isTerminalIngestionStatus` does appear test-only. `isIngestionStatus` appears unused even by tests, not test-only.

[OpenCode fact-check] Confirmed for `requireNonEmptyStringField`, `requirePositiveIntField`, and `requireUuid`: current references are test-only. These look like old validation helpers, but remove them with their tests or repurpose them deliberately.

---

## 5. Deprecated Routes (Active, No `@deprecated` Annotation)

These routes are registered in `src/routes/objects.ts` and still function, but are documented as "deprecated compatibility routes" in `docs/api-reference.md`. They carry no deprecation marker in the source code.

[OpenCode fact-check] Confirmed. These are active registered routes and are documented as deprecated compatibility routes. Removing them is an API/product compatibility decision, not ordinary dead-code cleanup. Adding source comments or route metadata would make the deprecation status clearer.

All are registered in the `objectRoutes` array at `src/routes/objects.ts:1170-1200`.

| Route Variable | Route Path | File:Line |
|----------------|------------|-----------|
| `patchObjectRoute` | `PATCH /api/objects/:object_id` | 168 |
| `leaseObjectDownloadRequestRoute` | `POST /api/object-download-requests/lease` | 699 |
| `heartbeatObjectDownloadRequestRoute` | `POST /api/object-download-requests/:id/lease/heartbeat` | 711 |
| `releaseObjectDownloadRequestRoute` | `POST /api/object-download-requests/:id/lease/release` | 734 |
| `presignObjectDownloadRequestArtifactRoute` | `POST /api/object-download-requests/:id/artifacts/presign` | 757 |
| `completeObjectDownloadRequestRoute` | `POST /api/object-download-requests/:id/complete` | 782 |
| `failObjectDownloadRequestRoute` | `POST /api/object-download-requests/:id/fail` | 807 |
| `workerUploadObjectArtifactLegacyRoute` | `PUT /api/object-download-requests/uploads/:token` | 854 |

---

## 6. Service Functions Exclusively Serving Deprecated Routes

These `object-service.ts` functions are called **only** by the deprecated routes listed above. They would become dead code if those routes were removed.

| Symbol | File | Line | Called only by |
|--------|------|------|----------------|
| `leaseNextObjectDownloadRequest` | `src/services/object-service.ts` | 1841 | `leaseObjectDownloadRequestRoute` (objects.ts:704) |
| `heartbeatObjectDownloadRequestLease` | `src/services/object-service.ts` | 1894 | `heartbeatObjectDownloadRequestRoute` (objects.ts:726) |
| `releaseObjectDownloadRequestLeaseByToken` | `src/services/object-service.ts` | 1936 | `releaseObjectDownloadRequestRoute` (objects.ts:749) |
| `presignObjectArtifactUpload` | `src/services/object-service.ts` | 1987 | `presignObjectDownloadRequestArtifactRoute` (objects.ts:774) |
| `completeObjectDownloadRequestByWorker` | `src/services/object-service.ts` | 2296 | `completeObjectDownloadRequestRoute` (objects.ts:799) |
| `failObjectDownloadRequestByWorker` | `src/services/object-service.ts` | 2344 | `failObjectDownloadRequestRoute` (objects.ts:824) |

[OpenCode fact-check] Confirmed. Current references show these service functions are only called by the deprecated compatibility routes listed above. They should be removed only together with those routes, or kept while compatibility support remains active.

---

## 7. Dead / Commented-Out Code

| Location | Description |
|----------|-------------|
| `src/routes/objects.ts:502-507` | Commented-out duplicate call to `listObjectDownloadRequestsForTenant` inside `listObjectDownloadRequestsRoute` |
| `src/routes/objects.ts:492-508` | Useless `try/catch` block that catches and immediately re-throws within `listObjectDownloadRequestsRoute` |

[OpenCode fact-check] Confirmed. The `try/catch` adds no behavior and the commented duplicate call is stale. This is low-risk cleanup.

---

## 8. Vestigial Files

| File | Content |
|------|---------|
| `test.js` | 2 lines of meaningless JavaScript (`var OO = ["A","B","C"]; var b = "D";`). Not referenced by any build script or config. |
| `test.ts` | Empty file (0 meaningful lines). Not referenced by any build script or config. |

[OpenCode fact-check] Confirmed. Both root-level files look vestigial and are not referenced by `package.json` scripts.

---

## Summary Counts

| Category | Count |
|----------|-------|
| Dead module (entire file unused) | 1 (11 exports) |
| Individually unused exports | 9 |
| Exported-but-private helpers | 9 |
| Test-only production exports | 8 |
| Deprecated routes (no annotation) | 8 |
| Service functions for deprecated routes | 6 |
| Dead/commented-out code snippets | 2 |
| Vestigial files | 2 |

[OpenCode fact-check] Summary adjustment recommended: count `AppError` and the CLI functions separately from dead code. Also adjust the test-only category because some listed symbols are production-used internally but exported only for tests, while `isIngestionStatus` appears unused rather than test-only.
