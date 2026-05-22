import { encodeCursor } from "../http/pagination.ts";
import {
  ConflictError,
  LockedError,
  NotFoundError,
  RevisionConflictError,
  UnprocessableEntityError,
} from "../http/errors.ts";
import type { AuthenticatedContext } from "../auth/guards.ts";
import { listArtifactsByObjectId, type ObjectArtifactRecord } from "../repos/object-repo.ts";
import {
  acquireObjectEditLock,
  findObjectEditById,
  listCuratedDocumentPages,
  listObjectEditEvents,
  releaseObjectEditLock,
  submitDocumentCuration,
  updateCuratedDocumentPages,
  updateObjectEditMetadata,
  type ObjectEditRecord,
} from "../repos/object-edit-repo.ts";
import {
  buildObjectArtifactStorageKey,
  createDownloadToken,
  resolveStagingPath,
} from "../storage/staging.ts";
import type {
  ObjectEditHistoryQuery,
  ObjectEditHistoryResponse,
  ObjectEditResponse,
  PatchObjectMetadataBody,
  PatchObjectMetadataResponse,
  PutDocumentCurationBody,
  PutDocumentCurationResponse,
  SubmitObjectCurationBody,
  SubmitObjectCurationResponse,
} from "../validation/object.ts";

interface DocumentEditPage {
  pageNumber: number;
  label: string | null;
  ocrTextArtifactId: string | null;
}

function isJsonObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function getMetadataDocumentPages(metadata: ObjectEditRecord["metadata"]): DocumentEditPage[] {
  const pages = metadata.pages;
  if (!Array.isArray(pages)) {
    return [];
  }

  return pages
    .map((entry) => {
      if (!isJsonObject(entry)) {
        return undefined;
      }

      const pageNumber = entry.page_number;
      if (typeof pageNumber !== "number" || !Number.isInteger(pageNumber) || pageNumber <= 0) {
        return undefined;
      }

      return {
        pageNumber,
        label: typeof entry.label === "string" ? entry.label : null,
        ocrTextArtifactId:
          typeof entry.ocr_text_artifact_id === "string" ? entry.ocr_text_artifact_id : null,
      };
    })
    .filter((page): page is DocumentEditPage => page !== undefined)
    .sort((left, right) => left.pageNumber - right.pageNumber);
}

function getMetadataPageCount(metadata: ObjectEditRecord["metadata"]): number | null {
  const value = metadata.page_count;
  return typeof value === "number" && Number.isInteger(value) && value > 0 ? value : null;
}

async function readArtifactText(
  artifact: ObjectArtifactRecord | undefined,
): Promise<string> {
  if (!artifact) {
    return "";
  }

  const file = Bun.file(resolveStagingPath(artifact.storageKey));
  if (!(await file.exists())) {
    return "";
  }

  return await file.text();
}

async function buildDocumentCurationPayload(
  record: ObjectEditRecord,
): Promise<Extract<ObjectEditResponse["curation_payload"], { kind: "document" }>> {
  const pages = getMetadataDocumentPages(record.metadata);
  const artifacts = await listArtifactsByObjectId({
    tenantId: record.tenantId,
    objectId: record.objectId,
  });
  const artifactById = new Map(artifacts.map((artifact) => [artifact.id, artifact]));
  const curatedPages = await listCuratedDocumentPages({
    tenantId: record.tenantId,
    objectId: record.objectId,
  });
  const curatedByPageNumber = new Map(
    curatedPages.map((page) => [page.pageNumber, page]),
  );

  const resolvedPages = await Promise.all(
    pages.map(async (page) => {
      const machineText = await readArtifactText(
        page.ocrTextArtifactId ? artifactById.get(page.ocrTextArtifactId) : undefined,
      );
      const curatedPage = curatedByPageNumber.get(page.pageNumber);

      return {
        page_number: page.pageNumber,
        label: page.label,
        machine_text: machineText,
        curated_text: curatedPage?.curatedText ?? null,
        status: curatedPage ? "edited" : "machine",
      } as const;
    }),
  );

  const machineOcrArtifact = artifacts.find((artifact) => artifact.kind === "ocr_text") ?? null;

  return {
    kind: "document",
    machine_ocr_artifact_id: machineOcrArtifact?.id ?? null,
    page_count: getMetadataPageCount(record.metadata),
    pages: resolvedPages,
  };
}

async function buildSubmittedDocumentText(record: ObjectEditRecord): Promise<string> {
  const payload = await buildDocumentCurationPayload(record);
  return payload.pages
    .map((page) => (page.curated_text ?? page.machine_text).replace(/\r\n/g, "\n"))
    .join("\n\f\n");
}

function mapObjectTypeToEditMediaType(
  type: ObjectEditRecord["type"],
): ObjectEditResponse["media_type"] {
  switch (type) {
    case "DOCUMENT":
      return "document";
    case "IMAGE":
      return "image";
    case "AUDIO":
      return "audio";
    case "VIDEO":
      return "video";
    default:
      return "other";
  }
}

async function serializeObjectEdit(record: ObjectEditRecord): Promise<ObjectEditResponse> {
  const mediaType = mapObjectTypeToEditMediaType(record.type);
  const curationPayload = mediaType === "document"
    ? await buildDocumentCurationPayload(record)
    : { kind: mediaType };

  return {
    object_id: record.objectId,
    media_type: mediaType,
    revision: record.revision,
    curation_state: record.curationState,
    lock: {
      locked: record.lockedBy !== null && record.lockedUntil !== null && record.lockedUntil > new Date(),
      locked_by: record.lockedBy,
      locked_until: record.lockedUntil?.toISOString() ?? null,
    },
    draft: record.editUpdatedAt && record.editUpdatedBy
      ? {
          updated_at: record.editUpdatedAt.toISOString(),
          updated_by: record.editUpdatedBy,
        }
      : null,
    metadata: {
      title: record.title,
      publication_date: record.publicationDate,
      date_precision: record.datePrecision,
      date_approximate: record.dateApproximate,
      language: record.languageCode,
      tags: record.tags,
      people: record.people,
      description: record.description,
    },
    rights: {
      access_level: record.accessLevel,
      rights_note: record.rightsNote,
      sensitivity_note: record.sensitivityNote,
    },
    capabilities: {
      can_edit_metadata: true,
      can_curate_text: mediaType === "document",
      can_submit_review: false,
    },
    curation_payload: curationPayload,
  };
}

export async function getObjectEditDetail(params: {
  auth: AuthenticatedContext;
  objectId: string;
}): Promise<ObjectEditResponse> {
  const record = await findObjectEditById({
    tenantId: params.auth.tenantId,
    objectId: params.objectId,
  });

  if (!record) {
    throw new NotFoundError(`Object '${params.objectId}' was not found.`);
  }

  const lockResult = await acquireObjectEditLock({
    tenantId: params.auth.tenantId,
    objectId: params.objectId,
    userId: params.auth.userId,
    durationMinutes: 60,
  });

  if (lockResult.status === "not_found") {
    throw new NotFoundError(`Object '${params.objectId}' was not found.`);
  }

  if (lockResult.status === "locked") {
    const response = await serializeObjectEdit(lockResult.record);
    response.capabilities.can_edit_metadata = false;
    response.capabilities.can_curate_text = false;
    response.capabilities.can_submit_review = false;
    return response;
  }

  const response = await serializeObjectEdit(lockResult.record);
  response.capabilities.can_edit_metadata =
    params.auth.role === "archiver" || params.auth.role === "admin";
  response.capabilities.can_curate_text =
    response.media_type === "document" &&
    (params.auth.role === "archiver" || params.auth.role === "admin");
  response.capabilities.can_submit_review = response.capabilities.can_curate_text;

  return response;
}

export async function patchObjectMetadataForTenant(params: {
  auth: AuthenticatedContext;
  objectId: string;
  body: PatchObjectMetadataBody;
}): Promise<PatchObjectMetadataResponse> {
  const lockRecord = await findObjectEditById({
    tenantId: params.auth.tenantId,
    objectId: params.objectId,
  });

  if (lockRecord && lockRecord.lockedBy && lockRecord.lockedUntil && lockRecord.lockedUntil > new Date()) {
    if (lockRecord.lockedBy !== params.auth.userId) {
      throw new LockedError("Object is currently being edited by another user.", {
        locked_by: lockRecord.lockedBy,
        locked_until: lockRecord.lockedUntil.toISOString(),
      });
    }
  }

  const result = await updateObjectEditMetadata({
    tenantId: params.auth.tenantId,
    objectId: params.objectId,
    actorUserId: params.auth.userId,
    revision: params.body.revision,
    title: params.body.metadata.title,
    publicationDate: params.body.metadata.publication_date,
    datePrecision: params.body.metadata.date_precision,
    dateApproximate: params.body.metadata.date_approximate,
    language: params.body.metadata.language,
    tags: params.body.metadata.tags,
    people: params.body.metadata.people,
    description: params.body.metadata.description,
    rightsNote: params.body.rights.rights_note,
    sensitivityNote: params.body.rights.sensitivity_note,
  });

  if (result.status === "not_found") {
    throw new NotFoundError(`Object '${params.objectId}' was not found.`);
  }

  if (result.status === "revision_conflict") {
    throw new RevisionConflictError("Object metadata revision is stale.", {
      latest_revision: result.latestRevision,
    });
  }

  return {
    object_id: result.record.objectId,
    revision: result.record.revision,
    curation_state: result.record.curationState,
    updated_at: (result.record.editUpdatedAt ?? result.record.updatedAt).toISOString(),
  };
}

export async function putDocumentCurationForTenant(params: {
  auth: AuthenticatedContext;
  objectId: string;
  body: PutDocumentCurationBody;
}): Promise<PutDocumentCurationResponse> {
  const lockRecord = await findObjectEditById({
    tenantId: params.auth.tenantId,
    objectId: params.objectId,
  });

  if (lockRecord && lockRecord.lockedBy && lockRecord.lockedUntil && lockRecord.lockedUntil > new Date()) {
    if (lockRecord.lockedBy !== params.auth.userId) {
      throw new LockedError("Object is currently being edited by another user.", {
        locked_by: lockRecord.lockedBy,
        locked_until: lockRecord.lockedUntil.toISOString(),
      });
    }
  }

  const result = await updateCuratedDocumentPages({
    tenantId: params.auth.tenantId,
    objectId: params.objectId,
    actorUserId: params.auth.userId,
    revision: params.body.revision,
    pages: params.body.pages.map((page) => ({
      pageNumber: page.page_number,
      curatedText: page.curated_text,
    })),
  });

  if (result.status === "not_found") {
    throw new NotFoundError(`Object '${params.objectId}' was not found.`);
  }

  if (result.status === "invalid_media_type") {
    throw new ConflictError(
      "Document OCR curation is only supported for document objects.",
      {
        code: "INVALID_MEDIA_TYPE_FOR_DOCUMENT_CURATION",
        object_id: params.objectId,
      },
    );
  }

  if (result.status === "revision_conflict") {
    throw new RevisionConflictError("Document curation revision is stale.", {
      latest_revision: result.latestRevision,
    });
  }

  if (result.status === "invalid_page_numbers") {
    throw new UnprocessableEntityError("Validation failed.", result.invalidPageNumbers.map((pageNumber) => ({
      path: "pages",
      code: "INVALID_PAGE_NUMBER",
      page_number: pageNumber,
    })));
  }

  return {
    object_id: result.record.objectId,
    revision: result.record.revision,
    updated_count: result.updatedCount,
    updated_at: (result.record.editUpdatedAt ?? result.record.updatedAt).toISOString(),
  };
}

export async function submitObjectCurationForTenant(params: {
  auth: AuthenticatedContext;
  objectId: string;
  body: SubmitObjectCurationBody;
}): Promise<SubmitObjectCurationResponse> {
  const record = await findObjectEditById({
    tenantId: params.auth.tenantId,
    objectId: params.objectId,
  });

  if (!record) {
    throw new NotFoundError(`Object '${params.objectId}' was not found.`);
  }

  if (record.type !== "DOCUMENT") {
    throw new ConflictError(
      "Submit is only supported for document OCR curation in the current implementation.",
      {
        code: "INVALID_MEDIA_TYPE_FOR_DOCUMENT_CURATION",
        object_id: params.objectId,
      },
    );
  }

  const lockRecord = await findObjectEditById({
    tenantId: params.auth.tenantId,
    objectId: params.objectId,
  });

  if (lockRecord && lockRecord.lockedBy && lockRecord.lockedUntil && lockRecord.lockedUntil > new Date()) {
    if (lockRecord.lockedBy !== params.auth.userId) {
      throw new LockedError("Object is currently being edited by another user.", {
        locked_by: lockRecord.lockedBy,
        locked_until: lockRecord.lockedUntil.toISOString(),
      });
    }
  }

  const documentPayload = await buildDocumentCurationPayload(record);
  if (documentPayload.pages.length === 0) {
    throw new ConflictError("Document page projection is unavailable for OCR submission.", {
      code: "PROJECTION_UNAVAILABLE",
      object_id: params.objectId,
    });
  }

  const submittedText = await buildSubmittedDocumentText(record);
  const requestId = crypto.randomUUID();
  const utcDay = new Date().toISOString().slice(0, 10).replace(/-/g, "");
  const nextRevision = record.revision + 1;
  const targetVersion = utcDay;
  const idempotencyKey = `${params.objectId}:ocr_curated:${utcDay}:vpsrev-${nextRevision}`;
  const storageKey = buildObjectArtifactStorageKey({
    tenantId: params.auth.tenantId,
    objectId: params.objectId,
    requestId,
    artifactKind: "ocr_text_curated_submit",
    variant: targetVersion,
    extension: "txt",
  });
  const filePath = resolveStagingPath(storageKey);
  await Bun.write(filePath, submittedText);
  const file = Bun.file(filePath);
  const sizeBytes = file.size;
  const downloadToken = createDownloadToken({
    ingestion_id: params.objectId,
    file_id: requestId,
    tenant_id: params.auth.tenantId,
    storage_key: storageKey,
    content_type: "text/plain; charset=utf-8",
    size_bytes: sizeBytes,
    expires_at: new Date(Date.now() + 60 * 60 * 1000).toISOString(),
  });

  const result = await submitDocumentCuration({
    tenantId: params.auth.tenantId,
    objectId: params.objectId,
    actorUserId: params.auth.userId,
    revision: params.body.revision,
    requestId,
    idempotencyKey,
    actionPayload: {
      object_id: params.objectId,
      curated_kind: "ocr_curated",
      target_version: targetVersion,
      source_ref: {
        type: "signed_download_url",
        url: `/api/worker/downloads/${downloadToken}`,
      },
      content_type: "text/plain",
      idempotency_key: idempotencyKey,
    },
    reviewNote: params.body.review_note,
  });

  if (result.status === "not_found") {
    throw new NotFoundError(`Object '${params.objectId}' was not found.`);
  }

  if (result.status === "invalid_media_type") {
    throw new ConflictError(
      "Submit is only supported for document OCR curation in the current implementation.",
      {
        code: "INVALID_MEDIA_TYPE_FOR_DOCUMENT_CURATION",
        object_id: params.objectId,
      },
    );
  }

  if (result.status === "revision_conflict") {
    throw new RevisionConflictError("Document curation revision is stale.", {
      latest_revision: result.latestRevision,
    });
  }

  return {
    object_id: result.record.objectId,
    revision: result.record.revision,
    curation_state: result.record.curationState,
    request: {
      id: result.request.id,
      action_type: result.request.actionType,
      status: result.request.status,
    },
    submitted_at: (result.record.editUpdatedAt ?? result.record.updatedAt).toISOString(),
    submitted_by: params.auth.userId,
  };
}

export async function releaseObjectEditLockForTenant(params: {
  auth: AuthenticatedContext;
  objectId: string;
}): Promise<{ object_id: string; released: boolean }> {
  const result = await releaseObjectEditLock({
    tenantId: params.auth.tenantId,
    objectId: params.objectId,
    userId: params.auth.userId,
  });

  if (result.status === "not_found") {
    throw new NotFoundError(`Object '${params.objectId}' was not found.`);
  }

  return {
    object_id: params.objectId,
    released: result.status === "released",
  };
}

export async function getObjectEditHistoryForTenant(params: {
  auth: AuthenticatedContext;
  objectId: string;
  query: ObjectEditHistoryQuery;
}): Promise<ObjectEditHistoryResponse> {
  const record = await findObjectEditById({
    tenantId: params.auth.tenantId,
    objectId: params.objectId,
  });

  if (!record) {
    throw new NotFoundError(`Object '${params.objectId}' was not found.`);
  }

  const result = await listObjectEditEvents({
    tenantId: params.auth.tenantId,
    objectId: params.objectId,
    limit: params.query.limit,
    cursorCreatedAt: params.query.cursor?.created_at,
    cursorEventId: params.query.cursor?.id,
  });

  const lastEvent = result.events.at(-1);

  return {
    object_id: params.objectId,
    events: result.events.map((event) => ({
      id: event.id,
      type: event.type,
      actor_user_id: event.actorUserId,
      at: event.createdAt.toISOString(),
      revision_before: event.revisionBefore,
      revision_after: event.revisionAfter,
      payload: event.payload,
    })),
    next_cursor:
      result.events.length === params.query.limit && lastEvent
        ? encodeCursor({
            created_at: lastEvent.createdAt.toISOString(),
            id: lastEvent.id,
          })
        : null,
  };
}
