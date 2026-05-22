import { mkdir, rm } from "node:fs/promises";
import { dirname } from "node:path";

import {
  ConflictError,
  NotFoundError,
  ValidationError,
} from "../http/errors.ts";
import { decodeCursor, encodeCursor } from "../http/pagination.ts";
import {
  claimNextPendingIngestionPreview,
  createIngestion,
  createIngestionFile,
  deleteIngestionFile,
  deleteIngestion,
  findIngestionById,
  findClaimedIngestionPreview,
  findIngestionFileById,
  listIngestionFiles,
  listIngestions,
  markIngestionFilePreviewFailed,
  markIngestionFilePreviewPending,
  markIngestionFilePreviewReady,
  markIngestionFilePreviewUnsupported,
  markIngestionFileUploaded,
  updateIngestionDetails,
  updateIngestionFileProcessingOverrides,
  updateIngestionPreviewUpload,
  type ClaimedIngestionPreviewRecord,
  updateIngestionStatus,
  type IngestionFileRecord,
  type IngestionRecord,
} from "../repos/ingestion-repo.ts";
import {
  assertIngestionStatusTransition,
  InvalidIngestionTransitionError,
  type IngestionStatus,
} from "../domain/ingestions/state-machine.ts";
import type { AuthenticatedContext } from "../auth/guards.ts";
import {
  buildIngestionPreviewStorageKey,
  buildStagingStorageKey,
  createDownloadToken,
  createUploadToken,
  parseUploadToken,
  resolveStagingPath,
} from "../storage/staging.ts";
import { parseIngestionSummary } from "../validation/catalog.ts";
import {
  EXTENSION_ALLOWLIST,
  MEDIA_KINDS,
  MIME_ALIASES,
  MIME_ALLOWLIST,
  type MediaKind,
  getMediaKindForMime,
  normalizeMime,
} from "../domain/ingestions/capabilities.ts";
import {
  getAllowedItemKindsForClassificationType,
  getAllowedMediaKindsForItemKind,
  isClassificationTypeCompatibleWithItemKind,
  isItemKindCompatibleWithMediaKind,
} from "../domain/ingestions/compatibility.ts";
import { hasActiveLease } from "../repos/lease-repo.ts";
import {
  type CancelIngestionResponse,
  type CommitUploadedFileResponse,
  type CommitUploadedFileBody,
  type CreateIngestionBody,
  type CreateIngestionDraftResponse,
  type CreatePresignedUploadBody,
  type CreatePresignedUploadResponse,
  type DeleteIngestionFileResponse,
  type DeleteIngestionResponse,
  type GetIngestionResponse,
  type IngestionCapabilitiesResponse,
  type IngestItemKind,
  type IngestionDto,
  type IngestionFileDto,
  type IngestionClassificationType,
  type IngestionListQuery,
  type IngestionListResult,
  type JsonObject,
  type RestoreIngestionResponse,
  type RetryIngestionResponse,
  type SubmitIngestionResponse,
  type UpdateIngestionFileOverridesResponse,
  type UpdateIngestionFileOverridesBody,
  type UploadFileBySignedTokenResponse,
  type UpdateIngestionBody,
  type UpdateIngestionResponse,
  type WorkerClaimIngestionPreviewResponse,
  type WorkerCompleteIngestionPreviewBody,
  type WorkerCompleteIngestionPreviewResponse,
  type WorkerFailIngestionPreviewBody,
  type WorkerFailIngestionPreviewResponse,
  type WorkerPresignIngestionPreviewUploadBody,
  type WorkerPresignIngestionPreviewUploadResponse,
  parseIngestionCursorPayload,
  parseIngestionFileProcessingOverrides,
  parseJsonObject,
} from "../validation/ingestion.ts";

const SHA256_PATTERN = /^[a-f0-9]{64}$/i;
const ONE_HOUR_MS = 60 * 60 * 1000;
const PREVIEW_CLAIM_TIMEOUT_MINUTES = 15;
const PREVIEW_DOWNLOAD_TTL_MS = 5 * 60 * 1000;
const MAX_INGESTION_PREVIEW_SIZE_BYTES = 5 * 1024 * 1024;
const MAX_INGESTION_PREVIEW_DIMENSION_PIXELS = 2048;
const INGESTION_PREVIEW_SOURCE_KINDS = new Set<MediaKind>(["image", "video"]);
const INGESTION_PREVIEW_OUTPUT_CONTENT_TYPES = new Set([
  "image/jpeg",
  "image/png",
  "image/webp",
  "image/gif",
  "image/avif",
]);

interface CursorPayload {
  created_at: string;
  id: string;
}

function normalizeSha256(value: string): string {
  const normalized = value.trim().toLowerCase();

  if (!SHA256_PATTERN.test(normalized)) {
    throw new ValidationError(
      "Field 'checksum_sha256' must be a valid SHA-256 hex string.",
    );
  }

  return normalized;
}

function requireMediaKind(contentType: string): MediaKind {
  const normalized = normalizeMime(contentType);
  const kind = getMediaKindForMime(normalized);

  if (!kind) {
    throw new ValidationError("Unsupported content type for ingestion files.", {
      content_type: normalized,
    });
  }

  return kind;
}

function assertClassificationTypeCompatibleWithItemKind(params: {
  classificationType: IngestionClassificationType;
  itemKind: IngestItemKind;
}): void {
  if (
    isClassificationTypeCompatibleWithItemKind({
      classificationType: params.classificationType,
      itemKind: params.itemKind,
    })
  ) {
    return;
  }

  throw new ConflictError(
    `Classification type '${params.classificationType}' is incompatible with item kind '${params.itemKind}'.`,
    {
      classification_type: params.classificationType,
      item_kind: params.itemKind,
      allowed_item_kinds: [
        ...getAllowedItemKindsForClassificationType(params.classificationType),
      ],
    },
  );
}

function assertItemKindCompatibleWithMediaKind(params: {
  itemKind: IngestItemKind;
  mediaKind: MediaKind;
}): void {
  if (
    isItemKindCompatibleWithMediaKind({
      itemKind: params.itemKind,
      mediaKind: params.mediaKind,
    })
  ) {
    return;
  }

  throw new ConflictError(
    `Item kind '${params.itemKind}' is incompatible with uploaded ${params.mediaKind} files.`,
    {
      item_kind: params.itemKind,
      actual_media_kind: params.mediaKind,
      allowed_media_kinds: [...getAllowedMediaKindsForItemKind(params.itemKind)],
    },
  );
}

function mapTransitionError(error: unknown): never {
  if (error instanceof InvalidIngestionTransitionError) {
    throw new ConflictError("Ingestion status transition is not allowed.", {
      from: error.from,
      to: error.to,
    });
  }

  throw error;
}

function requireIngestion(
  record: IngestionRecord | undefined,
  ingestionId: string,
): IngestionRecord {
  if (!record) {
    throw new NotFoundError(`Ingestion '${ingestionId}' was not found.`);
  }

  return record;
}

function serializeIngestion(record: IngestionRecord): IngestionDto {
  const summary = parseIngestionSummary(record.summary);

  return {
    id: record.id,
    batch_label: record.batchLabel,
    tenant_id: record.tenantId,
    status: record.status,
    created_by: record.createdBy,
    schema_version: record.schemaVersion,
    classification_type: record.classificationType,
    item_kind: record.itemKind,
    language_code: record.languageCode,
    pipeline_preset: record.pipelinePreset,
    access_level: record.accessLevel,
    embargo_until: record.embargoUntil
      ? record.embargoUntil.toISOString()
      : null,
    rights_note: record.rightsNote ?? null,
    sensitivity_note: record.sensitivityNote ?? null,
    summary,
    error_summary: parseJsonObject(record.errorSummary),
    created_at: record.createdAt.toISOString(),
    updated_at: record.updatedAt.toISOString(),
  };
}

function serializeFile(record: IngestionFileRecord): IngestionFileDto {
  const previewStatus = serializePreviewStatus(record);

  return {
    id: record.id,
    ingestion_id: record.ingestionId,
    filename: record.filename,
    content_type: record.contentType,
    size_bytes: record.sizeBytes,
    storage_key: record.storageKey,
    status: record.status,
    checksum_sha256: record.checksumSha256 ?? null,
    preview: {
      status: previewStatus,
      content_type: record.previewContentType ?? null,
      size_bytes: record.previewSizeBytes ?? null,
      width: record.previewWidth ?? null,
      height: record.previewHeight ?? null,
      url:
        previewStatus === "ready"
          ? `/api/ingestions/${record.ingestionId}/files/${record.id}/preview`
          : null,
      error: record.previewError ?? null,
    },
    processing_overrides: parseIngestionFileProcessingOverrides(
      record.processingOverrides,
    ),
    error: parseJsonObject(record.error),
    created_at: record.createdAt.toISOString(),
    updated_at: record.updatedAt.toISOString(),
  };
}

function getIngestionPreviewSourceKind(contentType: string): MediaKind | undefined {
  const normalized = normalizeMime(contentType);
  const kind = getMediaKindForMime(normalized);

  return kind && INGESTION_PREVIEW_SOURCE_KINDS.has(kind) ? kind : undefined;
}

function supportsIngestionPreview(contentType: string): boolean {
  return getIngestionPreviewSourceKind(contentType) !== undefined;
}

function serializePreviewStatus(
  record: IngestionFileRecord,
): IngestionFileDto["preview"]["status"] {
  if (record.previewStatus === "processing") {
    return "pending";
  }

  return record.previewStatus ??
    (supportsIngestionPreview(record.contentType) ? "pending" : "unsupported");
}

function previewExtensionFromContentType(contentType: string): string {
  const normalized = normalizeMime(contentType);

  if (normalized === "image/jpeg") {
    return "jpg";
  }

  if (normalized === "image/png") {
    return "png";
  }

  if (normalized === "image/webp") {
    return "webp";
  }

  if (normalized === "image/gif") {
    return "gif";
  }

  if (normalized === "image/avif") {
    return "avif";
  }

  return "bin";
}

function normalizePreviewOutputContentType(contentType: string): string {
  const normalized = normalizeMime(contentType);

  if (!INGESTION_PREVIEW_OUTPUT_CONTENT_TYPES.has(normalized)) {
    throw new ValidationError("Unsupported preview output content type.", {
      content_type: normalized,
      allowed_content_types: [...INGESTION_PREVIEW_OUTPUT_CONTENT_TYPES],
    });
  }

  return normalized;
}

function assertPreviewSizeAllowed(sizeBytes: number): void {
  if (sizeBytes > MAX_INGESTION_PREVIEW_SIZE_BYTES) {
    throw new ValidationError("Preview file exceeds maximum allowed size.", {
      max_size_bytes: MAX_INGESTION_PREVIEW_SIZE_BYTES,
      size_bytes: sizeBytes,
    });
  }
}

function assertPreviewDimensionAllowed(field: "width" | "height", value: number): void {
  if (value > MAX_INGESTION_PREVIEW_DIMENSION_PIXELS) {
    throw new ValidationError(`Preview ${field} exceeds maximum allowed dimension.`, {
      field,
      max_dimension_pixels: MAX_INGESTION_PREVIEW_DIMENSION_PIXELS,
      value,
    });
  }
}

export async function createIngestionDraft(params: {
  auth: AuthenticatedContext;
  body: CreateIngestionBody;
}): Promise<CreateIngestionDraftResponse> {
  assertClassificationTypeCompatibleWithItemKind({
    classificationType: params.body.classification_type,
    itemKind: params.body.item_kind,
  });

  const ingestion = await createIngestion({
    id: crypto.randomUUID(),
    batchLabel: params.body.batch_label,
    tenantId: params.auth.tenantId,
    createdBy: params.auth.userId,
    schemaVersion: params.body.schema_version,
    classificationType: params.body.classification_type,
    itemKind: params.body.item_kind,
    languageCode: params.body.language_code,
    pipelinePreset: params.body.pipeline_preset,
    accessLevel: params.body.access_level,
    embargoUntil: params.body.embargo_until
      ? new Date(params.body.embargo_until)
      : undefined,
    rightsNote: params.body.rights_note ?? undefined,
    sensitivityNote: params.body.sensitivity_note ?? undefined,
    summary: params.body.summary,
  });

  return {
    ingestion: serializeIngestion(ingestion),
  };
}

export async function getIngestion(params: {
  auth: AuthenticatedContext;
  ingestionId: string;
}): Promise<GetIngestionResponse> {
  const ingestion = requireIngestion(
    await findIngestionById(params.auth.tenantId, params.ingestionId),
    params.ingestionId,
  );
  const files = await listIngestionFiles({
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
  });

  return {
    ingestion: serializeIngestion(ingestion),
    files: files.map(serializeFile),
  };
}

export async function updateIngestion(params: {
  auth: AuthenticatedContext;
  ingestionId: string;
  body: UpdateIngestionBody;
}): Promise<UpdateIngestionResponse> {
  const ingestion = requireIngestion(
    await findIngestionById(params.auth.tenantId, params.ingestionId),
    params.ingestionId,
  );

  if (
    ingestion.status !== "DRAFT" &&
    ingestion.status !== "UPLOADING" &&
    ingestion.status !== "CANCELED"
  ) {
    throw new ConflictError(
      "Ingestion cannot be updated in its current state.",
      {
        ingestion_id: ingestion.id,
        status: ingestion.status,
      },
    );
  }

  if (await hasActiveLease(ingestion.id)) {
    throw new ConflictError(
      "Ingestion cannot be modified after lease assignment.",
      {
        ingestion_id: ingestion.id,
      },
    );
  }

  const hasBatchLabel = Object.prototype.hasOwnProperty.call(
    params.body,
    "batch_label",
  );
  const hasClassificationType = Object.prototype.hasOwnProperty.call(
    params.body,
    "classification_type",
  );
  const hasItemKind = Object.prototype.hasOwnProperty.call(
    params.body,
    "item_kind",
  );
  const hasLanguageCode = Object.prototype.hasOwnProperty.call(
    params.body,
    "language_code",
  );
  const hasPipelinePreset = Object.prototype.hasOwnProperty.call(
    params.body,
    "pipeline_preset",
  );
  const hasAccessLevel = Object.prototype.hasOwnProperty.call(
    params.body,
    "access_level",
  );
  const hasSummary = Object.prototype.hasOwnProperty.call(
    params.body,
    "summary",
  );
  const hasEmbargoUntil = Object.prototype.hasOwnProperty.call(
    params.body,
    "embargo_until",
  );
  const hasRightsNote = Object.prototype.hasOwnProperty.call(
    params.body,
    "rights_note",
  );
  const hasSensitivityNote = Object.prototype.hasOwnProperty.call(
    params.body,
    "sensitivity_note",
  );

  if (
    !hasBatchLabel &&
    !hasClassificationType &&
    !hasItemKind &&
    !hasLanguageCode &&
    !hasPipelinePreset &&
    !hasAccessLevel &&
    !hasSummary &&
    !hasEmbargoUntil &&
    !hasRightsNote &&
    !hasSensitivityNote
  ) {
    throw new ValidationError("Request body must include at least one field.");
  }

  const nextClassificationType =
    params.body.classification_type ?? ingestion.classificationType;
  const nextItemKind = params.body.item_kind ?? ingestion.itemKind;
  assertClassificationTypeCompatibleWithItemKind({
    classificationType: nextClassificationType,
    itemKind: nextItemKind,
  });

  const updated = await updateIngestionDetails({
    ingestionId: ingestion.id,
    tenantId: params.auth.tenantId,
    batchLabel: params.body.batch_label,
    classificationType: params.body.classification_type,
    itemKind: params.body.item_kind,
    languageCode: params.body.language_code,
    pipelinePreset: params.body.pipeline_preset,
    accessLevel: params.body.access_level,
    summary: params.body.summary,
    embargoUntil: params.body.embargo_until,
    rightsNote: params.body.rights_note,
    sensitivityNote: params.body.sensitivity_note,
    hasBatchLabel,
    hasClassificationType,
    hasItemKind,
    hasLanguageCode,
    hasPipelinePreset,
    hasAccessLevel,
    hasSummary,
    hasEmbargoUntil,
    hasRightsNote,
    hasSensitivityNote,
  });

  return {
    ingestion: serializeIngestion(
      requireIngestion(updated, params.ingestionId),
    ),
  };
}

export async function getIngestionList(params: {
  auth: AuthenticatedContext;
  query: IngestionListQuery;
}): Promise<IngestionListResult> {
  const pagination = params.query;
  let cursorPayload: CursorPayload | undefined;

  if (pagination.cursor) {
    const decoded = decodeCursor<JsonObject>(pagination.cursor);
    cursorPayload = parseIngestionCursorPayload(decoded);
  }

  const records = await listIngestions({
    tenantId: params.auth.tenantId,
    limit: pagination.limit + 1,
    cursorCreatedAt: cursorPayload?.created_at,
    cursorId: cursorPayload?.id,
  });

  const hasMore = records.length > pagination.limit;
  const visibleItems = hasMore ? records.slice(0, pagination.limit) : records;
  const lastItem = visibleItems.at(-1);

  return {
    items: visibleItems.map(serializeIngestion),
    nextCursor:
      hasMore && lastItem
        ? encodeCursor({
            created_at: lastItem.createdAt.toISOString(),
            id: lastItem.id,
          })
        : undefined,
  };
}

export function getIngestionCapabilities(): IngestionCapabilitiesResponse {
  return {
    media_kinds: [...MEDIA_KINDS],
    extensions_by_kind: {
      image: [...EXTENSION_ALLOWLIST.image],
      audio: [...EXTENSION_ALLOWLIST.audio],
      video: [...EXTENSION_ALLOWLIST.video],
      document: [...EXTENSION_ALLOWLIST.document],
    },
    mime_by_kind: {
      image: [...MIME_ALLOWLIST.image],
      audio: [...MIME_ALLOWLIST.audio],
      video: [...MIME_ALLOWLIST.video],
      document: [...MIME_ALLOWLIST.document],
    },
    mime_aliases: { ...MIME_ALIASES },
  };
}

function canMutateIngestionFiles(status: IngestionStatus): boolean {
  return status === "DRAFT" || status === "UPLOADING";
}

async function ensureIngestionNotProcessing(
  ingestion: IngestionRecord,
): Promise<void> {
  if (
    ingestion.status === "PROCESSING" ||
    ingestion.status === "COMPLETED" ||
    ingestion.status === "COMPLETED_WITH_ERRORS"
  ) {
    throw new ConflictError(
      "Ingestion cannot be modified after processing starts.",
      {
        ingestion_id: ingestion.id,
        status: ingestion.status,
      },
    );
  }

  if (await hasActiveLease(ingestion.id)) {
    throw new ConflictError(
      "Ingestion cannot be modified after lease assignment.",
      {
        ingestion_id: ingestion.id,
      },
    );
  }
}

async function reopenCanceledIngestion(params: {
  tenantId: string;
  ingestionId: string;
}): Promise<IngestionRecord> {
  const files = await listIngestionFiles({
    tenantId: params.tenantId,
    ingestionId: params.ingestionId,
  });
  const nextStatus: IngestionStatus =
    files.length === 0 ? "DRAFT" : "UPLOADING";

  return transitionIngestionStatus({
    tenantId: params.tenantId,
    ingestionId: params.ingestionId,
    to: nextStatus,
  });
}

export async function createPresignedUpload(params: {
  auth: AuthenticatedContext;
  ingestionId: string;
  body: CreatePresignedUploadBody;
}): Promise<CreatePresignedUploadResponse> {
  const payload = params.body;
  let ingestion = requireIngestion(
    await findIngestionById(params.auth.tenantId, params.ingestionId),
    params.ingestionId,
  );

  if (ingestion.status === "CANCELED") {
    ingestion = await reopenCanceledIngestion({
      tenantId: params.auth.tenantId,
      ingestionId: params.ingestionId,
    });
  }

  if (!canMutateIngestionFiles(ingestion.status)) {
    throw new ConflictError("Cannot add files after ingestion is submitted.", {
      ingestion_id: params.ingestionId,
      status: ingestion.status,
    });
  }

  let fileId: string;
  let filename: string;
  let contentType: string;
  let sizeBytes: number;
  let storageKey: string;

  if ("file_id" in payload) {
    const existingFile = await findIngestionFileById({
      tenantId: params.auth.tenantId,
      ingestionId: params.ingestionId,
      fileId: payload.file_id,
    });

    if (!existingFile) {
      throw new NotFoundError(
        `Ingestion file '${payload.file_id}' was not found.`,
      );
    }

    if (
      existingFile.status === "UPLOADED" ||
      existingFile.status === "VALIDATED"
    ) {
      throw new ConflictError(
        "Cannot re-presign a file that is already committed.",
        {
          file_id: existingFile.id,
          status: existingFile.status,
        },
      );
    }

    fileId = existingFile.id;
    filename = existingFile.filename;
    contentType = existingFile.contentType;
    sizeBytes = existingFile.sizeBytes;
    storageKey = existingFile.storageKey;
  } else {
    filename = payload.filename;
    contentType = payload.content_type;
    sizeBytes = payload.size_bytes;
    fileId = crypto.randomUUID();
    storageKey = buildStagingStorageKey({
      tenantId: params.auth.tenantId,
      ingestionId: params.ingestionId,
      fileId,
      filename,
    });

    await createIngestionFile({
      id: fileId,
      ingestionId: params.ingestionId,
      filename,
      contentType,
      sizeBytes,
      storageKey,
    });
  }

  if (ingestion.status === "DRAFT") {
    await updateIngestionStatus({
      ingestionId: params.ingestionId,
      tenantId: params.auth.tenantId,
      fromStatus: ingestion.status,
      toStatus: "UPLOADING",
    });
  }

  const expiresAt = new Date(Date.now() + ONE_HOUR_MS);
  const token = createUploadToken({
    ingestion_id: params.ingestionId,
    file_id: fileId,
    tenant_id: params.auth.tenantId,
    storage_key: storageKey,
    content_type: contentType,
    size_bytes: sizeBytes,
    expires_at: expiresAt.toISOString(),
  });

  return {
    file_id: fileId,
    storage_key: storageKey,
    upload_url: `/api/uploads/${token}`,
    expires_at: expiresAt.toISOString(),
    headers: {
      "content-type": contentType,
      "content-length": sizeBytes,
    },
  };
}

export async function commitUploadedFile(params: {
  auth: AuthenticatedContext;
  ingestionId: string;
  body: CommitUploadedFileBody;
}): Promise<CommitUploadedFileResponse> {
  const payload = params.body;
  const fileId = payload.file_id;
  const checksumSha256 = normalizeSha256(payload.checksum_sha256);

  const ingestion = requireIngestion(
    await findIngestionById(params.auth.tenantId, params.ingestionId),
    params.ingestionId,
  );

  if (!canMutateIngestionFiles(ingestion.status)) {
    throw new ConflictError(
      "Cannot commit files after ingestion is submitted.",
      {
        ingestion_id: params.ingestionId,
        status: ingestion.status,
      },
    );
  }

  const file = await findIngestionFileById({
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
    fileId,
  });

  if (!file) {
    throw new NotFoundError(`Ingestion file '${fileId}' was not found.`);
  }

  const files = await listIngestionFiles({
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
  });

  const fileKind = requireMediaKind(file.contentType);
  assertItemKindCompatibleWithMediaKind({
    itemKind: ingestion.itemKind,
    mediaKind: fileKind,
  });
  const otherKinds = new Set<MediaKind>();

  for (const other of files) {
    if (other.id === file.id) {
      continue;
    }

    otherKinds.add(requireMediaKind(other.contentType));
  }

  if (otherKinds.size > 0) {
    const expectedKinds = Array.from(otherKinds).sort();

    if (expectedKinds.length > 1 || !otherKinds.has(fileKind)) {
      throw new ConflictError(
        "All files in an ingestion must share the same media kind.",
        {
          expected_media_kinds: expectedKinds,
          actual_media_kind: fileKind,
          file_id: file.id,
        },
      );
    }
  }

  const stagingPath = resolveStagingPath(file.storageKey);
  const uploadedFile = Bun.file(stagingPath);

  if (!(await uploadedFile.exists())) {
    throw new ConflictError("Staged file was not uploaded yet.", {
      file_id: fileId,
    });
  }

  const bytes = await uploadedFile.bytes();

  if (bytes.byteLength !== file.sizeBytes) {
    throw new ConflictError(
      "Uploaded file size does not match presigned metadata.",
      {
        expected_size_bytes: file.sizeBytes,
        actual_size_bytes: bytes.byteLength,
      },
    );
  }

  const actualChecksum = new Bun.CryptoHasher("sha256")
    .update(bytes)
    .digest("hex");

  if (actualChecksum !== checksumSha256) {
    throw new ConflictError("Uploaded file checksum mismatch.", {
      expected_checksum_sha256: checksumSha256,
      actual_checksum_sha256: actualChecksum,
    });
  }

  const updated = await markIngestionFileUploaded({
    fileId,
    ingestionId: params.ingestionId,
    checksumSha256,
  });

  if (!updated) {
    throw new ConflictError(
      `Ingestion file '${fileId}' is not in a committable state.`,
      {
        file_id: fileId,
      },
    );
  }

  const previewUpdated = supportsIngestionPreview(updated.contentType)
    ? await markIngestionFilePreviewPending({
        fileId,
        ingestionId: params.ingestionId,
      })
    : await markIngestionFilePreviewUnsupported({
        fileId,
        ingestionId: params.ingestionId,
      });

  return {
    file: serializeFile(previewUpdated ?? updated),
  };
}

export async function updateIngestionFileOverrides(params: {
  auth: AuthenticatedContext;
  ingestionId: string;
  fileId: string;
  body: UpdateIngestionFileOverridesBody;
}): Promise<UpdateIngestionFileOverridesResponse> {
  const processingOverrides = params.body.processing_overrides;

  let ingestion = requireIngestion(
    await findIngestionById(params.auth.tenantId, params.ingestionId),
    params.ingestionId,
  );

  if (ingestion.status === "CANCELED") {
    ingestion = await reopenCanceledIngestion({
      tenantId: params.auth.tenantId,
      ingestionId: params.ingestionId,
    });
  }

  if (!canMutateIngestionFiles(ingestion.status)) {
    throw new ConflictError(
      "Cannot update file overrides after ingestion is submitted.",
      {
        ingestion_id: params.ingestionId,
        status: ingestion.status,
      },
    );
  }

  const file = await findIngestionFileById({
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
    fileId: params.fileId,
  });

  if (!file) {
    throw new NotFoundError(`Ingestion file '${params.fileId}' was not found.`);
  }

  const updated = await updateIngestionFileProcessingOverrides({
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
    fileId: params.fileId,
    processingOverrides,
  });

  if (!updated) {
    throw new ConflictError("Ingestion file overrides could not be updated.", {
      file_id: params.fileId,
    });
  }

  return {
    file: serializeFile(updated),
  };
}

export async function removeIngestionFile(params: {
  auth: AuthenticatedContext;
  ingestionId: string;
  fileId: string;
}): Promise<DeleteIngestionFileResponse> {
  let ingestion = requireIngestion(
    await findIngestionById(params.auth.tenantId, params.ingestionId),
    params.ingestionId,
  );

  const file = await findIngestionFileById({
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
    fileId: params.fileId,
  });

  if (!file) {
    throw new NotFoundError(`Ingestion file '${params.fileId}' was not found.`);
  }

  if (ingestion.status === "CANCELED") {
    const files = await listIngestionFiles({
      tenantId: params.auth.tenantId,
      ingestionId: params.ingestionId,
    });
    const remainingCount = files.filter((entry) => entry.id !== file.id).length;
    const nextStatus: IngestionStatus =
      remainingCount === 0 ? "DRAFT" : "UPLOADING";
    ingestion = await transitionIngestionStatus({
      tenantId: params.auth.tenantId,
      ingestionId: params.ingestionId,
      to: nextStatus,
    });
  }

  if (!canMutateIngestionFiles(ingestion.status)) {
    throw new ConflictError(
      "Cannot remove files after ingestion is submitted.",
      {
        ingestion_id: params.ingestionId,
        status: ingestion.status,
      },
    );
  }

  const stagingPath = resolveStagingPath(file.storageKey);
  await rm(stagingPath, { force: true });
  if (file.previewStorageKey) {
    await rm(resolveStagingPath(file.previewStorageKey), { force: true });
  }

  const deleted = await deleteIngestionFile({
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
    fileId: params.fileId,
  });

  if (!deleted) {
    throw new ConflictError("Ingestion file could not be deleted.", {
      file_id: file.id,
    });
  }

  return {
    status: "deleted",
    file_id: file.id,
  };
}

export async function streamIngestionFilePreview(params: {
  auth: AuthenticatedContext;
  ingestionId: string;
  fileId: string;
}): Promise<Response> {
  const file = await findIngestionFileById({
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
    fileId: params.fileId,
  });

  if (!file) {
    throw new NotFoundError(`Ingestion file '${params.fileId}' was not found.`);
  }

  if (file.previewStatus !== "ready" || !file.previewStorageKey) {
    throw new NotFoundError(`Preview for ingestion file '${params.fileId}' was not found.`);
  }

  const previewFile = Bun.file(resolveStagingPath(file.previewStorageKey));
  if (!(await previewFile.exists())) {
    throw new NotFoundError(`Preview for ingestion file '${params.fileId}' was not found.`);
  }

  const headers = new Headers({
    "content-type": file.previewContentType ?? "application/octet-stream",
    "cache-control": "private, no-store",
  });
  if (file.previewSizeBytes !== undefined) {
    headers.set("content-length", String(file.previewSizeBytes));
  }

  return new Response(previewFile, {
    status: 200,
    headers,
  });
}

export async function claimNextIngestionPreview(params: {
  workerId?: string;
}): Promise<WorkerClaimIngestionPreviewResponse> {
  const preview = await claimNextPendingIngestionPreview({
    workerId: params.workerId,
    claimTimeoutMinutes: PREVIEW_CLAIM_TIMEOUT_MINUTES,
  });

  if (!preview) {
    return {
      preview: null,
    };
  }

  const downloadToken = createDownloadToken({
    ingestion_id: preview.ingestionId,
    file_id: preview.fileId,
    tenant_id: preview.tenantId,
    storage_key: preview.storageKey,
    content_type: preview.contentType,
    size_bytes: preview.sizeBytes,
    expires_at: new Date(Date.now() + PREVIEW_DOWNLOAD_TTL_MS).toISOString(),
  });

  return {
    preview: {
      ingestion_id: preview.ingestionId,
      file_id: preview.fileId,
      tenant_id: preview.tenantId,
      batch_label: preview.batchLabel,
      filename: preview.filename,
      content_type: preview.contentType,
      size_bytes: preview.sizeBytes,
      download_url: `/api/worker/downloads/${downloadToken}`,
      claimed_by: preview.claimedBy ?? null,
      claimed_at: preview.claimedAt?.toISOString() ?? null,
    },
  };
}

async function requireClaimedIngestionPreview(params: {
  ingestionId: string;
  fileId: string;
  workerId?: string;
}): Promise<ClaimedIngestionPreviewRecord> {
  const preview = await findClaimedIngestionPreview({
    ingestionId: params.ingestionId,
    fileId: params.fileId,
    workerId: params.workerId,
  });

  if (!preview) {
    throw new ConflictError("Preview is not currently claimed by this worker.", {
      ingestion_id: params.ingestionId,
      file_id: params.fileId,
    });
  }

  return preview;
}

export async function presignIngestionPreviewUpload(params: {
  workerId?: string;
  ingestionId: string;
  fileId: string;
  body: WorkerPresignIngestionPreviewUploadBody;
}): Promise<WorkerPresignIngestionPreviewUploadResponse> {
  const preview = await requireClaimedIngestionPreview({
    ingestionId: params.ingestionId,
    fileId: params.fileId,
    workerId: params.workerId,
  });
  const contentType = normalizePreviewOutputContentType(params.body.content_type);
  assertPreviewSizeAllowed(params.body.size_bytes);
  const expiresAt = new Date(Date.now() + ONE_HOUR_MS).toISOString();
  const storageKey = buildIngestionPreviewStorageKey({
    tenantId: preview.tenantId,
    ingestionId: preview.ingestionId,
    fileId: preview.fileId,
    extension: previewExtensionFromContentType(contentType),
  });
  const uploadToken = createUploadToken({
    ingestion_id: preview.ingestionId,
    file_id: preview.fileId,
    tenant_id: preview.tenantId,
    storage_key: storageKey,
    content_type: contentType,
    size_bytes: params.body.size_bytes,
    expires_at: expiresAt,
  });

  const updated = await updateIngestionPreviewUpload({
    ingestionId: preview.ingestionId,
    fileId: preview.fileId,
    workerId: params.workerId,
    storageKey,
    contentType,
  });

  if (!updated) {
    throw new ConflictError("Preview upload could not be prepared.", {
      ingestion_id: preview.ingestionId,
      file_id: preview.fileId,
    });
  }

  return {
    upload_token: uploadToken,
    upload_url: `/api/uploads/${uploadToken}`,
    storage_key: storageKey,
    expires_at: expiresAt,
    headers: {
      "content-type": contentType,
      "content-length": params.body.size_bytes,
    },
  };
}

export async function completeIngestionPreview(params: {
  workerId?: string;
  ingestionId: string;
  fileId: string;
  body: WorkerCompleteIngestionPreviewBody;
}): Promise<WorkerCompleteIngestionPreviewResponse> {
  const preview = await requireClaimedIngestionPreview({
    ingestionId: params.ingestionId,
    fileId: params.fileId,
    workerId: params.workerId,
  });
  const upload = parseUploadToken(params.body.upload_token);
  normalizePreviewOutputContentType(upload.content_type);
  assertPreviewSizeAllowed(upload.size_bytes);
  assertPreviewDimensionAllowed("width", params.body.width);
  assertPreviewDimensionAllowed("height", params.body.height);

  if (
    upload.ingestion_id !== preview.ingestionId ||
    upload.file_id !== preview.fileId ||
    upload.tenant_id !== preview.tenantId
  ) {
    throw new ValidationError("Upload token does not match claimed preview context.");
  }

  const previewFile = Bun.file(resolveStagingPath(upload.storage_key));
  if (!(await previewFile.exists())) {
    throw new NotFoundError("Uploaded preview file was not found.");
  }

  const updated = await markIngestionFilePreviewReady({
    ingestionId: preview.ingestionId,
    fileId: preview.fileId,
    workerId: params.workerId,
    storageKey: upload.storage_key,
    contentType: upload.content_type,
    sizeBytes: upload.size_bytes,
    width: params.body.width,
    height: params.body.height,
  });

  if (!updated) {
    throw new ConflictError("Preview completion could not be recorded.", {
      ingestion_id: preview.ingestionId,
      file_id: preview.fileId,
    });
  }

  return {
    status: "ready",
    ingestion_id: preview.ingestionId,
    file_id: preview.fileId,
  };
}

export async function failIngestionPreview(params: {
  workerId?: string;
  ingestionId: string;
  fileId: string;
  body: WorkerFailIngestionPreviewBody;
}): Promise<WorkerFailIngestionPreviewResponse> {
  const preview = await requireClaimedIngestionPreview({
    ingestionId: params.ingestionId,
    fileId: params.fileId,
    workerId: params.workerId,
  });
  const updated = await markIngestionFilePreviewFailed({
    ingestionId: preview.ingestionId,
    fileId: preview.fileId,
    workerId: params.workerId,
    error: {
      message: params.body.error.message,
      ...(params.body.error.code ? { code: params.body.error.code } : {}),
      ...(params.body.error.retryable !== undefined
        ? { retryable: params.body.error.retryable }
        : {}),
      ...(params.body.error.details ? { details: params.body.error.details } : {}),
    },
  });

  if (!updated) {
    throw new ConflictError("Preview failure could not be recorded.", {
      ingestion_id: preview.ingestionId,
      file_id: preview.fileId,
    });
  }

  return {
    status: "failed",
    ingestion_id: preview.ingestionId,
    file_id: preview.fileId,
  };
}

export async function deleteIngestionRecord(params: {
  auth: AuthenticatedContext;
  ingestionId: string;
}): Promise<DeleteIngestionResponse> {
  const ingestion = requireIngestion(
    await findIngestionById(params.auth.tenantId, params.ingestionId),
    params.ingestionId,
  );

  await ensureIngestionNotProcessing(ingestion);

  if (
    ingestion.status === "QUEUED" ||
    ingestion.status === "COMPLETED" ||
    ingestion.status === "COMPLETED_WITH_ERRORS"
  ) {
    throw new ConflictError(
      "Ingestion cannot be deleted in its current state.",
      {
        ingestion_id: ingestion.id,
        status: ingestion.status,
      },
    );
  }

  if (
    ingestion.status !== "DRAFT" &&
    ingestion.status !== "UPLOADING" &&
    ingestion.status !== "CANCELED"
  ) {
    throw new ConflictError(
      "Ingestion cannot be deleted in its current state.",
      {
        ingestion_id: ingestion.id,
        status: ingestion.status,
      },
    );
  }

  const files = await listIngestionFiles({
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
  });

  for (const file of files) {
    const stagingPath = resolveStagingPath(file.storageKey);
    await rm(stagingPath, { force: true });
    if (file.previewStorageKey) {
      await rm(resolveStagingPath(file.previewStorageKey), { force: true });
    }
  }

  const deleted = await deleteIngestion({
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
  });

  if (!deleted) {
    throw new ConflictError("Ingestion could not be deleted.", {
      ingestion_id: ingestion.id,
    });
  }

  return {
    status: "deleted",
    ingestion_id: ingestion.id,
  };
}

async function transitionIngestionStatus(params: {
  tenantId: string;
  ingestionId: string;
  to: IngestionStatus;
}): Promise<IngestionRecord> {
  const ingestion = requireIngestion(
    await findIngestionById(params.tenantId, params.ingestionId),
    params.ingestionId,
  );

  try {
    assertIngestionStatusTransition(ingestion.status, params.to);
  } catch (error) {
    mapTransitionError(error);
  }

  const updated = await updateIngestionStatus({
    ingestionId: params.ingestionId,
    tenantId: params.tenantId,
    fromStatus: ingestion.status,
    toStatus: params.to,
  });

  return requireIngestion(updated, params.ingestionId);
}

export async function submitIngestion(params: {
  auth: AuthenticatedContext;
  ingestionId: string;
}): Promise<SubmitIngestionResponse> {
  const files = await listIngestionFiles({
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
  });

  if (files.length === 0) {
    throw new ConflictError("Cannot submit ingestion without uploaded files.");
  }

  if (
    !files.some(
      (file) => file.status === "UPLOADED" || file.status === "VALIDATED",
    )
  ) {
    throw new ConflictError(
      "Cannot submit ingestion before at least one file is committed.",
    );
  }

  const updated = await transitionIngestionStatus({
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
    to: "QUEUED",
  });

  return {
    ingestion: serializeIngestion(updated),
  };
}

export async function cancelIngestion(params: {
  auth: AuthenticatedContext;
  ingestionId: string;
}): Promise<CancelIngestionResponse> {
  const ingestion = requireIngestion(
    await findIngestionById(params.auth.tenantId, params.ingestionId),
    params.ingestionId,
  );

  await ensureIngestionNotProcessing(ingestion);

  if (ingestion.status === "CANCELED") {
    return {
      ingestion: serializeIngestion(ingestion),
    };
  }

  let nextStatus: IngestionStatus;

  if (ingestion.status === "QUEUED") {
    nextStatus = "UPLOADING";
  } else if (ingestion.status === "DRAFT" || ingestion.status === "UPLOADING") {
    nextStatus = "CANCELED";
  } else {
    throw new ConflictError(
      "Ingestion cannot be canceled in its current state.",
      {
        ingestion_id: ingestion.id,
        status: ingestion.status,
      },
    );
  }

  const updated = await transitionIngestionStatus({
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
    to: nextStatus,
  });

  return {
    ingestion: serializeIngestion(updated),
  };
}

export async function restoreIngestion(params: {
  auth: AuthenticatedContext;
  ingestionId: string;
}): Promise<RestoreIngestionResponse> {
  const ingestion = requireIngestion(
    await findIngestionById(params.auth.tenantId, params.ingestionId),
    params.ingestionId,
  );

  await ensureIngestionNotProcessing(ingestion);

  if (ingestion.status !== "CANCELED") {
    throw new ConflictError(
      "Ingestion cannot be restored in its current state.",
      {
        ingestion_id: ingestion.id,
        status: ingestion.status,
      },
    );
  }

  const files = await listIngestionFiles({
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
  });
  const nextStatus: IngestionStatus =
    files.length === 0 ? "DRAFT" : "UPLOADING";

  const updated = await transitionIngestionStatus({
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
    to: nextStatus,
  });

  return {
    ingestion: serializeIngestion(updated),
  };
}

export async function retryIngestion(params: {
  auth: AuthenticatedContext;
  ingestionId: string;
}): Promise<RetryIngestionResponse> {
  const updated = await transitionIngestionStatus({
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
    to: "QUEUED",
  });

  return {
    ingestion: serializeIngestion(updated),
  };
}

export async function uploadFileBySignedToken(params: {
  uploadToken: string;
  request: Request;
}): Promise<UploadFileBySignedTokenResponse> {
  const token = parseUploadToken(params.uploadToken);
  const requestContentType = params.request.headers
    .get("content-type")
    ?.split(";")[0]
    ?.trim();

  if (requestContentType !== token.content_type) {
    throw new ValidationError(
      "Upload content type does not match signed URL constraints.",
    );
  }

  const rawContentLength = params.request.headers.get("content-length");

  if (!rawContentLength) {
    throw new ValidationError(
      "Header 'content-length' is required for uploads.",
    );
  }

  const contentLength = Number.parseInt(rawContentLength, 10);

  if (!Number.isFinite(contentLength) || contentLength !== token.size_bytes) {
    throw new ValidationError(
      "Upload content length does not match signed URL constraints.",
    );
  }

  const bodyBytes = new Uint8Array(await params.request.arrayBuffer());

  if (bodyBytes.byteLength !== token.size_bytes) {
    throw new ValidationError(
      "Upload body size does not match signed URL constraints.",
    );
  }

  const destinationPath = resolveStagingPath(token.storage_key);
  await mkdir(dirname(destinationPath), { recursive: true });
  await Bun.write(destinationPath, bodyBytes);

  return {
    status: "ok",
    ingestion_id: token.ingestion_id,
    file_id: token.file_id,
    size_bytes: token.size_bytes,
  };
}
