import { mkdir, rm } from "node:fs/promises";
import { dirname } from "node:path";

import {
  ConflictError,
  NotFoundError,
  ValidationError,
} from "../http/errors.ts";
import { decodeCursor, encodeCursor } from "../http/pagination.ts";
import {
  createIngestion,
  createIngestionFile,
  deleteIngestionFile,
  deleteIngestion,
  findIngestionById,
  findIngestionFileById,
  listIngestionFiles,
  listIngestions,
  markIngestionFileUploaded,
  updateIngestionDetails,
  updateIngestionFileProcessingOverrides,
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
  buildStagingStorageKey,
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
  getMediaKindForMime,
  normalizeMime,
  type MediaKind,
} from "../domain/ingestions/capabilities.ts";
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
  type IngestionDto,
  type IngestionFileDto,
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
  parseIngestionCursorPayload,
  parseIngestionFileProcessingOverrides,
  parseJsonObject,
} from "../validation/ingestion.ts";

const SHA256_PATTERN = /^[a-f0-9]{64}$/i;
const ONE_HOUR_MS = 60 * 60 * 1000;

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
  return {
    id: record.id,
    ingestion_id: record.ingestionId,
    filename: record.filename,
    content_type: record.contentType,
    size_bytes: record.sizeBytes,
    storage_key: record.storageKey,
    status: record.status,
    checksum_sha256: record.checksumSha256 ?? null,
    processing_overrides: parseIngestionFileProcessingOverrides(
      record.processingOverrides,
    ),
    error: parseJsonObject(record.error),
    created_at: record.createdAt.toISOString(),
    updated_at: record.updatedAt.toISOString(),
  };
}

export async function createIngestionDraft(params: {
  auth: AuthenticatedContext;
  body: CreateIngestionBody;
}): Promise<CreateIngestionDraftResponse> {
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
  if (ingestion.status === "PROCESSING" || ingestion.status === "COMPLETED") {
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

  return {
    file: serializeFile(updated),
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

export async function deleteIngestionRecord(params: {
  auth: AuthenticatedContext;
  ingestionId: string;
}): Promise<DeleteIngestionResponse> {
  const ingestion = requireIngestion(
    await findIngestionById(params.auth.tenantId, params.ingestionId),
    params.ingestionId,
  );

  await ensureIngestionNotProcessing(ingestion);

  if (ingestion.status === "QUEUED" || ingestion.status === "COMPLETED") {
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
