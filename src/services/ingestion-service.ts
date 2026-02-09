import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";

import { ConflictError, NotFoundError, ValidationError } from "../http/errors.ts";
import { decodeCursor, encodeCursor, parsePaginationParams } from "../http/pagination.ts";
import {
  createIngestion,
  createIngestionFile,
  findIngestionById,
  findIngestionFileById,
  listIngestionFiles,
  listIngestions,
  markIngestionFileUploaded,
  updateIngestionStatus,
  type IngestionFileRecord,
  type IngestionRecord,
} from "../repos/ingestion-repo.ts";
import {
  assertIngestionStatusTransition,
  InvalidIngestionTransitionError,
  type IngestionStatus,
} from "../domain/ingestions/state-machine.ts";
import {
  buildStagingStorageKey,
  createUploadToken,
  parseUploadToken,
  resolveStagingPath,
} from "../storage/index.ts";

const SHA256_PATTERN = /^[a-f0-9]{64}$/i;
const ONE_HOUR_MS = 60 * 60 * 1000;

interface CursorPayload {
  created_at: string;
  id: string;
}

export interface IngestionListResult {
  items: Array<Record<string, unknown>>;
  nextCursor?: string;
}

function normalizeSha256(value: string): string {
  const normalized = value.trim().toLowerCase();

  if (!SHA256_PATTERN.test(normalized)) {
    throw new ValidationError("Field 'checksum_sha256' must be a valid SHA-256 hex string.");
  }

  return normalized;
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

function requireIngestion(record: IngestionRecord | undefined, ingestionId: string): IngestionRecord {
  if (!record) {
    throw new NotFoundError(`Ingestion '${ingestionId}' was not found.`);
  }

  return record;
}

function serializeIngestion(record: IngestionRecord): Record<string, unknown> {
  return {
    id: record.id,
    upload_id: record.uploadId,
    tenant_id: record.tenantId,
    status: record.status,
    created_by: record.createdBy,
    summary: record.summary,
    error_summary: record.errorSummary,
    created_at: record.createdAt.toISOString(),
    updated_at: record.updatedAt.toISOString(),
  };
}

function serializeFile(record: IngestionFileRecord): Record<string, unknown> {
  return {
    id: record.id,
    ingestion_id: record.ingestionId,
    filename: record.filename,
    content_type: record.contentType,
    size_bytes: record.sizeBytes,
    storage_key: record.storageKey,
    status: record.status,
    checksum_sha256: record.checksumSha256 ?? null,
    error: record.error,
    created_at: record.createdAt.toISOString(),
    updated_at: record.updatedAt.toISOString(),
  };
}

export async function createIngestionDraft(params: {
  tenantId: string;
  userId: string;
  uploadId: string;
}): Promise<Record<string, unknown>> {
  const uploadId = params.uploadId.trim();

  if (uploadId.length === 0) {
    throw new ValidationError("Field 'upload_id' is required.");
  }

  const ingestion = await createIngestion({
    id: crypto.randomUUID(),
    uploadId,
    tenantId: params.tenantId,
    createdBy: params.userId,
  });

  return {
    ingestion: serializeIngestion(ingestion),
  };
}

export async function getIngestion(params: {
  tenantId: string;
  ingestionId: string;
}): Promise<Record<string, unknown>> {
  const ingestion = requireIngestion(await findIngestionById(params.tenantId, params.ingestionId), params.ingestionId);
  const files = await listIngestionFiles({
    tenantId: params.tenantId,
    ingestionId: params.ingestionId,
  });

  return {
    ingestion: serializeIngestion(ingestion),
    files: files.map(serializeFile),
  };
}

export async function getIngestionList(params: {
  tenantId: string;
  url: URL;
}): Promise<IngestionListResult> {
  const pagination = parsePaginationParams(params.url);
  let cursorPayload: CursorPayload | undefined;

  if (pagination.cursor) {
    const decoded = decodeCursor<Record<string, unknown>>(pagination.cursor);

    if (typeof decoded.created_at !== "string" || typeof decoded.id !== "string") {
      throw new ValidationError("Query parameter 'cursor' is invalid.");
    }

    cursorPayload = {
      created_at: decoded.created_at,
      id: decoded.id,
    };
  }

  const records = await listIngestions({
    tenantId: params.tenantId,
    limit: pagination.limit + 1,
    cursorCreatedAt: cursorPayload?.created_at,
    cursorId: cursorPayload?.id,
  });

  const hasMore = records.length > pagination.limit;
  const visibleItems = hasMore ? records.slice(0, pagination.limit) : records;
  const lastItem = visibleItems.at(-1);

  return {
    items: visibleItems.map(serializeIngestion),
    nextCursor: hasMore && lastItem
      ? encodeCursor({
          created_at: lastItem.createdAt.toISOString(),
          id: lastItem.id,
        })
      : undefined,
  };
}

function requireStringField(payload: Record<string, unknown>, key: string): string {
  const value = payload[key];

  if (typeof value !== "string") {
    throw new ValidationError(`Field '${key}' must be a string.`);
  }

  const normalized = value.trim();

  if (normalized.length === 0) {
    throw new ValidationError(`Field '${key}' cannot be empty.`);
  }

  return normalized;
}

function requirePositiveIntField(payload: Record<string, unknown>, key: string): number {
  const value = payload[key];

  if (typeof value !== "number" || !Number.isInteger(value) || value < 1) {
    throw new ValidationError(`Field '${key}' must be a positive integer.`);
  }

  return value;
}

export async function createPresignedUpload(params: {
  tenantId: string;
  ingestionId: string;
  body: unknown;
}): Promise<Record<string, unknown>> {
  if (params.body === null || typeof params.body !== "object" || Array.isArray(params.body)) {
    throw new ValidationError("Request body must be an object.");
  }

  const payload = params.body as Record<string, unknown>;
  const ingestion = requireIngestion(await findIngestionById(params.tenantId, params.ingestionId), params.ingestionId);

  let fileId: string;
  let filename: string;
  let contentType: string;
  let sizeBytes: number;
  let storageKey: string;

  const requestedFileId = payload.file_id;

  if (requestedFileId !== undefined) {
    if (typeof requestedFileId !== "string" || requestedFileId.trim().length === 0) {
      throw new ValidationError("Field 'file_id' must be a non-empty string when provided.");
    }

    const existingFile = await findIngestionFileById({
      tenantId: params.tenantId,
      ingestionId: params.ingestionId,
      fileId: requestedFileId,
    });

    if (!existingFile) {
      throw new NotFoundError(`Ingestion file '${requestedFileId}' was not found.`);
    }

    if (existingFile.status === "UPLOADED" || existingFile.status === "VALIDATED") {
      throw new ConflictError("Cannot re-presign a file that is already committed.", {
        file_id: existingFile.id,
        status: existingFile.status,
      });
    }

    fileId = existingFile.id;
    filename = existingFile.filename;
    contentType = existingFile.contentType;
    sizeBytes = existingFile.sizeBytes;
    storageKey = existingFile.storageKey;
  } else {
    filename = requireStringField(payload, "filename");
    contentType = requireStringField(payload, "content_type");
    sizeBytes = requirePositiveIntField(payload, "size_bytes");
    fileId = crypto.randomUUID();
    storageKey = buildStagingStorageKey({
      tenantId: params.tenantId,
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
      tenantId: params.tenantId,
      status: "UPLOADING",
    });
  }

  const expiresAt = new Date(Date.now() + ONE_HOUR_MS);
  const token = createUploadToken({
    ingestion_id: params.ingestionId,
    file_id: fileId,
    tenant_id: params.tenantId,
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
  tenantId: string;
  ingestionId: string;
  body: unknown;
}): Promise<Record<string, unknown>> {
  if (params.body === null || typeof params.body !== "object" || Array.isArray(params.body)) {
    throw new ValidationError("Request body must be an object.");
  }

  const payload = params.body as Record<string, unknown>;
  const fileId = requireStringField(payload, "file_id");
  const checksumSha256 = normalizeSha256(requireStringField(payload, "checksum_sha256"));

  const file = await findIngestionFileById({
    tenantId: params.tenantId,
    ingestionId: params.ingestionId,
    fileId,
  });

  if (!file) {
    throw new NotFoundError(`Ingestion file '${fileId}' was not found.`);
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
    throw new ConflictError("Uploaded file size does not match presigned metadata.", {
      expected_size_bytes: file.sizeBytes,
      actual_size_bytes: bytes.byteLength,
    });
  }

  const actualChecksum = new Bun.CryptoHasher("sha256").update(bytes).digest("hex");

  if (actualChecksum !== checksumSha256) {
    throw new ConflictError("Uploaded file checksum mismatch.", {
      expected_checksum_sha256: checksumSha256,
      actual_checksum_sha256: actualChecksum,
    });
  }

  const updated = await markIngestionFileUploaded({
    fileId,
    checksumSha256,
  });

  if (!updated) {
    throw new NotFoundError(`Ingestion file '${fileId}' was not found.`);
  }

  return {
    file: serializeFile(updated),
  };
}

async function transitionIngestionStatus(params: {
  tenantId: string;
  ingestionId: string;
  to: IngestionStatus;
}): Promise<IngestionRecord> {
  const ingestion = requireIngestion(await findIngestionById(params.tenantId, params.ingestionId), params.ingestionId);

  try {
    assertIngestionStatusTransition(ingestion.status, params.to);
  } catch (error) {
    mapTransitionError(error);
  }

  const updated = await updateIngestionStatus({
    ingestionId: params.ingestionId,
    tenantId: params.tenantId,
    status: params.to,
  });

  return requireIngestion(updated, params.ingestionId);
}

export async function submitIngestion(params: {
  tenantId: string;
  ingestionId: string;
}): Promise<Record<string, unknown>> {
  const files = await listIngestionFiles({
    tenantId: params.tenantId,
    ingestionId: params.ingestionId,
  });

  if (files.length === 0) {
    throw new ConflictError("Cannot submit ingestion without uploaded files.");
  }

  if (!files.some(file => file.status === "UPLOADED" || file.status === "VALIDATED")) {
    throw new ConflictError("Cannot submit ingestion before at least one file is committed.");
  }

  const updated = await transitionIngestionStatus({
    tenantId: params.tenantId,
    ingestionId: params.ingestionId,
    to: "QUEUED",
  });

  return {
    ingestion: serializeIngestion(updated),
  };
}

export async function cancelIngestion(params: {
  tenantId: string;
  ingestionId: string;
}): Promise<Record<string, unknown>> {
  const updated = await transitionIngestionStatus({
    tenantId: params.tenantId,
    ingestionId: params.ingestionId,
    to: "CANCELED",
  });

  return {
    ingestion: serializeIngestion(updated),
  };
}

export async function retryIngestion(params: {
  tenantId: string;
  ingestionId: string;
}): Promise<Record<string, unknown>> {
  const updated = await transitionIngestionStatus({
    tenantId: params.tenantId,
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
}): Promise<Record<string, unknown>> {
  const token = parseUploadToken(params.uploadToken);
  const requestContentType = params.request.headers.get("content-type")?.split(";")[0]?.trim();

  if (requestContentType !== token.content_type) {
    throw new ValidationError("Upload content type does not match signed URL constraints.");
  }

  const rawContentLength = params.request.headers.get("content-length");

  if (!rawContentLength) {
    throw new ValidationError("Header 'content-length' is required for uploads.");
  }

  const contentLength = Number.parseInt(rawContentLength, 10);

  if (!Number.isFinite(contentLength) || contentLength !== token.size_bytes) {
    throw new ValidationError("Upload content length does not match signed URL constraints.");
  }

  const bodyBytes = new Uint8Array(await params.request.arrayBuffer());

  if (bodyBytes.byteLength !== token.size_bytes) {
    throw new ValidationError("Upload body size does not match signed URL constraints.");
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
