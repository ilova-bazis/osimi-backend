import { createHmac, timingSafeEqual } from "node:crypto";
import { join, normalize } from "node:path";

import { UnauthorizedError, ValidationError } from "../http/errors.ts";
import { getRuntimeConfig, resolveUploadSigningSecret } from "../runtime/config.ts";

const DEFAULT_STAGING_ROOT = ".staging";

interface UploadTokenPayload {
  purpose: "ingestion_original" | "ingestion_preview";
  upload_token_id: string;
  ingestion_id: string;
  file_id: string;
  tenant_id: string;
  storage_key: string;
  content_type: string;
  size_bytes: number;
  expires_at: string;
}

interface DownloadTokenPayload {
  ingestion_id: string;
  file_id: string;
  tenant_id: string;
  storage_key: string;
  content_type: string;
  size_bytes: number;
  expires_at: string;
}

interface ObjectArtifactUploadTokenPayload {
  upload_token_id: string;
  request_id: string;
  object_id: string;
  tenant_id: string;
  artifact_kind: string;
  variant: string | null;
  storage_key: string;
  content_type: string;
  size_bytes: number;
  expires_at: string;
}

function getSigningSecret(): string {
  return resolveUploadSigningSecret();
}

function secureEquals(left: string, right: string): boolean {
  const leftBuffer = Buffer.from(left, "utf8");
  const rightBuffer = Buffer.from(right, "utf8");

  return leftBuffer.length === rightBuffer.length && timingSafeEqual(leftBuffer, rightBuffer);
}

function toBase64Url(value: string): string {
  return Buffer.from(value, "utf8").toString("base64url");
}

function fromBase64Url(value: string): string {
  return Buffer.from(value, "base64url").toString("utf8");
}

function sign(payload: string): string {
  return createHmac("sha256", getSigningSecret()).update(payload).digest("base64url");
}

function safeStorageKeySegment(segment: string): string {
  return segment.replace(/[^A-Za-z0-9._-]/g, "_");
}

export function stagingRootPath(): string {
  const runtimeStagingRoot = getRuntimeConfig().stagingRoot;
  return runtimeStagingRoot?.trim() || process.env.STAGING_ROOT?.trim() || DEFAULT_STAGING_ROOT;
}

export function buildStagingStorageKey(params: {
  tenantId: string;
  ingestionId: string;
  fileId: string;
  filename: string;
}): string {
  return `tenants/${params.tenantId}/ingestions/${params.ingestionId}/original/${params.fileId}-${safeStorageKeySegment(params.filename)}`;
}

export function buildIngestionUploadStorageKey(params: {
  tenantId: string;
  ingestionId: string;
  fileId: string;
  uploadTokenId: string;
  filename: string;
}): string {
  return `tenants/${params.tenantId}/ingestions/${params.ingestionId}/original/${params.fileId}/uploads/${params.uploadTokenId}-${safeStorageKeySegment(params.filename)}`;
}

export function buildIngestionPreviewStorageKey(params: {
  tenantId: string;
  ingestionId: string;
  fileId: string;
  uploadTokenId: string;
  extension: string;
}): string {
  const safeExtension = safeStorageKeySegment(params.extension).replace(/^\./, "");
  return `tenants/${params.tenantId}/ingestions/${params.ingestionId}/preview/${params.fileId}/uploads/${params.uploadTokenId}.${safeExtension || "bin"}`;
}

export function buildObjectArtifactStorageKey(params: {
  tenantId: string;
  objectId: string;
  requestId: string;
  uploadTokenId?: string;
  artifactKind: string;
  variant: string | null;
  extension: string;
}): string {
  const safeVariant = params.variant ? `-${safeStorageKeySegment(params.variant)}` : "";
  const safeExtension = safeStorageKeySegment(params.extension).replace(/^\./, "");
  const uploadSegment = params.uploadTokenId ? `-${params.uploadTokenId}` : "";
  return `tenants/${params.tenantId}/objects/${params.objectId}/artifacts/${params.requestId}${uploadSegment}-${safeStorageKeySegment(params.artifactKind)}${safeVariant}.${safeExtension || "bin"}`;
}

export function createUploadToken(payload: UploadTokenPayload): string {
  const encodedPayload = toBase64Url(JSON.stringify(payload));
  const signature = sign(encodedPayload);
  return `${encodedPayload}.${signature}`;
}

export function parseUploadToken(token: string): UploadTokenPayload {
  const [encodedPayload, providedSignature, ...rest] = token.split(".");

  if (!encodedPayload || !providedSignature || rest.length > 0) {
    throw new UnauthorizedError("Upload token is invalid.");
  }

  const expectedSignature = sign(encodedPayload);

  if (!secureEquals(providedSignature, expectedSignature)) {
    throw new UnauthorizedError("Upload token signature is invalid.");
  }

  let payload: unknown;

  try {
    payload = JSON.parse(fromBase64Url(encodedPayload));
  } catch {
    throw new UnauthorizedError("Upload token payload is invalid.");
  }

  if (payload === null || typeof payload !== "object" || Array.isArray(payload)) {
    throw new UnauthorizedError("Upload token payload is invalid.");
  }

  const candidate = payload as Partial<UploadTokenPayload>;

  if (
    (candidate.purpose !== "ingestion_original" && candidate.purpose !== "ingestion_preview") ||
    typeof candidate.upload_token_id !== "string" ||
    typeof candidate.ingestion_id !== "string" ||
    typeof candidate.file_id !== "string" ||
    typeof candidate.tenant_id !== "string" ||
    typeof candidate.storage_key !== "string" ||
    typeof candidate.content_type !== "string" ||
    typeof candidate.size_bytes !== "number" ||
    !Number.isSafeInteger(candidate.size_bytes) ||
    candidate.size_bytes < 0 ||
    typeof candidate.expires_at !== "string"
  ) {
    throw new UnauthorizedError("Upload token payload is invalid.");
  }

  const expiresAt = new Date(candidate.expires_at);

  if (Number.isNaN(expiresAt.getTime()) || expiresAt.getTime() <= Date.now()) {
    throw new UnauthorizedError("Upload token has expired.");
  }

  return {
    purpose: candidate.purpose,
    upload_token_id: candidate.upload_token_id,
    ingestion_id: candidate.ingestion_id,
    file_id: candidate.file_id,
    tenant_id: candidate.tenant_id,
    storage_key: candidate.storage_key,
    content_type: candidate.content_type,
    size_bytes: candidate.size_bytes,
    expires_at: candidate.expires_at,
  };
}

export function createDownloadToken(payload: DownloadTokenPayload): string {
  const encodedPayload = toBase64Url(JSON.stringify(payload));
  const signature = sign(encodedPayload);
  return `${encodedPayload}.${signature}`;
}

export function parseDownloadToken(token: string): DownloadTokenPayload {
  const [encodedPayload, providedSignature, ...rest] = token.split(".");

  if (!encodedPayload || !providedSignature || rest.length > 0) {
    throw new UnauthorizedError("Download token is invalid.");
  }

  const expectedSignature = sign(encodedPayload);

  if (!secureEquals(providedSignature, expectedSignature)) {
    throw new UnauthorizedError("Download token signature is invalid.");
  }

  let payload: unknown;

  try {
    payload = JSON.parse(fromBase64Url(encodedPayload));
  } catch {
    throw new UnauthorizedError("Download token payload is invalid.");
  }

  if (payload === null || typeof payload !== "object" || Array.isArray(payload)) {
    throw new UnauthorizedError("Download token payload is invalid.");
  }

  const candidate = payload as Partial<DownloadTokenPayload>;

  if (
    typeof candidate.ingestion_id !== "string" ||
    typeof candidate.file_id !== "string" ||
    typeof candidate.tenant_id !== "string" ||
    typeof candidate.storage_key !== "string" ||
    typeof candidate.content_type !== "string" ||
    typeof candidate.size_bytes !== "number" ||
    !Number.isSafeInteger(candidate.size_bytes) ||
    candidate.size_bytes < 0 ||
    typeof candidate.expires_at !== "string"
  ) {
    throw new UnauthorizedError("Download token payload is invalid.");
  }

  const expiresAt = new Date(candidate.expires_at);

  if (Number.isNaN(expiresAt.getTime()) || expiresAt.getTime() <= Date.now()) {
    throw new UnauthorizedError("Download token has expired.");
  }

  return {
    ingestion_id: candidate.ingestion_id,
    file_id: candidate.file_id,
    tenant_id: candidate.tenant_id,
    storage_key: candidate.storage_key,
    content_type: candidate.content_type,
    size_bytes: candidate.size_bytes,
    expires_at: candidate.expires_at,
  };
}

export function createObjectArtifactUploadToken(
  payload: ObjectArtifactUploadTokenPayload,
): string {
  const encodedPayload = toBase64Url(JSON.stringify(payload));
  const signature = sign(encodedPayload);
  return `${encodedPayload}.${signature}`;
}

export function parseObjectArtifactUploadToken(
  token: string,
): ObjectArtifactUploadTokenPayload {
  const [encodedPayload, providedSignature, ...rest] = token.split(".");

  if (!encodedPayload || !providedSignature || rest.length > 0) {
    throw new UnauthorizedError("Upload token is invalid.");
  }

  const expectedSignature = sign(encodedPayload);

  if (!secureEquals(providedSignature, expectedSignature)) {
    throw new UnauthorizedError("Upload token signature is invalid.");
  }

  let payload: unknown;

  try {
    payload = JSON.parse(fromBase64Url(encodedPayload));
  } catch {
    throw new UnauthorizedError("Upload token payload is invalid.");
  }

  if (payload === null || typeof payload !== "object" || Array.isArray(payload)) {
    throw new UnauthorizedError("Upload token payload is invalid.");
  }

  const candidate = payload as Partial<ObjectArtifactUploadTokenPayload>;

  if (
    typeof candidate.upload_token_id !== "string" ||
    typeof candidate.request_id !== "string" ||
    typeof candidate.object_id !== "string" ||
    typeof candidate.tenant_id !== "string" ||
    typeof candidate.artifact_kind !== "string" ||
    typeof candidate.storage_key !== "string" ||
    typeof candidate.content_type !== "string" ||
    typeof candidate.size_bytes !== "number" ||
    !Number.isSafeInteger(candidate.size_bytes) ||
    candidate.size_bytes < 0 ||
    typeof candidate.expires_at !== "string"
  ) {
    throw new UnauthorizedError("Upload token payload is invalid.");
  }

  if (candidate.variant !== null && candidate.variant !== undefined && typeof candidate.variant !== "string") {
    throw new UnauthorizedError("Upload token payload is invalid.");
  }

  const expiresAt = new Date(candidate.expires_at);

  if (Number.isNaN(expiresAt.getTime()) || expiresAt.getTime() <= Date.now()) {
    throw new UnauthorizedError("Upload token has expired.");
  }

  return {
    upload_token_id: candidate.upload_token_id,
    request_id: candidate.request_id,
    object_id: candidate.object_id,
    tenant_id: candidate.tenant_id,
    artifact_kind: candidate.artifact_kind,
    variant: candidate.variant ?? null,
    storage_key: candidate.storage_key,
    content_type: candidate.content_type,
    size_bytes: candidate.size_bytes,
    expires_at: candidate.expires_at,
  };
}

export function resolveStagingPath(storageKey: string): string {
  const normalizedKey = normalize(storageKey).replace(/^[/\\]+/, "");

  if (normalizedKey.startsWith("..")) {
    throw new ValidationError("Storage key is invalid.");
  }

  return join(stagingRootPath(), normalizedKey);
}
