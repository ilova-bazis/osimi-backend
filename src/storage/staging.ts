import { createHmac } from "node:crypto";
import { join, normalize } from "node:path";

import { UnauthorizedError, ValidationError } from "../http/errors.ts";

const DEFAULT_STAGING_ROOT = ".staging";
const DEFAULT_SIGNING_SECRET = "dev-local-signing-secret";

interface UploadTokenPayload {
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

function getSigningSecret(): string {
  return process.env.UPLOAD_SIGNING_SECRET?.trim() || DEFAULT_SIGNING_SECRET;
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
  return process.env.STAGING_ROOT?.trim() || DEFAULT_STAGING_ROOT;
}

export function buildStagingStorageKey(params: {
  tenantId: string;
  ingestionId: string;
  fileId: string;
  filename: string;
}): string {
  return `tenants/${params.tenantId}/ingestions/${params.ingestionId}/original/${params.fileId}-${safeStorageKeySegment(params.filename)}`;
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

  if (providedSignature !== expectedSignature) {
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
    typeof candidate.ingestion_id !== "string" ||
    typeof candidate.file_id !== "string" ||
    typeof candidate.tenant_id !== "string" ||
    typeof candidate.storage_key !== "string" ||
    typeof candidate.content_type !== "string" ||
    typeof candidate.size_bytes !== "number" ||
    typeof candidate.expires_at !== "string"
  ) {
    throw new UnauthorizedError("Upload token payload is invalid.");
  }

  const expiresAt = new Date(candidate.expires_at);

  if (Number.isNaN(expiresAt.getTime()) || expiresAt.getTime() <= Date.now()) {
    throw new UnauthorizedError("Upload token has expired.");
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

  if (providedSignature !== expectedSignature) {
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

export function resolveStagingPath(storageKey: string): string {
  const normalizedKey = normalize(storageKey).replace(/^[/\\]+/, "");

  if (normalizedKey.startsWith("..")) {
    throw new ValidationError("Storage key is invalid.");
  }

  return join(stagingRootPath(), normalizedKey);
}
