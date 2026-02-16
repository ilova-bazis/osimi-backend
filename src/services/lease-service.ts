import { createHmac, timingSafeEqual } from "node:crypto";

import {
  ConflictError,
  NotFoundError,
  UnauthorizedError,
  ValidationError,
} from "../http/errors.ts";
import {
  findIngestionById,
  listIngestionFiles,
  updateIngestionStatus,
} from "../repos/ingestion-repo.ts";
import {
  extendLease,
  leaseNextQueuedIngestion,
  releaseLease,
  sweepExpiredLeases,
} from "../repos/lease-repo.ts";
import {
  createDownloadToken,
  parseDownloadToken,
  resolveStagingPath,
} from "../storage/index.ts";
import { getRuntimeConfig } from "../runtime/config.ts";

const DEFAULT_LEASE_TTL_SECONDS = 60 * 5;
const DEFAULT_LEASE_SIGNING_SECRET = "dev-local-lease-secret";

export interface LeaseTokenPayload {
  lease_id: string;
  lease_token_id: string;
  ingestion_id: string;
  tenant_id: string;
  worker_id?: string;
  exp: string;
}

function leaseSigningSecret(): string {
  const runtimeLeaseSigningSecret = getRuntimeConfig().leaseSigningSecret;
  return (
    runtimeLeaseSigningSecret?.trim() ||
    process.env.LEASE_SIGNING_SECRET?.trim() ||
    DEFAULT_LEASE_SIGNING_SECRET
  );
}

function encodePayload(value: LeaseTokenPayload): string {
  return Buffer.from(JSON.stringify(value), "utf8").toString("base64url");
}

function decodePayload(value: string): LeaseTokenPayload {
  let parsed: unknown;

  try {
    parsed = JSON.parse(Buffer.from(value, "base64url").toString("utf8"));
  } catch {
    throw new UnauthorizedError("Lease token payload is invalid.");
  }

  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new UnauthorizedError("Lease token payload is invalid.");
  }

  const candidate = parsed as Partial<LeaseTokenPayload>;

  if (
    typeof candidate.lease_id !== "string" ||
    typeof candidate.lease_token_id !== "string" ||
    typeof candidate.ingestion_id !== "string" ||
    typeof candidate.tenant_id !== "string" ||
    typeof candidate.exp !== "string"
  ) {
    throw new UnauthorizedError("Lease token payload is invalid.");
  }

  if (
    candidate.worker_id !== undefined &&
    typeof candidate.worker_id !== "string"
  ) {
    throw new UnauthorizedError("Lease token payload is invalid.");
  }

  return {
    lease_id: candidate.lease_id,
    lease_token_id: candidate.lease_token_id,
    ingestion_id: candidate.ingestion_id,
    tenant_id: candidate.tenant_id,
    worker_id: candidate.worker_id,
    exp: candidate.exp,
  };
}

function signPayload(encodedPayload: string): string {
  return createHmac("sha256", leaseSigningSecret())
    .update(encodedPayload)
    .digest("base64url");
}

function secureEquals(left: string, right: string): boolean {
  const leftBuffer = Buffer.from(left, "utf8");
  const rightBuffer = Buffer.from(right, "utf8");

  if (leftBuffer.length !== rightBuffer.length) {
    return false;
  }

  return timingSafeEqual(leftBuffer, rightBuffer);
}

function createLeaseToken(payload: LeaseTokenPayload): string {
  const encodedPayload = encodePayload(payload);
  const signature = signPayload(encodedPayload);
  return `${encodedPayload}.${signature}`;
}

export function parseLeaseToken(token: string): LeaseTokenPayload {
  const [encodedPayload, signature, ...rest] = token.split(".");

  if (!encodedPayload || !signature || rest.length > 0) {
    throw new UnauthorizedError("Lease token is invalid.");
  }

  const expectedSignature = signPayload(encodedPayload);

  if (!secureEquals(signature, expectedSignature)) {
    throw new UnauthorizedError("Lease token signature is invalid.");
  }

  const payload = decodePayload(encodedPayload);
  const expiresAt = new Date(payload.exp);

  if (Number.isNaN(expiresAt.getTime()) || expiresAt.getTime() <= Date.now()) {
    throw new UnauthorizedError("Lease token has expired.");
  }

  return payload;
}

function buildDownloadUrls(params: {
  tenantId: string;
  ingestionId: string;
  files: Array<{
    id: string;
    storageKey: string;
    contentType: string;
    sizeBytes: number;
    status: string;
  }>;
  expiresAt: Date;
}): Array<Record<string, unknown>> {
  return params.files
    .filter((file) => file.status === "UPLOADED" || file.status === "VALIDATED")
    .map((file) => {
      const token = createDownloadToken({
        ingestion_id: params.ingestionId,
        file_id: file.id,
        tenant_id: params.tenantId,
        storage_key: file.storageKey,
        content_type: file.contentType,
        size_bytes: file.sizeBytes,
        expires_at: params.expiresAt.toISOString(),
      });

      return {
        file_id: file.id,
        storage_key: file.storageKey,
        content_type: file.contentType,
        size_bytes: file.sizeBytes,
        download_url: `/api/worker/downloads/${token}`,
      };
    });
}

function leaseTtlSeconds(): number {
  return DEFAULT_LEASE_TTL_SECONDS;
}

export async function leaseNextIngestion(params: {
  workerId?: string;
}): Promise<Record<string, unknown>> {
  await sweepExpiredLeases();

  const leaseResult = await leaseNextQueuedIngestion({
    workerId: params.workerId,
    leaseDurationSeconds: leaseTtlSeconds(),
  });

  if (!leaseResult) {
    return {
      lease: null,
    };
  }

  const ingestionFiles = await listIngestionFiles({
    tenantId: leaseResult.ingestion.tenantId,
    ingestionId: leaseResult.ingestion.id,
  });

  const leaseToken = createLeaseToken({
    lease_id: leaseResult.lease.id,
    lease_token_id: leaseResult.lease.leaseTokenId,
    ingestion_id: leaseResult.ingestion.id,
    tenant_id: leaseResult.ingestion.tenantId,
    worker_id: params.workerId,
    exp: leaseResult.lease.leaseExpiresAt.toISOString(),
  });

  return {
    lease: {
      lease_id: leaseResult.lease.id,
      lease_token: leaseToken,
      lease_expires_at: leaseResult.lease.leaseExpiresAt.toISOString(),
      ingestion_id: leaseResult.ingestion.id,
      batch_label: leaseResult.ingestion.batchLabel,
      tenant_id: leaseResult.ingestion.tenantId,
      download_urls: buildDownloadUrls({
        tenantId: leaseResult.ingestion.tenantId,
        ingestionId: leaseResult.ingestion.id,
        files: ingestionFiles,
        expiresAt: leaseResult.lease.leaseExpiresAt,
      }),
    },
  };
}

function requireLeaseToken(body: unknown): string {
  if (body === null || typeof body !== "object" || Array.isArray(body)) {
    throw new ValidationError("Request body must be an object.");
  }

  const token = (body as Record<string, unknown>).lease_token;

  if (typeof token !== "string" || token.trim().length === 0) {
    throw new ValidationError(
      "Field 'lease_token' must be a non-empty string.",
    );
  }

  return token;
}

export async function heartbeatLease(params: {
  ingestionId: string;
  body: unknown;
}): Promise<Record<string, unknown>> {
  const leaseToken = requireLeaseToken(params.body);
  const payload = parseLeaseToken(leaseToken);

  if (payload.ingestion_id !== params.ingestionId) {
    throw new UnauthorizedError("Lease token does not match ingestion id.");
  }

  const updatedLease = await extendLease({
    ingestionId: payload.ingestion_id,
    leaseId: payload.lease_id,
    leaseTokenId: payload.lease_token_id,
    leaseDurationSeconds: leaseTtlSeconds(),
  });

  if (!updatedLease) {
    throw new ConflictError("Lease is no longer active.");
  }

  const ingestion = await findIngestionById(
    payload.tenant_id,
    payload.ingestion_id,
  );

  if (!ingestion) {
    throw new NotFoundError(
      `Ingestion '${payload.ingestion_id}' was not found.`,
    );
  }

  const ingestionFiles = await listIngestionFiles({
    tenantId: payload.tenant_id,
    ingestionId: payload.ingestion_id,
  });

  const refreshedToken = createLeaseToken({
    lease_id: updatedLease.id,
    lease_token_id: updatedLease.leaseTokenId,
    ingestion_id: payload.ingestion_id,
    tenant_id: payload.tenant_id,
    worker_id: payload.worker_id,
    exp: updatedLease.leaseExpiresAt.toISOString(),
  });

  return {
    lease: {
      lease_id: updatedLease.id,
      lease_token: refreshedToken,
      lease_expires_at: updatedLease.leaseExpiresAt.toISOString(),
      ingestion_id: payload.ingestion_id,
      batch_label: ingestion.batchLabel,
      tenant_id: payload.tenant_id,
      download_urls: buildDownloadUrls({
        tenantId: payload.tenant_id,
        ingestionId: payload.ingestion_id,
        files: ingestionFiles,
        expiresAt: updatedLease.leaseExpiresAt,
      }),
    },
  };
}

export async function releaseActiveLease(params: {
  ingestionId: string;
  body: unknown;
}): Promise<Record<string, unknown>> {
  const leaseToken = requireLeaseToken(params.body);
  const payload = parseLeaseToken(leaseToken);

  if (payload.ingestion_id !== params.ingestionId) {
    throw new UnauthorizedError("Lease token does not match ingestion id.");
  }

  const released = await releaseLease({
    ingestionId: payload.ingestion_id,
    leaseId: payload.lease_id,
    leaseTokenId: payload.lease_token_id,
  });

  if (!released) {
    throw new ConflictError("Lease is no longer active.");
  }

  const ingestion = await findIngestionById(
    payload.tenant_id,
    payload.ingestion_id,
  );

  if (!ingestion) {
    throw new NotFoundError(
      `Ingestion '${payload.ingestion_id}' was not found.`,
    );
  }

  if (ingestion.status === "PROCESSING") {
    await updateIngestionStatus({
      ingestionId: ingestion.id,
      tenantId: ingestion.tenantId,
      fromStatus: ingestion.status,
      toStatus: "QUEUED",
    });
  }

  return {
    status: "ok",
    ingestion_id: payload.ingestion_id,
    lease_id: payload.lease_id,
  };
}

export async function downloadStagedFileByToken(params: {
  token: string;
}): Promise<Response> {
  const payload = parseDownloadToken(params.token);
  const filePath = resolveStagingPath(payload.storage_key);
  const file = Bun.file(filePath);

  if (!(await file.exists())) {
    throw new NotFoundError("Requested staged file was not found.");
  }

  return new Response(file, {
    status: 200,
    headers: {
      "content-type": payload.content_type,
      "content-length": String(payload.size_bytes),
      "accept-ranges": "bytes",
    },
  });
}
