import { createHmac, timingSafeEqual } from "node:crypto";

import { ConflictError, UnauthorizedError } from "../http/errors.ts";
import { findActiveArchiveRequestLeaseByToken } from "../repos/archive-request-repo.ts";
import { resolveLeaseSigningSecret } from "../runtime/config.ts";
import type { ArtifactKind } from "../repos/object-repo.ts";

export interface DownloadRequestLeaseTokenPayload {
  request_id: string;
  lease_id: string;
  lease_token_id: string;
  object_id: string;
  tenant_id: string;
  artifact_kind: ArtifactKind;
  variant: string | null;
  worker_id?: string;
  exp: string;
}

export interface AuthorizedWorkerDownloadRequestLease {
  requestId: string;
  leaseId: string;
  leaseTokenId: string;
  objectId: string;
  tenantId: string;
  artifactKind: ArtifactKind;
  variant: string | null;
  workerId?: string;
}

function leaseSigningSecret(): string {
  return resolveLeaseSigningSecret();
}

function encodePayload(value: DownloadRequestLeaseTokenPayload): string {
  return Buffer.from(JSON.stringify(value), "utf8").toString("base64url");
}

function decodePayload(value: string): DownloadRequestLeaseTokenPayload {
  let parsed: unknown;

  try {
    parsed = JSON.parse(Buffer.from(value, "base64url").toString("utf8"));
  } catch {
    throw new UnauthorizedError("Download request lease token payload is invalid.");
  }

  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new UnauthorizedError("Download request lease token payload is invalid.");
  }

  const candidate = parsed as Partial<DownloadRequestLeaseTokenPayload>;

  if (
    typeof candidate.request_id !== "string" ||
    typeof candidate.lease_id !== "string" ||
    typeof candidate.lease_token_id !== "string" ||
    typeof candidate.object_id !== "string" ||
    typeof candidate.tenant_id !== "string" ||
    typeof candidate.artifact_kind !== "string" ||
    typeof candidate.exp !== "string"
  ) {
    throw new UnauthorizedError("Download request lease token payload is invalid.");
  }

  if (candidate.variant !== null && candidate.variant !== undefined && typeof candidate.variant !== "string") {
    throw new UnauthorizedError("Download request lease token payload is invalid.");
  }

  if (candidate.worker_id !== undefined && typeof candidate.worker_id !== "string") {
    throw new UnauthorizedError("Download request lease token payload is invalid.");
  }

  return {
    request_id: candidate.request_id,
    lease_id: candidate.lease_id,
    lease_token_id: candidate.lease_token_id,
    object_id: candidate.object_id,
    tenant_id: candidate.tenant_id,
    artifact_kind: candidate.artifact_kind as ArtifactKind,
    variant: candidate.variant ?? null,
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

export function createDownloadRequestLeaseToken(
  payload: DownloadRequestLeaseTokenPayload,
): string {
  const encodedPayload = encodePayload(payload);
  const signature = signPayload(encodedPayload);
  return `${encodedPayload}.${signature}`;
}

export function parseDownloadRequestLeaseToken(
  token: string,
  options: { allowExpired?: boolean } = {},
): DownloadRequestLeaseTokenPayload {
  const [encodedPayload, signature, ...rest] = token.split(".");

  if (!encodedPayload || !signature || rest.length > 0) {
    throw new UnauthorizedError("Download request lease token is invalid.");
  }

  const expectedSignature = signPayload(encodedPayload);

  if (!secureEquals(signature, expectedSignature)) {
    throw new UnauthorizedError(
      "Download request lease token signature is invalid.",
    );
  }

  const payload = decodePayload(encodedPayload);
  const expiresAt = new Date(payload.exp);

  if (Number.isNaN(expiresAt.getTime()) || (!options.allowExpired && expiresAt.getTime() <= Date.now())) {
    throw new UnauthorizedError("Download request lease token has expired.");
  }

  return payload;
}

export async function authorizeWorkerLeaseForDownloadRequest(params: {
  requestId: string;
  leaseToken: string;
  requireActiveLease?: boolean;
  allowExpired?: boolean;
}): Promise<AuthorizedWorkerDownloadRequestLease> {
  const payload = parseDownloadRequestLeaseToken(params.leaseToken, {
    allowExpired: params.allowExpired,
  });

  if (payload.request_id !== params.requestId) {
    throw new UnauthorizedError(
      "Lease token does not match object download request id.",
    );
  }

  if (params.requireActiveLease ?? true) {
    const activeRequest = await findActiveArchiveRequestLeaseByToken({
      requestId: payload.request_id,
      leaseId: payload.lease_id,
      leaseTokenId: payload.lease_token_id,
    });

    if (!activeRequest || activeRequest.actionType !== "artifact_fetch") {
      throw new ConflictError("Lease is no longer active.");
    }
  }

  return {
    requestId: payload.request_id,
    leaseId: payload.lease_id,
    leaseTokenId: payload.lease_token_id,
    objectId: payload.object_id,
    tenantId: payload.tenant_id,
    artifactKind: payload.artifact_kind,
    variant: payload.variant ?? null,
    workerId: payload.worker_id,
  };
}
