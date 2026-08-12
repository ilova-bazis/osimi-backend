import { createHmac, timingSafeEqual } from "node:crypto";

import { ConflictError, UnauthorizedError } from "../http/errors.ts";
import {
  findActiveArchiveRequestLeaseByToken,
  type ArchiveRequestActionType,
  type ArchiveRequestTargetType,
} from "../repos/archive-request-repo.ts";
import { resolveLeaseSigningSecret } from "../runtime/config.ts";

export interface ArchiveRequestLeaseTokenPayload {
  request_id: string;
  lease_id: string;
  lease_token_id: string;
  tenant_id: string;
  target_type: ArchiveRequestTargetType;
  target_id: string;
  action_type: ArchiveRequestActionType;
  worker_id?: string;
  exp: string;
}

export interface AuthorizedWorkerArchiveRequestLease {
  requestId: string;
  leaseId: string;
  leaseTokenId: string;
  tenantId: string;
  targetType: ArchiveRequestTargetType;
  targetId: string;
  actionType: ArchiveRequestActionType;
  workerId?: string;
}

function leaseSigningSecret(): string {
  return resolveLeaseSigningSecret();
}

function encodePayload(value: ArchiveRequestLeaseTokenPayload): string {
  return Buffer.from(JSON.stringify(value), "utf8").toString("base64url");
}

function decodePayload(value: string): ArchiveRequestLeaseTokenPayload {
  let parsed: unknown;

  try {
    parsed = JSON.parse(Buffer.from(value, "base64url").toString("utf8"));
  } catch {
    throw new UnauthorizedError("Archive request lease token payload is invalid.");
  }

  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new UnauthorizedError("Archive request lease token payload is invalid.");
  }

  const candidate = parsed as Partial<ArchiveRequestLeaseTokenPayload>;

  if (
    typeof candidate.request_id !== "string" ||
    typeof candidate.lease_id !== "string" ||
    typeof candidate.lease_token_id !== "string" ||
    typeof candidate.tenant_id !== "string" ||
    typeof candidate.target_type !== "string" ||
    typeof candidate.target_id !== "string" ||
    typeof candidate.action_type !== "string" ||
    typeof candidate.exp !== "string"
  ) {
    throw new UnauthorizedError("Archive request lease token payload is invalid.");
  }

  if (
    candidate.worker_id !== undefined &&
    typeof candidate.worker_id !== "string"
  ) {
    throw new UnauthorizedError("Archive request lease token payload is invalid.");
  }

  return {
    request_id: candidate.request_id,
    lease_id: candidate.lease_id,
    lease_token_id: candidate.lease_token_id,
    tenant_id: candidate.tenant_id,
    target_type: candidate.target_type as ArchiveRequestTargetType,
    target_id: candidate.target_id,
    action_type: candidate.action_type as ArchiveRequestActionType,
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

export function createArchiveRequestLeaseToken(
  payload: ArchiveRequestLeaseTokenPayload,
): string {
  const encodedPayload = encodePayload(payload);
  const signature = signPayload(encodedPayload);
  return `${encodedPayload}.${signature}`;
}

export function parseArchiveRequestLeaseToken(
  token: string,
  options: { allowExpired?: boolean } = {},
): ArchiveRequestLeaseTokenPayload {
  const [encodedPayload, signature, ...rest] = token.split(".");

  if (!encodedPayload || !signature || rest.length > 0) {
    throw new UnauthorizedError("Archive request lease token is invalid.");
  }

  const expectedSignature = signPayload(encodedPayload);

  if (!secureEquals(signature, expectedSignature)) {
    throw new UnauthorizedError(
      "Archive request lease token signature is invalid.",
    );
  }

  const payload = decodePayload(encodedPayload);
  const expiresAt = new Date(payload.exp);

  if (Number.isNaN(expiresAt.getTime()) || (!options.allowExpired && expiresAt.getTime() <= Date.now())) {
    throw new UnauthorizedError("Archive request lease token has expired.");
  }

  return payload;
}

export async function authorizeWorkerLeaseForArchiveRequest(params: {
  requestId: string;
  leaseToken: string;
  requireActiveLease?: boolean;
  allowExpired?: boolean;
}): Promise<AuthorizedWorkerArchiveRequestLease> {
  const payload = parseArchiveRequestLeaseToken(params.leaseToken, {
    allowExpired: params.allowExpired,
  });

  if (payload.request_id !== params.requestId) {
    throw new UnauthorizedError(
      "Lease token does not match archive request id.",
    );
  }

  if (params.requireActiveLease ?? true) {
    const activeRequest = await findActiveArchiveRequestLeaseByToken({
      requestId: payload.request_id,
      leaseId: payload.lease_id,
      leaseTokenId: payload.lease_token_id,
    });

    if (!activeRequest) {
      throw new ConflictError("Lease is no longer active.");
    }
  }

  return {
    requestId: payload.request_id,
    leaseId: payload.lease_id,
    leaseTokenId: payload.lease_token_id,
    tenantId: payload.tenant_id,
    targetType: payload.target_type,
    targetId: payload.target_id,
    actionType: payload.action_type,
    workerId: payload.worker_id,
  };
}
