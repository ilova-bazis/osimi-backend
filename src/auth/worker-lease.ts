import { createHmac, timingSafeEqual } from "node:crypto";

import { ConflictError, UnauthorizedError } from "../http/errors.ts";
import { findActiveLeaseByToken } from "../repos/lease-repo.ts";
import { getRuntimeConfig } from "../runtime/config.ts";

const DEFAULT_LEASE_SIGNING_SECRET = "dev-local-lease-secret";

export interface LeaseTokenPayload {
  lease_id: string;
  lease_token_id: string;
  ingestion_id: string;
  tenant_id: string;
  worker_id?: string;
  exp: string;
}

export interface AuthorizedWorkerLease {
  ingestionId: string;
  tenantId: string;
  leaseId: string;
  leaseTokenId: string;
  workerId?: string;
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

export function createLeaseToken(payload: LeaseTokenPayload): string {
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

export async function authorizeWorkerLeaseForIngestion(params: {
  ingestionId: string;
  leaseToken: string;
  requireActiveLease?: boolean;
}): Promise<AuthorizedWorkerLease> {
  const payload = parseLeaseToken(params.leaseToken);

  if (payload.ingestion_id !== params.ingestionId) {
    throw new UnauthorizedError("Lease token does not match ingestion id.");
  }

  if (params.requireActiveLease ?? true) {
    const activeLease = await findActiveLeaseByToken({
      ingestionId: payload.ingestion_id,
      leaseId: payload.lease_id,
      leaseTokenId: payload.lease_token_id,
    });

    if (!activeLease) {
      throw new ConflictError("Lease is no longer active.");
    }
  }

  return {
    ingestionId: payload.ingestion_id,
    tenantId: payload.tenant_id,
    leaseId: payload.lease_id,
    leaseTokenId: payload.lease_token_id,
    workerId: payload.worker_id,
  };
}
