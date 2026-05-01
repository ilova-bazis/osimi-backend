import { ConflictError, ValidationError } from "../http/errors.ts";
import {
  authorizeWorkerLeaseForArchiveRequest,
  createArchiveRequestLeaseToken,
} from "../auth/worker-archive-request.ts";
import {
  finalizeArtifactFetchArchiveRequest,
  presignArchiveRequestArtifactUpload,
} from "./object-service.ts";
import {
  completeArchiveRequest,
  extendArchiveRequestLease,
  failArchiveRequest,
  leaseNextPendingArchiveRequest,
  releaseArchiveRequestLease,
  sweepExpiredArchiveRequestLeases,
  type ArchiveRequestActionType,
  type ArchiveRequestRecord,
  type ArchiveRequestTargetType,
} from "../repos/archive-request-repo.ts";
import type { JsonObject } from "../validation/ingestion.ts";
import type {
  WorkerCompleteArchiveRequestBody,
  WorkerPresignObjectArtifactUploadBody,
  WorkerPresignObjectArtifactUploadResponse,
} from "../validation/object.ts";

const DEFAULT_ARCHIVE_REQUEST_LEASE_TTL_SECONDS = 60 * 5;

export interface ArchiveRequestDto {
  id: string;
  tenant_id: string;
  target_type: ArchiveRequestTargetType;
  target_id: string;
  action_type: ArchiveRequestActionType;
  action_payload: JsonObject;
  requested_by: string;
  dedupe_key: string | null;
  status: "PENDING" | "PROCESSING" | "COMPLETED" | "FAILED" | "CANCELED";
  failure_reason: string | null;
  failure_details: JsonObject | null;
  created_at: string;
  updated_at: string;
  completed_at: string | null;
}

export interface WorkerLeaseArchiveRequestResponse {
  request:
    | null
    | {
        request_id: string;
        lease_id: string;
        lease_token: string;
        lease_expires_at: string;
        tenant_id: string;
        target_type: ArchiveRequestTargetType;
        target_id: string;
        action_type: ArchiveRequestActionType;
        action_payload: JsonObject;
        requested_by: string;
        dedupe_key: string | null;
      };
}

export interface WorkerHeartbeatArchiveRequestResponse {
  request: {
    request_id: string;
    lease_id: string;
    lease_token: string;
    lease_expires_at: string;
  };
}

export interface WorkerReleaseArchiveRequestResponse {
  status: "ok";
  request_id: string;
}

export interface WorkerCompleteArchiveRequestResponse {
  status: "completed";
  request: ArchiveRequestDto;
}

export interface WorkerFailArchiveRequestBody {
  lease_token: string;
  failure: {
    code: string;
    message: string;
    retryable: boolean;
    details?: JsonObject;
  };
}

export interface WorkerFailArchiveRequestResponse {
  status: "failed";
  request_id: string;
  retryable: boolean;
}

export async function presignArchiveRequestArtifactByWorker(params: {
  requestId: string;
  body: WorkerPresignObjectArtifactUploadBody;
}): Promise<WorkerPresignObjectArtifactUploadResponse> {
  return await presignArchiveRequestArtifactUpload(params);
}

function archiveRequestLeaseTtlSeconds(): number {
  return DEFAULT_ARCHIVE_REQUEST_LEASE_TTL_SECONDS;
}

function serializeArchiveRequest(record: ArchiveRequestRecord): ArchiveRequestDto {
  return {
    id: record.id,
    tenant_id: record.tenantId,
    target_type: record.targetType,
    target_id: record.targetId,
    action_type: record.actionType,
    action_payload: record.actionPayload,
    requested_by: record.requestedBy,
    dedupe_key: record.dedupeKey,
    status: record.status,
    failure_reason: record.failureReason,
    failure_details: record.failureDetails,
    created_at: record.createdAt.toISOString(),
    updated_at: record.updatedAt.toISOString(),
    completed_at: record.completedAt ? record.completedAt.toISOString() : null,
  };
}

export async function leaseNextArchiveRequest(params: {
  workerId?: string;
  actionType?: ArchiveRequestActionType;
}): Promise<WorkerLeaseArchiveRequestResponse> {
  await sweepExpiredArchiveRequestLeases();

  const lease = await leaseNextPendingArchiveRequest({
    workerId: params.workerId,
    leaseDurationSeconds: archiveRequestLeaseTtlSeconds(),
    actionType: params.actionType,
  });

  if (!lease) {
    return { request: null };
  }

  const leaseToken = createArchiveRequestLeaseToken({
    request_id: lease.request.id,
    lease_id: lease.leaseId,
    lease_token_id: lease.leaseTokenId,
    tenant_id: lease.request.tenantId,
    target_type: lease.request.targetType,
    target_id: lease.request.targetId,
    action_type: lease.request.actionType,
    worker_id: params.workerId,
    exp: lease.leaseExpiresAt.toISOString(),
  });

  return {
    request: {
      request_id: lease.request.id,
      lease_id: lease.leaseId,
      lease_token: leaseToken,
      lease_expires_at: lease.leaseExpiresAt.toISOString(),
      tenant_id: lease.request.tenantId,
      target_type: lease.request.targetType,
      target_id: lease.request.targetId,
      action_type: lease.request.actionType,
      action_payload: lease.request.actionPayload,
      requested_by: lease.request.requestedBy,
      dedupe_key: lease.request.dedupeKey,
    },
  };
}

export async function heartbeatArchiveRequestLease(params: {
  requestId: string;
  leaseToken: string;
}): Promise<WorkerHeartbeatArchiveRequestResponse> {
  const authorizedLease = await authorizeWorkerLeaseForArchiveRequest({
    requestId: params.requestId,
    leaseToken: params.leaseToken,
  });

  const updated = await extendArchiveRequestLease({
    requestId: authorizedLease.requestId,
    leaseId: authorizedLease.leaseId,
    leaseTokenId: authorizedLease.leaseTokenId,
    leaseDurationSeconds: archiveRequestLeaseTtlSeconds(),
  });

  if (!updated) {
    throw new ConflictError("Lease is no longer active.");
  }

  const refreshedToken = createArchiveRequestLeaseToken({
    request_id: updated.request.id,
    lease_id: updated.leaseId,
    lease_token_id: updated.leaseTokenId,
    tenant_id: updated.request.tenantId,
    target_type: updated.request.targetType,
    target_id: updated.request.targetId,
    action_type: updated.request.actionType,
    worker_id: authorizedLease.workerId,
    exp: updated.leaseExpiresAt.toISOString(),
  });

  return {
    request: {
      request_id: updated.request.id,
      lease_id: updated.leaseId,
      lease_token: refreshedToken,
      lease_expires_at: updated.leaseExpiresAt.toISOString(),
    },
  };
}

export async function releaseArchiveRequestLeaseByToken(params: {
  requestId: string;
  leaseToken: string;
}): Promise<WorkerReleaseArchiveRequestResponse> {
  const authorizedLease = await authorizeWorkerLeaseForArchiveRequest({
    requestId: params.requestId,
    leaseToken: params.leaseToken,
  });

  const released = await releaseArchiveRequestLease({
    requestId: authorizedLease.requestId,
    leaseId: authorizedLease.leaseId,
    leaseTokenId: authorizedLease.leaseTokenId,
  });

  if (!released) {
    throw new ConflictError("Lease is no longer active.");
  }

  return {
    status: "ok",
    request_id: released.id,
  };
}

export async function completeArchiveRequestByWorker(params: {
  requestId: string;
  body: WorkerCompleteArchiveRequestBody;
}): Promise<WorkerCompleteArchiveRequestResponse> {
  const authorizedLease = await authorizeWorkerLeaseForArchiveRequest({
    requestId: params.requestId,
    leaseToken: params.body.lease_token,
  });

  if (authorizedLease.actionType === "artifact_fetch") {
    if (!params.body.upload_token) {
      throw new ValidationError(
        "Field 'upload_token' is required when completing artifact_fetch requests.",
      );
    }

    await finalizeArtifactFetchArchiveRequest({
      requestId: params.requestId,
      leaseToken: params.body.lease_token,
      uploadToken: params.body.upload_token,
    });
  }

  const completed = await completeArchiveRequest({
    requestId: authorizedLease.requestId,
    leaseId: authorizedLease.leaseId,
    leaseTokenId: authorizedLease.leaseTokenId,
  });

  if (!completed) {
    throw new ConflictError("Lease is no longer active.");
  }

  return {
    status: "completed",
    request: serializeArchiveRequest(completed),
  };
}

export async function failArchiveRequestByWorker(params: {
  requestId: string;
  body: WorkerFailArchiveRequestBody;
}): Promise<WorkerFailArchiveRequestResponse> {
  const authorizedLease = await authorizeWorkerLeaseForArchiveRequest({
    requestId: params.requestId,
    leaseToken: params.body.lease_token,
  });

  const failed = await failArchiveRequest({
    requestId: authorizedLease.requestId,
    leaseId: authorizedLease.leaseId,
    leaseTokenId: authorizedLease.leaseTokenId,
    failureReason: params.body.failure.message,
    failureDetails: {
      code: params.body.failure.code,
      message: params.body.failure.message,
      retryable: params.body.failure.retryable,
      details: params.body.failure.details ?? {},
    },
  });

  if (!failed) {
    throw new ConflictError("Lease is no longer active.");
  }

  return {
    status: "failed",
    request_id: failed.id,
    retryable: params.body.failure.retryable,
  };
}
