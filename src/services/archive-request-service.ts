import { createHash } from "node:crypto";

import { ConflictError, NotFoundError, ValidationError } from "../http/errors.ts";
import {
  authorizeWorkerLeaseForArchiveRequest,
  createArchiveRequestLeaseToken,
  parseArchiveRequestLeaseToken,
} from "../auth/worker-archive-request.ts";
import {
  finalizeArtifactFetchArchiveRequest,
  presignArchiveRequestArtifactUpload,
} from "./object-service.ts";
import {
  completeArchiveRequest,
  extendArchiveRequestLease,
  failArchiveRequest,
  findArchiveRequestById,
  leaseNextPendingArchiveRequest,
  releaseArchiveRequestLease,
  sweepExpiredArchiveRequestLeases,
  type ArchiveRequestActionType,
  type ArchiveRequestRecord,
  type ArchiveRequestTargetType,
} from "../repos/archive-request-repo.ts";
import { findCurationPublicationByRequestId } from "../repos/curation-publication-repo.ts";
import { findArchiveRequestSourceByRequestId } from "../repos/archive-request-source-repo.ts";
import { resolveStagingPath } from "../storage/staging.ts";
import type { JsonObject } from "../validation/ingestion.ts";
import {
  completeObjectRevisionApplyByWorker,
} from "./object-change-submission-service.ts";
import {
  parseWorkerObjectRevisionApplyResult,
  type WorkerCompleteArchiveRequestBody,
  type WorkerPresignObjectArtifactUploadBody,
  type WorkerPresignObjectArtifactUploadResponse,
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

export async function downloadArchiveRequestSource(params: {
  requestId: string;
  leaseToken: string;
  workerId?: string;
}): Promise<Response> {
  const authorizedLease = await authorizeWorkerLeaseForArchiveRequest({
    requestId: params.requestId,
    leaseToken: params.leaseToken,
  });
  if (
    authorizedLease.actionType !== "curation_apply" &&
    authorizedLease.actionType !== "object_revision_apply"
  ) {
    throw new ConflictError("Archive request does not expose a downloadable source.");
  }
  if (
    params.workerId &&
    authorizedLease.workerId &&
    params.workerId !== authorizedLease.workerId
  ) {
    throw new ConflictError("Lease belongs to a different worker.");
  }

  if (authorizedLease.actionType === "object_revision_apply") {
    return await serveArchiveRequestSourceFile({
      requestId: params.requestId,
      loadSource: async () => {
        const source = await findArchiveRequestSourceByRequestId({
          requestId: params.requestId,
        });
        if (!source) {
          return undefined;
        }
        return {
          storageKey: source.storageKey,
          contentType: source.contentType,
          sizeBytes: source.sizeBytes,
          checksumSha256: source.checksumSha256,
          purged: source.purgedAt !== null,
        };
      },
      missingMessage: "Archive request source was not found.",
    });
  }

  const publication = await findCurationPublicationByRequestId({
    requestId: params.requestId,
  });
  return await serveArchiveRequestSourceFile({
    requestId: params.requestId,
    loadSource: async () => {
      if (!publication) {
        return undefined;
      }
      return {
        storageKey: publication.storageKey,
        contentType: publication.contentType,
        sizeBytes: publication.sizeBytes,
        checksumSha256: publication.checksumSha256,
        purged: publication.purgedAt !== null,
      };
    },
    missingMessage: "Curation publication source was not found.",
  });
}

async function serveArchiveRequestSourceFile(params: {
  requestId: string;
  loadSource: () => Promise<
    | {
        storageKey: string;
        contentType: string;
        sizeBytes: number;
        checksumSha256: string;
        purged: boolean;
      }
    | undefined
  >;
  missingMessage: string;
}): Promise<Response> {
  const source = await params.loadSource();
  if (!source || source.purged) {
    throw new NotFoundError(params.missingMessage);
  }

  const file = Bun.file(resolveStagingPath(source.storageKey));
  if (!(await file.exists())) {
    throw new NotFoundError(params.missingMessage);
  }
  if (file.size !== source.sizeBytes) {
    throw new ConflictError("Archive request source size does not match its checkpoint.");
  }
  const bytes = await file.arrayBuffer();
  const checksum = createHash("sha256").update(new Uint8Array(bytes)).digest("hex");
  if (checksum !== source.checksumSha256) {
    throw new ConflictError("Archive request source checksum does not match its checkpoint.");
  }

  return new Response(bytes, {
    status: 200,
    headers: {
      "content-type": source.contentType,
      "content-length": String(source.sizeBytes),
      "x-content-sha256": source.checksumSha256,
      "cache-control": "no-store",
    },
  });
}

export async function downloadCurationPublicationSource(params: {
  requestId: string;
  leaseToken: string;
  workerId?: string;
}): Promise<Response> {
  return await downloadArchiveRequestSource(params);
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
  const tokenPayload = parseArchiveRequestLeaseToken(params.body.lease_token, {
    allowExpired: true,
  });
  const authorizedLease = await authorizeWorkerLeaseForArchiveRequest({
    requestId: params.requestId,
    leaseToken: params.body.lease_token,
    requireActiveLease: false,
    allowExpired: tokenPayload.action_type === "artifact_fetch",
  });

  if (authorizedLease.actionType === "artifact_fetch") {
    if (!params.body.upload_token) {
      throw new ValidationError(
        "Field 'upload_token' is required when completing artifact_fetch requests.",
      );
    }

    const finalized = await finalizeArtifactFetchArchiveRequest({
      requestId: params.requestId,
      leaseToken: params.body.lease_token,
      uploadToken: params.body.upload_token,
    });
    return {
      status: "completed",
      request: serializeArchiveRequest(finalized.request),
    };
  }

  if (authorizedLease.actionType === "object_revision_apply") {
    if (!params.body.result) {
      throw new ValidationError(
        "Field 'result' is required when completing object_revision_apply requests.",
      );
    }

    const result = parseWorkerObjectRevisionApplyResult(params.body.result);
    await completeObjectRevisionApplyByWorker({
      requestId: params.requestId,
      leaseId: authorizedLease.leaseId,
      leaseTokenId: authorizedLease.leaseTokenId,
      result,
    });

    const completedRequest = await findArchiveRequestById({
      requestId: authorizedLease.requestId,
    });
    if (!completedRequest) {
      throw new ConflictError("Archive request could not be reloaded after completion.");
    }

    return {
      status: "completed",
      request: serializeArchiveRequest(completedRequest),
    };
  }

  const current = await findArchiveRequestById({ requestId: authorizedLease.requestId });
  const completed = current?.status === "COMPLETED"
    ? current
    : await completeArchiveRequest({
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
