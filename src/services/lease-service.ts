import {
  ConflictError,
  NotFoundError,
} from "../http/errors.ts";
import {
  createLeaseToken,
} from "../auth/worker-lease.ts";
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
} from "../storage/staging.ts";
import type {
  HeartbeatLeaseInput,
  HeartbeatLeaseResponse,
  ReleaseLeaseInput,
  ReleaseLeaseResponse,
  WorkerDownloadUrl,
} from "../types/lease.ts";

const DEFAULT_LEASE_TTL_SECONDS = 60 * 5;

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
}): WorkerDownloadUrl[] {
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

export async function heartbeatLease(
  params: HeartbeatLeaseInput,
): Promise<HeartbeatLeaseResponse> {
  const { authorizedLease } = params;

  const updatedLease = await extendLease({
    ingestionId: authorizedLease.ingestionId,
    leaseId: authorizedLease.leaseId,
    leaseTokenId: authorizedLease.leaseTokenId,
    leaseDurationSeconds: leaseTtlSeconds(),
  });

  if (!updatedLease) {
    throw new ConflictError("Lease is no longer active.");
  }

  const ingestion = await findIngestionById(
    authorizedLease.tenantId,
    authorizedLease.ingestionId,
  );

  if (!ingestion) {
    throw new NotFoundError(
      `Ingestion '${authorizedLease.ingestionId}' was not found.`,
    );
  }

  const ingestionFiles = await listIngestionFiles({
    tenantId: authorizedLease.tenantId,
    ingestionId: authorizedLease.ingestionId,
  });

  const refreshedToken = createLeaseToken({
    lease_id: updatedLease.id,
    lease_token_id: updatedLease.leaseTokenId,
    ingestion_id: authorizedLease.ingestionId,
    tenant_id: authorizedLease.tenantId,
    worker_id: authorizedLease.workerId,
    exp: updatedLease.leaseExpiresAt.toISOString(),
  });

  return {
    lease: {
      lease_id: updatedLease.id,
      lease_token: refreshedToken,
      lease_expires_at: updatedLease.leaseExpiresAt.toISOString(),
      ingestion_id: authorizedLease.ingestionId,
      batch_label: ingestion.batchLabel,
      tenant_id: authorizedLease.tenantId,
      download_urls: buildDownloadUrls({
        tenantId: authorizedLease.tenantId,
        ingestionId: authorizedLease.ingestionId,
        files: ingestionFiles,
        expiresAt: updatedLease.leaseExpiresAt,
      }),
    },
  };
}

export async function releaseActiveLease(
  params: ReleaseLeaseInput,
): Promise<ReleaseLeaseResponse> {
  const { authorizedLease } = params;

  const released = await releaseLease({
    ingestionId: authorizedLease.ingestionId,
    leaseId: authorizedLease.leaseId,
    leaseTokenId: authorizedLease.leaseTokenId,
  });

  if (!released) {
    throw new ConflictError("Lease is no longer active.");
  }

  const ingestion = await findIngestionById(
    authorizedLease.tenantId,
    authorizedLease.ingestionId,
  );

  if (!ingestion) {
    throw new NotFoundError(
      `Ingestion '${authorizedLease.ingestionId}' was not found.`,
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
    ingestion_id: authorizedLease.ingestionId,
    lease_id: authorizedLease.leaseId,
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
