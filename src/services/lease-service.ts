import {
  ConflictError,
  NotFoundError,
} from "../http/errors.ts";
import {
  createLeaseToken,
} from "../auth/worker-lease.ts";
import {
  findIngestionWithCreator,
  listIngestionFiles,
  updateIngestionStatus,
} from "../repos/ingestion-repo.ts";
import { findObjectBySourceIngestion } from "../repos/object-repo.ts";
import {
  extendLease,
  leaseQueuedIngestionById,
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
  LeaseDto,
  ReleaseLeaseInput,
  ReleaseLeaseResponse,
  WorkerDownloadUrl,
} from "../types/lease.ts";
import { parseIngestionSummary } from "../validation/catalog.ts";

const DEFAULT_LEASE_TTL_SECONDS = 60 * 5;

function buildCatalogJson(params: {
  ingestion: Awaited<ReturnType<typeof findIngestionWithCreator>>;
  object?: Awaited<ReturnType<typeof findObjectBySourceIngestion>>;
}): Record<string, unknown> {
  const ingestion = params.ingestion;
  if (!ingestion) {
    throw new ConflictError("Catalog metadata is required before leasing this ingestion.");
  }

  if (!ingestion.createdByUsername) {
    throw new ConflictError("Catalog metadata requires a creator username.", {
      ingestion_id: ingestion.id,
    });
  }

  let summary: Record<string, unknown>;
  try {
    summary = parseIngestionSummary(ingestion.summary);
  } catch {
    throw new ConflictError("Catalog metadata is required before leasing this ingestion.", {
      ingestion_id: ingestion.id,
    });
  }

  const catalog: Record<string, unknown> = {
    schema_version: ingestion.schemaVersion,
    object_id: params.object?.objectId ?? null,
    updated_at: ingestion.updatedAt.toISOString(),
    updated_by: ingestion.createdByUsername,
    access: {
      level: ingestion.accessLevel,
      embargo_until: ingestion.embargoUntil ?? null,
      rights_note: ingestion.rightsNote ?? null,
      sensitivity_note: ingestion.sensitivityNote ?? null,
    },
    title: summary.title,
    classification: {
      ...(summary.classification as Record<string, unknown>),
      type: ingestion.classificationType,
      language: ingestion.languageCode,
    },
    dates: summary.dates,
  };

  catalog.item_kind = ingestion.itemKind;

  if (summary.processing !== undefined) {
    catalog.processing = summary.processing;
  }

  if (summary.publication !== undefined) {
    catalog.publication = summary.publication;
  }

  if (summary.people !== undefined) {
    catalog.people = summary.people;
  }

  if (summary.links !== undefined) {
    catalog.links = summary.links;
  }

  if (summary.notes !== undefined) {
    catalog.notes = summary.notes;
  }

  if (params.object) {
    const access = catalog.access as Record<string, unknown>;
    access.level = params.object.accessLevel;
    access.embargo_until = params.object.embargoUntil ?? null;
    access.rights_note = params.object.rightsNote ?? null;
    access.sensitivity_note = params.object.sensitivityNote ?? null;
    catalog.access = access;

    const title = (catalog.title ?? {}) as Record<string, unknown>;
    if (params.object.title && params.object.title.trim().length > 0) {
      title.primary = params.object.title;
    }
    catalog.title = title;

    const classification = (catalog.classification ?? {}) as Record<string, unknown>;
    if (params.object.languageCode) {
      classification.language = params.object.languageCode;
    }
    if (params.object.tags.length > 0) {
      classification.tags = params.object.tags;
    }
    catalog.classification = classification;

    catalog.object_id = params.object.objectId;
  }

  return catalog;
}

function buildDownloadUrls(params: {
  tenantId: string;
  ingestionId: string;
  files: Array<{
    id: string;
    storageKey: string;
    contentType: string;
    sizeBytes: number;
    checksumSha256?: string;
    processingOverrides: Record<string, unknown>;
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
        checksum_sha256: file.checksumSha256 ?? null,
        processing_overrides: file.processingOverrides,
        download_url: `/api/worker/downloads/${token}`,
      };
    });
}

function leaseTtlSeconds(): number {
  return DEFAULT_LEASE_TTL_SECONDS;
}

async function buildLeasePayload(params: {
  leaseId: string;
  leaseTokenId: string;
  leaseExpiresAt: Date;
  ingestionId: string;
  batchLabel: string;
  tenantId: string;
  workerId?: string;
}): Promise<LeaseDto> {
  const ingestionFiles = await listIngestionFiles({
    tenantId: params.tenantId,
    ingestionId: params.ingestionId,
  });

  const ingestion = await findIngestionWithCreator({
    tenantId: params.tenantId,
    ingestionId: params.ingestionId,
  });

  if (!ingestion) {
    throw new NotFoundError(`Ingestion '${params.ingestionId}' was not found.`);
  }

  const object = await findObjectBySourceIngestion({
    tenantId: params.tenantId,
    ingestionId: params.ingestionId,
  });

  const leaseToken = createLeaseToken({
    lease_id: params.leaseId,
    lease_token_id: params.leaseTokenId,
    ingestion_id: params.ingestionId,
    tenant_id: params.tenantId,
    worker_id: params.workerId,
    exp: params.leaseExpiresAt.toISOString(),
  });

  return {
    lease_id: params.leaseId,
    lease_token: leaseToken,
    lease_expires_at: params.leaseExpiresAt.toISOString(),
    ingestion_id: params.ingestionId,
    batch_label: params.batchLabel,
    tenant_id: params.tenantId,
    download_urls: buildDownloadUrls({
      tenantId: params.tenantId,
      ingestionId: params.ingestionId,
      files: ingestionFiles,
      expiresAt: params.leaseExpiresAt,
    }),
    catalog_json: buildCatalogJson({
      ingestion,
      object,
    }),
  };
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

  return {
    lease: await buildLeasePayload({
      leaseId: leaseResult.lease.id,
      leaseTokenId: leaseResult.lease.leaseTokenId,
      leaseExpiresAt: leaseResult.lease.leaseExpiresAt,
      ingestionId: leaseResult.ingestion.id,
      batchLabel: leaseResult.ingestion.batchLabel,
      tenantId: leaseResult.ingestion.tenantId,
      workerId: params.workerId,
    }),
  };
}

export async function leaseIngestionById(params: {
  ingestionId: string;
  workerId?: string;
}): Promise<Record<string, unknown>> {
  await sweepExpiredLeases();

  const leaseResult = await leaseQueuedIngestionById({
    ingestionId: params.ingestionId,
    workerId: params.workerId,
    leaseDurationSeconds: leaseTtlSeconds(),
  });

  if (leaseResult.status === "not_found") {
    throw new NotFoundError(`Ingestion '${params.ingestionId}' was not found.`);
  }

  if (leaseResult.status === "not_leasable") {
    throw new ConflictError("Ingestion is not available for leasing.", {
      ingestion_id: params.ingestionId,
    });
  }

  return {
    lease: await buildLeasePayload({
      leaseId: leaseResult.lease.id,
      leaseTokenId: leaseResult.lease.leaseTokenId,
      leaseExpiresAt: leaseResult.lease.leaseExpiresAt,
      ingestionId: leaseResult.ingestion.id,
      batchLabel: leaseResult.ingestion.batchLabel,
      tenantId: leaseResult.ingestion.tenantId,
      workerId: params.workerId,
    }),
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

  const ingestion = await findIngestionWithCreator({
    tenantId: authorizedLease.tenantId,
    ingestionId: authorizedLease.ingestionId,
  });

  if (!ingestion) {
    throw new NotFoundError(
      `Ingestion '${authorizedLease.ingestionId}' was not found.`,
    );
  }

  const ingestionFiles = await listIngestionFiles({
    tenantId: authorizedLease.tenantId,
    ingestionId: authorizedLease.ingestionId,
  });

  const object = await findObjectBySourceIngestion({
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
      catalog_json: buildCatalogJson({
        ingestion,
        object,
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

  const ingestion = await findIngestionWithCreator({
    tenantId: authorizedLease.tenantId,
    ingestionId: authorizedLease.ingestionId,
  });

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
