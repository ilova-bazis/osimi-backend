import {
  ConflictError,
  NotFoundError,
} from "../http/errors.ts";
import {
  createLeaseToken,
} from "../auth/worker-lease.ts";
import {
  findIngestionWithCreator,
} from "../repos/ingestion-repo.ts";
import {
  listIngestionItems,
  listLeasedIngestionFiles,
  type IngestionItemRecord,
} from "../repos/ingestion-item-repo.ts";
import { findObjectBySourceIngestionItem } from "../repos/object-repo.ts";
import {
  claimNextQueuedIngestion,
  claimQueuedIngestionById,
  extendLease,
  releaseLeaseAndRequeue,
  sweepExpiredLeases,
} from "../repos/lease-repo.ts";
import type { SqlExecutor } from "../db/client.ts";
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
  WorkerLeasedItem,
  WorkerLeasedItemFile,
} from "../types/lease.ts";
import type { JsonObject } from "../validation/ingestion.ts";
import { parseIngestionSummary } from "../validation/catalog.ts";

const DEFAULT_LEASE_TTL_SECONDS = 60 * 5;

type CatalogDateConfidence = "low" | "medium" | "high";

interface CatalogDateBlock {
  value: string | null;
  approximate: boolean;
  confidence: CatalogDateConfidence;
  note: string | null;
}

interface CatalogDates {
  published: CatalogDateBlock;
  created: CatalogDateBlock;
}

const DATE_VALUE_PATTERN = /^\d{4}(-\d{2})?(-\d{2})?$/;

function mergeJsonObjects(base: JsonObject, patch: JsonObject): JsonObject {
  const result: JsonObject = { ...base };

  for (const [key, patchValue] of Object.entries(patch)) {
    const baseValue = result[key];
    if (
      patchValue !== null &&
      typeof patchValue === "object" &&
      !Array.isArray(patchValue) &&
      baseValue !== null &&
      typeof baseValue === "object" &&
      !Array.isArray(baseValue)
    ) {
      result[key] = mergeJsonObjects(baseValue as JsonObject, patchValue as JsonObject);
      continue;
    }

    result[key] = patchValue;
  }

  return result;
}

function normalizeCatalogDateBlock(
  candidate: unknown,
  fallback: CatalogDateBlock,
): CatalogDateBlock {
  if (!candidate || typeof candidate !== "object" || Array.isArray(candidate)) {
    return fallback;
  }

  const raw = candidate as Record<string, unknown>;
  const value = raw.value;
  const approximate = raw.approximate;
  const confidence = raw.confidence;
  const note = raw.note;

  return {
    value:
      value === null || (typeof value === "string" && DATE_VALUE_PATTERN.test(value))
        ? value
        : fallback.value,
    approximate: typeof approximate === "boolean" ? approximate : fallback.approximate,
    confidence:
      confidence === "low" || confidence === "medium" || confidence === "high"
        ? confidence
        : fallback.confidence,
    note: note === null || typeof note === "string" ? note : fallback.note,
  };
}

function normalizeCatalogDates(candidate: unknown, fallback: CatalogDates): CatalogDates {
  if (!candidate || typeof candidate !== "object" || Array.isArray(candidate)) {
    return fallback;
  }

  const raw = candidate as Record<string, unknown>;

  return {
    published: normalizeCatalogDateBlock(raw.published, fallback.published),
    created: normalizeCatalogDateBlock(raw.created, fallback.created),
  };
}

function buildItemCatalogJson(params: {
  ingestion: Awaited<ReturnType<typeof findIngestionWithCreator>>;
  item: IngestionItemRecord;
  objectId?: string;
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

  const itemSummary = params.item.summary as JsonObject;
  const mergedSummary = mergeJsonObjects(summary as JsonObject, itemSummary);
  const fallbackDates = summary.dates as CatalogDates;
  const effectiveDates = normalizeCatalogDates(mergedSummary.dates, fallbackDates);

  const baseTitle = mergedSummary.title as Record<string, unknown> | undefined;
  const effectiveTitle =
    params.item.title && params.item.title.trim().length > 0
      ? {
        ...(baseTitle ?? {}),
        primary: params.item.title,
      }
      : (baseTitle ?? summary.title);

  const catalog: Record<string, unknown> = {
    schema_version: ingestion.schemaVersion,
    object_id: params.objectId ?? null,
    ingestion_item_id: params.item.id,
    item_index: params.item.itemIndex,
    updated_at: params.item.updatedAt.toISOString(),
    updated_by: ingestion.createdByUsername,
    access: {
      level: ingestion.accessLevel,
      embargo_until: ingestion.embargoUntil ?? null,
      rights_note: ingestion.rightsNote ?? null,
      sensitivity_note: ingestion.sensitivityNote ?? null,
    },
    title: effectiveTitle,
    classification: {
      ...((mergedSummary.classification as Record<string, unknown> | undefined) ?? {}),
      type: params.item.classificationType ?? ingestion.classificationType,
      language: params.item.languageCode ?? ingestion.languageCode,
    },
    dates: effectiveDates,
  };

  catalog.item_kind = params.item.itemKind ?? ingestion.itemKind;

  if (mergedSummary.processing !== undefined) {
    catalog.processing = mergedSummary.processing;
  }

  if (mergedSummary.publication !== undefined) {
    catalog.publication = mergedSummary.publication;
  }

  if (mergedSummary.people !== undefined) {
    catalog.people = mergedSummary.people;
  }

  if (mergedSummary.links !== undefined) {
    catalog.links = mergedSummary.links;
  }

  if (mergedSummary.notes !== undefined) {
    catalog.notes = mergedSummary.notes;
  }

  return catalog;
}

function buildLeasedItemFiles(params: {
  tenantId: string;
  ingestionId: string;
  files: Array<{
    id: string;
    filename: string;
    ingestionItemId?: string;
    itemIndex?: number;
    sortOrder?: number;
    storageKey: string;
    contentType: string;
    sizeBytes: number;
    checksumSha256?: string | null;
    processingOverrides: Record<string, unknown>;
  }>;
  expiresAt: Date;
}): WorkerLeasedItemFile[] {
  return params.files
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
        filename: file.filename,
        sort_order: file.sortOrder ?? 0,
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
  executor?: SqlExecutor;
}): Promise<LeaseDto> {
  const allIngestionFiles = await listLeasedIngestionFiles({
    tenantId: params.tenantId,
    ingestionId: params.ingestionId,
    executor: params.executor,
  });
  const ingestionFiles = allIngestionFiles.filter(
    (file) => file.status === "UPLOADED" || file.status === "VALIDATED",
  );

  const ingestion = await findIngestionWithCreator({
    tenantId: params.tenantId,
    ingestionId: params.ingestionId,
    executor: params.executor,
  });

  if (!ingestion) {
    throw new NotFoundError(`Ingestion '${params.ingestionId}' was not found.`);
  }

  const ingestionItems = await listIngestionItems({
    tenantId: params.tenantId,
    ingestionId: params.ingestionId,
    executor: params.executor,
  });

  const itemsById = new Map(ingestionItems.map((item) => [item.id, item]));
  const fileRowsByItemId = new Map<string, typeof ingestionFiles>();

  for (const file of ingestionFiles) {
    if (!file.ingestionItemId) {
      throw new ConflictError(
        "All committed ingestion files must be linked to ingestion items before leasing.",
        {
          ingestion_id: params.ingestionId,
          file_id: file.id,
        },
      );
    }

    if (!file.sortOrder || file.sortOrder <= 0) {
      throw new ConflictError(
        "All committed ingestion files must define sort_order before leasing.",
        {
          ingestion_id: params.ingestionId,
          file_id: file.id,
        },
      );
    }

    const item = itemsById.get(file.ingestionItemId);
    if (!item) {
      throw new ConflictError("File links an unknown ingestion item.", {
        ingestion_id: params.ingestionId,
        file_id: file.id,
        ingestion_item_id: file.ingestionItemId,
      });
    }

    const files = fileRowsByItemId.get(item.id);
    if (files) {
      files.push(file);
    } else {
      fileRowsByItemId.set(item.id, [file]);
    }
  }

  const leaseToken = createLeaseToken({
    lease_id: params.leaseId,
    lease_token_id: params.leaseTokenId,
    ingestion_id: params.ingestionId,
    tenant_id: params.tenantId,
    worker_id: params.workerId,
    exp: params.leaseExpiresAt.toISOString(),
  });

  const leasedItems: WorkerLeasedItem[] = [];

  for (const item of ingestionItems) {
    const filesForItem = fileRowsByItemId.get(item.id) ?? [];
    if (filesForItem.length === 0) {
      continue;
    }

      const existingObject = await findObjectBySourceIngestionItem({
        tenantId: params.tenantId,
        ingestionItemId: item.id,
        executor: params.executor,
    });

    leasedItems.push({
      ingestion_item_id: item.id,
      item_index: item.itemIndex,
      catalog_json: buildItemCatalogJson({
        ingestion,
        item,
        objectId: existingObject?.objectId,
      }),
      files: buildLeasedItemFiles({
        tenantId: params.tenantId,
        ingestionId: params.ingestionId,
        files: filesForItem,
        expiresAt: params.leaseExpiresAt,
      }),
    });
  }

  return {
    lease_id: params.leaseId,
    lease_token: leaseToken,
    lease_expires_at: params.leaseExpiresAt.toISOString(),
    ingestion_id: params.ingestionId,
    batch_label: params.batchLabel,
    tenant_id: params.tenantId,
    items: leasedItems,
  };
}

export async function leaseNextIngestion(params: {
  workerId?: string;
}): Promise<Record<string, unknown>> {
  await sweepExpiredLeases();

  const lease = await claimNextQueuedIngestion({
    workerId: params.workerId,
    leaseDurationSeconds: leaseTtlSeconds(),
    buildPayload: ({ candidate, grant, executor }) => {
      return buildLeasePayload({
        leaseId: grant.id,
        leaseTokenId: grant.tokenId,
        leaseExpiresAt: grant.expiresAt,
        ingestionId: candidate.id,
        batchLabel: candidate.batchLabel,
        tenantId: candidate.tenantId,
        workerId: params.workerId,
        executor,
      });
    },
  });

  if (!lease) {
    return {
      lease: null,
    };
  }

  if (lease.status === "invalid") {
    throw lease.error;
  }

  return {
    lease: lease.payload,
  };
}

export async function leaseIngestionById(params: {
  ingestionId: string;
  workerId?: string;
}): Promise<Record<string, unknown>> {
  await sweepExpiredLeases();

  const leaseResult = await claimQueuedIngestionById({
    ingestionId: params.ingestionId,
    workerId: params.workerId,
    leaseDurationSeconds: leaseTtlSeconds(),
    buildPayload: ({ candidate, grant, executor }) => {
      return buildLeasePayload({
        leaseId: grant.id,
        leaseTokenId: grant.tokenId,
        leaseExpiresAt: grant.expiresAt,
        ingestionId: candidate.id,
        batchLabel: candidate.batchLabel,
        tenantId: candidate.tenantId,
        workerId: params.workerId,
        executor,
      });
    },
  });

  if (leaseResult.status === "not_found") {
    throw new NotFoundError(`Ingestion '${params.ingestionId}' was not found.`);
  }

  if (leaseResult.status === "not_leasable") {
    throw new ConflictError("Ingestion is not available for leasing.", {
      ingestion_id: params.ingestionId,
    });
  }

  if (leaseResult.status === "invalid") {
    throw leaseResult.error;
  }

  return { lease: leaseResult.payload };
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
      items: (await buildLeasePayload({
        leaseId: updatedLease.id,
        leaseTokenId: updatedLease.leaseTokenId,
        leaseExpiresAt: updatedLease.leaseExpiresAt,
        ingestionId: authorizedLease.ingestionId,
        batchLabel: ingestion.batchLabel,
        tenantId: authorizedLease.tenantId,
        workerId: authorizedLease.workerId,
      })).items,
    },
  };
}

export async function releaseActiveLease(
  params: ReleaseLeaseInput,
): Promise<ReleaseLeaseResponse> {
  const { authorizedLease } = params;

  const released = await releaseLeaseAndRequeue({
    tenantId: authorizedLease.tenantId,
    ingestionId: authorizedLease.ingestionId,
    leaseId: authorizedLease.leaseId,
    leaseTokenId: authorizedLease.leaseTokenId,
  });

  if (released.status === "not_found") {
    throw new NotFoundError(
      `Ingestion '${authorizedLease.ingestionId}' was not found.`,
    );
  }

  if (released.status === "inactive") {
    throw new ConflictError("Lease is no longer active.");
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
