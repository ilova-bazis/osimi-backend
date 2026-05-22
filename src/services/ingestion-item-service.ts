import {
  getAllowedItemKindsForClassificationType,
  getAllowedMediaKindsForItemKind,
  isClassificationTypeCompatibleWithItemKind,
  isItemKindCompatibleWithMediaKind,
} from "../domain/ingestions/compatibility.ts";
import {
  getMediaKindForMime,
  normalizeMime,
  type MediaKind,
} from "../domain/ingestions/capabilities.ts";
import { findIngestionFileById } from "../repos/ingestion-repo.ts";
import {
  ConflictError,
  NotFoundError,
  ValidationError,
} from "../http/errors.ts";
import { hasActiveLease } from "../repos/lease-repo.ts";
import { findIngestionById } from "../repos/ingestion-repo.ts";
import {
  createIngestionItem,
  createIngestionItemFile,
  findIngestionItemById,
  type IngestionItemRecord,
  type IngestionItemFileRecord,
  listIngestionItemFiles,
  listIngestionItems,
  reorderIngestionItems,
  reorderIngestionItemFiles,
  updateIngestionItem,
} from "../repos/ingestion-item-repo.ts";
import type { AuthenticatedContext } from "../auth/guards.ts";
import type {
  AddIngestionItemFileBody,
  AddIngestionItemFileResponse,
  CreateIngestionItemBody,
  CreateIngestionItemResponse,
  IngestionItemDto,
  IngestionItemFileDto,
  IngestItemKind,
  IngestionClassificationType,
  ListIngestionItemFilesResponse,
  ListIngestionItemsResponse,
  ReorderIngestionItemsBody,
  ReorderIngestionItemsResponse,
  ReorderIngestionItemFilesBody,
  ReorderIngestionItemFilesResponse,
  UpdateIngestionItemBody,
  UpdateIngestionItemResponse,
  JsonObject,
} from "../validation/ingestion.ts";

function assertClassificationTypeCompatibleWithItemKind(params: {
  classificationType: IngestionClassificationType;
  itemKind: IngestItemKind;
}): void {
  if (
    isClassificationTypeCompatibleWithItemKind({
      classificationType: params.classificationType,
      itemKind: params.itemKind,
    })
  ) {
    return;
  }

  throw new ConflictError(
    `Classification type '${params.classificationType}' is incompatible with item kind '${params.itemKind}'.`,
    {
      classification_type: params.classificationType,
      item_kind: params.itemKind,
      allowed_item_kinds: [
        ...getAllowedItemKindsForClassificationType(params.classificationType),
      ],
    },
  );
}

function assertItemKindCompatibleWithMediaKind(params: {
  itemKind: IngestItemKind;
  mediaKind: MediaKind;
}): void {
  if (
    isItemKindCompatibleWithMediaKind({
      itemKind: params.itemKind,
      mediaKind: params.mediaKind,
    })
  ) {
    return;
  }

  throw new ConflictError(
    `Item kind '${params.itemKind}' is incompatible with ${params.mediaKind} files.`,
    {
      item_kind: params.itemKind,
      actual_media_kind: params.mediaKind,
      allowed_media_kinds: [...getAllowedMediaKindsForItemKind(params.itemKind)],
    },
  );
}

function resolveEffectiveItemKind(params: {
  itemKind?: IngestItemKind;
  ingestionItemKind: IngestItemKind;
}): IngestItemKind {
  return params.itemKind ?? params.ingestionItemKind;
}

function resolveEffectiveClassificationType(params: {
  classificationType?: IngestionClassificationType;
  ingestionClassificationType: IngestionClassificationType;
}): IngestionClassificationType {
  return params.classificationType ?? params.ingestionClassificationType;
}

function serializeIngestionItem(record: IngestionItemRecord): IngestionItemDto {
  return {
    id: record.id as string,
    ingestion_id: record.ingestionId as string,
    item_index: record.itemIndex as number,
    status: record.status as IngestionItemDto["status"],
    classification_type: (record.classificationType as IngestionItemDto["classification_type"]) ?? null,
    item_kind: (record.itemKind as IngestionItemDto["item_kind"]) ?? null,
    language_code: (record.languageCode as string | undefined) ?? null,
    title: (record.title as string | undefined) ?? null,
    summary: record.summary as IngestionItemDto["summary"],
    error_summary: record.errorSummary as IngestionItemDto["error_summary"],
    object_id: (record.objectId as string | undefined) ?? null,
    created_at: (record.createdAt as Date).toISOString(),
    updated_at: (record.updatedAt as Date).toISOString(),
  };
}

function serializeIngestionItemFile(record: IngestionItemFileRecord): IngestionItemFileDto {
  return {
    id: record.id,
    ingestion_item_id: record.ingestionItemId,
    ingestion_file_id: record.ingestionFileId,
    ingestion_id: record.ingestionId,
    role: record.role,
    sort_order: record.sortOrder,
    page_number: record.pageNumber ?? null,
    is_primary: record.isPrimary,
    logical_label: record.logicalLabel ?? null,
    created_at: record.createdAt.toISOString(),
  };
}

async function assertIngestionMutable(params: {
  tenantId: string;
  ingestionId: string;
}): Promise<void> {
  const ingestion = await findIngestionById(params.tenantId, params.ingestionId);
  if (!ingestion) {
    throw new NotFoundError(`Ingestion '${params.ingestionId}' was not found.`);
  }

  if (
    ingestion.status !== "DRAFT" &&
    ingestion.status !== "UPLOADING" &&
    ingestion.status !== "CANCELED"
  ) {
    throw new ConflictError(
      "Ingestion items cannot be modified in current ingestion state.",
      {
        ingestion_id: ingestion.id,
        status: ingestion.status,
      },
    );
  }

  if (await hasActiveLease(ingestion.id)) {
    throw new ConflictError("Ingestion items cannot be modified after lease assignment.", {
      ingestion_id: ingestion.id,
    });
  }
}

function mergeJsonObjects(base: JsonObject, patch: JsonObject): JsonObject {
  const result: JsonObject = { ...base };
  for (const key of Object.keys(patch)) {
    const patchValue = patch[key];
    if (patchValue === undefined) {
      continue;
    }
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

function normalizeTags(tags: string[]): string[] {
  const unique = new Set<string>();
  for (const tag of tags) {
    const normalized = tag.trim();
    if (normalized.length === 0) {
      continue;
    }
    unique.add(normalized);
  }

  return Array.from(unique);
}

function buildSummaryPatch(body: UpdateIngestionItemBody): JsonObject | undefined {
  const patch: JsonObject = {};

  if (body.description !== undefined) {
    patch.classification = {
      summary: body.description,
    };
  }

  if (body.tags !== undefined) {
    const normalizedTags = normalizeTags(body.tags);
    patch.classification = {
      ...((patch.classification as JsonObject | undefined) ?? {}),
      tags: normalizedTags,
    };
  }

  if (body.dates !== undefined) {
    const datesPatch: JsonObject = {};

    if (body.dates.published !== undefined) {
      datesPatch.published = body.dates.published as unknown as JsonObject;
    }

    if (body.dates.created !== undefined) {
      datesPatch.created = body.dates.created as unknown as JsonObject;
    }

    patch.dates = datesPatch;
  }

  return Object.keys(patch).length > 0 ? patch : undefined;
}

export async function createIngestionItemForIngestion(params: {
  auth: AuthenticatedContext;
  ingestionId: string;
  body: CreateIngestionItemBody;
}): Promise<CreateIngestionItemResponse> {
  await assertIngestionMutable({
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
  });

  const ingestion = await findIngestionById(params.auth.tenantId, params.ingestionId);
  if (!ingestion) {
    throw new NotFoundError(`Ingestion '${params.ingestionId}' was not found.`);
  }

  assertClassificationTypeCompatibleWithItemKind({
    classificationType: resolveEffectiveClassificationType({
      classificationType: params.body.classification_type,
      ingestionClassificationType: ingestion.classificationType,
    }),
    itemKind: resolveEffectiveItemKind({
      itemKind: params.body.item_kind,
      ingestionItemKind: ingestion.itemKind,
    }),
  });

  const item = await createIngestionItem({
    id: crypto.randomUUID(),
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
    itemIndex: params.body.item_index,
    classificationType: params.body.classification_type,
    itemKind: params.body.item_kind,
    languageCode: params.body.language_code,
    title: params.body.title,
    summary: params.body.summary,
  });

  return {
    item: serializeIngestionItem(item),
  };
}

export async function listIngestionItemsForIngestion(params: {
  auth: AuthenticatedContext;
  ingestionId: string;
}): Promise<ListIngestionItemsResponse> {
  const ingestion = await findIngestionById(params.auth.tenantId, params.ingestionId);
  if (!ingestion) {
    throw new NotFoundError(`Ingestion '${params.ingestionId}' was not found.`);
  }

  const items = await listIngestionItems({
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
  });

  return {
    items: items.map((item) => serializeIngestionItem(item)),
  };
}

export async function addIngestionFileToItem(params: {
  auth: AuthenticatedContext;
  ingestionId: string;
  ingestionItemId: string;
  body: AddIngestionItemFileBody;
}): Promise<AddIngestionItemFileResponse> {
  await assertIngestionMutable({
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
  });

  const ingestion = await findIngestionById(params.auth.tenantId, params.ingestionId);
  if (!ingestion) {
    throw new NotFoundError(`Ingestion '${params.ingestionId}' was not found.`);
  }

  const item = await findIngestionItemById({
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
    ingestionItemId: params.ingestionItemId,
  });

  if (!item) {
    throw new NotFoundError(`Ingestion item '${params.ingestionItemId}' was not found.`);
  }

  const file = await findIngestionFileById({
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
    fileId: params.body.ingestion_file_id,
  });
  if (!file) {
    throw new NotFoundError(`Ingestion file '${params.body.ingestion_file_id}' was not found.`);
  }

  const mediaKind = getMediaKindForMime(normalizeMime(file.contentType));
  if (mediaKind) {
    assertItemKindCompatibleWithMediaKind({
      itemKind: resolveEffectiveItemKind({
        itemKind: item.itemKind,
        ingestionItemKind: ingestion.itemKind,
      }),
      mediaKind,
    });
  }

  const linked = await createIngestionItemFile({
    id: crypto.randomUUID(),
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
    ingestionItemId: params.ingestionItemId,
    ingestionFileId: params.body.ingestion_file_id,
    role: params.body.role,
    sortOrder: params.body.sort_order,
    pageNumber: params.body.page_number,
    isPrimary: params.body.is_primary,
    logicalLabel: params.body.logical_label,
  });

  return {
    file: serializeIngestionItemFile(linked),
  };
}

export async function listIngestionItemFilesForIngestionItem(params: {
  auth: AuthenticatedContext;
  ingestionId: string;
  ingestionItemId: string;
}): Promise<ListIngestionItemFilesResponse> {
  const ingestion = await findIngestionById(params.auth.tenantId, params.ingestionId);
  if (!ingestion) {
    throw new NotFoundError(`Ingestion '${params.ingestionId}' was not found.`);
  }

  const item = await findIngestionItemById({
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
    ingestionItemId: params.ingestionItemId,
  });
  if (!item) {
    throw new NotFoundError(`Ingestion item '${params.ingestionItemId}' was not found.`);
  }

  const files = await listIngestionItemFiles({
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
    ingestionItemId: params.ingestionItemId,
  });

  return {
    files: files.map((file) => serializeIngestionItemFile(file)),
  };
}

export async function reorderFilesInIngestionItem(params: {
  auth: AuthenticatedContext;
  ingestionId: string;
  ingestionItemId: string;
  body: ReorderIngestionItemFilesBody;
}): Promise<ReorderIngestionItemFilesResponse> {
  await assertIngestionMutable({
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
  });

  const uniqueSortOrders = new Set(params.body.files.map((entry) => entry.sort_order));
  if (uniqueSortOrders.size !== params.body.files.length) {
    throw new ValidationError("Field 'files' must contain unique sort_order values.");
  }

  const uniqueFileIds = new Set(params.body.files.map((entry) => entry.ingestion_file_id));
  if (uniqueFileIds.size !== params.body.files.length) {
    throw new ValidationError("Field 'files' must contain unique ingestion_file_id values.");
  }

  const updated = await reorderIngestionItemFiles({
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
    ingestionItemId: params.ingestionItemId,
    files: params.body.files.map((entry) => ({
      ingestionFileId: entry.ingestion_file_id,
      sortOrder: entry.sort_order,
    })),
  });

  if (updated.length === 0) {
    throw new ConflictError("Reorder payload must contain exactly the files currently linked to item.", {
      ingestion_id: params.ingestionId,
      ingestion_item_id: params.ingestionItemId,
    });
  }

  return {
    files: updated.map((file) => serializeIngestionItemFile(file)),
  };
}

export async function reorderIngestionItemsForIngestion(params: {
  auth: AuthenticatedContext;
  ingestionId: string;
  body: ReorderIngestionItemsBody;
}): Promise<ReorderIngestionItemsResponse> {
  await assertIngestionMutable({
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
  });

  const uniqueItemIndexes = new Set(params.body.items.map((entry) => entry.item_index));
  if (uniqueItemIndexes.size !== params.body.items.length) {
    throw new ValidationError("Field 'items' must contain unique item_index values.");
  }

  const uniqueItemIds = new Set(params.body.items.map((entry) => entry.ingestion_item_id));
  if (uniqueItemIds.size !== params.body.items.length) {
    throw new ValidationError("Field 'items' must contain unique ingestion_item_id values.");
  }

  const reordered = await reorderIngestionItems({
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
    items: params.body.items.map((entry) => ({
      ingestionItemId: entry.ingestion_item_id,
      itemIndex: entry.item_index,
    })),
  });

  if (reordered.length === 0) {
    throw new ConflictError("Reorder payload must contain exactly the items currently linked to ingestion.", {
      ingestion_id: params.ingestionId,
    });
  }

  return {
    items: reordered.map((item) => serializeIngestionItem(item)),
  };
}

export async function updateIngestionItemMetadata(params: {
  auth: AuthenticatedContext;
  ingestionId: string;
  ingestionItemId: string;
  body: UpdateIngestionItemBody;
}): Promise<UpdateIngestionItemResponse> {
  await assertIngestionMutable({
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
  });

  const existingItem = await findIngestionItemById({
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
    ingestionItemId: params.ingestionItemId,
  });

  if (!existingItem) {
    throw new NotFoundError(`Ingestion item '${params.ingestionItemId}' was not found.`);
  }

  const ingestion = await findIngestionById(params.auth.tenantId, params.ingestionId);
  if (!ingestion) {
    throw new NotFoundError(`Ingestion '${params.ingestionId}' was not found.`);
  }

  assertClassificationTypeCompatibleWithItemKind({
    classificationType: resolveEffectiveClassificationType({
      classificationType:
        params.body.classification_type ?? existingItem.classificationType,
      ingestionClassificationType: ingestion.classificationType,
    }),
    itemKind: resolveEffectiveItemKind({
      itemKind: params.body.item_kind ?? existingItem.itemKind,
      ingestionItemKind: ingestion.itemKind,
    }),
  });

  const summaryPatch = buildSummaryPatch(params.body);
  const nextSummary = summaryPatch
    ? mergeJsonObjects(existingItem.summary, summaryPatch)
    : undefined;

  const updated = await updateIngestionItem({
    tenantId: params.auth.tenantId,
    ingestionId: params.ingestionId,
    ingestionItemId: params.ingestionItemId,
    classificationType: params.body.classification_type,
    itemKind: params.body.item_kind,
    languageCode: params.body.language_code,
    title: params.body.title,
    summary: nextSummary,
  });

  if (!updated) {
    throw new NotFoundError(`Ingestion item '${params.ingestionItemId}' was not found.`);
  }

  return {
    item: serializeIngestionItem(updated),
  };
}
