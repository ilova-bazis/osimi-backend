import {
  ConflictError,
  NotFoundError,
} from "../http/errors.ts";
import { withSchemaClient } from "../db/client.ts";
import type { SqlExecutor } from "../db/client.ts";
import { resolveStagingPath } from "../storage/staging.ts";
import { findIngestionByIdForUpdate } from "../repos/ingestion-repo.ts";
import {
  findIngestionItemById,
  listIngestionItemFiles,
  setIngestionItemStatus,
  summarizeIngestionItems,
} from "../repos/ingestion-item-repo.ts";
import {
  finalizeObjectEventWithExecutor,
  reserveObjectEventWithExecutor,
} from "../repos/event-repo.ts";
import {
  createOrGetObjectBySourceIngestion,
  findObjectBySourceIngestionItem,
  updateObjectIngestManifest,
  updateObjectMetadataPages,
  updateObjectProjectionState,
  type ObjectRecord,
} from "../repos/object-repo.ts";
import { parseIngestionSummary } from "../validation/catalog.ts";
import { jsonObjectSchema, type IngestItemKind } from "../validation/ingestion.ts";
import type {
  IngestWorkerEventsInput,
  IngestWorkerEventsResponse,
} from "../types/worker-events.ts";
import { applyStatusTransition } from "./ingestion-transition.ts";
import type { IngestionStatus } from "../domain/ingestions/state-machine.ts";

function mapItemKindToObjectType(
  itemKind: IngestItemKind,
): "GENERIC" | "IMAGE" | "AUDIO" | "VIDEO" | "DOCUMENT" {
  switch (itemKind) {
    case "photo":
      return "IMAGE";
    case "audio":
      return "AUDIO";
    case "video":
      return "VIDEO";
    case "scanned_document":
    case "document":
      return "DOCUMENT";
    case "other":
      return "GENERIC";
  }
}

async function populateObjectMetadataPagesFromIngestion(params: {
  tenantId: string;
  ingestionId: string;
  ingestionItemId?: string;
  objectRecord: ObjectRecord;
  executor?: SqlExecutor;
}): Promise<void> {
  if (params.objectRecord.type !== "DOCUMENT") {
    return;
  }

  const metadata = params.objectRecord.metadata;
  if (Array.isArray(metadata.pages)) {
    return;
  }

  const itemId = params.ingestionItemId ?? params.objectRecord.sourceIngestionItemId ?? undefined;
  if (!itemId) {
    return;
  }

  const itemFiles = await listIngestionItemFiles({
    tenantId: params.tenantId,
    ingestionId: params.ingestionId,
    ingestionItemId: itemId,
    executor: params.executor,
  });

  const pageFiles = itemFiles.filter(
    (file) => typeof file.pageNumber === "number" && file.pageNumber > 0,
  );

  if (pageFiles.length === 0) {
    return;
  }

  const pages = pageFiles
    .map((file) => ({
      page_number: file.pageNumber!,
      label: file.logicalLabel ?? String(file.pageNumber),
      image_artifact_id: null,
      ocr_text_artifact_id: null,
    }))
    .sort((left, right) => left.page_number - right.page_number);

  await updateObjectMetadataPages({
    tenantId: params.tenantId,
    objectId: params.objectRecord.objectId,
    pages,
    executor: params.executor,
  });
}

function deriveTerminalIngestionStatus(summary: {
  totalCount: number;
  completedCount: number;
  failedCount: number;
  skippedCount: number;
}): IngestionStatus | undefined {
  const terminalCount = summary.completedCount + summary.failedCount + summary.skippedCount;
  if (summary.totalCount === 0 || terminalCount !== summary.totalCount) {
    return undefined;
  }

  if (summary.failedCount === 0) {
    return "COMPLETED";
  }

  return summary.completedCount > 0 ? "COMPLETED_WITH_ERRORS" : "FAILED";
}

export async function ingestWorkerEvents(
  params: IngestWorkerEventsInput,
): Promise<IngestWorkerEventsResponse> {
  const { authorizedLease, events } = params;

  let insertedCount = 0;
  let duplicateCount = 0;
  const completedObjectIds = new Set<string>();

  for (const event of events) {
    const result = await withSchemaClient(async (sql) => {
      return sql.begin(async (executor) => {
        const ingestionRecord = await findIngestionByIdForUpdate(
          authorizedLease.tenantId,
          authorizedLease.ingestionId,
          executor,
        );

        if (!ingestionRecord) {
          throw new NotFoundError(`Ingestion '${authorizedLease.ingestionId}' was not found.`);
        }

        const createdAt = new Date(event.timestamp);
        const reservedEvent = await reserveObjectEventWithExecutor(executor, {
          eventId: event.event_id,
          tenantId: authorizedLease.tenantId,
          type: event.event_type,
          ingestionId: authorizedLease.ingestionId,
          objectId: event.object_id,
          ingestionItemId: event.ingestion_item_id,
          payload: event.payload,
          actorUserId: ingestionRecord.createdBy,
          createdAt,
        });

        if (reservedEvent.status === "duplicate") {
          return { status: "duplicate" as const, objectId: reservedEvent.objectId };
        }

        if (reservedEvent.status === "conflict") {
          throw new ConflictError("event_id was already used for a different worker event.", {
            event_id: event.event_id,
          });
        }

        let currentStatus = ingestionRecord.status;
        let eventObjectId: string | undefined;

        let itemCompletionObject:
      | Awaited<ReturnType<typeof createOrGetObjectBySourceIngestion>>
      | undefined;
        let scopedIngestionItem:
      | Awaited<ReturnType<typeof findIngestionItemById>>
      | undefined;

    if (event.event_type === "INGESTION_ITEM_COMPLETED") {
      if (!event.ingestion_item_id) {
        throw new ConflictError("Ingestion item event requires ingestion_item_id.");
      }

      scopedIngestionItem = await findIngestionItemById({
        tenantId: ingestionRecord.tenantId,
        ingestionId: ingestionRecord.id,
        ingestionItemId: event.ingestion_item_id,
        executor,
      });

      if (!scopedIngestionItem) {
        throw new NotFoundError(
          `Ingestion item '${event.ingestion_item_id}' was not found.`,
        );
      }

      const summary = parseIngestionSummary(ingestionRecord.summary);
      const parsedMetadata = jsonObjectSchema.safeParse(summary);
      const titleFromItem = scopedIngestionItem.title?.trim() ?? "";
      const titleFromEvent =
        typeof event.payload.title === "string" ? event.payload.title.trim() : "";
      const titleFromSummary =
        typeof summary.title.primary === "string" ? summary.title.primary.trim() : "";
      const title = titleFromItem || titleFromEvent || titleFromSummary;

      itemCompletionObject = await findObjectBySourceIngestionItem({
        tenantId: ingestionRecord.tenantId,
        ingestionItemId: event.ingestion_item_id,
        executor,
      });

      if (!itemCompletionObject) {
        itemCompletionObject = await createOrGetObjectBySourceIngestion({
          objectId: event.object_id,
          tenantId: ingestionRecord.tenantId,
          sourceIngestionId: ingestionRecord.id,
          sourceIngestionItemId: event.ingestion_item_id,
          type: mapItemKindToObjectType(
            scopedIngestionItem.itemKind ?? ingestionRecord.itemKind,
          ),
          title,
          languageCode: scopedIngestionItem.languageCode ?? ingestionRecord.languageCode,
          accessLevel: ingestionRecord.accessLevel,
          embargoKind: ingestionRecord.embargoUntil ? "timed" : "none",
          embargoUntil: ingestionRecord.embargoUntil,
          rightsNote: ingestionRecord.rightsNote,
          sensitivityNote: ingestionRecord.sensitivityNote,
          metadata: parsedMetadata.success ? parsedMetadata.data : {},
          tags: summary.classification.tags,
          executor,
        });
      }

      if (itemCompletionObject.objectId !== event.object_id) {
        throw new ConflictError("Conflicting object_id for this ingestion item.", {
          ingestion_id: ingestionRecord.id,
          ingestion_item_id: event.ingestion_item_id,
          expected_object_id: itemCompletionObject.objectId,
          received_object_id: event.object_id,
        });
      }

      if (itemCompletionObject) {
        await populateObjectMetadataPagesFromIngestion({
          tenantId: ingestionRecord.tenantId,
          ingestionId: ingestionRecord.id,
          ingestionItemId: event.ingestion_item_id,
          objectRecord: itemCompletionObject,
          executor,
        });
      }
    }

    if (
      event.event_type === "INGESTION_PROCESSING" &&
      currentStatus !== "PROCESSING"
    ) {
      currentStatus = await applyStatusTransition({
        ingestionId: ingestionRecord.id,
        tenantId: ingestionRecord.tenantId,
        fromStatus: currentStatus,
        toStatus: "PROCESSING",
        executor,
      });
    }

    if (event.event_type === "INGESTION_ITEM_PROCESSING") {
      if (!event.ingestion_item_id) {
        throw new ConflictError("Ingestion item event requires ingestion_item_id.");
      }

      const updatedItem = await setIngestionItemStatus({
        tenantId: ingestionRecord.tenantId,
        ingestionId: ingestionRecord.id,
        ingestionItemId: event.ingestion_item_id,
        toStatus: "PROCESSING",
        executor,
      });

      if (!updatedItem) {
        throw new NotFoundError(
          `Ingestion item '${event.ingestion_item_id}' was not found.`,
        );
      }

      if (currentStatus !== "PROCESSING") {
        currentStatus = await applyStatusTransition({
          ingestionId: ingestionRecord.id,
          tenantId: ingestionRecord.tenantId,
          fromStatus: currentStatus,
          toStatus: "PROCESSING",
          executor,
        });
      }
    }

    if (event.event_type === "INGESTION_ITEM_FAILED") {
      if (!event.ingestion_item_id) {
        throw new ConflictError("Ingestion item event requires ingestion_item_id.");
      }

      const updatedItem = await setIngestionItemStatus({
        tenantId: ingestionRecord.tenantId,
        ingestionId: ingestionRecord.id,
        ingestionItemId: event.ingestion_item_id,
        toStatus: "FAILED",
        executor,
      });

      if (!updatedItem) {
        throw new NotFoundError(
          `Ingestion item '${event.ingestion_item_id}' was not found.`,
        );
      }
    }

    if (event.event_type === "INGESTION_ITEM_COMPLETED") {
      if (!event.ingestion_item_id) {
        throw new ConflictError("Ingestion item event requires ingestion_item_id.");
      }

      const itemObject = itemCompletionObject;
      if (!itemObject) {
        throw new ConflictError("Item completion object resolution failed.", {
          ingestion_id: ingestionRecord.id,
          ingestion_item_id: event.ingestion_item_id,
        });
      }

      const projectedObject = await updateObjectProjectionState({
        tenantId: ingestionRecord.tenantId,
        objectId: itemObject.objectId,
        processingState: "index_done",
        availabilityState: "AVAILABLE",
        executor,
      });

      if (!projectedObject) {
        throw new NotFoundError(`Object '${itemObject.objectId}' was not found.`);
      }

      const ingestJson = event.payload.ingest_json;
      const parsedIngestJson = jsonObjectSchema.safeParse(ingestJson);
      if (parsedIngestJson.success) {
        await updateObjectIngestManifest({
          tenantId: ingestionRecord.tenantId,
          objectId: itemObject.objectId,
          ingestManifest: parsedIngestJson.data,
          executor,
        });
      }

      const updatedItem = await setIngestionItemStatus({
        tenantId: ingestionRecord.tenantId,
        ingestionId: ingestionRecord.id,
        ingestionItemId: event.ingestion_item_id,
        toStatus: "COMPLETED",
        objectId: itemObject.objectId,
        executor,
      });

      if (!updatedItem) {
        throw new NotFoundError(
          `Ingestion item '${event.ingestion_item_id}' was not found.`,
        );
      }

      eventObjectId = itemObject.objectId;
    }

    if (event.event_type === "INGESTION_CANCELED") {
      currentStatus = await applyStatusTransition({
        ingestionId: ingestionRecord.id,
        tenantId: ingestionRecord.tenantId,
        fromStatus: currentStatus,
        toStatus: "CANCELED",
        executor,
      });
    }

    if (event.event_type === "INGESTION_COMPLETED") {
      const itemSummary = await summarizeIngestionItems({
        tenantId: ingestionRecord.tenantId,
        ingestionId: ingestionRecord.id,
        executor,
      });

      const nextStatus = deriveTerminalIngestionStatus(itemSummary);
      if (nextStatus && currentStatus === "PROCESSING") {
        currentStatus = await applyStatusTransition({
          ingestionId: ingestionRecord.id,
          tenantId: ingestionRecord.tenantId,
          fromStatus: currentStatus,
          toStatus: nextStatus,
          executor,
        });
      }
    }

        if (
          currentStatus === "PROCESSING" &&
          (event.event_type === "INGESTION_ITEM_COMPLETED" ||
            event.event_type === "INGESTION_ITEM_FAILED")
        ) {
          const itemSummary = await summarizeIngestionItems({
            tenantId: ingestionRecord.tenantId,
            ingestionId: ingestionRecord.id,
            executor,
          });
          const nextStatus = deriveTerminalIngestionStatus(itemSummary);
          if (nextStatus) {
            currentStatus = await applyStatusTransition({
              ingestionId: ingestionRecord.id,
              tenantId: ingestionRecord.tenantId,
              fromStatus: currentStatus,
              toStatus: nextStatus,
              executor,
            });
          }
        }

        if (eventObjectId) {
          await finalizeObjectEventWithExecutor(executor, {
            id: reservedEvent.id,
            objectId: eventObjectId,
          });
        }

        return { status: "inserted" as const, objectId: eventObjectId };
      });
    });

    if (result.status === "duplicate") {
      duplicateCount += 1;
      if (result.objectId) {
        completedObjectIds.add(result.objectId);
      }
      continue;
    }

    insertedCount += 1;
    if (result.objectId) {
      completedObjectIds.add(result.objectId);
    }
  }

  return {
    status: "ok",
    ingestion_id: authorizedLease.ingestionId,
    inserted_events: insertedCount,
    duplicate_events: duplicateCount,
    object_ids: [...completedObjectIds],
  };
}

export async function downloadStagedArtifactByStorageKey(
  storageKey: string,
): Promise<Response> {
  const filePath = resolveStagingPath(storageKey);
  const file = Bun.file(filePath);

  if (!(await file.exists())) {
    throw new NotFoundError("Requested file was not found.");
  }

  return new Response(file, {
    status: 200,
  });
}
