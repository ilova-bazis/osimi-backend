import {
  ConflictError,
  NotFoundError,
} from "../http/errors.ts";
import { resolveStagingPath } from "../storage/staging.ts";
import { findIngestionById } from "../repos/ingestion-repo.ts";
import { insertObjectEvent } from "../repos/event-repo.ts";
import {
  createOrGetObjectBySourceIngestion,
  findObjectById,
  findObjectBySourceIngestion,
  updateObjectIngestManifest,
  updateObjectProjectionState,
} from "../repos/object-repo.ts";
import { jsonObjectSchema } from "../validation/ingestion.ts";
import type {
  IngestWorkerEventsInput,
  IngestWorkerEventsResponse,
} from "../types/worker-events.ts";
import { applyStatusTransition } from "./ingestion-transition.ts";

export async function ingestWorkerEvents(
  params: IngestWorkerEventsInput,
): Promise<IngestWorkerEventsResponse> {
  const { authorizedLease, events } = params;

  const ingestion = await findIngestionById(
    authorizedLease.tenantId,
    authorizedLease.ingestionId,
  );

  if (!ingestion) {
    throw new NotFoundError(`Ingestion '${authorizedLease.ingestionId}' was not found.`);
  }

  const ingestionRecord = ingestion;

  let insertedCount = 0;
  let duplicateCount = 0;
  let completedObjectId: string | undefined;
  let currentStatus = ingestionRecord.status;

  for (const event of events) {
    const createdAt = new Date(event.timestamp);

    let completionObject:
      | Awaited<ReturnType<typeof createOrGetObjectBySourceIngestion>>
      | undefined;

    if (event.event_type === "INGESTION_COMPLETED") {
      try {
        const parsedMetadata = jsonObjectSchema.safeParse(event.payload);
        completionObject = await createOrGetObjectBySourceIngestion({
          objectId: event.object_id,
          tenantId: ingestionRecord.tenantId,
          sourceIngestionId: ingestionRecord.id,
          type: "GENERIC",
          title:
            typeof event.payload.title === "string" ? event.payload.title : "",
          metadata: parsedMetadata.success ? parsedMetadata.data : {},
        });
      } catch (error) {
        if (!isObjectConflictError(error)) {
          throw error;
        }

        const existingByIngestion = await findObjectBySourceIngestion({
          tenantId: ingestionRecord.tenantId,
          ingestionId: ingestionRecord.id,
        });

        if (existingByIngestion) {
          completionObject = existingByIngestion;
        } else {
          const existingById = await findObjectById({
            tenantId: ingestionRecord.tenantId,
            objectId: event.object_id,
          });

          if (
            existingById &&
            existingById.sourceIngestionId === ingestionRecord.id
          ) {
            completionObject = existingById;
          } else {
            throw new ConflictError("Conflicting object_id for this ingestion.", {
              ingestion_id: ingestionRecord.id,
              received_object_id: event.object_id,
            });
          }
        }
      }

      if (completionObject.objectId !== event.object_id) {
        throw new ConflictError("Conflicting object_id for this ingestion.", {
          ingestion_id: ingestionRecord.id,
          expected_object_id: completionObject.objectId,
          received_object_id: event.object_id,
        });
      }
    }

    const inserted = await insertObjectEvent({
      eventId: event.event_id,
      tenantId: authorizedLease.tenantId,
      type: event.event_type,
      ingestionId: authorizedLease.ingestionId,
      objectId: event.object_id,
      payload: event.payload,
      actorUserId: ingestionRecord.createdBy,
      createdAt,
    });

    if (!inserted) {
      duplicateCount += 1;
      continue;
    }

    insertedCount += 1;

    if (
      event.event_type === "INGESTION_PROCESSING" &&
      currentStatus !== "PROCESSING"
    ) {
      currentStatus = await applyStatusTransition({
        ingestionId: ingestionRecord.id,
        tenantId: ingestionRecord.tenantId,
        fromStatus: currentStatus,
        toStatus: "PROCESSING",
      });
    }

    if (event.event_type === "INGESTION_FAILED") {
      currentStatus = await applyStatusTransition({
        ingestionId: ingestionRecord.id,
        tenantId: ingestionRecord.tenantId,
        fromStatus: currentStatus,
        toStatus: "FAILED",
      });
    }

    if (event.event_type === "INGESTION_CANCELED") {
      currentStatus = await applyStatusTransition({
        ingestionId: ingestionRecord.id,
        tenantId: ingestionRecord.tenantId,
        fromStatus: currentStatus,
        toStatus: "CANCELED",
      });
    }

    if (event.event_type === "INGESTION_COMPLETED") {
      if (!completionObject) {
        throw new ConflictError("Completion event object resolution failed.");
      }

      await updateObjectProjectionState({
        tenantId: ingestionRecord.tenantId,
        objectId: completionObject.objectId,
        processingState: "index_done",
        availabilityState: "AVAILABLE",
      });

      completedObjectId = completionObject.objectId;

      const ingestJson = event.payload.ingest_json;
      const parsedIngestJson = jsonObjectSchema.safeParse(ingestJson);
      if (parsedIngestJson.success) {
        await updateObjectIngestManifest({
          tenantId: ingestionRecord.tenantId,
          objectId: completionObject.objectId,
          ingestManifest: parsedIngestJson.data,
        });
      }

      currentStatus = await applyStatusTransition({
        ingestionId: ingestionRecord.id,
        tenantId: ingestionRecord.tenantId,
        fromStatus: currentStatus,
        toStatus: "COMPLETED",
      });
    }
  }

  return {
    status: "ok",
    ingestion_id: authorizedLease.ingestionId,
    inserted_events: insertedCount,
    duplicate_events: duplicateCount,
    object_id: completedObjectId ?? null,
  };
}

function isObjectConflictError(error: unknown): boolean {
  if (!error || typeof error !== "object") {
    return false;
  }

  const maybeError = error as { code?: unknown; errno?: unknown; constraint?: unknown };
  if (maybeError.code !== "23505" && maybeError.errno !== "23505") {
    return false;
  }

  return (
    maybeError.constraint === "objects_pkey" ||
    maybeError.constraint === "objects_source_ingestion_unique_idx"
  );
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
