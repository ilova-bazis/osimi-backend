import {
  ConflictError,
  NotFoundError,
} from "../http/errors.ts";
import { resolveStagingPath } from "../storage/staging.ts";
import { findIngestionById } from "../repos/ingestion-repo.ts";
import { insertObjectEvent } from "../repos/event-repo.ts";
import {
  createOrGetObjectBySourceIngestion,
  updateObjectIngestManifest,
} from "../repos/object-repo.ts";
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
      completionObject = await createOrGetObjectBySourceIngestion({
        objectId: event.object_id,
        tenantId: ingestionRecord.tenantId,
        sourceIngestionId: ingestionRecord.id,
        type: "GENERIC",
        title:
          typeof event.payload.title === "string" ? event.payload.title : "",
        metadata: event.payload,
      });

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

      completedObjectId = completionObject.objectId;

      const ingestJson = event.payload.ingest_json;
      if (
        ingestJson &&
        typeof ingestJson === "object" &&
        !Array.isArray(ingestJson)
      ) {
        await updateObjectIngestManifest({
          tenantId: ingestionRecord.tenantId,
          objectId: completionObject.objectId,
          ingestManifest: ingestJson as Record<string, unknown>,
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
