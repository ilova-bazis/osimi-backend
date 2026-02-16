import { mkdir } from "node:fs/promises";
import { dirname, join } from "node:path";

import {
  ConflictError,
  NotFoundError,
  UnauthorizedError,
  ValidationError,
} from "../http/errors.ts";
import {
  requireObject,
  requireOptionalStringField,
  requireStringField,
  requireUuid,
} from "../http/validation.ts";
import { resolveStagingPath, stagingRootPath } from "../storage/index.ts";
import { findIngestionById } from "../repos/ingestion-repo.ts";
import { insertObjectEvent } from "../repos/event-repo.ts";
import {
  createOrGetObjectBySourceIngestion,
  createObjectArtifact,
  findArtifactByStorageKey,
} from "../repos/object-repo.ts";
import { findActiveLeaseByToken } from "../repos/lease-repo.ts";
import { applyStatusTransition } from "./ingestion-transition.ts";
import { parseLeaseToken } from "./lease-service.ts";

const OBJECT_EVENT_TYPES_LIST = [
  "INGESTION_SUBMITTED",
  "INGESTION_QUEUED",
  "INGESTION_PROCESSING",
  "INGESTION_COMPLETED",
  "INGESTION_FAILED",
  "INGESTION_CANCELED",
  "LEASE_GRANTED",
  "LEASE_RENEWED",
  "LEASE_EXPIRED",
  "LEASE_RELEASED",
  "FILE_VALIDATED",
  "FILE_FAILED",
  "PIPELINE_STEP_STARTED",
  "PIPELINE_STEP_COMPLETED",
  "PIPELINE_STEP_FAILED",
  "OBJECT_CREATED",
  "ARTIFACT_CREATED",
] as const;

const OBJECT_EVENT_TYPES = new Set(OBJECT_EVENT_TYPES_LIST);

const OBJECT_ID_REQUIRED_EVENT_TYPES = new Set<ObjectEventType>([
  "INGESTION_COMPLETED",
  "OBJECT_CREATED",
  "ARTIFACT_CREATED",
]);

const OBJECT_ID_PATTERN = /^OBJ-[0-9]{8}-[A-Z0-9]+$/;

type ObjectEventType = (typeof OBJECT_EVENT_TYPES_LIST)[number];

interface IncomingEvent {
  event_id: string;
  event_type: ObjectEventType;
  timestamp: string;
  payload: Record<string, unknown>;
  object_id?: string;
}

function parseEvent(candidate: unknown): IncomingEvent {
  const event = requireObject(candidate, "Each event");
  const eventId = requireStringField(event, "event_id");
  requireUuid(eventId, "event_id");

  if (
    typeof event.event_type !== "string" ||
    !OBJECT_EVENT_TYPES.has(event.event_type as ObjectEventType)
  ) {
    throw new ValidationError("Field 'event_type' is invalid.");
  }

  if (typeof event.timestamp !== "string") {
    throw new ValidationError(
      "Field 'timestamp' must be an ISO timestamp string.",
    );
  }

  const timestamp = new Date(event.timestamp);

  if (Number.isNaN(timestamp.getTime())) {
    throw new ValidationError(
      "Field 'timestamp' must be a valid ISO timestamp.",
    );
  }

  const payload = requireObject(event.payload, "Field 'payload'");
  const objectId = requireOptionalStringField(event, "object_id");

  const eventType = event.event_type as ObjectEventType;

  if (OBJECT_ID_REQUIRED_EVENT_TYPES.has(eventType)) {
    if (!objectId || objectId.trim().length === 0) {
      throw new ValidationError(
        `Field 'object_id' is required for event type '${eventType}'.`,
      );
    }

    if (!OBJECT_ID_PATTERN.test(objectId)) {
      throw new ValidationError(
        "Field 'object_id' must match format 'OBJ-YYYYMMDD-XXXXXX'.",
      );
    }
  } else if (objectId !== undefined && !OBJECT_ID_PATTERN.test(objectId)) {
    throw new ValidationError(
      "Field 'object_id' must match format 'OBJ-YYYYMMDD-XXXXXX' when provided.",
    );
  }

  return {
    event_id: eventId,
    event_type: eventType,
    timestamp: event.timestamp,
    payload,
    object_id: objectId,
  };
}

async function storeIngestManifest(params: {
  tenantId: string;
  objectId: string;
  ingestJson: Record<string, unknown>;
}): Promise<{ storageKey: string; sizeBytes: number }> {
  const storageKey = `tenants/${params.tenantId}/objects/${params.objectId}/artifacts/ingest.json`;
  const absolutePath = join(stagingRootPath(), storageKey);
  const content = JSON.stringify(params.ingestJson, null, 2);
  await mkdir(dirname(absolutePath), { recursive: true });
  await Bun.write(absolutePath, content);

  return {
    storageKey,
    sizeBytes: Buffer.byteLength(content, "utf8"),
  };
}

export async function ingestWorkerEvents(params: {
  ingestionId: string;
  body: unknown;
}): Promise<Record<string, unknown>> {
  const payload = requireObject(params.body);
  const leaseTokenRaw = payload.lease_token;
  const eventsRaw = payload.events;

  if (typeof leaseTokenRaw !== "string" || leaseTokenRaw.trim().length === 0) {
    throw new ValidationError(
      "Field 'lease_token' must be a non-empty string.",
    );
  }

  if (!Array.isArray(eventsRaw)) {
    throw new ValidationError("Field 'events' must be an array.");
  }

  const leaseToken = parseLeaseToken(leaseTokenRaw);

  if (leaseToken.ingestion_id !== params.ingestionId) {
    throw new UnauthorizedError("Lease token does not match ingestion id.");
  }

  const activeLease = await findActiveLeaseByToken({
    ingestionId: leaseToken.ingestion_id,
    leaseId: leaseToken.lease_id,
    leaseTokenId: leaseToken.lease_token_id,
  });

  if (!activeLease) {
    throw new ConflictError("Lease is no longer active.");
  }

  const ingestion = await findIngestionById(
    leaseToken.tenant_id,
    params.ingestionId,
  );

  if (!ingestion) {
    throw new NotFoundError(`Ingestion '${params.ingestionId}' was not found.`);
  }

  const ingestionRecord = ingestion;

  let insertedCount = 0;
  let duplicateCount = 0;
  let completedObjectId: string | undefined;
  let currentStatus = ingestionRecord.status;

  for (const rawEvent of eventsRaw) {
    const event = parseEvent(rawEvent);
    const createdAt = new Date(event.timestamp);

    const inserted = await insertObjectEvent({
      eventId: event.event_id,
      tenantId: leaseToken.tenant_id,
      type: event.event_type,
      ingestionId: params.ingestionId,
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
      const completedObjectIdFromEvent = event.object_id;

      if (!completedObjectIdFromEvent) {
        throw new ValidationError(
          "Field 'object_id' is required for event type 'INGESTION_COMPLETED'.",
        );
      }

      const object = await createOrGetObjectBySourceIngestion({
        objectId: completedObjectIdFromEvent,
        tenantId: ingestionRecord.tenantId,
        sourceIngestionId: ingestionRecord.id,
        type: "GENERIC",
        title:
          typeof event.payload.title === "string" ? event.payload.title : "",
        metadata: event.payload,
      });

      if (object.objectId !== completedObjectIdFromEvent) {
        throw new ConflictError("Conflicting object_id for this ingestion.", {
          ingestion_id: ingestionRecord.id,
          expected_object_id: object.objectId,
          received_object_id: completedObjectIdFromEvent,
        });
      }

      completedObjectId = object.objectId;

      const ingestJson = event.payload.ingest_json;
      if (
        ingestJson &&
        typeof ingestJson === "object" &&
        !Array.isArray(ingestJson)
      ) {
        const manifest = await storeIngestManifest({
          tenantId: ingestionRecord.tenantId,
          objectId: object.objectId,
          ingestJson: ingestJson as Record<string, unknown>,
        });

        const existingArtifact = await findArtifactByStorageKey({
          objectId: object.objectId,
          storageKey: manifest.storageKey,
        });

        if (!existingArtifact) {
          await createObjectArtifact({
            objectId: object.objectId,
            kind: "ingest_json",
            storageKey: manifest.storageKey,
            contentType: "application/json",
            sizeBytes: manifest.sizeBytes,
          });
        }
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
    ingestion_id: params.ingestionId,
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
