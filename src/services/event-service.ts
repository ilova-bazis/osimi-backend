import { mkdir } from "node:fs/promises";
import { dirname, join } from "node:path";

import { ConflictError, NotFoundError, UnauthorizedError, ValidationError } from "../http/errors.ts";
import { resolveStagingPath, stagingRootPath } from "../storage/index.ts";
import { findIngestionById, updateIngestionStatus } from "../repos/ingestion-repo.ts";
import { insertObjectEvent } from "../repos/event-repo.ts";
import { createObject, createObjectArtifact, findObjectBySourceIngestion } from "../repos/object-repo.ts";
import { findActiveLeaseByToken } from "../repos/lease-repo.ts";
import { parseLeaseToken } from "./lease-service.ts";

const OBJECT_EVENT_TYPES = new Set([
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
] as const);

type ObjectEventType =
  | "INGESTION_SUBMITTED"
  | "INGESTION_QUEUED"
  | "INGESTION_PROCESSING"
  | "INGESTION_COMPLETED"
  | "INGESTION_FAILED"
  | "INGESTION_CANCELED"
  | "LEASE_GRANTED"
  | "LEASE_RENEWED"
  | "LEASE_EXPIRED"
  | "LEASE_RELEASED"
  | "FILE_VALIDATED"
  | "FILE_FAILED"
  | "PIPELINE_STEP_STARTED"
  | "PIPELINE_STEP_COMPLETED"
  | "PIPELINE_STEP_FAILED"
  | "OBJECT_CREATED"
  | "ARTIFACT_CREATED";

interface IncomingEvent {
  event_id: string;
  event_type: ObjectEventType;
  timestamp: string;
  payload: Record<string, unknown>;
  object_id?: string;
}

function validateUuid(value: string, fieldName: string): string {
  const pattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

  if (!pattern.test(value)) {
    throw new ValidationError(`Field '${fieldName}' must be a UUID.`);
  }

  return value;
}

function parseEvent(candidate: unknown): IncomingEvent {
  if (candidate === null || typeof candidate !== "object" || Array.isArray(candidate)) {
    throw new ValidationError("Each event must be an object.");
  }

  const event = candidate as Record<string, unknown>;

  if (typeof event.event_id !== "string") {
    throw new ValidationError("Field 'event_id' must be a string.");
  }

  validateUuid(event.event_id, "event_id");

  if (typeof event.event_type !== "string" || !OBJECT_EVENT_TYPES.has(event.event_type as ObjectEventType)) {
    throw new ValidationError("Field 'event_type' is invalid.");
  }

  if (typeof event.timestamp !== "string") {
    throw new ValidationError("Field 'timestamp' must be an ISO timestamp string.");
  }

  const timestamp = new Date(event.timestamp);

  if (Number.isNaN(timestamp.getTime())) {
    throw new ValidationError("Field 'timestamp' must be a valid ISO timestamp.");
  }

  if (event.payload === null || typeof event.payload !== "object" || Array.isArray(event.payload)) {
    throw new ValidationError("Field 'payload' must be an object.");
  }

  if (event.object_id !== undefined && typeof event.object_id !== "string") {
    throw new ValidationError("Field 'object_id' must be a string when provided.");
  }

  return {
    event_id: event.event_id,
    event_type: event.event_type as ObjectEventType,
    timestamp: event.timestamp,
    payload: event.payload as Record<string, unknown>,
    object_id: event.object_id as string | undefined,
  };
}

function generateObjectId(): string {
  const now = new Date();
  const y = String(now.getUTCFullYear());
  const m = String(now.getUTCMonth() + 1).padStart(2, "0");
  const d = String(now.getUTCDate()).padStart(2, "0");
  const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let suffix = "";

  for (let index = 0; index < 6; index += 1) {
    suffix += alphabet[Math.floor(Math.random() * alphabet.length)];
  }

  return `OBJ-${y}${m}${d}-${suffix}`;
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
  if (params.body === null || typeof params.body !== "object" || Array.isArray(params.body)) {
    throw new ValidationError("Request body must be an object.");
  }

  const payload = params.body as Record<string, unknown>;
  const leaseTokenRaw = payload.lease_token;
  const eventsRaw = payload.events;

  if (typeof leaseTokenRaw !== "string" || leaseTokenRaw.trim().length === 0) {
    throw new ValidationError("Field 'lease_token' must be a non-empty string.");
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

  const ingestion = await findIngestionById(leaseToken.tenant_id, params.ingestionId);

  if (!ingestion) {
    throw new NotFoundError(`Ingestion '${params.ingestionId}' was not found.`);
  }

  let insertedCount = 0;
  let duplicateCount = 0;
  let completedObjectId: string | undefined;

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
      actorUserId: ingestion.createdBy,
      createdAt,
    });

    if (!inserted) {
      duplicateCount += 1;
      continue;
    }

    insertedCount += 1;

    if (event.event_type === "INGESTION_PROCESSING" && ingestion.status !== "PROCESSING") {
      await updateIngestionStatus({
        ingestionId: ingestion.id,
        tenantId: ingestion.tenantId,
        status: "PROCESSING",
      });
    }

    if (event.event_type === "INGESTION_FAILED") {
      await updateIngestionStatus({
        ingestionId: ingestion.id,
        tenantId: ingestion.tenantId,
        status: "FAILED",
      });
    }

    if (event.event_type === "INGESTION_CANCELED") {
      await updateIngestionStatus({
        ingestionId: ingestion.id,
        tenantId: ingestion.tenantId,
        status: "CANCELED",
      });
    }

    if (event.event_type === "INGESTION_COMPLETED") {
      let object = await findObjectBySourceIngestion({
        tenantId: ingestion.tenantId,
        ingestionId: ingestion.id,
      });

      if (!object) {
        object = await createObject({
          objectId: generateObjectId(),
          tenantId: ingestion.tenantId,
          sourceIngestionId: ingestion.id,
          type: "GENERIC",
          title: typeof event.payload.title === "string" ? event.payload.title : "",
          metadata: event.payload,
        });
      }

      completedObjectId = object.objectId;

      const ingestJson = event.payload.ingest_json;
      if (ingestJson && typeof ingestJson === "object" && !Array.isArray(ingestJson)) {
        const manifest = await storeIngestManifest({
          tenantId: ingestion.tenantId,
          objectId: object.objectId,
          ingestJson: ingestJson as Record<string, unknown>,
        });

        await createObjectArtifact({
          objectId: object.objectId,
          kind: "ingest_json",
          storageKey: manifest.storageKey,
          contentType: "application/json",
          sizeBytes: manifest.sizeBytes,
        });
      }

      await updateIngestionStatus({
        ingestionId: ingestion.id,
        tenantId: ingestion.tenantId,
        status: "COMPLETED",
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

export async function downloadStagedArtifactByStorageKey(storageKey: string): Promise<Response> {
  const filePath = resolveStagingPath(storageKey);
  const file = Bun.file(filePath);

  if (!(await file.exists())) {
    throw new NotFoundError("Requested file was not found.");
  }

  return new Response(file, {
    status: 200,
  });
}
