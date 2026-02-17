import { NotFoundError, ValidationError } from "../http/errors.ts";
import { decodeCursor, encodeCursor, parsePaginationParams } from "../http/pagination.ts";
import {
  findArtifactById,
  findObjectById,
  listArtifactsByObjectId,
  listObjects,
  updateObjectTitle,
  type ObjectArtifactRecord,
  type ObjectRecord,
} from "../repos/object-repo.ts";
import { resolveStagingPath } from "../storage/staging.ts";

interface ObjectCursorPayload {
  created_at: string;
  object_id: string;
}

function serializeObject(
  record: ObjectRecord,
  options?: { includeIngestManifest?: boolean },
): Record<string, unknown> {
  const payload: Record<string, unknown> = {
    object_id: record.objectId,
    tenant_id: record.tenantId,
    type: record.type,
    title: record.title,
    metadata: record.metadata,
    source_ingestion_id: record.sourceIngestionId ?? null,
    status: record.status,
    created_at: record.createdAt.toISOString(),
  };

  if (options?.includeIngestManifest) {
    payload.ingest_manifest = record.ingestManifest ?? null;
  }

  return payload;
}

function serializeArtifact(record: ObjectArtifactRecord): Record<string, unknown> {
  return {
    id: record.id,
    object_id: record.objectId,
    kind: record.kind,
    storage_key: record.storageKey,
    content_type: record.contentType,
    size_bytes: record.sizeBytes,
    created_at: record.createdAt.toISOString(),
  };
}

function parseDateQueryParam(rawValue: string | null, fieldName: string): string | undefined {
  if (!rawValue) {
    return undefined;
  }

  const normalized = rawValue.trim();

  if (normalized.length === 0) {
    throw new ValidationError(`Query parameter '${fieldName}' cannot be empty.`);
  }

  const date = new Date(normalized);

  if (Number.isNaN(date.getTime())) {
    throw new ValidationError(`Query parameter '${fieldName}' must be a valid ISO timestamp.`);
  }

  return date.toISOString();
}

function parseTypeQueryParam(rawValue: string | null): ObjectRecord["type"] | undefined {
  if (!rawValue) {
    return undefined;
  }

  const normalized = rawValue.trim().toUpperCase();
  const allowed = ["GENERIC", "IMAGE", "AUDIO", "VIDEO", "DOCUMENT"] as const;

  if (!(allowed as readonly string[]).includes(normalized)) {
    throw new ValidationError("Query parameter 'type' is invalid.", {
      allowed_values: allowed,
    });
  }

  return normalized as ObjectRecord["type"];
}

export async function listObjectsForTenant(params: {
  tenantId: string;
  url: URL;
}): Promise<Record<string, unknown>> {
  const pagination = parsePaginationParams(params.url);
  const objectType = parseTypeQueryParam(params.url.searchParams.get("type"));
  const from = parseDateQueryParam(params.url.searchParams.get("from"), "from");
  const to = parseDateQueryParam(params.url.searchParams.get("to"), "to");
  const tag = params.url.searchParams.get("tag")?.trim() || undefined;

  let cursorPayload: ObjectCursorPayload | undefined;

  if (pagination.cursor) {
    const decoded = decodeCursor<Record<string, unknown>>(pagination.cursor);

    if (typeof decoded.created_at !== "string" || typeof decoded.object_id !== "string") {
      throw new ValidationError("Query parameter 'cursor' is invalid.");
    }

    cursorPayload = {
      created_at: decoded.created_at,
      object_id: decoded.object_id,
    };
  }

  const records = await listObjects({
    tenantId: params.tenantId,
    limit: pagination.limit + 1,
    cursorCreatedAt: cursorPayload?.created_at,
    cursorObjectId: cursorPayload?.object_id,
    type: objectType,
    fromCreatedAt: from,
    toCreatedAt: to,
    tag,
  });

  const hasMore = records.length > pagination.limit;
  const visible = hasMore ? records.slice(0, pagination.limit) : records;
  const lastItem = visible.at(-1);

  return {
    objects: visible.map((record) => serializeObject(record)),
    next_cursor: hasMore && lastItem
      ? encodeCursor({
          created_at: lastItem.createdAt.toISOString(),
          object_id: lastItem.objectId,
        })
      : null,
  };
}

export async function getObjectDetail(params: {
  tenantId: string;
  objectId: string;
}): Promise<Record<string, unknown>> {
  const objectRecord = await findObjectById({
    tenantId: params.tenantId,
    objectId: params.objectId,
  });

  if (!objectRecord) {
    throw new NotFoundError(`Object '${params.objectId}' was not found.`);
  }

  return {
    object: serializeObject(objectRecord, { includeIngestManifest: true }),
  };
}

export async function patchObjectTitleForTenant(params: {
  tenantId: string;
  objectId: string;
  body: unknown;
}): Promise<Record<string, unknown>> {
  if (params.body === null || typeof params.body !== "object" || Array.isArray(params.body)) {
    throw new ValidationError("Request body must be an object.");
  }

  const payload = params.body as Record<string, unknown>;

  if (Object.prototype.hasOwnProperty.call(payload, "metadata")) {
    throw new ValidationError("Field 'metadata' is not supported by PATCH /api/objects/:object_id in this phase.");
  }

  if (!Object.prototype.hasOwnProperty.call(payload, "title")) {
    throw new ValidationError("Field 'title' is required.");
  }

  if (typeof payload.title !== "string") {
    throw new ValidationError("Field 'title' must be a string.");
  }

  const title = payload.title.trim();

  if (title.length === 0) {
    throw new ValidationError("Field 'title' cannot be empty.");
  }

  const updated = await updateObjectTitle({
    tenantId: params.tenantId,
    objectId: params.objectId,
    title,
  });

  if (!updated) {
    throw new NotFoundError(`Object '${params.objectId}' was not found.`);
  }

  return {
    object: serializeObject(updated),
  };
}

export async function listObjectArtifactsForTenant(params: {
  tenantId: string;
  objectId: string;
}): Promise<Record<string, unknown>> {
  const objectRecord = await findObjectById({
    tenantId: params.tenantId,
    objectId: params.objectId,
  });

  if (!objectRecord) {
    throw new NotFoundError(`Object '${params.objectId}' was not found.`);
  }

  const artifacts = await listArtifactsByObjectId({
    tenantId: params.tenantId,
    objectId: params.objectId,
  });

  return {
    object_id: params.objectId,
    artifacts: artifacts.map(serializeArtifact),
  };
}

export async function downloadObjectArtifactForTenant(params: {
  tenantId: string;
  objectId: string;
  artifactId: string;
}): Promise<Response> {
  const artifact = await findArtifactById({
    tenantId: params.tenantId,
    objectId: params.objectId,
    artifactId: params.artifactId,
  });

  if (!artifact) {
    throw new NotFoundError(`Artifact '${params.artifactId}' was not found for object '${params.objectId}'.`);
  }

  const filePath = resolveStagingPath(artifact.storageKey);
  const file = Bun.file(filePath);

  if (!(await file.exists())) {
    throw new NotFoundError(`Artifact '${params.artifactId}' storage file was not found.`);
  }

  return new Response(file, {
    status: 200,
    headers: {
      "content-type": artifact.contentType,
      "content-length": String(artifact.sizeBytes),
      "content-disposition": `attachment; filename=artifact-${artifact.id}`,
    },
  });
}
