import { ConflictError, NotFoundError, ValidationError } from "../http/errors.ts";
import { decodeCursor, encodeCursor, parsePaginationParams } from "../http/pagination.ts";
import {
  buildAccessDecision,
  type AccessReasonCode,
} from "../domain/objects/access-policy.ts";
import {
  createObjectAccessRequest,
  findPendingObjectAccessRequestForUser,
  deleteObjectAccessAssignment,
  findObjectAccessAssignmentForUser,
  findObjectAccessRequestById,
  listObjectAccessAssignmentsByObjectId,
  listObjectAccessAssignmentsForUserByObjectIds,
  listObjectAccessRequests,
  updateObjectAccessRequestStatus,
  upsertObjectAccessAssignment,
} from "../repos/object-access-repo.ts";
import {
  findArtifactById,
  findObjectById,
  listArtifactsByObjectId,
  listObjects,
  type ObjectListSort,
  updateObjectAccessPolicy,
  updateObjectTitle,
  type ObjectArtifactRecord,
  type ObjectRecord,
} from "../repos/object-repo.ts";
import { resolveStagingPath } from "../storage/staging.ts";

interface ObjectCursorPayload {
  sort: ObjectListSort;
  created_at?: string;
  updated_at?: string;
  title?: string;
  object_id: string;
}

function isPendingAccessRequestUniqueViolation(error: unknown): boolean {
  if (!error || typeof error !== "object") {
    return false;
  }

  const maybeError = error as {
    code?: unknown;
    constraint?: unknown;
    message?: unknown;
  };

  if (maybeError.code !== "23505") {
    return false;
  }

  if (maybeError.constraint === "object_access_requests_one_pending_per_user_idx") {
    return true;
  }

  return (
    typeof maybeError.message === "string" &&
    maybeError.message.includes(
      "object_access_requests_one_pending_per_user_idx",
    )
  );
}

function computeAccessProjection(
  record: ObjectRecord,
  params: {
    role: "viewer" | "archiver" | "admin";
    assignmentLevel?: "family" | "private";
  },
): {
  isAuthorized: boolean;
  isDeliverable: boolean;
  canDownload: boolean;
  accessReasonCode: AccessReasonCode;
} {
  return buildAccessDecision({
    role: params.role,
    accessLevel: record.accessLevel,
    assignmentLevel: params.assignmentLevel,
    embargoKind: record.embargoKind,
    embargoUntil: record.embargoUntil,
    embargoCurationState: record.embargoCurationState,
    objectCurationState: record.curationState,
    availabilityState: record.availabilityState,
  });
}

function serializeObject(
  record: ObjectRecord,
  options?: { includeIngestManifest?: boolean },
): Record<string, unknown> {
  const payload: Record<string, unknown> = {
    id: record.objectId,
    object_id: record.objectId,
    tenant_id: record.tenantId,
    type: record.type,
    title: record.title,
    language: record.languageCode ?? null,
    tags: record.tags,
    metadata: record.metadata,
    source_ingestion_id: record.sourceIngestionId ?? null,
    source_batch_label: record.sourceBatchLabel ?? null,
    processing_state: record.processingState,
    curation_state: record.curationState,
    availability_state: record.availabilityState,
    access_level: record.accessLevel,
    embargo_kind: record.embargoKind,
    embargo_until: record.embargoUntil ?? null,
    embargo_curation_state: record.embargoCurationState ?? null,
    rights_note: record.rightsNote ?? null,
    sensitivity_note: record.sensitivityNote ?? null,
    created_at: record.createdAt.toISOString(),
    updated_at: record.updatedAt.toISOString(),
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

function parseAvailabilityStateQueryParam(
  rawValue: string | null,
): ObjectRecord["availabilityState"] | undefined {
  if (!rawValue) {
    return undefined;
  }

  const normalized = rawValue.trim().toUpperCase();
  const allowed = [
    "AVAILABLE",
    "ARCHIVED",
    "RESTORE_PENDING",
    "RESTORING",
    "UNAVAILABLE",
  ] as const;

  if (!(allowed as readonly string[]).includes(normalized)) {
    throw new ValidationError("Query parameter 'availability_state' is invalid.", {
      allowed_values: allowed,
    });
  }

  return normalized as ObjectRecord["availabilityState"];
}

function parseAccessLevelQueryParam(
  rawValue: string | null,
): ObjectRecord["accessLevel"] | undefined {
  if (!rawValue) {
    return undefined;
  }

  const normalized = rawValue.trim().toLowerCase();
  const allowed = ["private", "family", "public"] as const;

  if (!(allowed as readonly string[]).includes(normalized)) {
    throw new ValidationError("Query parameter 'access_level' is invalid.", {
      allowed_values: allowed,
    });
  }

  return normalized as ObjectRecord["accessLevel"];
}

function parseSortQueryParam(rawValue: string | null): ObjectListSort {
  if (!rawValue) {
    return "created_at_desc";
  }

  const normalized = rawValue.trim();
  const allowed: readonly ObjectListSort[] = [
    "created_at_desc",
    "created_at_asc",
    "updated_at_desc",
    "updated_at_asc",
    "title_asc",
    "title_desc",
  ];

  if (!(allowed as readonly string[]).includes(normalized)) {
    throw new ValidationError("Query parameter 'sort' is invalid.", {
      allowed_values: allowed,
    });
  }

  return normalized as ObjectListSort;
}

function parseOptionalNonEmptyQueryParam(
  rawValue: string | null,
  fieldName: string,
): string | undefined {
  if (!rawValue) {
    return undefined;
  }

  const normalized = rawValue.trim();
  if (normalized.length === 0) {
    throw new ValidationError(`Query parameter '${fieldName}' cannot be empty.`);
  }

  return normalized;
}

export async function listObjectsForTenant(params: {
  tenantId: string;
  userId: string;
  role: "viewer" | "archiver" | "admin";
  url: URL;
}): Promise<Record<string, unknown>> {
  const pagination = parsePaginationParams(params.url);
  const sort = parseSortQueryParam(params.url.searchParams.get("sort"));
  const query = parseOptionalNonEmptyQueryParam(params.url.searchParams.get("q"), "q");
  const availabilityState = parseAvailabilityStateQueryParam(
    params.url.searchParams.get("availability_state"),
  );
  const accessLevel = parseAccessLevelQueryParam(
    params.url.searchParams.get("access_level"),
  );
  const language = parseOptionalNonEmptyQueryParam(params.url.searchParams.get("language"), "language");
  const batchLabel = parseOptionalNonEmptyQueryParam(
    params.url.searchParams.get("batch_label"),
    "batch_label",
  );
  const objectType = parseTypeQueryParam(params.url.searchParams.get("type"));
  const from = parseDateQueryParam(params.url.searchParams.get("from"), "from");
  const to = parseDateQueryParam(params.url.searchParams.get("to"), "to");
  const tag = params.url.searchParams.get("tag")?.trim() || undefined;

  let cursorPayload: ObjectCursorPayload | undefined;

  if (pagination.cursor) {
    const decoded = decodeCursor<Record<string, unknown>>(pagination.cursor);

    if (
      typeof decoded.sort !== "string" ||
      decoded.sort !== sort ||
      typeof decoded.object_id !== "string"
    ) {
      throw new ValidationError("Query parameter 'cursor' is invalid.");
    }

    if (
      (sort === "created_at_desc" || sort === "created_at_asc") &&
      typeof decoded.created_at !== "string"
    ) {
      throw new ValidationError("Query parameter 'cursor' is invalid.");
    }

    if (
      (sort === "updated_at_desc" || sort === "updated_at_asc") &&
      typeof decoded.updated_at !== "string"
    ) {
      throw new ValidationError("Query parameter 'cursor' is invalid.");
    }

    if (
      (sort === "title_asc" || sort === "title_desc") &&
      typeof decoded.title !== "string"
    ) {
      throw new ValidationError("Query parameter 'cursor' is invalid.");
    }

    cursorPayload = {
      sort,
      created_at:
        typeof decoded.created_at === "string" ? decoded.created_at : undefined,
      updated_at:
        typeof decoded.updated_at === "string" ? decoded.updated_at : undefined,
      title: typeof decoded.title === "string" ? decoded.title : undefined,
      object_id: decoded.object_id,
    };
  }

  const result = await listObjects({
    tenantId: params.tenantId,
    limit: pagination.limit + 1,
    sort,
    cursorCreatedAt: cursorPayload?.created_at,
    cursorUpdatedAt: cursorPayload?.updated_at,
    cursorTitle: cursorPayload?.title,
    cursorObjectId: cursorPayload?.object_id,
    type: objectType,
    availabilityState,
    accessLevel,
    query,
    language,
    batchLabel,
    fromCreatedAt: from,
    toCreatedAt: to,
    tag,
  });

  const hasMore = result.items.length > pagination.limit;
  const visible = hasMore ? result.items.slice(0, pagination.limit) : result.items;
  const lastItem = visible.at(-1);

  const assignmentByObjectId = await listObjectAccessAssignmentsForUserByObjectIds(
    {
      tenantId: params.tenantId,
      userId: params.userId,
      objectIds: visible.map((item) => item.objectId),
    },
  );

  let nextCursor: string | null = null;
  if (hasMore && lastItem) {
    if (sort === "created_at_desc" || sort === "created_at_asc") {
      nextCursor = encodeCursor({
        sort,
        created_at: lastItem.createdAt.toISOString(),
        object_id: lastItem.objectId,
      });
    } else if (sort === "updated_at_desc" || sort === "updated_at_asc") {
      nextCursor = encodeCursor({
        sort,
        updated_at: lastItem.updatedAt.toISOString(),
        object_id: lastItem.objectId,
      });
    } else {
      nextCursor = encodeCursor({
        sort,
        title: lastItem.title,
        object_id: lastItem.objectId,
      });
    }
  }

  return {
    objects: visible.map((record) => {
      const projection = computeAccessProjection(record, {
        role: params.role,
        assignmentLevel: assignmentByObjectId.get(record.objectId),
      });
      return {
        ...serializeObject(record),
        can_download: projection.canDownload,
        access_reason_code: projection.accessReasonCode,
      };
    }),
    next_cursor: nextCursor,
    total_count: result.totalCount,
    filtered_count: result.filteredCount,
  };
}

export async function getObjectDetail(params: {
  tenantId: string;
  userId: string;
  role: "viewer" | "archiver" | "admin";
  objectId: string;
}): Promise<Record<string, unknown>> {
  const objectRecord = await findObjectById({
    tenantId: params.tenantId,
    objectId: params.objectId,
  });

  if (!objectRecord) {
    throw new NotFoundError(`Object '${params.objectId}' was not found.`);
  }

  const assignment = await findObjectAccessAssignmentForUser({
    tenantId: params.tenantId,
    objectId: params.objectId,
    userId: params.userId,
  });

  const projection = computeAccessProjection(objectRecord, {
    role: params.role,
    assignmentLevel: assignment?.grantedLevel,
  });

  return {
    object: {
      ...serializeObject(objectRecord, { includeIngestManifest: true }),
      is_authorized: projection.isAuthorized,
      is_deliverable: projection.isDeliverable,
      can_download: projection.canDownload,
      access_reason_code: projection.accessReasonCode,
    },
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
  userId: string;
  role: "viewer" | "archiver" | "admin";
  objectId: string;
  artifactId: string;
}): Promise<Response> {
  const objectRecord = await findObjectById({
    tenantId: params.tenantId,
    objectId: params.objectId,
  });

  if (!objectRecord) {
    throw new NotFoundError(`Object '${params.objectId}' was not found.`);
  }

  const assignment = await findObjectAccessAssignmentForUser({
    tenantId: params.tenantId,
    objectId: params.objectId,
    userId: params.userId,
  });

  const projection = computeAccessProjection(objectRecord, {
    role: params.role,
    assignmentLevel: assignment?.grantedLevel,
  });

  if (!projection.canDownload) {
    throw new ValidationError("Object artifact is not downloadable in the current access state.", {
      access_reason_code: projection.accessReasonCode,
    });
  }

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

export async function updateObjectAccessPolicyForTenant(params: {
  tenantId: string;
  objectId: string;
  body: unknown;
}): Promise<Record<string, unknown>> {
  if (params.body === null || typeof params.body !== "object" || Array.isArray(params.body)) {
    throw new ValidationError("Request body must be an object.");
  }

  const payload = params.body as Record<string, unknown>;

  const accessLevel = parseAccessLevelQueryParam(String(payload.access_level ?? ""));
  if (!accessLevel) {
    throw new ValidationError("Field 'access_level' is required.");
  }

  const embargoKindRaw = payload.embargo_kind;
  if (typeof embargoKindRaw !== "string") {
    throw new ValidationError("Field 'embargo_kind' is required.");
  }
  const embargoKind = embargoKindRaw.trim();
  if (!["none", "timed", "curation_state"].includes(embargoKind)) {
    throw new ValidationError("Field 'embargo_kind' is invalid.");
  }

  let embargoUntil: string | null = null;
  let embargoCurationState: ObjectRecord["curationState"] | null = null;
  if (embargoKind === "timed") {
    if (typeof payload.embargo_until !== "string") {
      throw new ValidationError("Field 'embargo_until' is required when embargo_kind is 'timed'.");
    }
    const date = new Date(payload.embargo_until);
    if (Number.isNaN(date.getTime())) {
      throw new ValidationError("Field 'embargo_until' must be a valid ISO timestamp.");
    }
    embargoUntil = date.toISOString();
  }

  if (embargoKind === "curation_state") {
    if (typeof payload.embargo_curation_state !== "string") {
      throw new ValidationError("Field 'embargo_curation_state' is required when embargo_kind is 'curation_state'.");
    }
    const normalized = payload.embargo_curation_state.trim();
    const allowed = [
      "needs_review",
      "review_in_progress",
      "reviewed",
      "curation_failed",
    ] as const;
    if (!(allowed as readonly string[]).includes(normalized)) {
      throw new ValidationError("Field 'embargo_curation_state' is invalid.");
    }
    embargoCurationState = normalized as ObjectRecord["curationState"];
  }

  const rightsNote =
    typeof payload.rights_note === "string" && payload.rights_note.trim().length > 0
      ? payload.rights_note.trim()
      : null;
  const sensitivityNote =
    typeof payload.sensitivity_note === "string" && payload.sensitivity_note.trim().length > 0
      ? payload.sensitivity_note.trim()
      : null;

  const updated = await updateObjectAccessPolicy({
    tenantId: params.tenantId,
    objectId: params.objectId,
    accessLevel,
    embargoKind: embargoKind as ObjectRecord["embargoKind"],
    embargoUntil,
    embargoCurationState,
    rightsNote,
    sensitivityNote,
  });

  if (!updated) {
    throw new NotFoundError(`Object '${params.objectId}' was not found.`);
  }

  return { object: serializeObject(updated, { includeIngestManifest: true }) };
}

export async function createObjectAccessRequestForTenant(params: {
  tenantId: string;
  objectId: string;
  userId: string;
  body: unknown;
}): Promise<Record<string, unknown>> {
  if (params.body === null || typeof params.body !== "object" || Array.isArray(params.body)) {
    throw new ValidationError("Request body must be an object.");
  }

  const payload = params.body as Record<string, unknown>;
  const level = payload.requested_level;
  if (typeof level !== "string" || !["family", "private"].includes(level)) {
    throw new ValidationError("Field 'requested_level' is invalid.");
  }
  const reason = typeof payload.reason === "string" ? payload.reason.trim() : undefined;

  const object = await findObjectById({ tenantId: params.tenantId, objectId: params.objectId });
  if (!object) {
    throw new NotFoundError(`Object '${params.objectId}' was not found.`);
  }

  const existingPending = await findPendingObjectAccessRequestForUser({
    tenantId: params.tenantId,
    objectId: params.objectId,
    requesterUserId: params.userId,
  });

  if (existingPending) {
    throw new ConflictError("A pending access request already exists for this object and user.", {
      request_id: existingPending.id,
      object_id: params.objectId,
      requester_user_id: params.userId,
    });
  }

  let request: Awaited<ReturnType<typeof createObjectAccessRequest>>;
  try {
    request = await createObjectAccessRequest({
      objectId: params.objectId,
      tenantId: params.tenantId,
      requesterUserId: params.userId,
      requestedLevel: level as "family" | "private",
      reason,
    });
  } catch (error) {
    if (!isPendingAccessRequestUniqueViolation(error)) {
      throw error;
    }

    const pending = await findPendingObjectAccessRequestForUser({
      tenantId: params.tenantId,
      objectId: params.objectId,
      requesterUserId: params.userId,
    });

    throw new ConflictError("A pending access request already exists for this object and user.", {
      request_id: pending?.id,
      object_id: params.objectId,
      requester_user_id: params.userId,
    });
  }

  return {
    request: {
      id: request.id,
      object_id: request.objectId,
      requester_user_id: request.requesterUserId,
      requested_level: request.requestedLevel,
      reason: request.reason ?? null,
      status: request.status,
      created_at: request.createdAt.toISOString(),
      updated_at: request.updatedAt.toISOString(),
    },
  };
}

export async function listObjectAccessRequestsForTenant(params: {
  tenantId: string;
  objectId: string;
}): Promise<Record<string, unknown>> {
  const object = await findObjectById({ tenantId: params.tenantId, objectId: params.objectId });
  if (!object) {
    throw new NotFoundError(`Object '${params.objectId}' was not found.`);
  }

  const requests = await listObjectAccessRequests({
    tenantId: params.tenantId,
    objectId: params.objectId,
  });

  return {
    object_id: params.objectId,
    requests: requests.map((request) => ({
      id: request.id,
      requester_user_id: request.requesterUserId,
      requested_level: request.requestedLevel,
      reason: request.reason ?? null,
      status: request.status,
      reviewed_by: request.reviewedBy ?? null,
      reviewed_at: request.reviewedAt?.toISOString() ?? null,
      decision_note: request.decisionNote ?? null,
      created_at: request.createdAt.toISOString(),
      updated_at: request.updatedAt.toISOString(),
    })),
  };
}

export async function resolveObjectAccessRequestForTenant(params: {
  tenantId: string;
  objectId: string;
  requestId: string;
  reviewerUserId: string;
  action: "approve" | "reject";
  body: unknown;
}): Promise<Record<string, unknown>> {
  const request = await findObjectAccessRequestById({
    requestId: params.requestId,
    objectId: params.objectId,
    tenantId: params.tenantId,
  });

  if (!request) {
    throw new NotFoundError(`Access request '${params.requestId}' was not found.`);
  }

  if (request.status !== "PENDING") {
    throw new ConflictError("Access request is already resolved and cannot be changed.", {
      request_id: request.id,
      status: request.status,
    });
  }

  let decisionNote: string | undefined;
  if (
    params.body !== null &&
    typeof params.body === "object" &&
    !Array.isArray(params.body) &&
    typeof (params.body as Record<string, unknown>).decision_note === "string"
  ) {
    decisionNote = ((params.body as Record<string, unknown>).decision_note as string).trim();
  }

  const updated = await updateObjectAccessRequestStatus({
    requestId: params.requestId,
    objectId: params.objectId,
    tenantId: params.tenantId,
    status: params.action === "approve" ? "APPROVED" : "REJECTED",
    reviewedBy: params.reviewerUserId,
    decisionNote,
  });

  if (!updated) {
    const latest = await findObjectAccessRequestById({
      requestId: params.requestId,
      objectId: params.objectId,
      tenantId: params.tenantId,
    });

    if (latest && latest.status !== "PENDING") {
      throw new ConflictError("Access request is already resolved and cannot be changed.", {
        request_id: latest.id,
        status: latest.status,
      });
    }

    throw new NotFoundError(`Access request '${params.requestId}' was not found.`);
  }

  if (params.action === "approve") {
    await upsertObjectAccessAssignment({
      objectId: updated.objectId,
      tenantId: updated.tenantId,
      userId: updated.requesterUserId,
      grantedLevel: updated.requestedLevel,
      createdBy: params.reviewerUserId,
    });
  }

  return {
    request: {
      id: updated.id,
      object_id: updated.objectId,
      requester_user_id: updated.requesterUserId,
      requested_level: updated.requestedLevel,
      status: updated.status,
      reviewed_by: updated.reviewedBy ?? null,
      reviewed_at: updated.reviewedAt?.toISOString() ?? null,
      decision_note: updated.decisionNote ?? null,
      created_at: updated.createdAt.toISOString(),
      updated_at: updated.updatedAt.toISOString(),
    },
  };
}

export async function listObjectAccessAssignmentsForTenant(params: {
  tenantId: string;
  objectId: string;
}): Promise<Record<string, unknown>> {
  const object = await findObjectById({ tenantId: params.tenantId, objectId: params.objectId });
  if (!object) {
    throw new NotFoundError(`Object '${params.objectId}' was not found.`);
  }

  const assignments = await listObjectAccessAssignmentsByObjectId({
    tenantId: params.tenantId,
    objectId: params.objectId,
  });

  return {
    object_id: params.objectId,
    assignments: assignments.map((assignment) => ({
      user_id: assignment.userId,
      granted_level: assignment.grantedLevel,
      created_by: assignment.createdBy,
      created_at: assignment.createdAt.toISOString(),
    })),
  };
}

export async function upsertObjectAccessAssignmentForTenant(params: {
  tenantId: string;
  objectId: string;
  actorUserId: string;
  body: unknown;
}): Promise<Record<string, unknown>> {
  if (params.body === null || typeof params.body !== "object" || Array.isArray(params.body)) {
    throw new ValidationError("Request body must be an object.");
  }

  const payload = params.body as Record<string, unknown>;
  const userId = payload.user_id;
  const grantedLevel = payload.granted_level;

  if (typeof userId !== "string" || userId.trim().length === 0) {
    throw new ValidationError("Field 'user_id' must be a non-empty string.");
  }

  if (
    typeof grantedLevel !== "string" ||
    !["family", "private"].includes(grantedLevel)
  ) {
    throw new ValidationError("Field 'granted_level' is invalid.");
  }

  const object = await findObjectById({ tenantId: params.tenantId, objectId: params.objectId });
  if (!object) {
    throw new NotFoundError(`Object '${params.objectId}' was not found.`);
  }

  const assignment = await upsertObjectAccessAssignment({
    objectId: params.objectId,
    tenantId: params.tenantId,
    userId,
    grantedLevel: grantedLevel as "family" | "private",
    createdBy: params.actorUserId,
  });

  return {
    assignment: {
      object_id: assignment.objectId,
      user_id: assignment.userId,
      granted_level: assignment.grantedLevel,
      created_by: assignment.createdBy,
      created_at: assignment.createdAt.toISOString(),
    },
  };
}

export async function deleteObjectAccessAssignmentForTenant(params: {
  tenantId: string;
  objectId: string;
  userId: string;
}): Promise<Record<string, unknown>> {
  const object = await findObjectById({ tenantId: params.tenantId, objectId: params.objectId });
  if (!object) {
    throw new NotFoundError(`Object '${params.objectId}' was not found.`);
  }

  const deleted = await deleteObjectAccessAssignment({
    objectId: params.objectId,
    userId: params.userId,
  });

  if (!deleted) {
    throw new NotFoundError(
      `Assignment for user '${params.userId}' was not found for object '${params.objectId}'.`,
    );
  }

  return {
    status: "ok",
    object_id: params.objectId,
    user_id: params.userId,
  };
}
