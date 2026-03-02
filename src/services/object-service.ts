import {
    ConflictError,
    NotFoundError,
    ValidationError,
} from "../http/errors.ts";
import { encodeCursor } from "../http/pagination.ts";
import {
    buildAccessDecision,
    type AccessReasonCode,
} from "../domain/objects/access-policy.ts";
import type { AuthenticatedContext } from "../auth/guards.ts";
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
    createObjectDownloadRequest,
    findActiveObjectDownloadRequest,
    listObjectDownloadRequestsByObjectId,
    type ObjectDownloadRequestRecord,
} from "../repos/object-download-request-repo.ts";
import {
    findLatestArtifactByKind,
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
import {
    parseObjectMetadata,
    type CreateAccessRequestBody,
    type CreateAccessRequestResponse,
    type CreateObjectDownloadRequestBody,
    type CreateObjectDownloadRequestResponse,
    type DeleteAccessAssignmentResponse,
    type ListObjectDownloadRequestsResponse,
    type ListAccessAssignmentsResponse,
    type ListAccessRequestsResponse,
    type ObjectArtifactsResponse,
    type ObjectDto,
    type ObjectDetailResponse,
    type ObjectListQuery,
    type ObjectListResponse,
    type PatchObjectTitleBody,
    type PatchObjectTitleResponse,
    type ResolveAccessRequestBody,
    type RequestedArtifactKind,
    type ResolveAccessRequestResponse,
    type ObjectArtifactDto,
    type ObjectDownloadRequestDto,
    type UpdateAccessPolicyBody,
    type UpdateAccessPolicyResponse,
    type UpsertAccessAssignmentBody,
    type UpsertAccessAssignmentResponse,
} from "../validation/object.ts";
import type { JsonObject } from "../validation/ingestion.ts";

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

    if (
        maybeError.constraint ===
        "object_access_requests_one_pending_per_user_idx"
    ) {
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

function serializeObject(record: ObjectRecord): ObjectDto;
function serializeObject(
    record: ObjectRecord,
    options: { includeIngestManifest: true },
): ObjectDto & { ingest_manifest: JsonObject | null };
function serializeObject(
    record: ObjectRecord,
    options?: { includeIngestManifest?: boolean },
): ObjectDto & { ingest_manifest?: JsonObject | null } {
    const payload: ObjectDto & { ingest_manifest?: JsonObject | null } = {
        id: record.objectId,
        object_id: record.objectId,
        tenant_id: record.tenantId,
        type: record.type,
        title: record.title,
        language: record.languageCode ?? null,
        tags: record.tags,
        metadata: parseObjectMetadata(record.metadata),
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
        payload.ingest_manifest = record.ingestManifest
            ? parseObjectMetadata(record.ingestManifest)
            : null;
    }

    return payload;
}

function serializeArtifact(
    record: ObjectArtifactRecord,
): ObjectArtifactDto {
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

function mapRequestedKindToArtifactKinds(
    kind: RequestedArtifactKind,
): ObjectArtifactRecord["kind"][] {
    if (kind === "ocr_text") {
        return ["ocr_text", "ocr"];
    }

    if (kind === "thumbnail") {
        return ["thumbnail", "preview"];
    }

    return [kind];
}

function serializeDownloadRequest(
    record: ObjectDownloadRequestRecord,
): ObjectDownloadRequestDto {
    return {
        id: record.id,
        object_id: record.objectId,
        requested_by: record.requestedBy,
        artifact_kind: record.artifactKind,
        status: record.status,
        failure_reason: record.failureReason,
        created_at: record.createdAt.toISOString(),
        updated_at: record.updatedAt.toISOString(),
        completed_at: record.completedAt ? record.completedAt.toISOString() : null,
    };
}

function isActiveDownloadRequestUniqueViolation(error: unknown): boolean {
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

    if (
        maybeError.constraint === "object_download_requests_one_active_kind_idx"
    ) {
        return true;
    }

    return (
        typeof maybeError.message === "string" &&
        maybeError.message.includes("object_download_requests_one_active_kind_idx")
    );
}

export interface CreateObjectDownloadRequestResult {
    response: CreateObjectDownloadRequestResponse;
    outcome: "available" | "created" | "deduped";
}

export async function createObjectDownloadRequestForTenant(params: {
    auth: AuthenticatedContext;
    objectId: string;
    body: CreateObjectDownloadRequestBody;
}): Promise<CreateObjectDownloadRequestResult> {
    const object = await findObjectById({
        tenantId: params.auth.tenantId,
        objectId: params.objectId,
    });

    if (!object) {
        throw new NotFoundError(`Object '${params.objectId}' was not found.`);
    }

    const existingArtifact = await findLatestArtifactByKind({
        tenantId: params.auth.tenantId,
        objectId: params.objectId,
        kinds: mapRequestedKindToArtifactKinds(params.body.artifact_kind),
    });

    if (existingArtifact) {
        return {
            outcome: "available",
            response: {
                status: "available",
                object_id: params.objectId,
                artifact: serializeArtifact(existingArtifact),
            },
        };
    }

    const activeRequest = await findActiveObjectDownloadRequest({
        tenantId: params.auth.tenantId,
        objectId: params.objectId,
        artifactKind: params.body.artifact_kind,
    });

    if (activeRequest) {
        return {
            outcome: "deduped",
            response: {
                status: "queued",
                object_id: params.objectId,
                request: serializeDownloadRequest(activeRequest),
            },
        };
    }

    let created: ObjectDownloadRequestRecord;

    try {
        created = await createObjectDownloadRequest({
            tenantId: params.auth.tenantId,
            objectId: params.objectId,
            requestedBy: params.auth.userId,
            artifactKind: params.body.artifact_kind,
        });
    } catch (error) {
        if (!isActiveDownloadRequestUniqueViolation(error)) {
            throw error;
        }

        const winner = await findActiveObjectDownloadRequest({
            tenantId: params.auth.tenantId,
            objectId: params.objectId,
            artifactKind: params.body.artifact_kind,
        });

        if (!winner) {
            throw error;
        }

        return {
            outcome: "deduped",
            response: {
                status: "queued",
                object_id: params.objectId,
                request: serializeDownloadRequest(winner),
            },
        };
    }

    return {
        outcome: "created",
        response: {
            status: "queued",
            object_id: params.objectId,
            request: serializeDownloadRequest(created),
        },
    };
}

export async function listObjectDownloadRequestsForTenant(params: {
    auth: AuthenticatedContext;
    objectId: string;
}): Promise<ListObjectDownloadRequestsResponse> {
    const object = await findObjectById({
        tenantId: params.auth.tenantId,
        objectId: params.objectId,
    });

    if (!object) {
        throw new NotFoundError(`Object '${params.objectId}' was not found.`);
    }

    const requests = await listObjectDownloadRequestsByObjectId({
        tenantId: params.auth.tenantId,
        objectId: params.objectId,
    });

    return {
        object_id: params.objectId,
        requests: requests.map(serializeDownloadRequest),
    };
}

export async function listObjectsForTenant(params: {
    auth: AuthenticatedContext;
    query: ObjectListQuery;
}): Promise<ObjectListResponse> {
    const pagination = params.query;
    const sort = pagination.sort;
    const cursorPayload = pagination.cursor;
    const result = await listObjects({
        tenantId: params.auth.tenantId,
        limit: pagination.limit + 1,
        sort,
        cursorCreatedAt: cursorPayload?.created_at,
        cursorUpdatedAt: cursorPayload?.updated_at,
        cursorTitle: cursorPayload?.title,
        cursorObjectId: cursorPayload?.object_id,
        type: pagination.type,
        availabilityState: pagination.availabilityState,
        accessLevel: pagination.accessLevel,
        query: pagination.query,
        language: pagination.language,
        batchLabel: pagination.batchLabel,
        fromCreatedAt: pagination.from,
        toCreatedAt: pagination.to,
        tag: pagination.tag,
    });
    const hasMore = result.items.length > pagination.limit;
    const visible = hasMore
        ? result.items.slice(0, pagination.limit)
        : result.items;
    const lastItem = visible.at(-1);

    const assignmentByObjectId =
        await listObjectAccessAssignmentsForUserByObjectIds({
            tenantId: params.auth.tenantId,
            userId: params.auth.userId,
            objectIds: visible.map((item) => item.objectId),
        });
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
    let returnResponse = {
        objects: visible.map((record) => {
            const projection = computeAccessProjection(record, {
                role: params.auth.role,
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
    return returnResponse;
}

export async function getObjectDetail(params: {
    auth: AuthenticatedContext;
    objectId: string;
}): Promise<ObjectDetailResponse> {
    const objectRecord = await findObjectById({
        tenantId: params.auth.tenantId,
        objectId: params.objectId,
    });

    if (!objectRecord) {
        throw new NotFoundError(`Object '${params.objectId}' was not found.`);
    }

    const assignment = await findObjectAccessAssignmentForUser({
        tenantId: params.auth.tenantId,
        objectId: params.objectId,
        userId: params.auth.userId,
    });

    const projection = computeAccessProjection(objectRecord, {
        role: params.auth.role,
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
    auth: AuthenticatedContext;
    objectId: string;
    body: PatchObjectTitleBody;
}): Promise<PatchObjectTitleResponse> {
    const updated = await updateObjectTitle({
        tenantId: params.auth.tenantId,
        objectId: params.objectId,
        title: params.body.title,
    });

    if (!updated) {
        throw new NotFoundError(`Object '${params.objectId}' was not found.`);
    }

    return {
        object: serializeObject(updated),
    };
}

export async function listObjectArtifactsForTenant(params: {
    auth: AuthenticatedContext;
    objectId: string;
}): Promise<ObjectArtifactsResponse> {
    const objectRecord = await findObjectById({
        tenantId: params.auth.tenantId,
        objectId: params.objectId,
    });

    if (!objectRecord) {
        throw new NotFoundError(`Object '${params.objectId}' was not found.`);
    }

    const artifacts = await listArtifactsByObjectId({
        tenantId: params.auth.tenantId,
        objectId: params.objectId,
    });

    return {
        object_id: params.objectId,
        artifacts: artifacts.map(serializeArtifact),
    };
}

export async function downloadObjectArtifactForTenant(params: {
    auth: AuthenticatedContext;
    objectId: string;
    artifactId: string;
}): Promise<Response> {
    const objectRecord = await findObjectById({
        tenantId: params.auth.tenantId,
        objectId: params.objectId,
    });

    if (!objectRecord) {
        throw new NotFoundError(`Object '${params.objectId}' was not found.`);
    }

    const assignment = await findObjectAccessAssignmentForUser({
        tenantId: params.auth.tenantId,
        objectId: params.objectId,
        userId: params.auth.userId,
    });

    const projection = computeAccessProjection(objectRecord, {
        role: params.auth.role,
        assignmentLevel: assignment?.grantedLevel,
    });

    if (!projection.canDownload) {
        throw new ValidationError(
            "Object artifact is not downloadable in the current access state.",
            {
                access_reason_code: projection.accessReasonCode,
            },
        );
    }

    const artifact = await findArtifactById({
        tenantId: params.auth.tenantId,
        objectId: params.objectId,
        artifactId: params.artifactId,
    });

    if (!artifact) {
        throw new NotFoundError(
            `Artifact '${params.artifactId}' was not found for object '${params.objectId}'.`,
        );
    }

    const filePath = resolveStagingPath(artifact.storageKey);
    const file = Bun.file(filePath);

    if (!(await file.exists())) {
        throw new NotFoundError(
            `Artifact '${params.artifactId}' storage file was not found.`,
        );
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
    auth: AuthenticatedContext;
    objectId: string;
    body: UpdateAccessPolicyBody;
}): Promise<UpdateAccessPolicyResponse> {
    const updated = await updateObjectAccessPolicy({
        tenantId: params.auth.tenantId,
        objectId: params.objectId,
        accessLevel: params.body.access_level,
        embargoKind: params.body.embargo_kind,
        embargoUntil:
            params.body.embargo_kind === "timed"
                ? (params.body.embargo_until ?? null)
                : null,
        embargoCurationState:
            params.body.embargo_kind === "curation_state"
                ? (params.body.embargo_curation_state ?? null)
                : null,
        rightsNote: params.body.rights_note ?? null,
        sensitivityNote: params.body.sensitivity_note ?? null,
    });

    if (!updated) {
        throw new NotFoundError(`Object '${params.objectId}' was not found.`);
    }

    return {
        object: serializeObject(updated, { includeIngestManifest: true }),
    };
}

export async function createObjectAccessRequestForTenant(params: {
    auth: AuthenticatedContext;
    objectId: string;
    body: CreateAccessRequestBody;
}): Promise<CreateAccessRequestResponse> {
    const object = await findObjectById({
        tenantId: params.auth.tenantId,
        objectId: params.objectId,
    });
    if (!object) {
        throw new NotFoundError(`Object '${params.objectId}' was not found.`);
    }

    const existingPending = await findPendingObjectAccessRequestForUser({
        tenantId: params.auth.tenantId,
        objectId: params.objectId,
        requesterUserId: params.auth.userId,
    });

    if (existingPending) {
        throw new ConflictError(
            "A pending access request already exists for this object and user.",
            {
                request_id: existingPending.id,
                object_id: params.objectId,
                requester_user_id: params.auth.userId,
            },
        );
    }

    let request: Awaited<ReturnType<typeof createObjectAccessRequest>>;
    try {
        request = await createObjectAccessRequest({
            objectId: params.objectId,
            tenantId: params.auth.tenantId,
            requesterUserId: params.auth.userId,
            requestedLevel: params.body.requested_level,
            reason: params.body.reason,
        });
    } catch (error) {
        if (!isPendingAccessRequestUniqueViolation(error)) {
            throw error;
        }

        const pending = await findPendingObjectAccessRequestForUser({
            tenantId: params.auth.tenantId,
            objectId: params.objectId,
            requesterUserId: params.auth.userId,
        });

        throw new ConflictError(
            "A pending access request already exists for this object and user.",
            {
                request_id: pending?.id,
                object_id: params.objectId,
                requester_user_id: params.auth.userId,
            },
        );
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
    auth: AuthenticatedContext;
    objectId: string;
}): Promise<ListAccessRequestsResponse> {
    const object = await findObjectById({
        tenantId: params.auth.tenantId,
        objectId: params.objectId,
    });
    if (!object) {
        throw new NotFoundError(`Object '${params.objectId}' was not found.`);
    }

    const requests = await listObjectAccessRequests({
        tenantId: params.auth.tenantId,
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
    auth: AuthenticatedContext;
    objectId: string;
    requestId: string;
    action: "approve" | "reject";
    body: ResolveAccessRequestBody;
}): Promise<ResolveAccessRequestResponse> {
    const request = await findObjectAccessRequestById({
        requestId: params.requestId,
        objectId: params.objectId,
        tenantId: params.auth.tenantId,
    });

    if (!request) {
        throw new NotFoundError(
            `Access request '${params.requestId}' was not found.`,
        );
    }

    if (request.status !== "PENDING") {
        throw new ConflictError(
            "Access request is already resolved and cannot be changed.",
            {
                request_id: request.id,
                status: request.status,
            },
        );
    }

    const updated = await updateObjectAccessRequestStatus({
        requestId: params.requestId,
        objectId: params.objectId,
        tenantId: params.auth.tenantId,
        status: params.action === "approve" ? "APPROVED" : "REJECTED",
        reviewedBy: params.auth.userId,
        decisionNote: params.body.decision_note,
    });

    if (!updated) {
        const latest = await findObjectAccessRequestById({
            requestId: params.requestId,
            objectId: params.objectId,
            tenantId: params.auth.tenantId,
        });

        if (latest && latest.status !== "PENDING") {
            throw new ConflictError(
                "Access request is already resolved and cannot be changed.",
                {
                    request_id: latest.id,
                    status: latest.status,
                },
            );
        }

        throw new NotFoundError(
            `Access request '${params.requestId}' was not found.`,
        );
    }

    if (params.action === "approve") {
        await upsertObjectAccessAssignment({
            objectId: updated.objectId,
            tenantId: updated.tenantId,
            userId: updated.requesterUserId,
            grantedLevel: updated.requestedLevel,
            createdBy: params.auth.userId,
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
    auth: AuthenticatedContext;
    objectId: string;
}): Promise<ListAccessAssignmentsResponse> {
    const object = await findObjectById({
        tenantId: params.auth.tenantId,
        objectId: params.objectId,
    });
    if (!object) {
        throw new NotFoundError(`Object '${params.objectId}' was not found.`);
    }

    const assignments = await listObjectAccessAssignmentsByObjectId({
        tenantId: params.auth.tenantId,
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
    auth: AuthenticatedContext;
    objectId: string;
    body: UpsertAccessAssignmentBody;
}): Promise<UpsertAccessAssignmentResponse> {
    const object = await findObjectById({
        tenantId: params.auth.tenantId,
        objectId: params.objectId,
    });
    if (!object) {
        throw new NotFoundError(`Object '${params.objectId}' was not found.`);
    }

    const assignment = await upsertObjectAccessAssignment({
        objectId: params.objectId,
        tenantId: params.auth.tenantId,
        userId: params.body.user_id,
        grantedLevel: params.body.granted_level,
        createdBy: params.auth.userId,
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
    auth: AuthenticatedContext;
    objectId: string;
    userId: string;
}): Promise<DeleteAccessAssignmentResponse> {
    const object = await findObjectById({
        tenantId: params.auth.tenantId,
        objectId: params.objectId,
    });
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
