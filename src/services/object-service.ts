import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";

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
    findAvailableFileById,
    listAvailableFilesByObjectId,
    replaceObjectAvailableFiles,
    type ObjectAvailableFileRecord,
} from "../repos/object-available-file-repo.ts";
import {
    completeArchiveRequest,
    createArchiveRequest,
    extendArchiveRequestLease,
    failArchiveRequest,
    findActiveArchiveRequestByDedupeKey,
    findArchiveRequestById,
    leaseNextPendingArchiveRequest,
    listArchiveRequests,
    listArchiveRequestsByTarget,
    releaseArchiveRequestLease,
    sweepExpiredArchiveRequestLeases,
    type ArchiveRequestRecord,
    type ArchiveRequestTargetType,
} from "../repos/archive-request-repo.ts";
import {
    findLatestArtifactByKind,
    findArtifactByStorageKey,
    findArtifactById,
    findPreferredThumbnailArtifactIdByObjectId,
    findObjectById,
    findObjectByIdUnscoped,
    listPreferredThumbnailArtifactIdsByObjectIds,
    listArtifactsByObjectId,
    listObjects,
    createObjectArtifact,
    type ArtifactKind,
    type ObjectListSort,
    updateObjectAccessPolicy,
    updateObjectTitle,
    type ObjectArtifactRecord,
    type ObjectRecord,
} from "../repos/object-repo.ts";
import { resolveStagingPath } from "../storage/staging.ts";
import {
    buildObjectArtifactStorageKey,
    createObjectArtifactUploadToken,
    parseObjectArtifactUploadToken,
} from "../storage/staging.ts";
import {
    authorizeWorkerLeaseForDownloadRequest,
    createDownloadRequestLeaseToken,
    type AuthorizedWorkerDownloadRequestLease,
} from "../auth/worker-download-request.ts";
import {
    parseObjectMetadata,
    type CreateAccessRequestBody,
    type CreateAccessRequestResponse,
    type CreateObjectDownloadRequestBody,
    type CreateObjectDownloadRequestResponse,
    type ArchiveRequestListQuery,
    type DeleteAccessAssignmentResponse,
    type ListArchiveRequestsResponse,
    type ListObjectDownloadRequestsResponse,
    type ListAccessAssignmentsResponse,
    type ListAccessRequestsResponse,
    type ObjectArtifactsResponse,
    type ObjectDto,
    type ObjectDetailResponse,
    type ObjectListQuery,
    type ObjectListResponse,
    type ObjectAvailableFileDto,
    type ListObjectAvailableFilesResponse,
    type PatchObjectTitleBody,
    type PatchObjectTitleResponse,
    type ResolveAccessRequestBody,
    type ReplaceObjectAvailableFilesBody,
    type ReplaceObjectAvailableFilesResponse,
    type WorkerFailObjectDownloadRequestBody,
    type WorkerHeartbeatObjectDownloadRequestResponse,
    type WorkerLeaseObjectDownloadRequestResponse,
    type WorkerPresignObjectArtifactUploadBody,
    type WorkerPresignObjectArtifactUploadResponse,
    type WorkerReleaseObjectDownloadRequestResponse,
    type WorkerCompleteObjectDownloadRequestBody,
    type WorkerCompleteObjectDownloadRequestResponse,
    type WorkerFailObjectDownloadRequestResponse,
    type WorkerUploadObjectArtifactByTokenResponse,
    type ResolveAccessRequestResponse,
    type ObjectArtifactDto,
    type ObjectDownloadRequestDto,
    type UpdateAccessPolicyBody,
    type UpdateAccessPolicyResponse,
    type UpsertAccessAssignmentBody,
    type UpsertAccessAssignmentResponse,
} from "../validation/object.ts";
import type { JsonObject } from "../validation/ingestion.ts";

const DEFAULT_DOWNLOAD_REQUEST_LEASE_TTL_SECONDS = 60 * 5;
const DEFAULT_WORKER_UPLOAD_TTL_SECONDS = 60 * 15;
const SYSTEM_DOWNLOAD_REQUEST_USER_ID = "00000000-0000-0000-0000-000000000000";
const AUTO_REQUEST_ARTIFACT_KINDS: ArtifactKind[] = ["thumbnail", "ocr_text"];

interface SyncAvailableFileCandidate {
    archiveFileKey: string;
    artifactKind: ReplaceObjectAvailableFilesBody["files"][number]["artifact_kind"];
    variant: string | null;
    displayName: string;
    contentType: string | null;
    sizeBytes: number | null;
    checksumSha256: string | null;
    metadata: JsonObject;
    isAvailable: boolean;
}

function selectAutoRequestCandidate(params: {
    files: SyncAvailableFileCandidate[];
    artifactKind: ArtifactKind;
}): SyncAvailableFileCandidate | undefined {
    const candidates = params.files.filter(
        (file) =>
            file.artifactKind === params.artifactKind && file.isAvailable,
    );

    if (candidates.length === 0) {
        return undefined;
    }

    return candidates
        .slice()
        .sort((left, right) => {
            if (left.variant === null && right.variant !== null) {
                return -1;
            }

            if (left.variant !== null && right.variant === null) {
                return 1;
            }

            return left.archiveFileKey.localeCompare(right.archiveFileKey);
        })[0];
}

async function enqueueAutoArtifactRequestsFromSnapshot(params: {
    tenantId: string;
    objectId: string;
    files: SyncAvailableFileCandidate[];
}): Promise<void> {
    const selectedCandidates = AUTO_REQUEST_ARTIFACT_KINDS.map((artifactKind) => ({
        artifactKind,
        candidate: selectAutoRequestCandidate({
            files: params.files,
            artifactKind,
        }),
    })).filter(
        (
            item,
        ): item is {
            artifactKind: ArtifactKind;
            candidate: SyncAvailableFileCandidate;
        } => item.candidate !== undefined,
    );

    if (selectedCandidates.length === 0) {
        return;
    }

    const existingArtifacts = await listArtifactsByObjectId({
        tenantId: params.tenantId,
        objectId: params.objectId,
    });
    const existingRequests = await listArchiveRequestsByTarget({
        tenantId: params.tenantId,
        targetType: "object",
        targetId: params.objectId,
    });
    const availableFiles = await listAvailableFilesByObjectId({
        tenantId: params.tenantId,
        objectId: params.objectId,
    });

    for (const { artifactKind, candidate } of selectedCandidates) {
        const hasArtifact = existingArtifacts.some(
            (artifact) => artifact.kind === artifactKind,
        );

        if (hasArtifact) {
            continue;
        }

        const hasActiveRequest = existingRequests.some(
            (request) =>
                request.actionType === "artifact_fetch" &&
                parseArtifactFetchActionPayload(request).artifact_kind ===
                    artifactKind &&
                (request.status === "PENDING" ||
                    request.status === "PROCESSING"),
        );

        if (hasActiveRequest) {
            continue;
        }

        const selectedAvailableFile = availableFiles.find(
            (file) =>
                file.archiveFileKey === candidate.archiveFileKey &&
                file.artifactKind === artifactKind &&
                file.variant === candidate.variant,
        );

        if (!selectedAvailableFile) {
            continue;
        }

        try {
            await createArchiveRequest({
                tenantId: params.tenantId,
                targetType: "object",
                targetId: params.objectId,
                actionType: "artifact_fetch",
                actionPayload: buildArtifactFetchActionPayload({
                    availableFileId: selectedAvailableFile.id,
                    artifactKind: selectedAvailableFile.artifactKind,
                    variant: selectedAvailableFile.variant,
                }),
                requestedBy: SYSTEM_DOWNLOAD_REQUEST_USER_ID,
                dedupeKey: artifactFetchDedupeKey({
                    objectId: params.objectId,
                    artifactKind: selectedAvailableFile.artifactKind,
                    variant: selectedAvailableFile.variant,
                }),
            });
        } catch (error) {
            if (!isActiveArchiveRequestDedupeViolation(error)) {
                throw error;
            }
        }
    }
}

function isPendingAccessRequestUniqueViolation(error: unknown): boolean {
    if (!error || typeof error !== "object") {
        return false;
    }

    const maybeError = error as {
        code?: unknown;
        errno?: unknown;
        constraint?: unknown;
        message?: unknown;
    };

    if (maybeError.code !== "23505" && maybeError.errno !== "23505") {
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
        thumbnail_artifact_id: null,
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

function serializeArtifact(record: ObjectArtifactRecord): ObjectArtifactDto {
    return {
        id: record.id,
        object_id: record.objectId,
        kind: record.kind,
        variant: record.variant,
        storage_key: record.storageKey,
        content_type: record.contentType,
        size_bytes: record.sizeBytes,
        created_at: record.createdAt.toISOString(),
    };
}

function serializeAvailableFile(
    record: ObjectAvailableFileRecord,
): ObjectAvailableFileDto {
    return {
        id: record.id,
        object_id: record.objectId,
        archive_file_key: record.archiveFileKey,
        artifact_kind: record.artifactKind,
        variant: record.variant,
        display_name: record.displayName,
        content_type: record.contentType,
        size_bytes: record.sizeBytes,
        checksum_sha256: record.checksumSha256,
        metadata: parseObjectMetadata(record.metadata),
        is_available: record.isAvailable,
        synced_at: record.syncedAt.toISOString(),
    };
}


interface ArtifactFetchActionPayload extends JsonObject {
    available_file_id: string;
    artifact_kind: ArtifactKind;
    variant: string | null;
}

function artifactFetchDedupeKey(params: {
    objectId: string;
    artifactKind: ArtifactKind;
    variant: string | null;
}): string {
    return `artifact_fetch:${params.objectId}:${params.artifactKind}:${
        params.variant ?? ""
    }`;
}

function buildArtifactFetchActionPayload(params: {
    availableFileId: string;
    artifactKind: ArtifactKind;
    variant: string | null;
}): ArtifactFetchActionPayload {
    return {
        available_file_id: params.availableFileId,
        artifact_kind: params.artifactKind,
        variant: params.variant,
    };
}

function parseArtifactFetchActionPayload(
    record: ArchiveRequestRecord,
): ArtifactFetchActionPayload {
    const payload = record.actionPayload as Partial<ArtifactFetchActionPayload>;

    if (
        typeof payload.available_file_id !== "string" ||
        payload.available_file_id.trim().length === 0 ||
        typeof payload.artifact_kind !== "string" ||
        (payload.variant !== null &&
            payload.variant !== undefined &&
            typeof payload.variant !== "string")
    ) {
        throw new ConflictError(
            `Archive request '${record.id}' has invalid artifact_fetch payload.`,
        );
    }

    return {
        available_file_id: payload.available_file_id,
        artifact_kind: payload.artifact_kind,
        variant: payload.variant ?? null,
    };
}

function serializeDownloadRequestFromArchive(
    record: ArchiveRequestRecord,
): ObjectDownloadRequestDto {
    const payload = parseArtifactFetchActionPayload(record);

    return {
        id: record.id,
        object_id: record.targetId,
        available_file_id: payload.available_file_id,
        requested_by: record.requestedBy,
        artifact_kind: payload.artifact_kind,
        variant: payload.variant,
        status: record.status,
        failure_reason: record.failureReason,
        failure_details: record.failureDetails,
        created_at: record.createdAt.toISOString(),
        updated_at: record.updatedAt.toISOString(),
        completed_at: record.completedAt ? record.completedAt.toISOString() : null,
    };
}

export interface CreateObjectDownloadRequestResult {
    response: CreateObjectDownloadRequestResponse;
    outcome: "available" | "created" | "deduped";
}

export interface ObjectResyncRequestDto {
    id: string;
    tenant_id: string;
    target_type: ArchiveRequestTargetType;
    target_id: string;
    action_type: "object_resync";
    action_payload: JsonObject;
    requested_by: string;
    dedupe_key: string | null;
    status: "PENDING" | "PROCESSING" | "COMPLETED" | "FAILED" | "CANCELED";
    failure_reason: string | null;
    failure_details: JsonObject | null;
    created_at: string;
    updated_at: string;
    completed_at: string | null;
}

export interface RequestObjectResyncResponse {
    status: "queued";
    object_id: string;
    request: ObjectResyncRequestDto;
}

export interface RequestObjectResyncResult {
    response: RequestObjectResyncResponse;
    outcome: "created" | "deduped";
}

export interface ListObjectResyncRequestsResponse {
    object_id: string;
    requests: ObjectResyncRequestDto[];
}

interface ArchiveRequestListItemDto {
    id: string;
    tenant_id: string;
    target_type: ArchiveRequestTargetType;
    target_id: string;
    action_type: "object_resync" | "artifact_fetch";
    action_payload?: JsonObject;
    requested_by: string;
    dedupe_key: string | null;
    status: "PENDING" | "PROCESSING" | "COMPLETED" | "FAILED" | "CANCELED";
    failure_reason: string | null;
    failure_details: JsonObject | null;
    created_at: string;
    updated_at: string;
    completed_at: string | null;
}

function serializeObjectResyncRequest(
    record: ArchiveRequestRecord,
): ObjectResyncRequestDto {
    return {
        id: record.id,
        tenant_id: record.tenantId,
        target_type: record.targetType,
        target_id: record.targetId,
        action_type: "object_resync",
        action_payload: record.actionPayload,
        requested_by: record.requestedBy,
        dedupe_key: record.dedupeKey,
        status: record.status,
        failure_reason: record.failureReason,
        failure_details: record.failureDetails,
        created_at: record.createdAt.toISOString(),
        updated_at: record.updatedAt.toISOString(),
        completed_at: record.completedAt
            ? record.completedAt.toISOString()
            : null,
    };
}

function serializeArchiveRequestListItem(
    record: ArchiveRequestRecord,
    includePayload: boolean,
): ArchiveRequestListItemDto {
    const payload: ArchiveRequestListItemDto = {
        id: record.id,
        tenant_id: record.tenantId,
        target_type: record.targetType,
        target_id: record.targetId,
        action_type: record.actionType,
        requested_by: record.requestedBy,
        dedupe_key: record.dedupeKey,
        status: record.status,
        failure_reason: record.failureReason,
        failure_details: record.failureDetails,
        created_at: record.createdAt.toISOString(),
        updated_at: record.updatedAt.toISOString(),
        completed_at: record.completedAt
            ? record.completedAt.toISOString()
            : null,
    };

    if (includePayload) {
        payload.action_payload = record.actionPayload;
    }

    return payload;
}

function objectResyncDedupeKey(objectId: string): string {
    return `object_resync:${objectId}`;
}

function isActiveArchiveRequestDedupeViolation(error: unknown): boolean {
    if (!error || typeof error !== "object") {
        return false;
    }

    const maybeError = error as {
        code?: unknown;
        errno?: unknown;
        constraint?: unknown;
        message?: unknown;
    };

    if (maybeError.code !== "23505" && maybeError.errno !== "23505") {
        return false;
    }

    if (maybeError.constraint === "archive_requests_active_dedupe_idx") {
        return true;
    }

    return (
        typeof maybeError.message === "string" &&
        maybeError.message.includes("archive_requests_active_dedupe_idx")
    );
}

export async function requestObjectResyncForTenant(params: {
    auth: AuthenticatedContext;
    objectId: string;
    actionPayload?: JsonObject;
}): Promise<RequestObjectResyncResult> {
    const object = await findObjectById({
        tenantId: params.auth.tenantId,
        objectId: params.objectId,
    });

    if (!object) {
        throw new NotFoundError(`Object '${params.objectId}' was not found.`);
    }

    const dedupeKey = objectResyncDedupeKey(params.objectId);
    const existing = await findActiveArchiveRequestByDedupeKey({
        tenantId: params.auth.tenantId,
        actionType: "object_resync",
        dedupeKey,
    });

    if (existing) {
        return {
            outcome: "deduped",
            response: {
                status: "queued",
                object_id: params.objectId,
                request: serializeObjectResyncRequest(existing),
            },
        };
    }

    let created: ArchiveRequestRecord;

    try {
        created = await createArchiveRequest({
            tenantId: params.auth.tenantId,
            targetType: "object",
            targetId: params.objectId,
            actionType: "object_resync",
            actionPayload: params.actionPayload ?? {},
            requestedBy: params.auth.userId,
            dedupeKey,
        });
    } catch (error) {
        if (!isActiveArchiveRequestDedupeViolation(error)) {
            throw error;
        }

        const winner = await findActiveArchiveRequestByDedupeKey({
            tenantId: params.auth.tenantId,
            actionType: "object_resync",
            dedupeKey,
        });

        if (!winner) {
            throw error;
        }

        return {
            outcome: "deduped",
            response: {
                status: "queued",
                object_id: params.objectId,
                request: serializeObjectResyncRequest(winner),
            },
        };
    }

    return {
        outcome: "created",
        response: {
            status: "queued",
            object_id: params.objectId,
            request: serializeObjectResyncRequest(created),
        },
    };
}

export async function listObjectResyncRequestsForTenant(params: {
    auth: AuthenticatedContext;
    objectId: string;
}): Promise<ListObjectResyncRequestsResponse> {
    const object = await findObjectById({
        tenantId: params.auth.tenantId,
        objectId: params.objectId,
    });

    if (!object) {
        throw new NotFoundError(`Object '${params.objectId}' was not found.`);
    }

    const requests = await listArchiveRequestsByTarget({
        tenantId: params.auth.tenantId,
        targetType: "object",
        targetId: params.objectId,
    });

    return {
        object_id: params.objectId,
        requests: requests
            .filter((request) => request.actionType === "object_resync")
            .map(serializeObjectResyncRequest),
    };
}

export async function listArchiveRequestsForTenant(params: {
    auth: AuthenticatedContext;
    query: ArchiveRequestListQuery;
}): Promise<ListArchiveRequestsResponse> {
    const result = await listArchiveRequests({
        tenantId: params.auth.tenantId,
        limit: params.query.limit + 1,
        cursorCreatedAt: params.query.cursor?.created_at,
        cursorRequestId: params.query.cursor?.id,
        targetType: params.query.targetType,
        targetId: params.query.targetId,
        actionType: params.query.actionType,
        statuses: params.query.statuses,
    });

    const hasNextPage = result.requests.length > params.query.limit;
    const page = hasNextPage
        ? result.requests.slice(0, params.query.limit)
        : result.requests;

    let nextCursor: string | null = null;
    if (hasNextPage) {
        const last = page[page.length - 1];
        if (last) {
            nextCursor = encodeCursor({
                sort: params.query.sort,
                created_at: last.createdAt.toISOString(),
                id: last.id,
            });
        }
    }

    return {
        requests: page.map((request) =>
            serializeArchiveRequestListItem(request, params.query.includePayload),
        ),
        next_cursor: nextCursor,
        filtered_count: result.filteredCount,
    };
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

    const availableFile = await findAvailableFileById({
        tenantId: params.auth.tenantId,
        objectId: params.objectId,
        availableFileId: params.body.available_file_id,
    });

    if (!availableFile) {
        throw new NotFoundError(
            `Available file '${params.body.available_file_id}' was not found for object '${params.objectId}'.`,
        );
    }

    const existingArtifact = await findLatestArtifactByKind({
        tenantId: params.auth.tenantId,
        objectId: params.objectId,
        kind: availableFile.artifactKind,
        variant: availableFile.variant,
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

    const dedupeKey = artifactFetchDedupeKey({
        objectId: params.objectId,
        artifactKind: availableFile.artifactKind,
        variant: availableFile.variant,
    });

    const activeRequest = await findActiveArchiveRequestByDedupeKey({
        tenantId: params.auth.tenantId,
        actionType: "artifact_fetch",
        dedupeKey,
    });

    if (activeRequest) {
        return {
            outcome: "deduped",
            response: {
                status: "queued",
                object_id: params.objectId,
                request: serializeDownloadRequestFromArchive(activeRequest),
            },
        };
    }

    let created: ArchiveRequestRecord;

    try {
        created = await createArchiveRequest({
            tenantId: params.auth.tenantId,
            targetType: "object",
            targetId: params.objectId,
            actionType: "artifact_fetch",
            actionPayload: buildArtifactFetchActionPayload({
                availableFileId: availableFile.id,
                artifactKind: availableFile.artifactKind,
                variant: availableFile.variant,
            }),
            requestedBy: params.auth.userId,
            dedupeKey,
        });
    } catch (error) {
        if (!isActiveArchiveRequestDedupeViolation(error)) {
            throw error;
        }

        const winner = await findActiveArchiveRequestByDedupeKey({
            tenantId: params.auth.tenantId,
            actionType: "artifact_fetch",
            dedupeKey,
        });

        if (!winner) {
            throw error;
        }

        return {
            outcome: "deduped",
            response: {
                status: "queued",
                object_id: params.objectId,
                request: serializeDownloadRequestFromArchive(winner),
            },
        };
    }

    return {
        outcome: "created",
        response: {
            status: "queued",
            object_id: params.objectId,
            request: serializeDownloadRequestFromArchive(created),
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

    const requests = await listArchiveRequestsByTarget({
        tenantId: params.auth.tenantId,
        targetType: "object",
        targetId: params.objectId,
    });

    return {
        object_id: params.objectId,
        requests: requests
            .filter((request) => request.actionType === "artifact_fetch")
            .map(serializeDownloadRequestFromArchive),
    };
}

export async function listObjectAvailableFilesForTenant(params: {
    auth: AuthenticatedContext;
    objectId: string;
}): Promise<ListObjectAvailableFilesResponse> {
    const object = await findObjectById({
        tenantId: params.auth.tenantId,
        objectId: params.objectId,
    });

    if (!object) {
        throw new NotFoundError(`Object '${params.objectId}' was not found.`);
    }

    const availableFiles = await listAvailableFilesByObjectId({
        tenantId: params.auth.tenantId,
        objectId: params.objectId,
    });

    return {
        object_id: params.objectId,
        available_files: availableFiles.map(serializeAvailableFile),
    };
}

export async function replaceObjectAvailableFilesSnapshot(params: {
    objectId: string;
    body: ReplaceObjectAvailableFilesBody;
}): Promise<ReplaceObjectAvailableFilesResponse> {
    const object = await findObjectByIdUnscoped({
        objectId: params.objectId,
    });

    if (!object) {
        throw new NotFoundError(`Object '${params.objectId}' was not found.`);
    }

    const files: SyncAvailableFileCandidate[] = params.body.files.map((file) => ({
        archiveFileKey: file.archive_file_key,
        artifactKind: file.artifact_kind,
        variant: file.variant ?? null,
        displayName: file.display_name,
        contentType: file.content_type ?? null,
        sizeBytes: file.size_bytes ?? null,
        checksumSha256: file.checksum_sha256 ?? null,
        metadata: file.metadata,
        isAvailable: file.is_available,
    }));

    const syncedFiles = await replaceObjectAvailableFiles({
        tenantId: object.tenantId,
        objectId: params.objectId,
        files,
    });

    await enqueueAutoArtifactRequestsFromSnapshot({
        tenantId: object.tenantId,
        objectId: params.objectId,
        files,
    });

    return {
        object_id: params.objectId,
        synced_files: syncedFiles,
    };
}

function downloadRequestLeaseTtlSeconds(): number {
    return DEFAULT_DOWNLOAD_REQUEST_LEASE_TTL_SECONDS;
}

function workerUploadTtlSeconds(): number {
    return DEFAULT_WORKER_UPLOAD_TTL_SECONDS;
}

function isObjectArtifactStorageConflict(error: unknown): boolean {
    if (!error || typeof error !== "object") {
        return false;
    }

    const maybeError = error as {
        code?: unknown;
        errno?: unknown;
        constraint?: unknown;
        message?: unknown;
    };

    if (maybeError.code !== "23505" && maybeError.errno !== "23505") {
        return false;
    }

    if (maybeError.constraint === "object_artifacts_storage_key_key") {
        return true;
    }

    return (
        typeof maybeError.message === "string" &&
        maybeError.message.includes("object_artifacts_storage_key_key")
    );
}

async function resolveAvailableFileForArtifactFetch(params: {
    tenantId: string;
    objectId: string;
    availableFileId: string;
}): Promise<ObjectAvailableFileDto | null> {
    if (!params.availableFileId) {
        return null;
    }

    const availableFile = await findAvailableFileById({
        tenantId: params.tenantId,
        objectId: params.objectId,
        availableFileId: params.availableFileId,
    });

    return availableFile ? serializeAvailableFile(availableFile) : null;
}

export async function leaseNextObjectDownloadRequest(params: {
    workerId?: string;
}): Promise<WorkerLeaseObjectDownloadRequestResponse> {
    await sweepExpiredArchiveRequestLeases();

    const lease = await leaseNextPendingArchiveRequest({
        workerId: params.workerId,
        leaseDurationSeconds: downloadRequestLeaseTtlSeconds(),
        actionType: "artifact_fetch",
    });

    if (!lease) {
        return { request: null };
    }

    const payload = parseArtifactFetchActionPayload(lease.request);

    const leaseToken = createDownloadRequestLeaseToken({
        request_id: lease.request.id,
        lease_id: lease.leaseId,
        lease_token_id: lease.leaseTokenId,
        object_id: lease.request.targetId,
        tenant_id: lease.request.tenantId,
        artifact_kind: payload.artifact_kind,
        variant: payload.variant,
        worker_id: params.workerId,
        exp: lease.leaseExpiresAt.toISOString(),
    });

    return {
        request: {
            request_id: lease.request.id,
            lease_id: lease.leaseId,
            lease_token: leaseToken,
            lease_expires_at: lease.leaseExpiresAt.toISOString(),
            object_id: lease.request.targetId,
            tenant_id: lease.request.tenantId,
            available_file_id: payload.available_file_id,
            artifact_kind: payload.artifact_kind,
            variant: payload.variant,
            available_file: await resolveAvailableFileForArtifactFetch({
                tenantId: lease.request.tenantId,
                objectId: lease.request.targetId,
                availableFileId: payload.available_file_id,
            }),
        },
    };
}

export async function heartbeatObjectDownloadRequestLease(params: {
    requestId: string;
    leaseToken: string;
}): Promise<WorkerHeartbeatObjectDownloadRequestResponse> {
    const authorizedLease = await authorizeWorkerLeaseForDownloadRequest({
        requestId: params.requestId,
        leaseToken: params.leaseToken,
    });

    const updated = await extendArchiveRequestLease({
        requestId: authorizedLease.requestId,
        leaseId: authorizedLease.leaseId,
        leaseTokenId: authorizedLease.leaseTokenId,
        leaseDurationSeconds: downloadRequestLeaseTtlSeconds(),
    });

    if (!updated) {
        throw new ConflictError("Lease is no longer active.");
    }

    const refreshedToken = createDownloadRequestLeaseToken({
        request_id: updated.request.id,
        lease_id: updated.leaseId,
        lease_token_id: updated.leaseTokenId,
        object_id: updated.request.targetId,
        tenant_id: updated.request.tenantId,
        artifact_kind: authorizedLease.artifactKind,
        variant: authorizedLease.variant,
        worker_id: authorizedLease.workerId,
        exp: updated.leaseExpiresAt.toISOString(),
    });

    return {
        request: {
            request_id: updated.request.id,
            lease_id: updated.leaseId,
            lease_token: refreshedToken,
            lease_expires_at: updated.leaseExpiresAt.toISOString(),
        },
    };
}

export async function releaseObjectDownloadRequestLeaseByToken(params: {
    requestId: string;
    leaseToken: string;
}): Promise<WorkerReleaseObjectDownloadRequestResponse> {
    const authorizedLease = await authorizeWorkerLeaseForDownloadRequest({
        requestId: params.requestId,
        leaseToken: params.leaseToken,
    });

    const released = await releaseArchiveRequestLease({
        requestId: authorizedLease.requestId,
        leaseId: authorizedLease.leaseId,
        leaseTokenId: authorizedLease.leaseTokenId,
    });

    if (!released) {
        throw new ConflictError("Lease is no longer active.");
    }

    return {
        status: "ok",
        request_id: released.id,
    };
}

function extensionFromContentType(contentType: string): string {
    const normalized = contentType.toLowerCase();

    if (normalized === "application/pdf") {
        return "pdf";
    }

    if (normalized.startsWith("text/plain")) {
        return "txt";
    }

    if (normalized.startsWith("image/jpeg")) {
        return "jpg";
    }

    if (normalized.startsWith("image/png")) {
        return "png";
    }

    if (normalized.startsWith("application/json")) {
        return "json";
    }

    return "bin";
}

export async function presignObjectArtifactUpload(params: {
    requestId: string;
    body: WorkerPresignObjectArtifactUploadBody;
}): Promise<WorkerPresignObjectArtifactUploadResponse> {
    const authorizedLease = await authorizeWorkerLeaseForDownloadRequest({
        requestId: params.requestId,
        leaseToken: params.body.lease_token,
    });

    const expiresAt = new Date(
        Date.now() + workerUploadTtlSeconds() * 1000,
    ).toISOString();
    const storageKey = buildObjectArtifactStorageKey({
        tenantId: authorizedLease.tenantId,
        objectId: authorizedLease.objectId,
        requestId: authorizedLease.requestId,
        artifactKind: authorizedLease.artifactKind,
        variant: authorizedLease.variant,
        extension:
            params.body.extension ||
            extensionFromContentType(params.body.content_type),
    });

    const uploadToken = createObjectArtifactUploadToken({
        request_id: authorizedLease.requestId,
        object_id: authorizedLease.objectId,
        tenant_id: authorizedLease.tenantId,
        artifact_kind: authorizedLease.artifactKind,
        variant: authorizedLease.variant,
        storage_key: storageKey,
        content_type: params.body.content_type,
        size_bytes: params.body.size_bytes,
        expires_at: expiresAt,
    });

    return {
        upload_token: uploadToken,
        upload_url: `/api/object-download-requests/uploads/${uploadToken}`,
        storage_key: storageKey,
        expires_at: expiresAt,
        headers: {
            "content-type": params.body.content_type,
            "content-length": params.body.size_bytes,
        },
    };
}

export async function uploadObjectArtifactBySignedToken(params: {
    uploadToken: string;
    request: Request;
}): Promise<WorkerUploadObjectArtifactByTokenResponse> {
    const token = parseObjectArtifactUploadToken(params.uploadToken);
    const requestContentType = params.request.headers
        .get("content-type")
        ?.split(";")[0]
        ?.trim();

    if (requestContentType !== token.content_type) {
        throw new ValidationError(
            "Upload content type does not match signed URL constraints.",
        );
    }

    const rawContentLength = params.request.headers.get("content-length");
    if (!rawContentLength) {
        throw new ValidationError(
            "Header 'content-length' is required for uploads.",
        );
    }

    const contentLength = Number.parseInt(rawContentLength, 10);
    if (!Number.isFinite(contentLength) || contentLength !== token.size_bytes) {
        throw new ValidationError(
            "Upload content length does not match signed URL constraints.",
        );
    }

    const bodyBytes = new Uint8Array(await params.request.arrayBuffer());
    if (bodyBytes.byteLength !== token.size_bytes) {
        throw new ValidationError(
            "Upload body size does not match signed URL constraints.",
        );
    }

    const destinationPath = resolveStagingPath(token.storage_key);
    await mkdir(dirname(destinationPath), { recursive: true });
    await Bun.write(destinationPath, bodyBytes);

    return {
        status: "ok",
        request_id: token.request_id,
        size_bytes: token.size_bytes,
    };
}

async function resolveArtifactForCompletion(params: {
    lease: AuthorizedWorkerDownloadRequestLease;
    uploadToken: string;
}): Promise<ObjectArtifactRecord> {
    const upload = parseObjectArtifactUploadToken(params.uploadToken);

    if (
        upload.request_id !== params.lease.requestId ||
        upload.object_id !== params.lease.objectId ||
        upload.tenant_id !== params.lease.tenantId ||
        upload.artifact_kind !== params.lease.artifactKind ||
        upload.variant !== params.lease.variant
    ) {
        throw new ValidationError(
            "Upload token does not match download request lease context.",
        );
    }

    const existing = await findLatestArtifactByKind({
        tenantId: params.lease.tenantId,
        objectId: params.lease.objectId,
        kind: params.lease.artifactKind,
        variant: params.lease.variant,
    });

    if (existing) {
        return existing;
    }

    const file = Bun.file(resolveStagingPath(upload.storage_key));
    if (!(await file.exists())) {
        throw new NotFoundError("Uploaded artifact file was not found.");
    }

    try {
        return await createObjectArtifact({
            objectId: params.lease.objectId,
            kind: params.lease.artifactKind,
            variant: params.lease.variant,
            storageKey: upload.storage_key,
            contentType: upload.content_type,
            sizeBytes: upload.size_bytes,
        });
    } catch (error) {
        if (!isObjectArtifactStorageConflict(error)) {
            throw error;
        }

        const byStorageKey = await findArtifactByStorageKey({
            objectId: params.lease.objectId,
            storageKey: upload.storage_key,
        });

        if (!byStorageKey) {
            throw error;
        }

        return byStorageKey;
    }
}

export async function completeObjectDownloadRequestByWorker(params: {
    requestId: string;
    body: WorkerCompleteObjectDownloadRequestBody;
}): Promise<WorkerCompleteObjectDownloadRequestResponse> {
    const authorizedLease = await authorizeWorkerLeaseForDownloadRequest({
        requestId: params.requestId,
        leaseToken: params.body.lease_token,
        requireActiveLease: false,
    });

    const artifact = await resolveArtifactForCompletion({
        lease: authorizedLease,
        uploadToken: params.body.upload_token,
    });

    const request = await findArchiveRequestById({ requestId: params.requestId });

    if (!request) {
        throw new NotFoundError(
            `Download request '${params.requestId}' was not found.`,
        );
    }

    if (request.status !== "COMPLETED") {
        const completed = await completeArchiveRequest({
            requestId: authorizedLease.requestId,
            leaseId: authorizedLease.leaseId,
            leaseTokenId: authorizedLease.leaseTokenId,
        });

        if (!completed) {
            throw new ConflictError("Lease is no longer active.");
        }
    }

    return {
        status: "completed",
        request_id: params.requestId,
        object_id: artifact.objectId,
        artifact: serializeArtifact(artifact),
    };
}

export async function failObjectDownloadRequestByWorker(params: {
    requestId: string;
    body: WorkerFailObjectDownloadRequestBody;
}): Promise<WorkerFailObjectDownloadRequestResponse> {
    const authorizedLease = await authorizeWorkerLeaseForDownloadRequest({
        requestId: params.requestId,
        leaseToken: params.body.lease_token,
    });

    const failed = await failArchiveRequest({
        requestId: authorizedLease.requestId,
        leaseId: authorizedLease.leaseId,
        leaseTokenId: authorizedLease.leaseTokenId,
        failureReason: params.body.failure.message,
        failureDetails: {
            code: params.body.failure.code,
            message: params.body.failure.message,
            retryable: params.body.failure.retryable,
            details: params.body.failure.details ?? {},
        },
    });

    if (!failed) {
        throw new ConflictError("Lease is no longer active.");
    }

    return {
        status: "failed",
        request_id: failed.id,
        retryable: params.body.failure.retryable,
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
    const thumbnailArtifactIdByObjectId =
        await listPreferredThumbnailArtifactIdsByObjectIds({
            tenantId: params.auth.tenantId,
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
                thumbnail_artifact_id:
                    thumbnailArtifactIdByObjectId.get(record.objectId) ?? null,
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
    const thumbnailArtifactId = await findPreferredThumbnailArtifactIdByObjectId({
        tenantId: params.auth.tenantId,
        objectId: params.objectId,
    });

    return {
        object: {
            ...serializeObject(objectRecord, { includeIngestManifest: true }),
            thumbnail_artifact_id: thumbnailArtifactId ?? null,
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

    const thumbnailArtifactId = await findPreferredThumbnailArtifactIdByObjectId({
        tenantId: params.auth.tenantId,
        objectId: params.objectId,
    });

    return {
        object: {
            ...serializeObject(updated),
            thumbnail_artifact_id: thumbnailArtifactId ?? null,
        },
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

    const thumbnailArtifactId = await findPreferredThumbnailArtifactIdByObjectId({
        tenantId: params.auth.tenantId,
        objectId: params.objectId,
    });

    return {
        object: {
            ...serializeObject(updated, { includeIngestManifest: true }),
            thumbnail_artifact_id: thumbnailArtifactId ?? null,
        },
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
