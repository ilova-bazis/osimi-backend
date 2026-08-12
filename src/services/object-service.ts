import {
    ConflictError,
    NotFoundError,
    ValidationError,
} from "../http/errors.ts";
import { encodeCursor } from "../http/pagination.ts";
import { withSchemaClient } from "../db/client.ts";
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
    findObjectAvailableFileById,
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
    findActiveArchiveRequestLeaseByToken,
    findArchiveRequestById,
    leaseNextPendingArchiveRequest,
    listArchiveRequests,
    listArchiveRequestsByTarget,
    releaseArchiveRequestLease,
    sweepExpiredArchiveRequestLeases,
    findArchiveRequestByIdForUpdate,
    transferArchiveRequestUploadToBackend,
    type ArchiveRequestRecord,
    type ArchiveRequestTargetType,
} from "../repos/archive-request-repo.ts";
import {
    createAuthorizedArchiveArtifactUploadAttempt,
    findArchiveArtifactUploadAttemptById,
    findArchiveArtifactUploadAttemptByIdForUpdate,
    verifyArchiveArtifactUploadAttempt,
} from "../repos/archive-artifact-upload-attempt-repo.ts";
import {
    findLatestArtifactByKind,
    findArtifactById,
    findPreferredThumbnailArtifactIdByObjectId,
    findObjectById,
    findObjectByIdUnscoped,
    listObjectArtifactSummariesByObjectIds,
    listArtifactsByObjectId,
    listObjects,
    type ArtifactKind,
    type ObjectListSort,
    updateObjectAccessPolicy,
    updateObjectMetadataPages,
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
    parseContentLength,
    stageImmutableUpload,
    streamUploadToImmutablePath,
} from "../storage/upload.ts";
import { resolveMaxUploadSizeBytes } from "../runtime/config.ts";
import { parseMediaType } from "../http/media-type.ts";
import { finalizeVerifiedArchiveArtifactUpload } from "./archive-artifact-finalization-service.ts";
import {
    authorizeWorkerLeaseForArchiveRequest,
    type AuthorizedWorkerArchiveRequestLease,
} from "../auth/worker-archive-request.ts";
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
    type ObjectViewer,
    type ObjectViewerActiveRequest,
    type ObjectViewerArtifactRef,
    type ObjectViewerPayload,
    type UpdateAccessPolicyBody,
    type UpdateAccessPolicyResponse,
    type UpsertAccessAssignmentBody,
    type UpsertAccessAssignmentResponse,
} from "../validation/object.ts";
import { ifRangeMatches, parseSingleByteRange } from "../http/range.ts";
import type { JsonObject } from "../validation/ingestion.ts";

const DEFAULT_DOWNLOAD_REQUEST_LEASE_TTL_SECONDS = 60 * 5;
const DEFAULT_WORKER_UPLOAD_TTL_SECONDS = 60 * 15;
const SYSTEM_DOWNLOAD_REQUEST_USER_ID = "00000000-0000-0000-0000-000000000000";
const AUTO_REQUEST_ARTIFACT_KINDS: ArtifactKind[] = [
    "thumbnail",
    "ocr_text",
    "web_version",
];

function canAutoRequestArtifactKindForObjectType(params: {
    artifactKind: ArtifactKind;
    objectType: ObjectRecord["type"];
}): boolean {
    if (params.artifactKind !== "web_version") {
        return true;
    }

    return params.objectType === "IMAGE";
}

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

function isPageOcrVariant(variant: string | null): variant is string {
    return variant !== null && /^page_\d{4}$/.test(variant);
}

function isCombinedOcrVariant(variant: string | null): variant is string | null {
    return variant === null || (variant !== null && variant.startsWith("full_"));
}

function extractPageNumberFromVariant(variant: string): number {
    return parseInt(variant.replace(/^page_0*/, ""), 10);
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

function selectCombinedOcrCandidate(
    files: SyncAvailableFileCandidate[],
): SyncAvailableFileCandidate | undefined {
    const candidates = files.filter(
        (file) =>
            file.artifactKind === "ocr_text" &&
            file.isAvailable &&
            isCombinedOcrVariant(file.variant),
    );

    if (candidates.length === 0) {
        return undefined;
    }

    return candidates
        .slice()
        .sort((left, right) => {
            const leftIsFullV1 = left.variant === "full_v1";
            const rightIsFullV1 = right.variant === "full_v1";
            const leftIsNull = left.variant === null;
            const rightIsNull = right.variant === null;

            if (leftIsFullV1 && !rightIsFullV1) return -1;
            if (!leftIsFullV1 && rightIsFullV1) return 1;
            if (leftIsNull && !rightIsNull) return -1;
            if (!leftIsNull && rightIsNull) return 1;

            return left.archiveFileKey.localeCompare(right.archiveFileKey);
        })[0];
}

function selectPageOcrCandidates(
    files: SyncAvailableFileCandidate[],
): SyncAvailableFileCandidate[] {
    return files.filter(
        (file) =>
            file.artifactKind === "ocr_text" &&
            file.isAvailable &&
            isPageOcrVariant(file.variant),
    );
}

async function enqueueAutoArtifactRequestsFromSnapshot(params: {
    tenantId: string;
    objectId: string;
    objectType: ObjectRecord["type"];
    files: SyncAvailableFileCandidate[];
}): Promise<void> {
    const selectedCandidates: Array<{
        artifactKind: ArtifactKind;
        candidate: SyncAvailableFileCandidate;
    }> = [];

    for (const artifactKind of AUTO_REQUEST_ARTIFACT_KINDS) {
        if (
            !canAutoRequestArtifactKindForObjectType({
                artifactKind,
                objectType: params.objectType,
            })
        ) {
            continue;
        }

        if (artifactKind === "ocr_text") {
            const combinedCandidate = selectCombinedOcrCandidate(params.files);
            if (combinedCandidate) {
                selectedCandidates.push({
                    artifactKind,
                    candidate: combinedCandidate,
                });
            }

            const pageCandidates = selectPageOcrCandidates(params.files);
            for (const candidate of pageCandidates) {
                selectedCandidates.push({ artifactKind, candidate });
            }
        } else {
            const candidate = selectAutoRequestCandidate({
                files: params.files,
                artifactKind,
            });
            if (candidate) {
                selectedCandidates.push({ artifactKind, candidate });
            }
        }
    }

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
        const candidateIsPageOcr =
            artifactKind === "ocr_text" && isPageOcrVariant(candidate.variant);
        const candidateIsCombinedOcr =
            artifactKind === "ocr_text" && isCombinedOcrVariant(candidate.variant);

        let hasArtifact: boolean;
        if (candidateIsPageOcr) {
            hasArtifact = existingArtifacts.some(
                (artifact) =>
                    artifact.kind === artifactKind &&
                    artifact.variant === candidate.variant,
            );
        } else if (candidateIsCombinedOcr) {
            hasArtifact = existingArtifacts.some(
                (artifact) =>
                    artifact.kind === artifactKind &&
                    isCombinedOcrVariant(artifact.variant),
            );
        } else {
            hasArtifact = existingArtifacts.some(
                (artifact) => artifact.kind === artifactKind,
            );
        }

        if (hasArtifact) {
            continue;
        }

        let hasActiveRequest: boolean;
        if (candidateIsPageOcr) {
            hasActiveRequest = existingRequests.some((request) => {
                if (
                    request.actionType !== "artifact_fetch" ||
                    request.status !== "PENDING" &&
                        request.status !== "PROCESSING"
                ) {
                    return false;
                }
                const payload = parseArtifactFetchActionPayload(request);
                return (
                    payload.artifact_kind === artifactKind &&
                    payload.variant === candidate.variant
                );
            });
        } else if (candidateIsCombinedOcr) {
            hasActiveRequest = existingRequests.some((request) => {
                if (
                    request.actionType !== "artifact_fetch" ||
                    request.status !== "PENDING" &&
                        request.status !== "PROCESSING"
                ) {
                    return false;
                }
                const payload = parseArtifactFetchActionPayload(request);
                return (
                    payload.artifact_kind === artifactKind &&
                    isCombinedOcrVariant(payload.variant)
                );
            });
        } else {
            hasActiveRequest = existingRequests.some(
                (request) =>
                    request.actionType === "artifact_fetch" &&
                    parseArtifactFetchActionPayload(request).artifact_kind ===
                        artifactKind &&
                    (request.status === "PENDING" ||
                        request.status === "PROCESSING"),
            );
        }

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

            if (
                params.objectType === "IMAGE" &&
                selectedAvailableFile.artifactKind === "thumbnail"
            ) {
                console.info(
                    `artifact_auto_request_created object_id=${params.objectId} tenant_id=${params.tenantId} artifact_kind=${selectedAvailableFile.artifactKind} variant=${selectedAvailableFile.variant ?? "null"} available_file_id=${selectedAvailableFile.id} archive_file_key=${selectedAvailableFile.archiveFileKey}`,
                );
            }
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

type ViewerMediaType = NonNullable<ObjectDetailResponse["viewer"]>["media_type"];
type ViewerSourceType = NonNullable<
    ObjectDetailResponse["viewer"]
>["primary_source"]["source_type"];

interface ViewerSourceCandidate {
    artifactKind: ArtifactKind;
    sourceType: ViewerSourceType;
}

interface ViewerBuildInput {
    objectRecord: ObjectRecord;
    projection: ReturnType<typeof computeAccessProjection>;
    artifacts: ObjectArtifactRecord[];
    availableFiles: ObjectAvailableFileRecord[];
    requests: ArchiveRequestRecord[];
    thumbnailArtifactId?: string;
}

function isJsonObject(value: unknown): value is JsonObject {
    return typeof value === "object" && value !== null && !Array.isArray(value);
}

function getMetadataNumber(
    metadata: JsonObject,
    key: string,
): number | null {
    const value = metadata[key];
    return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function getMetadataString(
    metadata: JsonObject,
    key: string,
): string | null {
    const value = metadata[key];
    return typeof value === "string" ? value : null;
}

function getMetadataPages(
    metadata: JsonObject,
): Array<{
    page_number: number;
    label: string | null;
    image_artifact_id: string | null;
    ocr_text_artifact_id: string | null;
}> | undefined {
    const value = metadata.pages;
    if (!Array.isArray(value)) {
        return undefined;
    }

    const pages = value
        .map((entry) => {
            if (!isJsonObject(entry)) {
                return undefined;
            }

            const pageNumber = entry.page_number;
            if (typeof pageNumber !== "number" || !Number.isInteger(pageNumber) || pageNumber <= 0) {
                return undefined;
            }

            return {
                page_number: pageNumber,
                label: typeof entry.label === "string" ? entry.label : null,
                image_artifact_id:
                    typeof entry.image_artifact_id === "string"
                        ? entry.image_artifact_id
                        : null,
                ocr_text_artifact_id:
                    typeof entry.ocr_text_artifact_id === "string"
                        ? entry.ocr_text_artifact_id
                        : null,
            };
        })
        .filter((entry): entry is NonNullable<typeof entry> => entry !== undefined)
        .sort((left, right) => left.page_number - right.page_number);

    return pages.length > 0 ? pages : undefined;
}

function mapObjectTypeToViewerMediaType(
    type: ObjectRecord["type"],
): ViewerMediaType | undefined {
    switch (type) {
        case "DOCUMENT":
            return "document";
        case "IMAGE":
            return "image";
        case "AUDIO":
            return "audio";
        case "VIDEO":
            return "video";
        default:
            return undefined;
    }
}

function getPrimarySourceCandidates(
    mediaType: ViewerMediaType,
): ViewerSourceCandidate[] {
    switch (mediaType) {
        case "document":
            return [
                { artifactKind: "pdf", sourceType: "access_copy" },
                { artifactKind: "web_version", sourceType: "access_copy" },
                { artifactKind: "original", sourceType: "original" },
                { artifactKind: "preview", sourceType: "preview" },
                { artifactKind: "other", sourceType: "other" },
            ];
        case "image":
            return [
                { artifactKind: "web_version", sourceType: "access_copy" },
                { artifactKind: "preview", sourceType: "preview" },
                { artifactKind: "original", sourceType: "original" },
                { artifactKind: "other", sourceType: "other" },
            ];
        case "audio":
            return [
                { artifactKind: "web_version", sourceType: "stream" },
                { artifactKind: "original", sourceType: "original" },
                { artifactKind: "other", sourceType: "other" },
            ];
        case "video":
            return [
                { artifactKind: "web_version", sourceType: "stream" },
                { artifactKind: "original", sourceType: "original" },
                { artifactKind: "preview", sourceType: "preview" },
                { artifactKind: "other", sourceType: "other" },
            ];
    }
}

function compareArtifacts(
    left: Pick<ObjectArtifactRecord, "variant" | "createdAt" | "id">,
    right: Pick<ObjectArtifactRecord, "variant" | "createdAt" | "id">,
): number {
    if (left.variant === null && right.variant !== null) {
        return -1;
    }

    if (left.variant !== null && right.variant === null) {
        return 1;
    }

    const createdAtOrder = right.createdAt.getTime() - left.createdAt.getTime();
    if (createdAtOrder !== 0) {
        return createdAtOrder;
    }

    return right.id.localeCompare(left.id);
}

function compareAvailableFiles(
    left: Pick<ObjectAvailableFileRecord, "variant" | "archiveFileKey">,
    right: Pick<ObjectAvailableFileRecord, "variant" | "archiveFileKey">,
): number {
    if (left.variant === null && right.variant !== null) {
        return -1;
    }

    if (left.variant !== null && right.variant === null) {
        return 1;
    }

    return left.archiveFileKey.localeCompare(right.archiveFileKey);
}

function isBrowserViewableArtifact(record: {
    contentType: string;
}): boolean {
    const contentType = record.contentType.toLowerCase();
    return (
        contentType === "application/pdf" ||
        contentType === "text/html" ||
        contentType === "text/plain" ||
        contentType.startsWith("image/") ||
        contentType.startsWith("audio/") ||
        contentType.startsWith("video/")
    );
}

function isPrimaryArtifactViewable(
    mediaType: ViewerMediaType,
    artifact: ObjectArtifactRecord,
): boolean {
    if (!isBrowserViewableArtifact(artifact)) {
        return false;
    }

    const contentType = artifact.contentType.toLowerCase();
    switch (mediaType) {
        case "document":
            return (
                contentType === "application/pdf" ||
                contentType === "text/html" ||
                contentType === "text/plain" ||
                contentType.startsWith("image/")
            );
        case "image":
            return contentType.startsWith("image/");
        case "audio":
            return contentType.startsWith("audio/");
        case "video":
            return contentType.startsWith("video/");
    }
}

function findBestArtifactByKind(
    artifacts: ObjectArtifactRecord[],
    artifactKind: ArtifactKind,
): ObjectArtifactRecord | undefined {
    return artifacts
        .filter((artifact) => artifact.kind === artifactKind)
        .slice()
        .sort(compareArtifacts)[0];
}

function findMatchingAvailableFile(
    availableFiles: ObjectAvailableFileRecord[],
    artifactKind: ArtifactKind,
    variant: string | null,
): ObjectAvailableFileRecord | undefined {
    return availableFiles.find(
        (file) =>
            file.artifactKind === artifactKind &&
            file.variant === variant,
    );
}

function findBestAvailableFileByKind(
    availableFiles: ObjectAvailableFileRecord[],
    artifactKind: ArtifactKind,
): ObjectAvailableFileRecord | undefined {
    return availableFiles
        .filter((file) => file.artifactKind === artifactKind)
        .slice()
        .sort(compareAvailableFiles)[0];
}

function serializeViewerArtifactRef(
    artifact: ObjectArtifactRecord,
    displayName: string | null,
    metadata: JsonObject = {},
): ObjectViewerArtifactRef {
    return {
        available: true,
        artifact_id: artifact.id,
        content_type: artifact.contentType,
        display_name: displayName,
        metadata,
    };
}

function findRelevantActiveRequest(
    requests: ArchiveRequestRecord[],
    objectId: string,
    artifactKind: ArtifactKind,
    variant: string | null,
): ObjectViewerActiveRequest | null {
    const dedupeKey = artifactFetchDedupeKey({
        objectId,
        artifactKind,
        variant,
    });
    const request = requests.find(
        (record) =>
            record.actionType === "artifact_fetch" &&
            record.status !== "COMPLETED" &&
            record.status !== "FAILED" &&
            record.status !== "CANCELED" &&
            record.dedupeKey === dedupeKey,
    );

    if (!request) {
        return null;
    }

    return {
        id: request.id,
        action_type: "artifact_fetch",
        status: request.status === "PROCESSING" ? "PROCESSING" : "PENDING",
        created_at: request.createdAt.toISOString(),
        updated_at: request.updatedAt.toISOString(),
    };
}

function buildViewerPayload(params: {
    mediaType: ViewerMediaType;
    primaryArtifact: ObjectArtifactRecord | undefined;
    primaryAvailableFile: ObjectAvailableFileRecord | undefined;
    ocrArtifact: ObjectArtifactRecord | undefined;
    transcriptArtifact: ObjectArtifactRecord | undefined;
    posterArtifact: ObjectArtifactRecord | undefined;
}): ObjectViewerPayload {
    const metadata = params.primaryAvailableFile?.metadata ?? {};
    switch (params.mediaType) {
        case "document": {
            const pageCount =
                getMetadataNumber(metadata, "page_count") ??
                getMetadataNumber(metadata, "pageCount");
            const pages = getMetadataPages(metadata);
            return {
                kind: "document",
                artifact_id: params.primaryArtifact?.id ?? null,
                content_type: params.primaryArtifact?.contentType ?? null,
                ocr_text_artifact_id: params.ocrArtifact?.id ?? null,
                page_count: pageCount,
                ...(pages ? { pages } : {}),
            };
        }
        case "image":
            return {
                kind: "image",
                artifact_id: params.primaryArtifact?.id ?? null,
                content_type: params.primaryArtifact?.contentType ?? null,
                width:
                    getMetadataNumber(metadata, "width") ??
                    getMetadataNumber(metadata, "pixel_width"),
                height:
                    getMetadataNumber(metadata, "height") ??
                    getMetadataNumber(metadata, "pixel_height"),
            };
        case "audio":
            return {
                kind: "audio",
                artifact_id: params.primaryArtifact?.id ?? null,
                content_type: params.primaryArtifact?.contentType ?? null,
                transcript_artifact_id: params.transcriptArtifact?.id ?? null,
                duration_seconds:
                    getMetadataNumber(metadata, "duration_seconds") ??
                    getMetadataNumber(metadata, "duration"),
            };
        case "video":
            return {
                kind: "video",
                artifact_id: params.primaryArtifact?.id ?? null,
                content_type: params.primaryArtifact?.contentType ?? null,
                poster_artifact_id: params.posterArtifact?.id ?? null,
                transcript_artifact_id: params.transcriptArtifact?.id ?? null,
                captions_artifact_id: null,
                duration_seconds:
                    getMetadataNumber(metadata, "duration_seconds") ??
                    getMetadataNumber(metadata, "duration"),
            };
    }
}

function buildObjectViewer(params: ViewerBuildInput): ObjectViewer | null {
    const mediaType = mapObjectTypeToViewerMediaType(params.objectRecord.type);
    if (!mediaType) {
        return null;
    }

    const sourceCandidates = getPrimarySourceCandidates(mediaType);
    const fallbackCandidate = sourceCandidates[0];
    if (!fallbackCandidate) {
        return null;
    }

    const ocrArtifact = findBestArtifactByKind(params.artifacts, "ocr_text");
    const transcriptArtifact = findBestArtifactByKind(
        params.artifacts,
        "transcript",
    );
    const thumbnailArtifact = params.thumbnailArtifactId
        ? params.artifacts.find((artifact) => artifact.id === params.thumbnailArtifactId)
        : findBestArtifactByKind(params.artifacts, "thumbnail");
    const posterArtifact = findBestArtifactByKind(params.artifacts, "preview");

    const selectedCandidate = sourceCandidates.find((candidate) => {
        const artifact = findBestArtifactByKind(params.artifacts, candidate.artifactKind);
        if (artifact && isPrimaryArtifactViewable(mediaType, artifact)) {
            return true;
        }

        return Boolean(
            findBestAvailableFileByKind(params.availableFiles, candidate.artifactKind),
        );
    }) ?? fallbackCandidate;

    const primaryArtifact = findBestArtifactByKind(
        params.artifacts,
        selectedCandidate.artifactKind,
    );
    const usablePrimaryArtifact =
        primaryArtifact && isPrimaryArtifactViewable(mediaType, primaryArtifact)
            ? primaryArtifact
            : undefined;
    const primaryAvailableFile = usablePrimaryArtifact
        ? findMatchingAvailableFile(
              params.availableFiles,
              usablePrimaryArtifact.kind,
              usablePrimaryArtifact.variant,
          )
        : findBestAvailableFileByKind(
              params.availableFiles,
              selectedCandidate.artifactKind,
          );
    const primaryRequest = findRelevantActiveRequest(
        params.requests,
        params.objectRecord.objectId,
        selectedCandidate.artifactKind,
        primaryAvailableFile?.variant ?? usablePrimaryArtifact?.variant ?? null,
    );

    let status: ObjectViewer["primary_source"]["status"];
    if (!params.projection.isAuthorized) {
        status = "restricted";
    } else if (usablePrimaryArtifact) {
        status = "available";
    } else if (primaryRequest) {
        status = "request_pending";
    } else if (primaryAvailableFile) {
        status = "request_required";
    } else if (!params.projection.isDeliverable) {
        status = "temporarily_unavailable";
    } else {
        status = "temporarily_unavailable";
    }

    const previewThumbnail = thumbnailArtifact && isBrowserViewableArtifact(thumbnailArtifact)
        ? serializeViewerArtifactRef(thumbnailArtifact, "Thumbnail")
        : null;
    const previewPoster =
        mediaType === "video" && posterArtifact && isBrowserViewableArtifact(posterArtifact)
            ? serializeViewerArtifactRef(posterArtifact, "Poster")
            : null;
    const previewOcr = ocrArtifact
        ? serializeViewerArtifactRef(ocrArtifact, "OCR Text", {
              ...(primaryAvailableFile?.metadata.page_count !== undefined
                  ? { page_count: primaryAvailableFile.metadata.page_count }
                  : {}),
          })
        : null;
    const previewTranscript = transcriptArtifact
        ? serializeViewerArtifactRef(transcriptArtifact, "Transcript")
        : null;

    return {
        media_type: mediaType,
        primary_source: {
            source_type: selectedCandidate.sourceType,
            artifact_kind: selectedCandidate.artifactKind,
            variant: usablePrimaryArtifact?.variant ?? primaryAvailableFile?.variant ?? null,
            status,
            available_file_id: primaryAvailableFile?.id ?? null,
            artifact_id: usablePrimaryArtifact?.id ?? null,
            display_name: primaryAvailableFile?.displayName ?? null,
            content_type:
                usablePrimaryArtifact?.contentType ?? primaryAvailableFile?.contentType ?? null,
            size_bytes: usablePrimaryArtifact?.sizeBytes ?? primaryAvailableFile?.sizeBytes ?? null,
            access_reason_code:
                status === "available"
                    ? "OK"
                    : status === "request_required"
                      ? "RESTORE_REQUIRED"
                      : status === "request_pending"
                        ? "RESTORE_IN_PROGRESS"
                        : params.projection.accessReasonCode,
        },
        active_request: primaryRequest,
        preview_artifacts: {
            thumbnail: previewThumbnail,
            poster: previewPoster,
            ocr_text: previewOcr,
            transcript: previewTranscript,
            captions: null,
        },
        viewer_payload: buildViewerPayload({
            mediaType,
            primaryArtifact: usablePrimaryArtifact,
            primaryAvailableFile,
            ocrArtifact,
            transcriptArtifact,
            posterArtifact,
        }),
    };
}

function getViewableArtifactIds(viewer: ObjectViewer | null): Set<string> {
    const ids = new Set<string>();

    if (!viewer) {
        return ids;
    }

    if (
        viewer.primary_source.status === "available" &&
        viewer.primary_source.artifact_id
    ) {
        ids.add(viewer.primary_source.artifact_id);
    }

    for (const artifact of Object.values(viewer.preview_artifacts)) {
        if (artifact?.artifact_id) {
            ids.add(artifact.artifact_id);
        }
    }

    switch (viewer.viewer_payload.kind) {
        case "document":
            if (viewer.viewer_payload.artifact_id) {
                ids.add(viewer.viewer_payload.artifact_id);
            }
            if (viewer.viewer_payload.ocr_text_artifact_id) {
                ids.add(viewer.viewer_payload.ocr_text_artifact_id);
            }
            for (const page of viewer.viewer_payload.pages ?? []) {
                if (page.image_artifact_id) {
                    ids.add(page.image_artifact_id);
                }
                if (page.ocr_text_artifact_id) {
                    ids.add(page.ocr_text_artifact_id);
                }
            }
            break;
        case "image":
        case "audio":
            if (viewer.viewer_payload.artifact_id) {
                ids.add(viewer.viewer_payload.artifact_id);
            }
            break;
        case "video":
            if (viewer.viewer_payload.artifact_id) {
                ids.add(viewer.viewer_payload.artifact_id);
            }
            if (viewer.viewer_payload.poster_artifact_id) {
                ids.add(viewer.viewer_payload.poster_artifact_id);
            }
            if (viewer.viewer_payload.transcript_artifact_id) {
                ids.add(viewer.viewer_payload.transcript_artifact_id);
            }
            break;
    }

    return ids;
}

async function buildViewerForObject(params: {
    auth: AuthenticatedContext;
    objectRecord: ObjectRecord;
    projection: ReturnType<typeof computeAccessProjection>;
    thumbnailArtifactId?: string;
}): Promise<ObjectViewer | null> {
    const [artifacts, availableFiles, requests] = await Promise.all([
        listArtifactsByObjectId({
            tenantId: params.auth.tenantId,
            objectId: params.objectRecord.objectId,
        }),
        listAvailableFilesByObjectId({
            tenantId: params.auth.tenantId,
            objectId: params.objectRecord.objectId,
        }),
        listArchiveRequestsByTarget({
            tenantId: params.auth.tenantId,
            targetType: "object",
            targetId: params.objectRecord.objectId,
        }),
    ]);

    return buildObjectViewer({
        objectRecord: params.objectRecord,
        projection: params.projection,
        artifacts,
        availableFiles,
        requests,
        thumbnailArtifactId: params.thumbnailArtifactId,
    });
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
    action_type: ArchiveRequestRecord["actionType"];
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
        objectType: object.type,
        files,
    });

    await reconcileObjectMetadataPagesFromAvailableFiles({
        tenantId: object.tenantId,
        objectId: params.objectId,
        files,
    });

    return {
        object_id: params.objectId,
        synced_files: syncedFiles,
    };
}

async function reconcileObjectMetadataPagesFromAvailableFiles(params: {
    tenantId: string;
    objectId: string;
    files: SyncAvailableFileCandidate[];
}): Promise<void> {
    const pageFiles = params.files.filter(
        (file) =>
            file.isAvailable &&
            isPageOcrVariant(file.variant),
    );

    if (pageFiles.length === 0) {
        return;
    }

    const objectRecord = await findObjectById({
        tenantId: params.tenantId,
        objectId: params.objectId,
    });

    if (!objectRecord) {
        return;
    }

    const currentPages = getMetadataPages(objectRecord.metadata) ?? [];
    const pagesByNumber = new Map(currentPages.map((page) => [page.page_number, page]));

    for (const file of pageFiles) {
        const pageNumber =
            typeof file.metadata.page_number === "number" &&
            Number.isInteger(file.metadata.page_number) &&
            file.metadata.page_number > 0
                ? file.metadata.page_number
                : extractPageNumberFromVariant(file.variant!);

        const existing = pagesByNumber.get(pageNumber);
        if (existing) {
            if (
                typeof file.metadata.label === "string" &&
                existing.label !== file.metadata.label
            ) {
                existing.label = file.metadata.label;
            }
        } else {
            pagesByNumber.set(pageNumber, {
                page_number: pageNumber,
                label:
                    typeof file.metadata.label === "string"
                        ? file.metadata.label
                        : String(pageNumber),
                image_artifact_id: null,
                ocr_text_artifact_id: null,
            });
        }
    }

    const newPages = Array.from(pagesByNumber.values()).sort(
        (left, right) => left.page_number - right.page_number,
    );

    const hasChanges =
        currentPages.length !== newPages.length ||
        currentPages.some((page, index) => {
            const other = newPages[index];
            if (!other) return true;
            return (
                page.page_number !== other.page_number ||
                page.label !== other.label ||
                page.image_artifact_id !== other.image_artifact_id ||
                page.ocr_text_artifact_id !== other.ocr_text_artifact_id
            );
        });

    if (hasChanges) {
        await updateObjectMetadataPages({
            tenantId: params.tenantId,
            objectId: params.objectId,
            pages: newPages,
        });
    }
}

function downloadRequestLeaseTtlSeconds(): number {
    return DEFAULT_DOWNLOAD_REQUEST_LEASE_TTL_SECONDS;
}

function workerUploadTtlSeconds(): number {
    return DEFAULT_WORKER_UPLOAD_TTL_SECONDS;
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

    console.info(
        `📦 artifact_fetch_leased request_id=${lease.request.id} worker_id=${params.workerId ?? "unknown"} object_id=${lease.request.targetId} tenant_id=${lease.request.tenantId} artifact_kind=${payload.artifact_kind} variant=${payload.variant ?? "null"} available_file_id=${payload.available_file_id}`,
    );

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

    return await buildArtifactUploadPresignResponse({
        lease: authorizedLease,
        body: params.body,
    });
}

async function buildArtifactUploadPresignResponse(params: {
    lease: AuthorizedWorkerDownloadRequestLease;
    body: WorkerPresignObjectArtifactUploadBody;
}): Promise<WorkerPresignObjectArtifactUploadResponse> {
    const maxUploadSizeBytes = resolveMaxUploadSizeBytes();

    if (params.body.size_bytes > maxUploadSizeBytes) {
        throw new ValidationError("Artifact size exceeds the configured upload limit.", {
            max_upload_size_bytes: maxUploadSizeBytes,
        });
    }

    const expiresAt = new Date(
        Date.now() + workerUploadTtlSeconds() * 1000,
    ).toISOString();
    const uploadTokenId = crypto.randomUUID();
    const storageKey = buildObjectArtifactStorageKey({
        tenantId: params.lease.tenantId,
        objectId: params.lease.objectId,
        requestId: params.lease.requestId,
        uploadTokenId,
        artifactKind: params.lease.artifactKind,
        variant: params.lease.variant,
        extension:
            params.body.extension ||
            extensionFromContentType(params.body.content_type),
    });
    const prepared = await withSchemaClient(async (sql) => sql.begin(async (transaction) => {
        const request = await findArchiveRequestByIdForUpdate({
            requestId: params.lease.requestId,
            executor: transaction,
        });
        if (!request) return undefined;
        const payload = parseArtifactFetchActionPayload(request);
        const availableFile = await findObjectAvailableFileById({
            tenantId: request.tenantId,
            objectId: request.targetId,
            availableFileId: payload.available_file_id,
            executor: transaction,
        });
        if (!availableFile) {
            return { outcome: "artifact_source_missing" as const };
        }
        if (
            availableFile.artifactKind !== payload.artifact_kind ||
            availableFile.variant !== payload.variant
        ) {
            return { outcome: "artifact_source_identity_changed" as const };
        }
        const expectedSha256 = availableFile.checksumSha256?.toLowerCase() ?? null;
        if (expectedSha256 !== null && !/^[0-9a-f]{64}$/.test(expectedSha256)) {
            return { outcome: "artifact_source_checksum_invalid" as const };
        }
        const attempt = await createAuthorizedArchiveArtifactUploadAttempt(transaction, {
            requestId: params.lease.requestId,
            leaseId: params.lease.leaseId,
            leaseTokenId: params.lease.leaseTokenId,
            uploadTokenId,
            storageKey,
            contentType: params.body.content_type,
            sizeBytes: params.body.size_bytes,
            expectedSha256,
            expiresAt: new Date(expiresAt),
        });
        return attempt ? { outcome: "authorized" as const } : undefined;
    }));

    if (!prepared) {
        throw new ConflictError("Lease is no longer active.");
    }
    if (prepared.outcome === "artifact_source_missing") {
        throw new ConflictError(
            "Artifact source is no longer available for this request.",
            { reason: prepared.outcome },
        );
    }
    if (prepared.outcome === "artifact_source_identity_changed") {
        throw new ConflictError(
            "Artifact source identity changed after this request was queued.",
            { reason: prepared.outcome },
        );
    }
    if (prepared.outcome === "artifact_source_checksum_invalid") {
        throw new ConflictError(
            "Artifact source checksum is invalid for this request.",
            { reason: prepared.outcome },
        );
    }

    const uploadToken = createObjectArtifactUploadToken({
        upload_token_id: uploadTokenId,
        request_id: params.lease.requestId,
        object_id: params.lease.objectId,
        tenant_id: params.lease.tenantId,
        artifact_kind: params.lease.artifactKind,
        variant: params.lease.variant,
        storage_key: storageKey,
        content_type: params.body.content_type,
        size_bytes: params.body.size_bytes,
        expires_at: expiresAt,
    });

    return {
        upload_token: uploadToken,
        upload_url: `/api/archive-requests/uploads/${uploadToken}`,
        storage_key: storageKey,
        expires_at: expiresAt,
        headers: {
            "content-type": params.body.content_type,
            "content-length": params.body.size_bytes,
        },
    };
}

export async function authorizeArtifactFetchArchiveRequestLease(params: {
    requestId: string;
    leaseToken: string;
    requireActiveLease?: boolean;
    allowExpired?: boolean;
}): Promise<ArtifactFetchCompletionLease> {
    const authorizedLease = await authorizeWorkerLeaseForArchiveRequest({
        requestId: params.requestId,
        leaseToken: params.leaseToken,
        requireActiveLease: params.requireActiveLease,
        allowExpired: params.allowExpired,
    });

    return await resolveArtifactFetchLeaseContext(authorizedLease);
}

interface ArtifactFetchCompletionLease extends AuthorizedWorkerDownloadRequestLease {
    availableFileId: string;
}

export async function presignArchiveRequestArtifactUpload(params: {
    requestId: string;
    body: WorkerPresignObjectArtifactUploadBody;
}): Promise<WorkerPresignObjectArtifactUploadResponse> {
    const authorizedLease = await authorizeArtifactFetchArchiveRequestLease({
        requestId: params.requestId,
        leaseToken: params.body.lease_token,
    });

    return await buildArtifactUploadPresignResponse({
        lease: authorizedLease,
        body: params.body,
    });
}

async function resolveArtifactFetchLeaseContext(
    authorizedLease: AuthorizedWorkerArchiveRequestLease,
): Promise<ArtifactFetchCompletionLease> {
    if (authorizedLease.actionType !== "artifact_fetch") {
        throw new ConflictError(
            `Archive request '${authorizedLease.requestId}' is not an artifact_fetch request.`,
        );
    }

    if (authorizedLease.targetType !== "object") {
        throw new ConflictError(
            `Archive request '${authorizedLease.requestId}' does not target an object.`,
        );
    }

    const request = await findArchiveRequestById({
        requestId: authorizedLease.requestId,
    });

    if (!request) {
        throw new NotFoundError(
            `Archive request '${authorizedLease.requestId}' was not found.`,
        );
    }

    if (
        request.tenantId !== authorizedLease.tenantId ||
        request.targetType !== authorizedLease.targetType ||
        request.targetId !== authorizedLease.targetId ||
        request.actionType !== authorizedLease.actionType
    ) {
        throw new ConflictError(
            `Archive request '${authorizedLease.requestId}' does not match its lease context.`,
        );
    }

    const payload = parseArtifactFetchActionPayload(request);

    return {
        requestId: authorizedLease.requestId,
        leaseId: authorizedLease.leaseId,
        leaseTokenId: authorizedLease.leaseTokenId,
        objectId: authorizedLease.targetId,
        tenantId: authorizedLease.tenantId,
        artifactKind: payload.artifact_kind,
        variant: payload.variant,
        availableFileId: payload.available_file_id,
        workerId: authorizedLease.workerId,
    };
}

async function resolveLegacyArtifactFetchLeaseContext(
    authorizedLease: AuthorizedWorkerDownloadRequestLease,
): Promise<ArtifactFetchCompletionLease> {
    const request = await findArchiveRequestById({ requestId: authorizedLease.requestId });
    if (!request) {
        throw new NotFoundError(
            `Download request '${authorizedLease.requestId}' was not found.`,
        );
    }
    if (
        request.actionType !== "artifact_fetch" ||
        request.targetType !== "object" ||
        request.tenantId !== authorizedLease.tenantId ||
        request.targetId !== authorizedLease.objectId
    ) {
        throw new ConflictError(
            `Download request '${authorizedLease.requestId}' does not match its lease context.`,
        );
    }

    const payload = parseArtifactFetchActionPayload(request);
    return {
        ...authorizedLease,
        artifactKind: payload.artifact_kind,
        variant: payload.variant,
        availableFileId: payload.available_file_id,
    };
}

export async function uploadObjectArtifactBySignedToken(params: {
    uploadToken: string;
    request: Request;
}): Promise<WorkerUploadObjectArtifactByTokenResponse> {
    const token = parseObjectArtifactUploadToken(params.uploadToken);
    const maxUploadSizeBytes = resolveMaxUploadSizeBytes();

    if (token.size_bytes > maxUploadSizeBytes) {
        throw new ValidationError("Upload size exceeds the configured upload limit.", {
            max_upload_size_bytes: maxUploadSizeBytes,
        });
    }
    const signedContentType = parseMediaType(token.content_type);
    const requestContentType = params.request.headers.get("content-type");
    const parsedRequestContentType = requestContentType
        ? parseMediaType(requestContentType)
        : undefined;

    if (
        !signedContentType ||
        !parsedRequestContentType ||
        parsedRequestContentType.essence !== signedContentType.essence
    ) {
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

    const contentLength = parseContentLength(rawContentLength);
    if (!Number.isFinite(contentLength) || contentLength !== token.size_bytes) {
        throw new ValidationError(
            "Upload content length does not match signed URL constraints.",
        );
    }

    const attempt = await findArchiveArtifactUploadAttemptById({
        uploadTokenId: token.upload_token_id,
    });
    if (
        !attempt || attempt.requestId !== token.request_id ||
        attempt.invalidatedAt !== null || attempt.storageKey !== token.storage_key ||
        attempt.contentType !== token.content_type || attempt.sizeBytes !== token.size_bytes
    ) {
        throw new ConflictError("Upload token is no longer active.");
    }

    if (attempt.state === "AUTHORIZED") {
        const activeLease = await findActiveArchiveRequestLeaseByToken({
            requestId: attempt.requestId,
            leaseId: attempt.authorizedLeaseId,
            leaseTokenId: attempt.authorizedLeaseTokenId,
        });
        if (!activeLease) {
            const refreshed = await findArchiveArtifactUploadAttemptById({
                uploadTokenId: token.upload_token_id,
            });
            if (
                refreshed === attempt ||
                (refreshed?.state !== "VERIFIED" && refreshed?.state !== "MATERIALIZED")
            ) {
                throw new ConflictError("Upload token is no longer active.");
            }
        }
    }

    const staged = await stageImmutableUpload({
        body: params.request.body,
        destinationPath: resolveStagingPath(token.storage_key),
        expectedSizeBytes: token.size_bytes,
        maxSizeBytes: maxUploadSizeBytes,
    });
    try {
        const recorded = await withSchemaClient(async (sql) => sql.begin(async (transaction) => {
            const request = await findArchiveRequestByIdForUpdate({
                requestId: token.request_id,
                executor: transaction,
            });
            const lockedAttempt = await findArchiveArtifactUploadAttemptByIdForUpdate({
                uploadTokenId: token.upload_token_id,
                executor: transaction,
            });
            if (
                !request || !lockedAttempt ||
                lockedAttempt.requestId !== token.request_id ||
                lockedAttempt.invalidatedAt !== null ||
                lockedAttempt.storageKey !== token.storage_key ||
                lockedAttempt.contentType !== token.content_type ||
                lockedAttempt.sizeBytes !== token.size_bytes
            ) return undefined;

            if (lockedAttempt.state === "VERIFIED" || lockedAttempt.state === "MATERIALIZED") {
                if (!lockedAttempt.computedSha256) return undefined;
                if (
                    lockedAttempt.sizeBytes !== staged.inspection.sizeBytes ||
                    lockedAttempt.computedSha256 !== staged.inspection.checksumSha256
                ) {
                    return {
                        acceptedCheckpointMismatch: {
                            expected: lockedAttempt.computedSha256,
                            actual: staged.inspection.checksumSha256,
                        },
                    } as const;
                }
                try {
                    await staged.publish({
                        sizeBytes: lockedAttempt.sizeBytes,
                        checksumSha256: lockedAttempt.computedSha256,
                    });
                } catch (error) {
                    if (error instanceof ConflictError) {
                        return { acceptedCheckpointStorageConflict: true } as const;
                    }
                    throw error;
                }
                return { attempt: lockedAttempt };
            }

            if (
                request.status !== "PROCESSING" ||
                request.leaseId !== lockedAttempt.authorizedLeaseId ||
                request.leaseTokenId !== lockedAttempt.authorizedLeaseTokenId ||
                request.releasedAt !== null || !request.leaseExpiresAt ||
                request.leaseExpiresAt.getTime() <= Date.now()
            ) return undefined;

            if (
                lockedAttempt.expectedSha256 !== null &&
                lockedAttempt.expectedSha256 !== staged.inspection.checksumSha256
            ) {
                return {
                    expectedChecksumMismatch: {
                        expected: lockedAttempt.expectedSha256,
                        actual: staged.inspection.checksumSha256,
                    },
                } as const;
            }

            try {
                await staged.publish({
                    sizeBytes: lockedAttempt.sizeBytes,
                    checksumSha256: lockedAttempt.expectedSha256 ?? staged.inspection.checksumSha256,
                });
            } catch (error) {
                if (error instanceof ConflictError) return { contentConflict: true } as const;
                throw error;
            }
            const verified = await verifyArchiveArtifactUploadAttempt({
                uploadTokenId: token.upload_token_id,
                requestId: token.request_id,
                leaseId: lockedAttempt.authorizedLeaseId,
                leaseTokenId: lockedAttempt.authorizedLeaseTokenId,
                computedSizeBytes: staged.inspection.sizeBytes,
                computedSha256: staged.inspection.checksumSha256,
                executor: transaction,
            });
            if (!verified) return undefined;
            const transferred = await transferArchiveRequestUploadToBackend({
                requestId: token.request_id,
                leaseId: lockedAttempt.authorizedLeaseId,
                leaseTokenId: lockedAttempt.authorizedLeaseTokenId,
                executor: transaction,
            });
            return transferred ? { attempt: verified } : undefined;
        }));

        if (!recorded) {
            throw new ConflictError("Upload token is no longer active.");
        }
        if (
            "expectedChecksumMismatch" in recorded &&
            recorded.expectedChecksumMismatch
        ) {
            throw new ConflictError(
                "Uploaded artifact checksum does not match the expected source checksum.",
                {
                    reason: "expected_checksum_mismatch",
                    expected_checksum_sha256: recorded.expectedChecksumMismatch.expected,
                    actual_checksum_sha256: recorded.expectedChecksumMismatch.actual,
                    retry_action: "retry_same_upload_url",
                },
            );
        }
        if (
            "acceptedCheckpointMismatch" in recorded &&
            recorded.acceptedCheckpointMismatch
        ) {
            throw new ConflictError(
                "Upload body does not match the accepted artifact checkpoint.",
                {
                    reason: "accepted_checkpoint_mismatch",
                    expected_checksum_sha256: recorded.acceptedCheckpointMismatch.expected,
                    actual_checksum_sha256: recorded.acceptedCheckpointMismatch.actual,
                    retry_action: "retry_exact_accepted_bytes_only",
                },
            );
        }
        if ("acceptedCheckpointStorageConflict" in recorded) {
            throw new ConflictError(
                "Immutable artifact storage does not match the accepted checkpoint.",
                {
                    reason: "accepted_checkpoint_storage_conflict",
                    retry_action: "operator_repair_required",
                },
            );
        }
        if ("contentConflict" in recorded) {
            throw new ConflictError("Upload body does not match the accepted artifact checkpoint.");
        }

        if (recorded.attempt.state === "VERIFIED") {
            try {
                await finalizeVerifiedArchiveArtifactUpload({ uploadTokenId: token.upload_token_id });
            } catch (error) {
                console.error(
                    `artifact_upload_finalization_deferred request_id=${token.request_id} upload_token_id=${token.upload_token_id} error=${error instanceof Error ? error.message : "unexpected"}`,
                );
            }
        }

        return {
            status: "ok",
            request_id: token.request_id,
            size_bytes: staged.inspection.sizeBytes,
        };
    } finally {
        try {
            await staged.cleanup();
        } catch (error) {
            console.error(
                `artifact_upload_cleanup_failed request_id=${token.request_id} upload_token_id=${token.upload_token_id} temporary_path=${staged.temporaryPath} error=${error instanceof Error ? error.message : "unexpected"}`,
            );
        }
    }
}

export async function finalizeArtifactFetchArchiveRequest(params: {
    requestId: string;
    leaseToken: string;
    uploadToken: string;
}): Promise<{
    lease: AuthorizedWorkerDownloadRequestLease;
    artifact: ObjectArtifactRecord;
    request: ArchiveRequestRecord;
}> {
    const authorizedLease = await authorizeArtifactFetchArchiveRequestLease({
        requestId: params.requestId,
        leaseToken: params.leaseToken,
        requireActiveLease: false,
        allowExpired: true,
    });
    const upload = parseObjectArtifactUploadToken(params.uploadToken, { allowExpired: true });
    if (
        upload.request_id !== authorizedLease.requestId ||
        upload.object_id !== authorizedLease.objectId ||
        upload.tenant_id !== authorizedLease.tenantId ||
        upload.artifact_kind !== authorizedLease.artifactKind ||
        upload.variant !== authorizedLease.variant
    ) {
        throw new ValidationError("Upload token does not match artifact fetch lease context.");
    }
    const attempt = await findArchiveArtifactUploadAttemptById({
        uploadTokenId: upload.upload_token_id,
    });
    if (
        !attempt || attempt.requestId !== authorizedLease.requestId ||
        attempt.authorizedLeaseId !== authorizedLease.leaseId ||
        attempt.authorizedLeaseTokenId !== authorizedLease.leaseTokenId ||
        attempt.storageKey !== upload.storage_key
    ) {
        throw new ConflictError("Upload token does not belong to this lease.");
    }
    const outcome = attempt.state === "VERIFIED"
        ? await finalizeVerifiedArchiveArtifactUpload({
            uploadTokenId: attempt.uploadTokenId,
            ignoreRetrySchedule: true,
        })
        : { outcome: "pending" as const };
    const finalizedAttempt = await findArchiveArtifactUploadAttemptById({
        uploadTokenId: attempt.uploadTokenId,
    });
    if (!finalizedAttempt || finalizedAttempt.state !== "MATERIALIZED" || !finalizedAttempt.artifactId) {
        if (outcome.outcome === "pending") {
            throw new ConflictError("Uploaded artifact has not been verified or finalization is already in progress.");
        }
        throw new ConflictError("Uploaded artifact could not be materialized.");
    }
    const artifact = await findArtifactById({
        tenantId: authorizedLease.tenantId,
        objectId: authorizedLease.objectId,
        artifactId: finalizedAttempt.artifactId,
    });
    const request = await findArchiveRequestById({ requestId: authorizedLease.requestId });
    if (!artifact || !request || request.status !== "COMPLETED") {
        throw new ConflictError("Uploaded artifact completion is inconsistent.");
    }
    return {
        lease: authorizedLease,
        artifact,
        request,
    };
}

export async function completeObjectDownloadRequestByWorker(params: {
    requestId: string;
    body: WorkerCompleteObjectDownloadRequestBody;
}): Promise<WorkerCompleteObjectDownloadRequestResponse> {
    const tokenLease = await authorizeWorkerLeaseForDownloadRequest({
        requestId: params.requestId,
        leaseToken: params.body.lease_token,
        requireActiveLease: false,
        allowExpired: true,
    });
    const authorizedLease = await resolveLegacyArtifactFetchLeaseContext(tokenLease);

    const upload = parseObjectArtifactUploadToken(params.body.upload_token, { allowExpired: true });
    if (
        upload.request_id !== authorizedLease.requestId ||
        upload.object_id !== authorizedLease.objectId ||
        upload.tenant_id !== authorizedLease.tenantId ||
        upload.artifact_kind !== authorizedLease.artifactKind ||
        upload.variant !== authorizedLease.variant
    ) throw new ValidationError("Upload token does not match artifact fetch lease context.");
    const attempt = await findArchiveArtifactUploadAttemptById({ uploadTokenId: upload.upload_token_id });
    if (
        !attempt || attempt.requestId !== authorizedLease.requestId ||
        attempt.authorizedLeaseId !== authorizedLease.leaseId ||
        attempt.authorizedLeaseTokenId !== authorizedLease.leaseTokenId ||
        attempt.storageKey !== upload.storage_key
    ) throw new ConflictError("Upload token does not belong to this lease.");
    if (attempt.state === "VERIFIED") {
        await finalizeVerifiedArchiveArtifactUpload({
            uploadTokenId: attempt.uploadTokenId,
            ignoreRetrySchedule: true,
        });
    }
    const materialized = await findArchiveArtifactUploadAttemptById({ uploadTokenId: attempt.uploadTokenId });
    if (!materialized?.artifactId || materialized.state !== "MATERIALIZED") {
        throw new ConflictError("Uploaded artifact has not been materialized.");
    }
    const artifact = await findArtifactById({
        tenantId: authorizedLease.tenantId,
        objectId: authorizedLease.objectId,
        artifactId: materialized.artifactId,
    });
    if (!artifact) throw new ConflictError("Materialized artifact was not found.");

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
        userId: params.auth.userId,
        role: params.auth.role,
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

    const [assignmentByObjectId, artifactSummaryByObjectId] = await Promise.all([
        listObjectAccessAssignmentsForUserByObjectIds({
            tenantId: params.auth.tenantId,
            userId: params.auth.userId,
            objectIds: visible.map((item) => item.objectId),
        }),
        listObjectArtifactSummariesByObjectIds({
            tenantId: params.auth.tenantId,
            objectIds: visible.map((item) => item.objectId),
        }),
    ]);
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
            const artifactSummary = artifactSummaryByObjectId.get(
                record.objectId,
            );
            return {
                ...serializeObject(record),
                thumbnail_artifact_id:
                    artifactSummary?.thumbnailArtifactId ?? null,
                has_access_pdf: artifactSummary?.hasAccessPdf ?? false,
                has_ocr: artifactSummary?.hasOcr ?? false,
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
    const viewer = await buildViewerForObject({
        auth: params.auth,
        objectRecord,
        projection,
        thumbnailArtifactId,
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
        viewer,
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

export async function viewObjectArtifactForTenant(params: {
    auth: AuthenticatedContext;
    objectId: string;
    artifactId: string;
    rangeHeader: string | null;
    ifRangeHeader: string | null;
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

    if (!projection.isAuthorized) {
        throw new ValidationError(
            "Object artifact is not viewable in the current access state.",
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

    const thumbnailArtifactId = await findPreferredThumbnailArtifactIdByObjectId({
        tenantId: params.auth.tenantId,
        objectId: params.objectId,
    });
    const viewer = await buildViewerForObject({
        auth: params.auth,
        objectRecord,
        projection,
        thumbnailArtifactId,
    });
    const viewableArtifactIds = getViewableArtifactIds(viewer);
    if (!viewableArtifactIds.has(params.artifactId)) {
        throw new ConflictError(
            `Artifact '${params.artifactId}' is not viewable for object '${params.objectId}'.`,
        );
    }

    if (!isBrowserViewableArtifact(artifact)) {
        throw new ConflictError(
            `Artifact '${params.artifactId}' is not browser-viewable.`,
        );
    }

    const filePath = resolveStagingPath(artifact.storageKey);
    const file = Bun.file(filePath);

    if (!(await file.exists())) {
        throw new NotFoundError(
            `Artifact '${params.artifactId}' storage file was not found.`,
        );
    }

    const etag = `"artifact-${artifact.id}"`;
    const headers = {
        "content-type": artifact.contentType,
        "content-disposition": `inline; filename=artifact-${artifact.id}`,
        "accept-ranges": "bytes",
        etag,
        "last-modified": artifact.createdAt.toUTCString(),
    };
    const range = parseSingleByteRange(params.rangeHeader, artifact.sizeBytes);
    const applies =
        range.kind !== "ignore" &&
        ifRangeMatches(params.ifRangeHeader, etag, artifact.createdAt);

    if (range.kind === "unsatisfiable" && applies) {
        return new Response(null, {
            status: 416,
            headers: {
                ...headers,
                "content-range": `bytes */${artifact.sizeBytes}`,
            },
        });
    }

    if (range.kind === "range" && applies) {
        const length = range.range.end - range.range.start + 1;
        return new Response(file.slice(range.range.start, range.range.end + 1, artifact.contentType), {
            status: 206,
            headers: {
                ...headers,
                "content-length": String(length),
                "content-range": `bytes ${range.range.start}-${range.range.end}/${artifact.sizeBytes}`,
            },
        });
    }

    return new Response(file, {
        status: 200,
        headers: {
            ...headers,
            "content-length": String(artifact.sizeBytes),
        },
    });
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
