import { withSchemaClient } from "../db/client.ts";
import { ConflictError, NotFoundError } from "../http/errors.ts";
import {
  claimVerifiedArchiveArtifactUploadAttempt,
  findArchiveArtifactUploadAttemptByIdForUpdate,
  markArchiveArtifactUploadAttemptMaterialized,
  recordArchiveArtifactUploadFinalizationFailure,
  type ArchiveArtifactUploadAttemptRecord,
} from "../repos/archive-artifact-upload-attempt-repo.ts";
import {
  completeVerifiedArtifactRequest,
  findArchiveRequestById,
  findArchiveRequestByIdForUpdate,
  type ArchiveRequestRecord,
} from "../repos/archive-request-repo.ts";
import {
  createOrFindObjectArtifactByStorageKey,
  findObjectById,
  lockObjectForUpdate,
  updateObjectMetadataPages,
  type ArtifactKind,
  type ObjectArtifactRecord,
} from "../repos/object-repo.ts";
import {
  classifyArtifactSearchText,
  decodeArtifactSearchText,
  persistArtifactSearchProjection,
  type ArtifactSearchTextResult,
} from "./artifact-search-indexing-service.ts";
import { resolveMaxArtifactSearchTextBytes } from "../runtime/config.ts";
import { resolveStagingPath } from "../storage/staging.ts";
import { inspectImmutableUpload } from "../storage/upload.ts";
import type { JsonObject } from "../validation/ingestion.ts";

const DEFAULT_CLAIM_TIMEOUT_SECONDS = 300;
const RETRY_BASE_SECONDS = 15;
const RETRY_MAX_SECONDS = 3600;

interface ArtifactFetchPayload {
  available_file_id: string;
  artifact_kind: ArtifactKind;
  variant: string | null;
}

function parseArtifactFetchPayload(request: ArchiveRequestRecord): ArtifactFetchPayload {
  const payload = request.actionPayload as Partial<ArtifactFetchPayload>;
  if (
    request.actionType !== "artifact_fetch" ||
    typeof payload.available_file_id !== "string" ||
    payload.available_file_id.trim().length === 0 ||
    typeof payload.artifact_kind !== "string" ||
    (payload.variant !== null && payload.variant !== undefined && typeof payload.variant !== "string")
  ) {
    throw new ConflictError(`Archive request '${request.id}' has invalid artifact_fetch payload.`);
  }
  return {
    available_file_id: payload.available_file_id,
    artifact_kind: payload.artifact_kind,
    variant: payload.variant ?? null,
  };
}

function metadataPages(metadata: JsonObject): Array<{
  page_number: number;
  label: string | null;
  image_artifact_id?: string | null;
  ocr_text_artifact_id?: string | null;
}> {
  const pages = metadata.pages;
  if (!Array.isArray(pages)) return [];
  return pages.flatMap((page) => {
    if (!page || typeof page !== "object" || Array.isArray(page)) return [];
    const value = page as Record<string, unknown>;
    if (typeof value.page_number !== "number" || !Number.isInteger(value.page_number)) return [];
    return [{
      page_number: value.page_number,
      label: typeof value.label === "string" ? value.label : null,
      image_artifact_id: typeof value.image_artifact_id === "string" ? value.image_artifact_id : null,
      ocr_text_artifact_id: typeof value.ocr_text_artifact_id === "string" ? value.ocr_text_artifact_id : null,
    }];
  });
}

function retryDelaySeconds(attemptCount: number): number {
  return Math.min(RETRY_MAX_SECONDS, RETRY_BASE_SECONDS * 2 ** Math.max(0, attemptCount - 1));
}

async function prepareVerifiedArtifactSearchText(
  attempt: ArchiveArtifactUploadAttemptRecord,
  kind: ArtifactKind,
): Promise<{
  inspected: Awaited<ReturnType<typeof inspectImmutableUpload>>;
  prepared: ArtifactSearchTextResult;
}> {
  if (!attempt.computedSha256) {
    throw new ConflictError(`Verified artifact upload '${attempt.uploadTokenId}' has no checksum.`);
  }
  const maxSearchTextBytes = resolveMaxArtifactSearchTextBytes();
  const eligibility = classifyArtifactSearchText({
    kind,
    contentType: attempt.contentType,
    sizeBytes: attempt.sizeBytes,
    maxBytes: maxSearchTextBytes,
  });
  const inspected = await inspectImmutableUpload({
    path: resolveStagingPath(attempt.storageKey),
    captureBytes: eligibility.outcome === "eligible",
    maxCaptureBytes: maxSearchTextBytes,
  });
  if (inspected.sizeBytes !== attempt.sizeBytes) {
    throw new ConflictError(
      `verified_storage_size_mismatch: upload attempt '${attempt.uploadTokenId}'`,
    );
  }
  if (inspected.checksumSha256 !== attempt.computedSha256) {
    throw new ConflictError(
      `verified_storage_checksum_mismatch: upload attempt '${attempt.uploadTokenId}'`,
    );
  }
  return {
    inspected,
    prepared: eligibility.outcome === "skipped"
      ? eligibility
      : decodeArtifactSearchText(inspected.bytes!),
  };
}

async function finalizeClaimedAttempt(
  attempt: ArchiveArtifactUploadAttemptRecord,
): Promise<{ request: ArchiveRequestRecord; artifact: ObjectArtifactRecord }> {
  if (!attempt.finalizationClaimToken) {
    throw new Error(`Upload attempt '${attempt.uploadTokenId}' is not claimed.`);
  }
  const request = await findArchiveRequestById({ requestId: attempt.requestId });
  if (!request) throw new NotFoundError(`Archive request '${attempt.requestId}' was not found.`);
  const payload = parseArtifactFetchPayload(request);
  const { inspected, prepared } = await prepareVerifiedArtifactSearchText(
    attempt,
    payload.artifact_kind,
  );

  return await withSchemaClient(async (sql) => sql.begin(async (transaction) => {
    const lockedRequest = await findArchiveRequestByIdForUpdate({
      requestId: attempt.requestId,
      executor: transaction,
    });
    if (!lockedRequest || lockedRequest.status !== "PROCESSING") {
      throw new ConflictError("Archive request is not awaiting artifact finalization.");
    }
    const lockedAttempt = await findArchiveArtifactUploadAttemptByIdForUpdate({
      uploadTokenId: attempt.uploadTokenId,
      executor: transaction,
    });
    if (
      !lockedAttempt ||
      lockedAttempt.state !== "VERIFIED" ||
      lockedAttempt.finalizationClaimToken !== attempt.finalizationClaimToken ||
      lockedAttempt.storageKey !== attempt.storageKey ||
      lockedAttempt.contentType !== attempt.contentType ||
      lockedAttempt.sizeBytes !== inspected.sizeBytes ||
      lockedAttempt.computedSha256 !== inspected.checksumSha256
    ) {
      throw new ConflictError("Artifact upload finalization claim is no longer active.");
    }
    const lockedPayload = parseArtifactFetchPayload(lockedRequest);
    if (
      lockedPayload.available_file_id !== payload.available_file_id ||
      lockedPayload.artifact_kind !== payload.artifact_kind ||
      lockedPayload.variant !== payload.variant
    ) {
      throw new ConflictError("Archive request payload changed during artifact finalization.");
    }
    const artifact = await createOrFindObjectArtifactByStorageKey({
      objectId: lockedRequest.targetId,
      kind: lockedPayload.artifact_kind,
      variant: lockedPayload.variant,
      storageKey: lockedAttempt.storageKey,
      contentType: lockedAttempt.contentType,
      sizeBytes: lockedAttempt.sizeBytes,
      executor: transaction,
    });
    if (
      artifact.objectId !== lockedRequest.targetId ||
      artifact.kind !== lockedPayload.artifact_kind ||
      artifact.variant !== lockedPayload.variant ||
      artifact.contentType !== lockedAttempt.contentType ||
      artifact.sizeBytes !== lockedAttempt.sizeBytes
    ) {
      throw new ConflictError("Existing artifact does not match the verified upload.");
    }

    await persistArtifactSearchProjection({
      tenantId: lockedRequest.tenantId,
      objectId: lockedRequest.targetId,
      availableFileId: lockedPayload.available_file_id,
      artifact,
      prepared,
      executor: transaction,
    });

    if (artifact.kind === "ocr_text" && artifact.variant && /^page_\d{4}$/.test(artifact.variant)) {
      const locked = await lockObjectForUpdate({
        tenantId: lockedRequest.tenantId,
        objectId: lockedRequest.targetId,
        executor: transaction,
      });
      if (!locked) throw new NotFoundError(`Object '${lockedRequest.targetId}' was not found.`);
      const object = await findObjectById({
        tenantId: lockedRequest.tenantId,
        objectId: lockedRequest.targetId,
        executor: transaction,
      });
      if (!object) throw new NotFoundError(`Object '${lockedRequest.targetId}' was not found.`);
      const pageNumber = Number.parseInt(artifact.variant.slice("page_".length), 10);
      const pages = metadataPages(object.metadata);
      const page = pages.find((item) => item.page_number === pageNumber);
      if (page) page.ocr_text_artifact_id = artifact.id;
      else pages.push({
        page_number: pageNumber,
        label: String(pageNumber),
        image_artifact_id: null,
        ocr_text_artifact_id: artifact.id,
      });
      pages.sort((left, right) => left.page_number - right.page_number);
      await updateObjectMetadataPages({
        tenantId: lockedRequest.tenantId,
        objectId: lockedRequest.targetId,
        pages,
        executor: transaction,
      });
    }

    const materialized = await markArchiveArtifactUploadAttemptMaterialized(transaction, {
      uploadTokenId: lockedAttempt.uploadTokenId,
      claimToken: attempt.finalizationClaimToken!,
      artifactId: artifact.id,
    });
    if (!materialized) throw new ConflictError("Artifact upload finalization claim was lost.");
    const completed = await completeVerifiedArtifactRequest({
      requestId: lockedRequest.id,
      executor: transaction,
    });
    if (!completed) throw new ConflictError("Archive request could not be completed.");
    return { request: completed, artifact };
  }));
}

export async function finalizeVerifiedArchiveArtifactUpload(params: {
  uploadTokenId: string;
  ignoreRetrySchedule?: boolean;
}): Promise<
  | { outcome: "completed"; request: ArchiveRequestRecord; artifact: ObjectArtifactRecord }
  | { outcome: "pending" }
> {
  const claimed = await claimVerifiedArchiveArtifactUploadAttempt({
    uploadTokenId: params.uploadTokenId,
    claimTimeoutSeconds: DEFAULT_CLAIM_TIMEOUT_SECONDS,
    ignoreRetrySchedule: params.ignoreRetrySchedule,
  });
  if (!claimed) return { outcome: "pending" };
  try {
    const finalized = await finalizeClaimedAttempt(claimed);
    return { outcome: "completed", ...finalized };
  } catch (error) {
    await recordArchiveArtifactUploadFinalizationFailure({
      uploadTokenId: claimed.uploadTokenId,
      claimToken: claimed.finalizationClaimToken!,
      retryDelaySeconds: retryDelaySeconds(claimed.finalizationAttemptCount),
      lastError: error instanceof Error ? error.message : "Unexpected finalization failure",
    });
    throw error;
  }
}

export async function finalizeClaimedArchiveArtifactUpload(
  attempt: ArchiveArtifactUploadAttemptRecord,
): Promise<void> {
  try {
    await finalizeClaimedAttempt(attempt);
  } catch (error) {
    await recordArchiveArtifactUploadFinalizationFailure({
      uploadTokenId: attempt.uploadTokenId,
      claimToken: attempt.finalizationClaimToken!,
      retryDelaySeconds: retryDelaySeconds(attempt.finalizationAttemptCount),
      lastError: error instanceof Error ? error.message : "Unexpected finalization failure",
    });
    throw error;
  }
}
