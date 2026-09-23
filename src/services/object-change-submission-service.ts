import { mkdir, rm } from "node:fs/promises";
import { dirname } from "node:path";
import { createHash } from "node:crypto";

import type { AuthenticatedContext } from "../auth/guards.ts";
import {
  ConflictError,
  ForbiddenError,
  LockedError,
  NotFoundError,
  RevisionConflictError,
  ServiceUnavailableError,
} from "../http/errors.ts";
import { requireObjectEditAccess } from "./object-edit-authorization.ts";
import {
  listArtifactsByObjectId,
  type ObjectArtifactRecord,
} from "../repos/object-repo.ts";
import {
  findObjectEditById,
  listCuratedDocumentPages,
  type ObjectEditRecord,
} from "../repos/object-edit-repo.ts";
import {
  buildObjectRevisionApplyDedupeKey,
  completeObjectRevisionApply,
  createObjectRevisionSubmission,
  findLatestActiveObjectRevisionSubmission,
  findLatestAppliedObjectRevisionSubmission,
  findLatestObjectRevisionSubmission,
  objectRevisionPackageSchemaVersion,
  retryObjectRevisionSubmission,
  type ObjectRevisionSubmissionRecord,
} from "../repos/object-change-submission-repo.ts";
import {
  buildArchiveRequestSourceStorageKey,
  resolveStagingPath,
} from "../storage/staging.ts";
import type {
  ObjectChangeStatusResponse,
  SubmitObjectChangesBody,
  SubmitObjectChangesResponse,
  WorkerObjectRevisionApplyResult,
} from "../validation/object.ts";
import type { JsonObject } from "../validation/ingestion.ts";

const PACKAGE_CONTENT_TYPE = "application/vnd.osimi.object-revision+json; version=1";
const PACKAGE_SCHEMA_VERSION = "1.0";

export function isObjectRevisionApplyEnabled(): boolean {
  return process.env.OBJECT_REVISION_APPLY_ENABLED?.trim() === "true";
}

interface DocumentEditPage {
  pageNumber: number;
  label: string | null;
  ocrTextArtifactId: string | null;
}

function isJsonObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function getMetadataDocumentPages(metadata: ObjectEditRecord["metadata"]): DocumentEditPage[] {
  const pages = metadata.pages;
  if (!Array.isArray(pages)) {
    return [];
  }

  return pages
    .map((entry) => {
      if (!isJsonObject(entry)) {
        return undefined;
      }

      const pageNumber = entry.page_number;
      if (typeof pageNumber !== "number" || !Number.isInteger(pageNumber) || pageNumber <= 0) {
        return undefined;
      }

      return {
        pageNumber,
        label: typeof entry.label === "string" ? entry.label : null,
        ocrTextArtifactId:
          typeof entry.ocr_text_artifact_id === "string" ? entry.ocr_text_artifact_id : null,
      };
    })
    .filter((page): page is DocumentEditPage => page !== undefined)
    .sort((left, right) => left.pageNumber - right.pageNumber);
}

async function readMachineOcrCheckpoint(artifact: ObjectArtifactRecord | undefined): Promise<{
  text: string;
  sizeBytes: number;
  checksumSha256: string;
} | null> {
  if (!artifact) {
    return null;
  }

  const file = Bun.file(resolveStagingPath(artifact.storageKey));
  if (!(await file.exists())) {
    throw new ConflictError("Machine OCR artifact storage file is unavailable for submission.", {
      code: "MACHINE_OCR_UNAVAILABLE",
      artifact_id: artifact.id,
    });
  }

  const bytes = await file.arrayBuffer();
  if (bytes.byteLength !== artifact.sizeBytes) {
    throw new ConflictError("Machine OCR artifact size does not match its checkpoint.", {
      code: "MACHINE_OCR_CHECKPOINT_MISMATCH",
      artifact_id: artifact.id,
    });
  }

  const checksumSha256 = createHash("sha256").update(new Uint8Array(bytes)).digest("hex");
  return {
    text: new TextDecoder().decode(bytes),
    sizeBytes: bytes.byteLength,
    checksumSha256,
  };
}

async function buildDocumentOcrPackage(record: ObjectEditRecord): Promise<JsonObject | null> {
  if (record.type !== "DOCUMENT") {
    return null;
  }

  const pages = getMetadataDocumentPages(record.metadata);
  const artifacts = await listArtifactsByObjectId({
    tenantId: record.tenantId,
    objectId: record.objectId,
  });
  const artifactById = new Map(artifacts.map((artifact) => [artifact.id, artifact]));
  const curatedPages = await listCuratedDocumentPages({
    tenantId: record.tenantId,
    objectId: record.objectId,
  });
  const curatedByPageNumber = new Map(
    curatedPages.map((page) => [page.pageNumber, page]),
  );

  const resolvedPages: JsonObject[] = [];
  for (const page of pages) {
    const machine = await readMachineOcrCheckpoint(
      page.ocrTextArtifactId ? artifactById.get(page.ocrTextArtifactId) : undefined,
    );
    const curatedPage = curatedByPageNumber.get(page.pageNumber);
    const curatedText = curatedPage?.curatedText ?? null;

    resolvedPages.push({
      page_number: page.pageNumber,
      label: page.label,
      text: (curatedText ?? machine?.text ?? "").replace(/\r\n/g, "\n"),
      text_origin: curatedText !== null ? "curated" : "machine",
      machine_ocr_artifact_id: page.ocrTextArtifactId,
      machine_ocr_size_bytes: machine?.sizeBytes ?? null,
      machine_ocr_checksum_sha256: machine?.checksumSha256 ?? null,
    });
  }

  return {
    mode: "full_replacement",
    pages: resolvedPages,
    combined_text: resolvedPages
      .map((page) => page.text)
      .join("\n\f\n"),
  };
}

function buildObjectRevisionPackage(params: {
  submissionId: string;
  record: ObjectEditRecord;
  documentOcr: JsonObject | null;
}): JsonObject {
  return {
    schema_version: PACKAGE_SCHEMA_VERSION,
    submission_id: params.submissionId,
    object_id: params.record.objectId,
    object_revision: params.record.revision,
    generated_at: new Date().toISOString(),
    metadata: {
      title: params.record.title,
      publication_date: params.record.publicationDate,
      date_precision: params.record.datePrecision,
      date_approximate: params.record.dateApproximate,
      language: params.record.languageCode,
      tags: params.record.tags,
      people: params.record.people,
      description: params.record.description,
    },
    access_policy: {
      access_level: params.record.accessLevel,
      embargo_kind: params.record.embargoKind,
      embargo_until: params.record.embargoUntil
        ? params.record.embargoUntil.toISOString()
        : null,
      embargo_curation_state: params.record.embargoCurationState,
    },
    rights: {
      rights_note: params.record.rightsNote,
      sensitivity_note: params.record.sensitivityNote,
    },
    document_ocr: params.documentOcr,
  };
}

function serializeSubmissionForResponse(
  submission: ObjectRevisionSubmissionRecord,
): SubmitObjectChangesResponse["submission"] {
  return {
    id: submission.submissionId,
    request_id: submission.requestId,
    action_type: "object_revision_apply",
    status: submission.requestStatus ?? "PENDING",
    submitted_at: (submission.requestCreatedAt ?? submission.createdAt).toISOString(),
    submitted_by: submission.submittedBy,
  };
}

function serializeSubmissionForStatus(
  submission: ObjectRevisionSubmissionRecord,
): NonNullable<ObjectChangeStatusResponse["latest_submission"]> {
  return {
    id: submission.submissionId,
    request_id: submission.requestId,
    submitted_revision: submission.editRevision,
    status: submission.requestStatus ?? "PENDING",
    submitted_at: (submission.requestCreatedAt ?? submission.createdAt).toISOString(),
    submitted_by: submission.submittedBy,
    completed_at: submission.requestCompletedAt
      ? submission.requestCompletedAt.toISOString()
      : null,
    failure_reason: submission.requestFailureReason ?? null,
  };
}

export async function submitObjectChangesForTenant(params: {
  auth: AuthenticatedContext;
  objectId: string;
  body: SubmitObjectChangesBody;
}): Promise<{ outcome: "created" | "replayed"; response: SubmitObjectChangesResponse }> {
  if (!isObjectRevisionApplyEnabled()) {
    throw new ServiceUnavailableError(
      "Object change submission is not enabled for this environment.",
    );
  }

  await requireObjectEditAccess({
    auth: params.auth,
    objectId: params.objectId,
  });

  const record = await findObjectEditById({
    tenantId: params.auth.tenantId,
    objectId: params.objectId,
  });
  if (!record) {
    throw new NotFoundError(`Object '${params.objectId}' was not found.`);
  }

  if (record.revision !== params.body.revision) {
    throw new RevisionConflictError("Object revision is stale.", {
      latest_revision: record.revision,
    });
  }

  const submissionId = crypto.randomUUID();
  const requestId = crypto.randomUUID();
  const documentOcr = await buildDocumentOcrPackage(record);
  const packageBody = buildObjectRevisionPackage({
    submissionId,
    record,
    documentOcr,
  });
  const packageBytes = new TextEncoder().encode(JSON.stringify(packageBody));
  const sizeBytes = packageBytes.byteLength;
  const checksumSha256 = createHash("sha256").update(packageBytes).digest("hex");
  const storageKey = buildArchiveRequestSourceStorageKey({
    tenantId: params.auth.tenantId,
    objectId: params.objectId,
    requestId,
    extension: "json",
  });
  const filePath = resolveStagingPath(storageKey);

  let keepStagedSource = false;
  let sourceWritten = false;

  try {
    await mkdir(dirname(filePath), { recursive: true });
    await Bun.write(filePath, packageBytes);
    sourceWritten = true;

    const result = await createObjectRevisionSubmission({
      tenantId: params.auth.tenantId,
      objectId: params.objectId,
      actorUserId: params.auth.userId,
      actorRole: params.auth.role,
      revision: params.body.revision,
      requestId,
      submissionId,
      submissionNote: params.body.submission_note,
      source: {
        storageKey,
        contentType: PACKAGE_CONTENT_TYPE,
        sizeBytes,
        checksumSha256,
      },
      actionPayload: {
        schema_version: "1.0",
        submission_id: submissionId,
        object_id: params.objectId,
        object_revision: params.body.revision,
        package_schema_version: PACKAGE_SCHEMA_VERSION,
        source_ref: {
          type: "request_source",
          url: `/api/archive-requests/${requestId}/source`,
        },
        content_type: PACKAGE_CONTENT_TYPE,
        size_bytes: sizeBytes,
        checksum_sha256: checksumSha256,
        idempotency_key: buildObjectRevisionApplyDedupeKey({
          tenantId: params.auth.tenantId,
          objectId: params.objectId,
          editRevision: params.body.revision,
        }),
      },
    });

    if (result.status === "not_found") {
      throw new NotFoundError(`Object '${params.objectId}' was not found.`);
    }
    if (result.status === "unauthorized") {
      throw new ForbiddenError("You are not authorized to edit this object.");
    }
    if (result.status === "locked") {
      throw new LockedError("Object is currently being edited by another user.", {
        locked_by: result.lockedBy,
        locked_until: result.lockedUntil.toISOString(),
      });
    }
    if (result.status === "revision_conflict") {
      throw new RevisionConflictError("Object revision is stale.", {
        latest_revision: result.latestRevision,
      });
    }
    if (result.status === "mutation_active") {
      throw new ConflictError(
        "An object update is already active for this object.",
        {
          code: "CHANGES_ALREADY_ACTIVE",
          existing_request_id: result.request.id,
          existing_request_status: result.request.status,
        },
      );
    }

    keepStagedSource = result.status === "submitted";

    if (result.status === "deduped") {
      return {
        outcome: "replayed",
        response: {
          object_id: result.record.objectId,
          current_revision: result.record.revision,
          submitted_revision: params.body.revision,
          submission: serializeSubmissionForResponse(result.submission),
        },
      };
    }

    return {
      outcome: "created",
      response: {
        object_id: result.record.objectId,
        current_revision: result.record.revision,
        submitted_revision: params.body.revision,
        submission: {
          id: submissionId,
          request_id: result.request.id,
          action_type: "object_revision_apply",
          status: result.request.status,
          submitted_at: result.request.createdAt.toISOString(),
          submitted_by: result.request.requestedBy,
        },
      },
    };
  } finally {
    if (!keepStagedSource && sourceWritten) {
      try {
        await rm(filePath, { force: true });
      } catch (cleanupError) {
        console.error("object_revision_source_cleanup_failed", {
          object_id: params.objectId,
          request_id: requestId,
          storage_key: storageKey,
          error: cleanupError instanceof Error ? cleanupError.message : String(cleanupError),
        });
      }
    }
  }
}

export async function getObjectChangeStatusForTenant(params: {
  auth: AuthenticatedContext;
  objectId: string;
}): Promise<ObjectChangeStatusResponse> {
  await requireObjectEditAccess({
    auth: params.auth,
    objectId: params.objectId,
  });

  const record = await findObjectEditById({
    tenantId: params.auth.tenantId,
    objectId: params.objectId,
  });
  if (!record) {
    throw new NotFoundError(`Object '${params.objectId}' was not found.`);
  }

  const [latestSubmission, latestApplied, activeSubmission] = await Promise.all([
    findLatestObjectRevisionSubmission({
      tenantId: params.auth.tenantId,
      objectId: params.objectId,
    }),
    findLatestAppliedObjectRevisionSubmission({
      tenantId: params.auth.tenantId,
      objectId: params.objectId,
    }),
    findLatestActiveObjectRevisionSubmission({
      tenantId: params.auth.tenantId,
      objectId: params.objectId,
    }),
  ]);

  const archiveOutOfSync = latestApplied
    ? record.revision > latestApplied.editRevision
    : record.revision > 0;

  return {
    object_id: params.objectId,
    current_revision: record.revision,
    latest_submitted_revision: latestSubmission?.editRevision ?? null,
    latest_applied_revision: latestApplied?.editRevision ?? null,
    archive_out_of_sync: archiveOutOfSync,
    active_submission: activeSubmission
      ? serializeSubmissionForStatus(activeSubmission)
      : null,
    latest_submission: latestSubmission
      ? serializeSubmissionForStatus(latestSubmission)
      : null,
  };
}

export async function retryObjectChangeSubmissionForTenant(params: {
  auth: AuthenticatedContext;
  objectId: string;
  requestId: string;
  retryReason: string | null;
}): Promise<{ outcome: "requeued" | "unchanged"; response: SubmitObjectChangesResponse }> {
  await requireObjectEditAccess({
    auth: params.auth,
    objectId: params.objectId,
  });

  const record = await findObjectEditById({
    tenantId: params.auth.tenantId,
    objectId: params.objectId,
  });
  if (!record) {
    throw new NotFoundError(`Object '${params.objectId}' was not found.`);
  }

  const result = await retryObjectRevisionSubmission({
    tenantId: params.auth.tenantId,
    objectId: params.objectId,
    requestId: params.requestId,
    actorUserId: params.auth.userId,
    actorRole: params.auth.role,
    retryReason: params.retryReason,
  });

  if (result.status === "not_found") {
    throw new NotFoundError(`Submission request '${params.requestId}' was not found.`);
  }
  if (result.status === "unauthorized") {
    throw new ForbiddenError("You are not authorized to edit this object.");
  }
  if (result.status === "locked") {
    throw new LockedError("Object is currently being edited by another user.", {
      locked_by: result.lockedBy,
      locked_until: result.lockedUntil.toISOString(),
    });
  }
  if (result.status === "superseded") {
    throw new ConflictError(
      "A newer object revision has already been applied to the archive.",
      {
        code: "RETRY_SUPERSEDED",
        latest_applied_revision: result.latestAppliedRevision,
      },
    );
  }
  if (result.status === "source_unavailable") {
    throw new ConflictError("The submission source is no longer available for retry.", {
      code: "SOURCE_UNAVAILABLE",
    });
  }
  if (result.status === "mutation_active") {
    throw new ConflictError("An object update is already active for this object.", {
      code: "CHANGES_ALREADY_ACTIVE",
    });
  }
  if (result.status === "not_retryable") {
    throw new ConflictError("The submission is not in a retryable state.", {
      code: "NOT_RETRYABLE",
    });
  }

  return {
    outcome: result.status === "requeued" ? "requeued" : "unchanged",
    response: {
      object_id: params.objectId,
      current_revision: record.revision,
      submitted_revision: result.record.editRevision,
      submission: serializeSubmissionForResponse(result.record),
    },
  };
}

export async function completeObjectRevisionApplyByWorker(params: {
  requestId: string;
  leaseId: string;
  leaseTokenId: string;
  result: WorkerObjectRevisionApplyResult;
}): Promise<{ status: "completed" }> {
  const outcome = await completeObjectRevisionApply({
    requestId: params.requestId,
    leaseId: params.leaseId,
    leaseTokenId: params.leaseTokenId,
    result: {
      submissionId: params.result.submission_id,
      objectId: params.result.object_id,
      objectRevision: params.result.object_revision,
      packageSha256: params.result.package_sha256,
      archiveRevisionId: params.result.archive_revision_id,
      appliedAt: params.result.applied_at,
    },
  });

  if (outcome.status === "not_found") {
    throw new NotFoundError(`Archive request '${params.requestId}' was not found.`);
  }
  if (outcome.status === "lease_inactive") {
    throw new ConflictError("Lease is no longer active.");
  }
  if (outcome.status === "result_mismatch") {
    throw new ConflictError(`Worker result does not match the submission: ${outcome.reason}`, {
      code: "RESULT_MISMATCH",
    });
  }

  return { status: "completed" };
}
