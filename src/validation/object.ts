import { decodeCursor } from "../http/pagination.ts";
import { z } from "zod";

import type { JsonObject } from "./ingestion.ts";
import { jsonObjectSchema } from "./ingestion.ts";
import {
    mapZodErrorToUnprocessable,
    mapZodErrorToValidation,
} from "./zod-errors.ts";

const OBJECT_ID_PATTERN = /^OBJ-[0-9]{8}-[A-Z0-9]+$/;

export const objectIdParamSchema = z.string().regex(OBJECT_ID_PATTERN, {
    message: "object_id must match format OBJ-YYYYMMDD-XXXXXX.",
});
export const artifactIdParamSchema = z.uuid();
export const objectDownloadRequestIdParamSchema = z.uuid();
export const archiveRequestIdParamSchema = z.uuid();
export const accessRequestIdParamSchema = z.uuid();
export const userIdParamSchema = z.uuid();

export const objectListSortSchema = z.enum([
    "created_at_desc",
    "created_at_asc",
    "updated_at_desc",
    "updated_at_asc",
    "title_asc",
    "title_desc",
]);

export const archiveRequestListSortSchema = z.enum(["created_at_desc"]);

export const objectTypeSchema = z.enum([
    "GENERIC",
    "IMAGE",
    "AUDIO",
    "VIDEO",
    "DOCUMENT",
]);

export const availabilityStateSchema = z.enum([
    "AVAILABLE",
    "ARCHIVED",
    "RESTORE_PENDING",
    "RESTORING",
    "UNAVAILABLE",
]);

export const accessLevelSchema = z.enum(["private", "family", "public"]);

export const artifactKindSchema = z.enum([
    "ingest_json",
    "pipeline_json",
    "catalog_json",
    "original",
    "preview",
    "ocr",
    "transcript",
    "metadata",
    "pdf",
    "ocr_text",
    "thumbnail",
    "web_version",
    "other",
]);

export const objectDownloadRequestStatusSchema = z.enum([
    "PENDING",
    "PROCESSING",
    "COMPLETED",
    "FAILED",
    "CANCELED",
]);

export const archiveRequestStatusSchema = z.enum([
    "PENDING",
    "PROCESSING",
    "COMPLETED",
    "FAILED",
    "CANCELED",
]);

export const archiveRequestTargetTypeSchema = z.enum(["object", "ingestion"]);

export const archiveRequestActionTypeSchema = z.enum([
    "object_resync",
    "artifact_fetch",
    "curation_apply",
]);

export const objectDatePrecisionSchema = z.enum(["none", "year", "month", "day"]);

const nonEmptyStringArraySchema = z
    .array(z.string().trim().min(1))
    .transform((values) => [...new Set(values)]);

const nullableTrimmedStringSchema = z
    .string()
    .trim()
    .transform((value) => (value.length === 0 ? null : value))
    .nullable();

const objectEditMetadataSchema = z
    .strictObject({
        title: z.string().trim().min(1),
        publication_date: z.string().trim(),
        date_precision: objectDatePrecisionSchema,
        date_approximate: z.boolean(),
        language: nullableTrimmedStringSchema,
        tags: nonEmptyStringArraySchema,
        people: nonEmptyStringArraySchema,
        description: nullableTrimmedStringSchema,
    })
    .superRefine((value, context) => {
        if (value.date_precision === "none") {
            return;
        }

        const patternByPrecision = {
            year: /^\d{4}$/,
            month: /^\d{4}-\d{2}$/,
            day: /^\d{4}-\d{2}-\d{2}$/,
        } as const;

        if (!patternByPrecision[value.date_precision].test(value.publication_date)) {
            context.addIssue({
                code: z.ZodIssueCode.custom,
                message: "publication_date does not match date_precision.",
                path: ["publication_date"],
            });
        }
    })
    .transform((value) => ({
        ...value,
        publication_date:
            value.date_precision === "none" ? "" : value.publication_date,
        date_approximate:
            value.date_precision === "none" ? false : value.date_approximate,
        tags: value.tags.map((tag) => tag.toLowerCase()),
    }));

const objectEditRightsSchema = z.strictObject({
    rights_note: nullableTrimmedStringSchema,
    sensitivity_note: nullableTrimmedStringSchema,
});

export const patchObjectMetadataBodySchema = z.strictObject({
    metadata: objectEditMetadataSchema,
    rights: objectEditRightsSchema,
});

export const submitObjectCurationBodySchema = z.strictObject({
    review_note: nullableTrimmedStringSchema,
});

const documentCurationPageSchema = z.strictObject({
    page_number: z.number().int().positive(),
    label: z.string().nullable().optional(),
    machine_text: z.string(),
    curated_text: z.string().nullable(),
    status: z.enum(["machine", "edited"]).optional(),
});

export const putDocumentCurationBodySchema = z
    .strictObject({
        pages: z.array(
            z.strictObject({
                page_number: z.number().int().positive(),
                curated_text: z.string(),
            }),
        ).min(1),
    })
    .superRefine((value, context) => {
        const seen = new Set<number>();
        for (const [index, page] of value.pages.entries()) {
            if (seen.has(page.page_number)) {
                context.addIssue({
                    code: z.ZodIssueCode.custom,
                    message: "Duplicate page_number is not allowed.",
                    path: ["pages", index, "page_number"],
                });
            }

            seen.add(page.page_number);
        }
    });

const objectEditMediaTypeSchema = z.enum([
    "document",
    "image",
    "audio",
    "video",
    "other",
]);

const objectEditCurationStateSchema = z.enum([
    "needs_review",
    "review_in_progress",
    "reviewed",
    "curation_failed",
]);

const objectEditDraftSchema = z.object({
    updated_at: z.string(),
    updated_by: z.string().nullable(),
});

const objectEditCapabilitiesSchema = z.object({
    can_edit_metadata: z.boolean(),
    can_curate_text: z.boolean(),
    can_submit_review: z.boolean(),
});

const objectEditDocumentCurationPayloadSchema = z.object({
    kind: z.literal("document"),
    machine_ocr_artifact_id: z.string().nullable(),
    page_count: z.number().int().positive().nullable(),
    pages: z.array(documentCurationPageSchema),
});

const objectEditGenericCurationPayloadSchema = z.object({
    kind: z.enum(["image", "audio", "video", "other"]),
});

const objectEditCurationPayloadSchema = z.discriminatedUnion("kind", [
    objectEditDocumentCurationPayloadSchema,
    objectEditGenericCurationPayloadSchema,
]);

const objectEditLockSchema = z.object({
    locked: z.boolean(),
    locked_by: z.string().nullable(),
    locked_until: z.string().nullable(),
});

const objectEditResponseSchema = z.object({
    object_id: objectIdParamSchema,
    media_type: objectEditMediaTypeSchema,
    curation_state: objectEditCurationStateSchema,
    lock: objectEditLockSchema,
    draft: objectEditDraftSchema.nullable(),
    metadata: objectEditMetadataSchema,
    rights: objectEditRightsSchema.extend({
        access_level: accessLevelSchema,
    }),
    capabilities: objectEditCapabilitiesSchema,
    curation_payload: objectEditCurationPayloadSchema,
});

const patchObjectMetadataResponseSchema = z.object({
    object_id: objectIdParamSchema,
    curation_state: objectEditCurationStateSchema,
    updated_at: z.string(),
});

const putDocumentCurationResponseSchema = z.object({
    object_id: objectIdParamSchema,
    updated_count: z.number().int().min(1),
    updated_at: z.string(),
});

const submitObjectCurationResponseSchema = z.object({
    object_id: objectIdParamSchema,
    curation_state: objectEditCurationStateSchema,
    request: z.object({
        id: z.uuid(),
        action_type: z.literal("curation_apply"),
        status: z.enum(["PENDING", "PROCESSING", "COMPLETED", "FAILED", "CANCELED"]),
    }),
    submitted_at: z.string(),
    submitted_by: z.string(),
});

const objectEditHistoryEventTypeSchema = z.enum([
    "METADATA_UPDATED",
    "RIGHTS_UPDATED",
    "DOCUMENT_PAGE_UPDATED",
    "CURATION_SUBMITTED",
]);

const objectEditHistoryEventSchema = z.object({
    id: z.uuid(),
    type: objectEditHistoryEventTypeSchema,
    actor_user_id: z.string().nullable(),
    at: z.string(),
    revision_before: z.number().int().min(0).nullable(),
    revision_after: z.number().int().min(0).nullable(),
    payload: jsonObjectSchema,
});

export const objectEditHistoryResponseSchema = z.object({
    object_id: objectIdParamSchema,
    events: z.array(objectEditHistoryEventSchema),
    next_cursor: z.string().nullable(),
});

const objectEditHistoryCursorSchema = z.strictObject({
    created_at: z.iso.datetime({ offset: true }),
    id: z.uuid(),
});

export const embargoKindSchema = z.enum(["none", "timed", "curation_state"]);

export const curationStateSchema = z.enum([
    "needs_review",
    "review_in_progress",
    "reviewed",
    "curation_failed",
]);

export const processingStateSchema = z.enum([
    "queued",
    "ingesting",
    "ingested",
    "derivatives_running",
    "derivatives_done",
    "ocr_running",
    "ocr_done",
    "index_running",
    "index_done",
    "processing_failed",
    "processing_skipped",
]);

export const objectListQuerySchema = z.strictObject({
    limit: z.coerce.number().int().min(1).max(200).default(50),
    cursor: z.string().trim().min(1).optional(),
    sort: objectListSortSchema.default("created_at_desc"),
    q: z.string().trim().min(1).optional(),
    availability_state: availabilityStateSchema.optional(),
    access_level: accessLevelSchema.optional(),
    language: z.string().trim().min(1).optional(),
    batch_label: z.string().trim().min(1).optional(),
    type: objectTypeSchema.optional(),
    from: z.iso.datetime({ offset: true }).optional(),
    to: z.iso.datetime({ offset: true }).optional(),
    tag: z.string().trim().min(1).optional(),
});

const objectListQueryWithCursorSchema = objectListQuerySchema.transform(
    (data, context): ObjectListQuery => {
        let cursor: ObjectCursorPayload | undefined;

        if (data.cursor) {
            let decoded: JsonObject;
            try {
                decoded = decodeCursor<JsonObject>(data.cursor);
            } catch {
                context.addIssue({
                    code: z.ZodIssueCode.custom,
                    message: "Query parameter 'cursor' is invalid.",
                    path: ["cursor"],
                });
                return z.NEVER;
            }

            const payload = objectCursorPayloadSchema.safeParse(decoded);
            if (!payload.success) {
                for (const issue of payload.error.issues) {
                    context.addIssue({
                        code: z.ZodIssueCode.custom,
                        message: issue.message,
                        path: ["cursor", ...issue.path],
                    });
                }
                return z.NEVER;
            }

            if (payload.data.sort !== data.sort) {
                context.addIssue({
                    code: z.ZodIssueCode.custom,
                    message: "Query parameter 'cursor' is invalid.",
                    path: ["cursor"],
                });
                return z.NEVER;
            }

            cursor = payload.data;
        }

        return {
            limit: data.limit,
            cursor,
            sort: data.sort,
            query: data.q,
            availabilityState: data.availability_state,
            accessLevel: data.access_level,
            language: data.language,
            batchLabel: data.batch_label,
            type: data.type,
            from: data.from,
            to: data.to,
            tag: data.tag,
        };
    },
);

export const objectCursorPayloadSchema = z
    .strictObject({
        sort: objectListSortSchema,
        created_at: z.iso.datetime({ offset: true }).optional(),
        updated_at: z.iso.datetime({ offset: true }).optional(),
        title: z.string().optional(),
        object_id: objectIdParamSchema,
    })
    .superRefine((value, context) => {
        if (
            (value.sort === "created_at_desc" ||
                value.sort === "created_at_asc") &&
            !value.created_at
        ) {
            context.addIssue({
                code: z.ZodIssueCode.custom,
                message: "cursor created_at is required for created_at sort.",
                path: ["created_at"],
            });
        }

        if (
            (value.sort === "updated_at_desc" ||
                value.sort === "updated_at_asc") &&
            !value.updated_at
        ) {
            context.addIssue({
                code: z.ZodIssueCode.custom,
                message: "cursor updated_at is required for updated_at sort.",
                path: ["updated_at"],
            });
        }

        if (
            (value.sort === "title_asc" || value.sort === "title_desc") &&
            !value.title
        ) {
            context.addIssue({
                code: z.ZodIssueCode.custom,
                message: "cursor title is required for title sort.",
                path: ["title"],
            });
        }
    });

const archiveRequestCursorPayloadSchema = z.strictObject({
    sort: archiveRequestListSortSchema,
    created_at: z.iso.datetime({ offset: true }),
    id: z.uuid(),
});

const archiveRequestListQuerySchema = z
    .strictObject({
        limit: z.coerce.number().int().min(1).max(200).default(50),
        cursor: z.string().trim().min(1).optional(),
        sort: archiveRequestListSortSchema.default("created_at_desc"),
        target_type: archiveRequestTargetTypeSchema.optional(),
        target_id: z.string().trim().min(1).optional(),
        action_type: archiveRequestActionTypeSchema.optional(),
        status: z.array(archiveRequestStatusSchema).optional(),
        active_only: z.coerce.boolean().default(false),
        include_payload: z.coerce.boolean().default(false),
    })
    .superRefine((value, context) => {
        if (value.target_type === undefined && value.target_id !== undefined) {
            context.addIssue({
                code: z.ZodIssueCode.custom,
                message: "target_type is required when target_id is provided.",
                path: ["target_type"],
            });
        }

        if (
            value.target_type === "object" &&
            value.target_id !== undefined &&
            !OBJECT_ID_PATTERN.test(value.target_id)
        ) {
            context.addIssue({
                code: z.ZodIssueCode.custom,
                message: "target_id must match format OBJ-YYYYMMDD-XXXXXX for object target_type.",
                path: ["target_id"],
            });
        }
    });

const archiveRequestListQueryWithCursorSchema = archiveRequestListQuerySchema.transform(
    (data, context): ArchiveRequestListQuery => {
        let cursor: ArchiveRequestCursorPayload | undefined;

        if (data.cursor) {
            let decoded: JsonObject;
            try {
                decoded = decodeCursor<JsonObject>(data.cursor);
            } catch {
                context.addIssue({
                    code: z.ZodIssueCode.custom,
                    message: "Query parameter 'cursor' is invalid.",
                    path: ["cursor"],
                });
                return z.NEVER;
            }

            const payload = archiveRequestCursorPayloadSchema.safeParse(decoded);
            if (!payload.success) {
                for (const issue of payload.error.issues) {
                    context.addIssue({
                        code: z.ZodIssueCode.custom,
                        message: issue.message,
                        path: ["cursor", ...issue.path],
                    });
                }
                return z.NEVER;
            }

            if (payload.data.sort !== data.sort) {
                context.addIssue({
                    code: z.ZodIssueCode.custom,
                    message: "Query parameter 'cursor' is invalid.",
                    path: ["cursor"],
                });
                return z.NEVER;
            }

            cursor = payload.data;
        }

        const statuses = data.active_only
            ? (["PENDING", "PROCESSING"] as Array<
                  z.infer<typeof archiveRequestStatusSchema>
              >)
            : data.status;

        return {
            limit: data.limit,
            cursor,
            sort: data.sort,
            targetType: data.target_type,
            targetId: data.target_id,
            actionType: data.action_type,
            statuses,
            includePayload: data.include_payload,
        };
    },
);

export const patchObjectTitleBodySchema = z.strictObject({
    title: z.string().trim().min(1),
});

export const createObjectDownloadRequestBodySchema = z.strictObject({
    available_file_id: z.uuid(),
});

export const createObjectResyncBodySchema = z
    .strictObject({
        action_payload: jsonObjectSchema.optional(),
    })
    .transform((value) => ({
        action_payload: value.action_payload ?? {},
    }));

export const workerLeaseArchiveRequestBodySchema = z.strictObject({
    action_type: archiveRequestActionTypeSchema.optional(),
});

export const workerPresignObjectArtifactUploadBodySchema = z.strictObject({
    lease_token: z.string().trim().min(1),
    content_type: z.string().trim().min(1),
    size_bytes: z.number().int().min(0),
    extension: z.string().trim().min(1),
});

export const workerCompleteObjectDownloadRequestBodySchema = z.strictObject({
    lease_token: z.string().trim().min(1),
    upload_token: z.string().trim().min(1),
});

const workerFailureSchema = z.strictObject({
    code: z.string().trim().min(1),
    message: z.string().trim().min(1),
    retryable: z.boolean(),
    details: jsonObjectSchema.optional(),
});

export const workerFailObjectDownloadRequestBodySchema = z.strictObject({
    lease_token: z.string().trim().min(1),
    failure: workerFailureSchema,
});

export const workerCompleteArchiveRequestBodySchema = z.strictObject({
    lease_token: z.string().trim().min(1),
    upload_token: z.string().trim().min(1).optional(),
});

export const workerFailArchiveRequestBodySchema = z.strictObject({
    lease_token: z.string().trim().min(1),
    failure: workerFailureSchema,
});

const objectAvailableFileSchema = z.object({
    id: z.string(),
    object_id: objectIdParamSchema,
    archive_file_key: z.string(),
    artifact_kind: artifactKindSchema,
    variant: z.string().nullable(),
    display_name: z.string(),
    content_type: z.string().nullable(),
    size_bytes: z.number().nullable(),
    checksum_sha256: z.string().nullable(),
    metadata: jsonObjectSchema,
    is_available: z.boolean(),
    synced_at: z.string(),
});

export const listObjectAvailableFilesResponseSchema = z.object({
    object_id: objectIdParamSchema,
    available_files: z.array(objectAvailableFileSchema),
});

const replaceObjectAvailableFilesItemSchema = z
    .strictObject({
        archive_file_key: z.string().trim().min(1),
        artifact_kind: artifactKindSchema,
        variant: z.string().trim().min(1).nullable().optional(),
        display_name: z.string().trim().min(1),
        content_type: z.string().trim().min(1).nullable().optional(),
        size_bytes: z.number().int().min(0).nullable().optional(),
        checksum_sha256: z.string().trim().min(1).nullable().optional(),
        metadata: jsonObjectSchema.optional(),
        is_available: z.boolean().optional(),
    })
    .transform((item) => ({
        ...item,
        metadata: item.metadata ?? {},
        is_available: item.is_available ?? true,
    }));

export const replaceObjectAvailableFilesBodySchema = z.strictObject({
    files: z.array(replaceObjectAvailableFilesItemSchema),
});

export const replaceObjectAvailableFilesResponseSchema = z.object({
    object_id: objectIdParamSchema,
    synced_files: z.number().int().nonnegative(),
});

export const updateAccessPolicyBodySchema = z
    .strictObject({
        access_level: accessLevelSchema,
        embargo_kind: embargoKindSchema,
        embargo_until: z.iso.datetime({ offset: true }).nullable().optional(),
        embargo_curation_state: curationStateSchema.nullable().optional(),
        rights_note: z.string().trim().min(1).nullable().optional(),
        sensitivity_note: z.string().trim().min(1).nullable().optional(),
    })
    .superRefine((value, context) => {
        if (value.embargo_kind === "timed" && value.embargo_until == null) {
            context.addIssue({
                code: z.ZodIssueCode.custom,
                message:
                    "embargo_until is required when embargo_kind is 'timed'.",
                path: ["embargo_until"],
            });
        }

        if (
            value.embargo_kind === "curation_state" &&
            value.embargo_curation_state == null
        ) {
            context.addIssue({
                code: z.ZodIssueCode.custom,
                message:
                    "embargo_curation_state is required when embargo_kind is 'curation_state'.",
                path: ["embargo_curation_state"],
            });
        }
    });

export const createAccessRequestBodySchema = z.strictObject({
    requested_level: z.enum(["family", "private"]),
    reason: z.string().trim().min(1).optional(),
});

export const resolveAccessRequestBodySchema = z.strictObject({
    decision_note: z.string().trim().min(1).optional(),
});

export const upsertAccessAssignmentBodySchema = z.strictObject({
    user_id: z.uuid(),
    granted_level: z.enum(["family", "private"]),
});

export const objectDtoSchema = z.object({
    id: z.string(),
    object_id: objectIdParamSchema,
    thumbnail_artifact_id: z.string().nullable(),
    tenant_id: z.string(),
    type: objectTypeSchema,
    title: z.string(),
    language: z.string().nullable(),
    tags: z.array(z.string()),
    metadata: jsonObjectSchema,
    source_ingestion_id: z.string().nullable(),
    source_batch_label: z.string().nullable(),
    processing_state: processingStateSchema,
    curation_state: curationStateSchema,
    availability_state: availabilityStateSchema,
    access_level: accessLevelSchema,
    embargo_kind: embargoKindSchema,
    embargo_until: z.string().nullable(),
    embargo_curation_state: curationStateSchema.nullable(),
    rights_note: z.string().nullable(),
    sensitivity_note: z.string().nullable(),
    created_at: z.string(),
    updated_at: z.string(),
});

export const objectListItemSchema = objectDtoSchema.extend({
    can_download: z.boolean(),
    access_reason_code: z.enum([
        "OK",
        "FORBIDDEN_POLICY",
        "EMBARGO_ACTIVE",
        "RESTORE_REQUIRED",
        "RESTORE_IN_PROGRESS",
        "TEMP_UNAVAILABLE",
    ]),
});

export const objectListResponseSchema = z.object({
    objects: z.array(objectListItemSchema),
    next_cursor: z.string().nullable(),
    total_count: z.number(),
    filtered_count: z.number(),
});

const objectViewerMediaTypeSchema = z.enum([
    "document",
    "image",
    "audio",
    "video",
]);

const objectViewerSourceTypeSchema = z.enum([
    "original",
    "access_copy",
    "stream",
    "preview",
    "other",
]);

const objectViewerPrimarySourceStatusSchema = z.enum([
    "available",
    "request_required",
    "request_pending",
    "restricted",
    "temporarily_unavailable",
]);

const objectViewerArtifactRefSchema = z.object({
    available: z.literal(true),
    artifact_id: z.string(),
    content_type: z.string().nullable(),
    display_name: z.string().nullable(),
    metadata: jsonObjectSchema,
});

const documentViewerPageSchema = z.object({
    page_number: z.number().int().positive(),
    label: z.string().nullable(),
    image_artifact_id: z.string().nullable(),
    ocr_text_artifact_id: z.string().nullable(),
});

const objectViewerPrimarySourceSchema = z.object({
    source_type: objectViewerSourceTypeSchema,
    artifact_kind: artifactKindSchema,
    variant: z.string().nullable(),
    status: objectViewerPrimarySourceStatusSchema,
    available_file_id: z.string().nullable(),
    artifact_id: z.string().nullable(),
    display_name: z.string().nullable(),
    content_type: z.string().nullable(),
    size_bytes: z.number().nullable(),
    access_reason_code: objectListItemSchema.shape.access_reason_code,
});

const objectViewerActiveRequestSchema = z.object({
    id: z.uuid(),
    action_type: z.literal("artifact_fetch"),
    status: z.enum(["PENDING", "PROCESSING"]),
    created_at: z.string(),
    updated_at: z.string(),
});

const objectViewerPreviewArtifactsSchema = z.object({
    thumbnail: objectViewerArtifactRefSchema.nullable(),
    poster: objectViewerArtifactRefSchema.nullable(),
    ocr_text: objectViewerArtifactRefSchema.nullable(),
    transcript: objectViewerArtifactRefSchema.nullable(),
    captions: objectViewerArtifactRefSchema.nullable(),
});

const documentViewerPayloadSchema = z.object({
    kind: z.literal("document"),
    artifact_id: z.string().nullable(),
    content_type: z.string().nullable(),
    ocr_text_artifact_id: z.string().nullable(),
    page_count: z.number().int().positive().nullable(),
    pages: z.array(documentViewerPageSchema).optional(),
});

const imageViewerPayloadSchema = z.object({
    kind: z.literal("image"),
    artifact_id: z.string().nullable(),
    content_type: z.string().nullable(),
    width: z.number().int().positive().nullable(),
    height: z.number().int().positive().nullable(),
});

const audioViewerPayloadSchema = z.object({
    kind: z.literal("audio"),
    artifact_id: z.string().nullable(),
    content_type: z.string().nullable(),
    transcript_artifact_id: z.string().nullable(),
    duration_seconds: z.number().nonnegative().nullable(),
});

const videoViewerPayloadSchema = z.object({
    kind: z.literal("video"),
    artifact_id: z.string().nullable(),
    content_type: z.string().nullable(),
    poster_artifact_id: z.string().nullable(),
    transcript_artifact_id: z.string().nullable(),
    captions_artifact_id: z.string().nullable(),
    duration_seconds: z.number().nonnegative().nullable(),
});

const objectViewerPayloadSchema = z.discriminatedUnion("kind", [
    documentViewerPayloadSchema,
    imageViewerPayloadSchema,
    audioViewerPayloadSchema,
    videoViewerPayloadSchema,
]);

const objectViewerSchema = z.object({
    media_type: objectViewerMediaTypeSchema,
    primary_source: objectViewerPrimarySourceSchema,
    active_request: objectViewerActiveRequestSchema.nullable(),
    preview_artifacts: objectViewerPreviewArtifactsSchema,
    viewer_payload: objectViewerPayloadSchema,
});

export const objectDetailResponseSchema = z.object({
    object: objectDtoSchema.extend({
        ingest_manifest: jsonObjectSchema.nullable(),
        is_authorized: z.boolean(),
        is_deliverable: z.boolean(),
        can_download: z.boolean(),
        access_reason_code: objectListItemSchema.shape.access_reason_code,
    }),
    viewer: objectViewerSchema.nullable(),
});

export const objectArtifactSchema = z.object({
    id: z.string(),
    object_id: objectIdParamSchema,
    kind: artifactKindSchema,
    variant: z.string().nullable(),
    storage_key: z.string(),
    content_type: z.string(),
    size_bytes: z.number(),
    created_at: z.string(),
});

const objectDownloadRequestSchema = z.object({
    id: z.string(),
    object_id: objectIdParamSchema,
    available_file_id: z.uuid().nullable(),
    requested_by: z.string(),
    artifact_kind: artifactKindSchema,
    variant: z.string().nullable(),
    status: objectDownloadRequestStatusSchema,
    failure_reason: z.string().nullable(),
    failure_details: jsonObjectSchema.nullable(),
    created_at: z.string(),
    updated_at: z.string(),
    completed_at: z.string().nullable(),
});

const archiveRequestSchema = z.object({
    id: z.uuid(),
    tenant_id: z.uuid(),
    target_type: archiveRequestTargetTypeSchema,
    target_id: z.string().min(1),
    action_type: archiveRequestActionTypeSchema,
    action_payload: jsonObjectSchema,
    requested_by: z.uuid(),
    dedupe_key: z.string().nullable(),
    status: archiveRequestStatusSchema,
    failure_reason: z.string().nullable(),
    failure_details: jsonObjectSchema.nullable(),
    created_at: z.string(),
    updated_at: z.string(),
    completed_at: z.string().nullable(),
});

const archiveRequestListItemSchema = archiveRequestSchema
    .omit({ action_payload: true })
    .extend({
        action_payload: jsonObjectSchema.optional(),
    });

export const listArchiveRequestsResponseSchema = z.object({
    requests: z.array(archiveRequestListItemSchema),
    next_cursor: z.string().nullable(),
    filtered_count: z.number().int().nonnegative(),
});

export const requestObjectResyncResponseSchema = z.object({
    status: z.literal("queued"),
    object_id: objectIdParamSchema,
    request: archiveRequestSchema.extend({
        target_type: z.literal("object"),
        action_type: z.literal("object_resync"),
    }),
});

export const listObjectResyncRequestsResponseSchema = z.object({
    object_id: objectIdParamSchema,
    requests: z.array(
        archiveRequestSchema.extend({
            target_type: z.literal("object"),
            action_type: z.literal("object_resync"),
        }),
    ),
});

export const workerLeaseArchiveRequestResponseSchema = z.object({
    request: z
        .object({
            request_id: z.uuid(),
            lease_id: z.uuid(),
            lease_token: z.string(),
            lease_expires_at: z.string(),
            tenant_id: z.uuid(),
            target_type: archiveRequestTargetTypeSchema,
            target_id: z.string(),
            action_type: archiveRequestActionTypeSchema,
            action_payload: jsonObjectSchema,
            requested_by: z.uuid(),
            dedupe_key: z.string().nullable(),
        })
        .nullable(),
});

export const workerHeartbeatArchiveRequestResponseSchema = z.object({
    request: z.object({
        request_id: z.uuid(),
        lease_id: z.uuid(),
        lease_token: z.string(),
        lease_expires_at: z.string(),
    }),
});

export const workerReleaseArchiveRequestResponseSchema = z.object({
    status: z.literal("ok"),
    request_id: z.uuid(),
});

export const workerCompleteArchiveRequestResponseSchema = z.object({
    status: z.literal("completed"),
    request: archiveRequestSchema,
});

export const workerFailArchiveRequestResponseSchema = z.object({
    status: z.literal("failed"),
    request_id: z.uuid(),
    retryable: z.boolean(),
});

export const createObjectDownloadRequestResponseSchema = z.discriminatedUnion(
    "status",
    [
        z.object({
            status: z.literal("available"),
            object_id: objectIdParamSchema,
            artifact: objectArtifactSchema,
        }),
        z.object({
            status: z.literal("queued"),
            object_id: objectIdParamSchema,
            request: objectDownloadRequestSchema,
        }),
    ],
);

export const listObjectDownloadRequestsResponseSchema = z.object({
    object_id: objectIdParamSchema,
    requests: z.array(objectDownloadRequestSchema),
});

export const workerLeaseObjectDownloadRequestResponseSchema = z.object({
    request: z
        .object({
            request_id: z.string(),
            lease_id: z.string(),
            lease_token: z.string(),
            lease_expires_at: z.string(),
            object_id: objectIdParamSchema,
            tenant_id: z.string(),
            available_file_id: z.uuid().nullable(),
            artifact_kind: artifactKindSchema,
            variant: z.string().nullable(),
            available_file: objectAvailableFileSchema.nullable(),
        })
        .nullable(),
});

export const workerHeartbeatObjectDownloadRequestResponseSchema = z.object({
    request: z.object({
        request_id: z.string(),
        lease_id: z.string(),
        lease_token: z.string(),
        lease_expires_at: z.string(),
    }),
});

export const workerReleaseObjectDownloadRequestResponseSchema = z.object({
    status: z.literal("ok"),
    request_id: z.string(),
});

export const workerPresignObjectArtifactUploadResponseSchema = z.object({
    upload_token: z.string(),
    upload_url: z.string(),
    storage_key: z.string(),
    expires_at: z.string(),
    headers: z.object({
        "content-type": z.string(),
        "content-length": z.number(),
    }),
});

export const workerCompleteObjectDownloadRequestResponseSchema = z.object({
    status: z.literal("completed"),
    request_id: z.string(),
    object_id: objectIdParamSchema,
    artifact: objectArtifactSchema,
});

export const workerFailObjectDownloadRequestResponseSchema = z.object({
    status: z.literal("failed"),
    request_id: z.string(),
    retryable: z.boolean(),
});

export const workerUploadObjectArtifactByTokenResponseSchema = z.object({
    status: z.literal("ok"),
    request_id: z.string(),
    size_bytes: z.number(),
});

export const objectArtifactsResponseSchema = z.object({
    object_id: objectIdParamSchema,
    artifacts: z.array(objectArtifactSchema),
});

export const patchObjectTitleResponseSchema = z.object({
    object: objectDtoSchema,
});

export const updateAccessPolicyResponseSchema = z.object({
    object: objectDtoSchema.extend({
        ingest_manifest: jsonObjectSchema.nullable(),
    }),
});

export const createAccessRequestResponseSchema = z.object({
    request: z.object({
        id: z.string(),
        object_id: objectIdParamSchema,
        requester_user_id: z.string(),
        requested_level: z.enum(["family", "private"]),
        reason: z.string().nullable(),
        status: z.string(),
        created_at: z.string(),
        updated_at: z.string(),
    }),
});

export const listAccessRequestsResponseSchema = z.object({
    object_id: objectIdParamSchema,
    requests: z.array(
        z.object({
            id: z.string(),
            requester_user_id: z.string(),
            requested_level: z.enum(["family", "private"]),
            reason: z.string().nullable(),
            status: z.string(),
            reviewed_by: z.string().nullable(),
            reviewed_at: z.string().nullable(),
            decision_note: z.string().nullable(),
            created_at: z.string(),
            updated_at: z.string(),
        }),
    ),
});

export const resolveAccessRequestResponseSchema = z.object({
    request: z.object({
        id: z.string(),
        object_id: objectIdParamSchema,
        requester_user_id: z.string(),
        requested_level: z.enum(["family", "private"]),
        status: z.string(),
        reviewed_by: z.string().nullable(),
        reviewed_at: z.string().nullable(),
        decision_note: z.string().nullable(),
        created_at: z.string(),
        updated_at: z.string(),
    }),
});

export const listAccessAssignmentsResponseSchema = z.object({
    object_id: objectIdParamSchema,
    assignments: z.array(
        z.object({
            user_id: z.string(),
            granted_level: z.enum(["family", "private"]),
            created_by: z.string(),
            created_at: z.string(),
        }),
    ),
});

export const upsertAccessAssignmentResponseSchema = z.object({
    assignment: z.object({
        object_id: objectIdParamSchema,
        user_id: z.string(),
        granted_level: z.enum(["family", "private"]),
        created_by: z.string(),
        created_at: z.string(),
    }),
});

export const deleteAccessAssignmentResponseSchema = z.object({
    status: z.literal("ok"),
    object_id: objectIdParamSchema,
    user_id: z.string(),
});

export interface ObjectListQuery {
    limit: number;
    cursor?: ObjectCursorPayload;
    sort: z.infer<typeof objectListSortSchema>;
    query?: string;
    availabilityState?: z.infer<typeof availabilityStateSchema>;
    accessLevel?: z.infer<typeof accessLevelSchema>;
    language?: string;
    batchLabel?: string;
    type?: z.infer<typeof objectTypeSchema>;
    from?: string;
    to?: string;
    tag?: string;
}
export interface ArchiveRequestCursorPayload {
    sort: z.infer<typeof archiveRequestListSortSchema>;
    created_at: string;
    id: string;
}

export interface ArchiveRequestListQuery {
    limit: number;
    cursor?: ArchiveRequestCursorPayload;
    sort: z.infer<typeof archiveRequestListSortSchema>;
    targetType?: z.infer<typeof archiveRequestTargetTypeSchema>;
    targetId?: string;
    actionType?: z.infer<typeof archiveRequestActionTypeSchema>;
    statuses?: Array<z.infer<typeof archiveRequestStatusSchema>>;
    includePayload: boolean;
}
export interface ObjectEditHistoryQuery {
    limit: number;
    cursor?: z.infer<typeof objectEditHistoryCursorSchema>;
}
export type ObjectCursorPayload = z.infer<typeof objectCursorPayloadSchema>;
export type PatchObjectTitleBody = z.infer<typeof patchObjectTitleBodySchema>;
export type PatchObjectMetadataBody = z.infer<typeof patchObjectMetadataBodySchema>;
export type PutDocumentCurationBody = z.infer<typeof putDocumentCurationBodySchema>;
export type SubmitObjectCurationBody = z.infer<typeof submitObjectCurationBodySchema>;
export type WorkerPresignObjectArtifactUploadBody = z.infer<
    typeof workerPresignObjectArtifactUploadBodySchema
>;
export type WorkerCompleteObjectDownloadRequestBody = z.infer<
    typeof workerCompleteObjectDownloadRequestBodySchema
>;
export type WorkerFailObjectDownloadRequestBody = z.infer<
    typeof workerFailObjectDownloadRequestBodySchema
>;
export type WorkerCompleteArchiveRequestBody = z.infer<
    typeof workerCompleteArchiveRequestBodySchema
>;
export type WorkerFailArchiveRequestBody = z.infer<
    typeof workerFailArchiveRequestBodySchema
>;
export type CreateObjectDownloadRequestBody = z.infer<
    typeof createObjectDownloadRequestBodySchema
>;
export type CreateObjectResyncBody = z.infer<typeof createObjectResyncBodySchema>;
export type WorkerLeaseArchiveRequestBody = z.infer<
    typeof workerLeaseArchiveRequestBodySchema
>;
export type ReplaceObjectAvailableFilesBody = z.infer<
    typeof replaceObjectAvailableFilesBodySchema
>;
export type UpdateAccessPolicyBody = z.infer<
    typeof updateAccessPolicyBodySchema
>;
export type CreateAccessRequestBody = z.infer<
    typeof createAccessRequestBodySchema
>;
export type ResolveAccessRequestBody = z.infer<
    typeof resolveAccessRequestBodySchema
>;
export type UpsertAccessAssignmentBody = z.infer<
    typeof upsertAccessAssignmentBodySchema
>;
export type ObjectListResponse = z.infer<typeof objectListResponseSchema>;
export type ObjectDto = z.infer<typeof objectDtoSchema>;
export type ObjectListItem = z.infer<typeof objectListItemSchema>;
export type ObjectDetailResponse = z.infer<typeof objectDetailResponseSchema>;
export type ObjectEditResponse = z.infer<typeof objectEditResponseSchema>;
export type ObjectViewer = z.infer<typeof objectViewerSchema>;
export type ObjectViewerArtifactRef = z.infer<typeof objectViewerArtifactRefSchema>;
export type ObjectViewerPrimarySource = z.infer<
    typeof objectViewerPrimarySourceSchema
>;
export type ObjectViewerActiveRequest = z.infer<
    typeof objectViewerActiveRequestSchema
>;
export type ObjectViewerPreviewArtifacts = z.infer<
    typeof objectViewerPreviewArtifactsSchema
>;
export type ObjectViewerPayload = z.infer<typeof objectViewerPayloadSchema>;
export type ObjectArtifactsResponse = z.infer<
    typeof objectArtifactsResponseSchema
>;
export type PatchObjectTitleResponse = z.infer<
    typeof patchObjectTitleResponseSchema
>;
export type PatchObjectMetadataResponse = z.infer<
    typeof patchObjectMetadataResponseSchema
>;
export type PutDocumentCurationResponse = z.infer<
    typeof putDocumentCurationResponseSchema
>;
export type SubmitObjectCurationResponse = z.infer<
    typeof submitObjectCurationResponseSchema
>;
export type CreateObjectDownloadRequestResponse = z.infer<
    typeof createObjectDownloadRequestResponseSchema
>;
export type RequestObjectResyncResponse = z.infer<
    typeof requestObjectResyncResponseSchema
>;
export type ListObjectResyncRequestsResponse = z.infer<
    typeof listObjectResyncRequestsResponseSchema
>;
export type ListArchiveRequestsResponse = z.infer<
    typeof listArchiveRequestsResponseSchema
>;
export type ObjectArtifactDto = z.infer<typeof objectArtifactSchema>;
export type ObjectDownloadRequestDto = z.infer<
    typeof objectDownloadRequestSchema
>;
export type ObjectAvailableFileDto = z.infer<typeof objectAvailableFileSchema>;
export type ListObjectAvailableFilesResponse = z.infer<
    typeof listObjectAvailableFilesResponseSchema
>;
export type ReplaceObjectAvailableFilesResponse = z.infer<
    typeof replaceObjectAvailableFilesResponseSchema
>;
export type ListObjectDownloadRequestsResponse = z.infer<
    typeof listObjectDownloadRequestsResponseSchema
>;
export type WorkerLeaseObjectDownloadRequestResponse = z.infer<
    typeof workerLeaseObjectDownloadRequestResponseSchema
>;
export type WorkerHeartbeatObjectDownloadRequestResponse = z.infer<
    typeof workerHeartbeatObjectDownloadRequestResponseSchema
>;
export type WorkerReleaseObjectDownloadRequestResponse = z.infer<
    typeof workerReleaseObjectDownloadRequestResponseSchema
>;
export type WorkerPresignObjectArtifactUploadResponse = z.infer<
    typeof workerPresignObjectArtifactUploadResponseSchema
>;
export type WorkerCompleteObjectDownloadRequestResponse = z.infer<
    typeof workerCompleteObjectDownloadRequestResponseSchema
>;
export type WorkerFailObjectDownloadRequestResponse = z.infer<
    typeof workerFailObjectDownloadRequestResponseSchema
>;
export type WorkerLeaseArchiveRequestResponse = z.infer<
    typeof workerLeaseArchiveRequestResponseSchema
>;
export type WorkerHeartbeatArchiveRequestResponse = z.infer<
    typeof workerHeartbeatArchiveRequestResponseSchema
>;
export type WorkerReleaseArchiveRequestResponse = z.infer<
    typeof workerReleaseArchiveRequestResponseSchema
>;
export type WorkerCompleteArchiveRequestResponse = z.infer<
    typeof workerCompleteArchiveRequestResponseSchema
>;
export type WorkerFailArchiveRequestResponse = z.infer<
    typeof workerFailArchiveRequestResponseSchema
>;
export type WorkerUploadObjectArtifactByTokenResponse = z.infer<
    typeof workerUploadObjectArtifactByTokenResponseSchema
>;
export type UpdateAccessPolicyResponse = z.infer<
    typeof updateAccessPolicyResponseSchema
>;
export type CreateAccessRequestResponse = z.infer<
    typeof createAccessRequestResponseSchema
>;
export type ListAccessRequestsResponse = z.infer<
    typeof listAccessRequestsResponseSchema
>;
export type ResolveAccessRequestResponse = z.infer<
    typeof resolveAccessRequestResponseSchema
>;
export type ListAccessAssignmentsResponse = z.infer<
    typeof listAccessAssignmentsResponseSchema
>;
export type UpsertAccessAssignmentResponse = z.infer<
    typeof upsertAccessAssignmentResponseSchema
>;
export type DeleteAccessAssignmentResponse = z.infer<
    typeof deleteAccessAssignmentResponseSchema
>;
export type ObjectEditHistoryResponse = z.infer<
    typeof objectEditHistoryResponseSchema
>;

export function parseObjectIdParam(value: string): string {
    const parsed = objectIdParamSchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseArtifactIdParam(value: string): string {
    const parsed = artifactIdParamSchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseObjectDownloadRequestIdParam(value: string): string {
    const parsed = objectDownloadRequestIdParamSchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseArchiveRequestIdParam(value: string): string {
    const parsed = archiveRequestIdParamSchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseAccessRequestIdParam(value: string): string {
    const parsed = accessRequestIdParamSchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseUserIdParam(value: string): string {
    const parsed = userIdParamSchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseObjectListQuery(url: URL): {
    limit: number;
    cursor?: ObjectCursorPayload;
    sort: z.infer<typeof objectListSortSchema>;
    query?: string;
    availabilityState?: z.infer<typeof availabilityStateSchema>;
    accessLevel?: z.infer<typeof accessLevelSchema>;
    language?: string;
    batchLabel?: string;
    type?: z.infer<typeof objectTypeSchema>;
    from?: string;
    to?: string;
    tag?: string;
} {
    const parsed = objectListQueryWithCursorSchema.safeParse({
        limit: url.searchParams.get("limit") ?? undefined,
        cursor: url.searchParams.get("cursor") ?? undefined,
        sort: url.searchParams.get("sort") ?? undefined,
        q: url.searchParams.get("q") ?? undefined,
        availability_state:
            url.searchParams.get("availability_state") ?? undefined,
        access_level: url.searchParams.get("access_level") ?? undefined,
        language: url.searchParams.get("language") ?? undefined,
        batch_label: url.searchParams.get("batch_label") ?? undefined,
        type: url.searchParams.get("type") ?? undefined,
        from: url.searchParams.get("from") ?? undefined,
        to: url.searchParams.get("to") ?? undefined,
        tag: url.searchParams.get("tag") ?? undefined,
    });

    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseArchiveRequestListQuery(url: URL): ArchiveRequestListQuery {
    const rawStatuses = url.searchParams
        .getAll("status")
        .flatMap((value) => value.split(","))
        .map((value) => value.trim())
        .filter((value) => value.length > 0);

    const parsed = archiveRequestListQueryWithCursorSchema.safeParse({
        limit: url.searchParams.get("limit") ?? undefined,
        cursor: url.searchParams.get("cursor") ?? undefined,
        sort: url.searchParams.get("sort") ?? undefined,
        target_type: url.searchParams.get("target_type") ?? undefined,
        target_id: url.searchParams.get("target_id") ?? undefined,
        action_type: url.searchParams.get("action_type") ?? undefined,
        status: rawStatuses.length > 0 ? rawStatuses : undefined,
        active_only: url.searchParams.get("active_only") ?? undefined,
        include_payload: url.searchParams.get("include_payload") ?? undefined,
    });

    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseObjectEditHistoryQuery(url: URL): ObjectEditHistoryQuery {
    const limit = z.coerce.number().int().min(1).max(200).default(50).safeParse(
        url.searchParams.get("limit") ?? undefined,
    );

    if (!limit.success) {
        throw mapZodErrorToValidation(limit.error);
    }

    const rawCursor = url.searchParams.get("cursor") ?? undefined;
    if (!rawCursor) {
        return {
            limit: limit.data,
        };
    }

    const decoded = decodeCursor<JsonObject>(rawCursor);
    const parsedCursor = objectEditHistoryCursorSchema.safeParse(decoded);
    if (!parsedCursor.success) {
        throw mapZodErrorToValidation(parsedCursor.error);
    }

    return {
        limit: limit.data,
        cursor: parsedCursor.data,
    };
}

export function parsePatchObjectTitleBody(
    value: unknown,
): PatchObjectTitleBody {
    const parsed = patchObjectTitleBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parsePatchObjectMetadataBody(
    value: unknown,
): PatchObjectMetadataBody {
    const parsed = patchObjectMetadataBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToUnprocessable(parsed.error);
    }

    return parsed.data;
}

export function parsePutDocumentCurationBody(
    value: unknown,
): PutDocumentCurationBody {
    const parsed = putDocumentCurationBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToUnprocessable(parsed.error);
    }

    return parsed.data;
}

export function parseSubmitObjectCurationBody(
    value: unknown,
): SubmitObjectCurationBody {
    const parsed = submitObjectCurationBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToUnprocessable(parsed.error);
    }

    return parsed.data;
}

export function parseCreateObjectDownloadRequestBody(
    value: unknown,
): CreateObjectDownloadRequestBody {
    const parsed = createObjectDownloadRequestBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseCreateObjectResyncBody(
    value: unknown,
): CreateObjectResyncBody {
    const parsed = createObjectResyncBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseWorkerLeaseArchiveRequestBody(
    value: unknown,
): WorkerLeaseArchiveRequestBody {
    const parsed = workerLeaseArchiveRequestBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseWorkerPresignObjectArtifactUploadBody(
    value: unknown,
): WorkerPresignObjectArtifactUploadBody {
    const parsed = workerPresignObjectArtifactUploadBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseWorkerCompleteObjectDownloadRequestBody(
    value: unknown,
): WorkerCompleteObjectDownloadRequestBody {
    const parsed = workerCompleteObjectDownloadRequestBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseWorkerFailObjectDownloadRequestBody(
    value: unknown,
): WorkerFailObjectDownloadRequestBody {
    const parsed = workerFailObjectDownloadRequestBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseWorkerCompleteArchiveRequestBody(
    value: unknown,
): WorkerCompleteArchiveRequestBody {
    const parsed = workerCompleteArchiveRequestBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseWorkerFailArchiveRequestBody(
    value: unknown,
): WorkerFailArchiveRequestBody {
    const parsed = workerFailArchiveRequestBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseReplaceObjectAvailableFilesBody(
    value: unknown,
): ReplaceObjectAvailableFilesBody {
    const parsed = replaceObjectAvailableFilesBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseUpdateAccessPolicyBody(
    value: unknown,
): UpdateAccessPolicyBody {
    const parsed = updateAccessPolicyBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseCreateAccessRequestBody(
    value: unknown,
): CreateAccessRequestBody {
    const parsed = createAccessRequestBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseResolveAccessRequestBody(
    value: unknown,
): ResolveAccessRequestBody {
    const parsed = resolveAccessRequestBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseUpsertAccessAssignmentBody(
    value: unknown,
): UpsertAccessAssignmentBody {
    const parsed = upsertAccessAssignmentBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseObjectMetadata(value: unknown): JsonObject {
    const parsed = jsonObjectSchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }
    return parsed.data;
}

const objectTextManifestArtifactSchema = z.strictObject({
    kind: z.string().trim().min(1),
    version: z.string().trim().min(1),
    is_active: z.boolean(),
    metadata: jsonObjectSchema.optional(),
});

const objectTextManifestMediaTypeSchema = z.enum([
    "document",
    "audio",
    "video",
    "photo",
    "other",
]);

export const replaceObjectTextManifestBodySchema = z.strictObject({
    object_text_manifest: z
        .strictObject({
            object_id: objectIdParamSchema,
            media_type: objectTextManifestMediaTypeSchema,
            projection_version: z.string().trim().min(1),
            generated_at: z.iso.datetime({ offset: true }),
            text_artifacts: z.array(objectTextManifestArtifactSchema),
        })
        .superRefine((manifest, context) => {
            const activeKinds = new Set<string>();

            for (const [index, artifact] of manifest.text_artifacts.entries()) {
                if (!artifact.is_active) {
                    continue;
                }

                if (activeKinds.has(artifact.kind)) {
                    context.addIssue({
                        code: z.ZodIssueCode.custom,
                        message: `Multiple active text artifacts are not allowed for kind '${artifact.kind}'.`,
                        path: ["text_artifacts", index, "is_active"],
                    });
                    continue;
                }

                activeKinds.add(artifact.kind);
            }
        }),
});

export type ReplaceObjectTextManifestBody = z.infer<
    typeof replaceObjectTextManifestBodySchema
>;

export function parseReplaceObjectTextManifestBody(
    value: unknown,
): ReplaceObjectTextManifestBody {
    const parsed = replaceObjectTextManifestBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}
