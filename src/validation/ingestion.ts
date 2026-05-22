import { z } from "zod";

import { ingestionSummarySchema } from "./catalog.ts";
import { mapZodErrorToValidation } from "./zod-errors.ts";

export type JsonValue =
    | string
    | number
    | boolean
    | null
    | JsonObject
    | JsonArray;

export interface JsonObject {
    [key: string]: JsonValue;
}

export type JsonArray = JsonValue[];

export const jsonValueSchema: z.ZodType<JsonValue> = z.lazy(() =>
    z.union([
        z.string(),
        z.number(),
        z.boolean(),
        z.null(),
        jsonObjectSchema,
        jsonArraySchema,
    ]),
);

export const jsonObjectSchema: z.ZodType<JsonObject> = z.lazy(() =>
    z.record(z.string(), jsonValueSchema),
);

export const jsonArraySchema: z.ZodType<JsonArray> = z.lazy(() =>
    z.array(jsonValueSchema),
);

export const ingestionStatusSchema = z.enum([
    "DRAFT",
    "UPLOADING",
    "QUEUED",
    "PROCESSING",
    "COMPLETED",
    "COMPLETED_WITH_ERRORS",
    "FAILED",
    "CANCELED",
]);

export const ingestionClassificationTypeSchema = z.enum([
    "newspaper_article",
    "magazine_article",
    "book_chapter",
    "book",
    "letter",
    "speech",
    "interview",
    "report",
    "manuscript",
    "image",
    "document",
    "other",
]);

export const ingestItemKindSchema = z.enum([
    "photo",
    "audio",
    "video",
    "scanned_document",
    "document",
    "other",
]);

export const ingestionPipelinePresetSchema = z.enum([
    "auto",
    "none",
    "ocr_text",
    "audio_transcript",
    "video_transcript",
    "ocr_and_audio_transcript",
    "ocr_and_video_transcript",
]);

export const accessLevelSchema = z.enum(["private", "family", "public"]);

export const ingestionFileStatusSchema = z.enum([
    "PENDING",
    "UPLOADED",
    "VALIDATED",
    "FAILED",
]);

export const ingestionPreviewStatusSchema = z.enum([
    "pending",
    "processing",
    "ready",
    "failed",
    "unsupported",
]);

const workerPreviewErrorSchema = z.strictObject({
    message: z.string().trim().min(1),
    code: z.string().trim().min(1).optional(),
    retryable: z.boolean().optional(),
    details: jsonObjectSchema.optional(),
});

export const ingestionItemStatusSchema = z.enum([
    "PENDING",
    "READY",
    "PROCESSING",
    "COMPLETED",
    "FAILED",
    "SKIPPED",
]);

export const ingestionItemFileRoleSchema = z.enum([
    "primary",
    "front",
    "back",
    "page",
    "attachment",
    "transcript_source",
    "side_a",
    "side_b",
    "other",
]);

const processingOverrideSchema = z.strictObject({
    enabled: z.boolean(),
    language: z.string().trim().min(1).optional(),
});

export const ingestionFileProcessingOverridesSchema = z.strictObject({
    ocr_text: processingOverrideSchema.optional(),
    audio_transcript: processingOverrideSchema.optional(),
    video_transcript: processingOverrideSchema.optional(),
});

export const createIngestionBodySchema = z.strictObject({
    batch_label: z.string().trim().min(1),
    schema_version: z.literal("1.0").default("1.0"),
    classification_type: ingestionClassificationTypeSchema,
    item_kind: ingestItemKindSchema,
    language_code: z.string().trim().min(1),
    pipeline_preset: ingestionPipelinePresetSchema,
    access_level: accessLevelSchema,
    summary: ingestionSummarySchema,
    embargo_until: z.iso.datetime({ offset: true }).nullable().optional(),
    rights_note: z.string().nullable().optional(),
    sensitivity_note: z.string().nullable().optional(),
});

export const updateIngestionBodySchema = z.strictObject({
    batch_label: z.string().trim().min(1).optional(),
    classification_type: ingestionClassificationTypeSchema.optional(),
    item_kind: ingestItemKindSchema.optional(),
    language_code: z.string().trim().min(1).optional(),
    pipeline_preset: ingestionPipelinePresetSchema.optional(),
    access_level: accessLevelSchema.optional(),
    summary: ingestionSummarySchema.optional(),
    embargo_until: z.iso.datetime({ offset: true }).nullable().optional(),
    rights_note: z.string().nullable().optional(),
    sensitivity_note: z.string().nullable().optional(),
});

export const ingestionCursorPayloadSchema = z.strictObject({
    created_at: z.iso.datetime({ offset: true }),
    id: z.uuid(),
});

export const ingestionListQuerySchema = z.strictObject({
    limit: z.coerce.number().int().min(1).max(200).default(50),
    cursor: z.string().trim().min(1).optional(),
});

export const ingestionIdParamSchema = z.uuid();
export const ingestionFileIdParamSchema = z.uuid();
export const ingestionItemIdParamSchema = z.uuid();
export const uploadTokenParamSchema = z.string().trim().min(1);

export const presignNewFileBodySchema = z.strictObject({
    filename: z.string().trim().min(1),
    content_type: z.string().trim().min(1),
    size_bytes: z.number().int().min(1),
});

export const presignExistingFileBodySchema = z.strictObject({
    file_id: z.uuid(),
});

export const createPresignedUploadBodySchema = z.union([
    presignNewFileBodySchema,
    presignExistingFileBodySchema,
]);

export const commitUploadedFileBodySchema = z.strictObject({
    file_id: z.uuid(),
    checksum_sha256: z
        .string()
        .trim()
        .min(1)
        .regex(/^[a-f0-9]{64}$/i),
});

export const workerClaimIngestionPreviewResponseSchema = z.object({
    preview: z.object({
        ingestion_id: z.string(),
        file_id: z.string(),
        tenant_id: z.string(),
        batch_label: z.string(),
        filename: z.string(),
        content_type: z.string(),
        size_bytes: z.number(),
        download_url: z.string(),
        claimed_by: z.string().nullable(),
        claimed_at: z.string().nullable(),
    }).nullable(),
});

export const workerPresignIngestionPreviewUploadBodySchema = z.strictObject({
    content_type: z.string().trim().min(1),
    size_bytes: z.number().int().min(1),
    extension: z.string().trim().min(1).optional(),
});

export const workerPresignIngestionPreviewUploadResponseSchema = z.object({
    upload_token: z.string(),
    upload_url: z.string(),
    storage_key: z.string(),
    expires_at: z.string(),
    headers: z.object({
        "content-type": z.string(),
        "content-length": z.number(),
    }),
});

export const workerCompleteIngestionPreviewBodySchema = z.strictObject({
    upload_token: z.string().trim().min(1),
    width: z.number().int().min(1),
    height: z.number().int().min(1),
});

export const workerCompleteIngestionPreviewResponseSchema = z.object({
    status: z.literal("ready"),
    ingestion_id: z.string(),
    file_id: z.string(),
});

export const workerFailIngestionPreviewBodySchema = z.strictObject({
    error: workerPreviewErrorSchema,
});

export const workerFailIngestionPreviewResponseSchema = z.object({
    status: z.literal("failed"),
    ingestion_id: z.string(),
    file_id: z.string(),
});

export const updateIngestionFileOverridesBodySchema = z.strictObject({
    processing_overrides: ingestionFileProcessingOverridesSchema,
});

export const createIngestionItemBodySchema = z.strictObject({
    item_index: z.number().int().min(1),
    classification_type: ingestionClassificationTypeSchema.optional(),
    item_kind: ingestItemKindSchema.optional(),
    language_code: z.string().trim().min(1).optional(),
    title: z.string().trim().min(1).optional(),
    summary: jsonObjectSchema.optional(),
});

export const addIngestionItemFileBodySchema = z.strictObject({
    ingestion_file_id: z.uuid(),
    role: ingestionItemFileRoleSchema.optional(),
    sort_order: z.number().int().min(1),
    page_number: z.number().int().min(1).optional(),
    is_primary: z.boolean().optional(),
    logical_label: z.string().trim().min(1).optional(),
});

export const reorderIngestionItemFilesBodySchema = z.strictObject({
    files: z.array(z.strictObject({
        ingestion_file_id: z.uuid(),
        sort_order: z.number().int().min(1),
    })).min(1),
});

export const reorderIngestionItemsBodySchema = z.strictObject({
    items: z.array(z.strictObject({
        ingestion_item_id: z.uuid(),
        item_index: z.number().int().min(1),
    })).min(1),
});

const ingestionItemDateValueSchema = z
    .string()
    .regex(/^\d{4}(-\d{2})?(-\d{2})?$/, {
        message: "date value must be YYYY, YYYY-MM, or YYYY-MM-DD",
    })
    .nullable();

const ingestionItemDateConfidenceSchema = z.enum(["low", "medium", "high"]);

const ingestionItemDatePatchBlockSchema = z
    .strictObject({
        value: ingestionItemDateValueSchema.optional(),
        approximate: z.boolean().optional(),
        confidence: ingestionItemDateConfidenceSchema.optional(),
        note: z.string().nullable().optional(),
    })
    .refine((value) => Object.keys(value).length > 0, {
        message: "date block patch must include at least one field",
    });

const ingestionItemDatesPatchSchema = z
    .strictObject({
        published: ingestionItemDatePatchBlockSchema.optional(),
        created: ingestionItemDatePatchBlockSchema.optional(),
    })
    .refine((value) => value.published !== undefined || value.created !== undefined, {
        message: "dates patch must include published or created",
    });

export const updateIngestionItemBodySchema = z
    .strictObject({
        classification_type: ingestionClassificationTypeSchema.optional(),
        item_kind: ingestItemKindSchema.optional(),
        language_code: z.string().trim().min(1).optional(),
        title: z.string().trim().min(1).nullable().optional(),
        description: z.string().nullable().optional(),
        tags: z.array(z.string().trim().min(1)).optional(),
        dates: ingestionItemDatesPatchSchema.optional(),
    })
    .refine((value) => Object.keys(value).length > 0, {
        message: "At least one field must be provided.",
    });

export const ingestionDtoSchema = z.object({
    id: z.string(),
    batch_label: z.string(),
    tenant_id: z.string(),
    status: ingestionStatusSchema,
    created_by: z.string(),
    schema_version: z.string(),
    classification_type: ingestionClassificationTypeSchema,
    item_kind: ingestItemKindSchema,
    language_code: z.string(),
    pipeline_preset: ingestionPipelinePresetSchema,
    access_level: accessLevelSchema,
    embargo_until: z.string().nullable(),
    rights_note: z.string().nullable(),
    sensitivity_note: z.string().nullable(),
    summary: ingestionSummarySchema,
    error_summary: jsonObjectSchema,
    created_at: z.string(),
    updated_at: z.string(),
});

export const ingestionFileDtoSchema = z.object({
    id: z.string(),
    ingestion_id: z.string(),
    filename: z.string(),
    content_type: z.string(),
    size_bytes: z.number(),
    storage_key: z.string(),
    status: ingestionFileStatusSchema,
    checksum_sha256: z.string().nullable(),
    preview: z.object({
        status: ingestionPreviewStatusSchema,
        content_type: z.string().nullable(),
        size_bytes: z.number().nullable(),
        width: z.number().int().nullable(),
        height: z.number().int().nullable(),
        url: z.string().nullable(),
        error: jsonObjectSchema.nullable(),
    }),
    processing_overrides: ingestionFileProcessingOverridesSchema,
    error: jsonObjectSchema,
    created_at: z.string(),
    updated_at: z.string(),
});

export const ingestionItemDtoSchema = z.object({
    id: z.string(),
    ingestion_id: z.string(),
    item_index: z.number(),
    status: ingestionItemStatusSchema,
    classification_type: ingestionClassificationTypeSchema.nullable(),
    item_kind: ingestItemKindSchema.nullable(),
    language_code: z.string().nullable(),
    title: z.string().nullable(),
    summary: jsonObjectSchema,
    error_summary: jsonObjectSchema,
    object_id: z.string().nullable(),
    created_at: z.string(),
    updated_at: z.string(),
});

export const ingestionItemFileDtoSchema = z.object({
    id: z.string(),
    ingestion_item_id: z.string(),
    ingestion_file_id: z.string(),
    ingestion_id: z.string(),
    role: ingestionItemFileRoleSchema,
    sort_order: z.number(),
    page_number: z.number().nullable(),
    is_primary: z.boolean(),
    logical_label: z.string().nullable(),
    created_at: z.string(),
});

export const ingestionListResultSchema = z.object({
    items: z.array(ingestionDtoSchema),
    nextCursor: z.string().optional(),
});

export const createIngestionDraftResponseSchema = z.object({
    ingestion: ingestionDtoSchema,
});

export const updateIngestionResponseSchema = z.object({
    ingestion: ingestionDtoSchema,
});

export const getIngestionResponseSchema = z.object({
    ingestion: ingestionDtoSchema,
    files: z.array(ingestionFileDtoSchema),
});

export const createPresignedUploadResponseSchema = z.object({
    file_id: z.string(),
    storage_key: z.string(),
    upload_url: z.string(),
    expires_at: z.string(),
    headers: z.object({
        "content-type": z.string(),
        "content-length": z.number(),
    }),
});

export const commitUploadedFileResponseSchema = z.object({
    file: ingestionFileDtoSchema,
});

export const updateIngestionFileOverridesResponseSchema = z.object({
    file: ingestionFileDtoSchema,
});

export const deleteIngestionFileResponseSchema = z.object({
    status: z.literal("deleted"),
    file_id: z.string(),
});

export const deleteIngestionResponseSchema = z.object({
    status: z.literal("deleted"),
    ingestion_id: z.string(),
});

export const submitIngestionResponseSchema = z.object({
    ingestion: ingestionDtoSchema,
});

export const cancelIngestionResponseSchema = z.object({
    ingestion: ingestionDtoSchema,
});

export const restoreIngestionResponseSchema = z.object({
    ingestion: ingestionDtoSchema,
});

export const retryIngestionResponseSchema = z.object({
    ingestion: ingestionDtoSchema,
});

export const uploadFileBySignedTokenResponseSchema = z.object({
    status: z.literal("ok"),
    ingestion_id: z.string(),
    file_id: z.string(),
    size_bytes: z.number(),
});

export const createIngestionItemResponseSchema = z.object({
    item: ingestionItemDtoSchema,
});

export const listIngestionItemsResponseSchema = z.object({
    items: z.array(ingestionItemDtoSchema),
});

export const addIngestionItemFileResponseSchema = z.object({
    file: ingestionItemFileDtoSchema,
});

export const listIngestionItemFilesResponseSchema = z.object({
    files: z.array(ingestionItemFileDtoSchema),
});

export const reorderIngestionItemFilesResponseSchema = z.object({
    files: z.array(ingestionItemFileDtoSchema),
});

export const reorderIngestionItemsResponseSchema = z.object({
    items: z.array(ingestionItemDtoSchema),
});

export const updateIngestionItemResponseSchema = z.object({
    item: ingestionItemDtoSchema,
});

export const ingestionCapabilitiesResponseSchema = z.object({
    media_kinds: z.array(z.string()),
    extensions_by_kind: z.object({
        image: z.array(z.string()),
        audio: z.array(z.string()),
        video: z.array(z.string()),
        document: z.array(z.string()),
    }),
    mime_by_kind: z.object({
        image: z.array(z.string()),
        audio: z.array(z.string()),
        video: z.array(z.string()),
        document: z.array(z.string()),
    }),
    mime_aliases: z.record(z.string(), z.string()),
});

export type CreateIngestionBody = z.infer<typeof createIngestionBodySchema>;
export type UpdateIngestionBody = z.infer<typeof updateIngestionBodySchema>;
export type IngestionCursorPayload = z.infer<
    typeof ingestionCursorPayloadSchema
>;
export type IngestionListQuery = z.infer<typeof ingestionListQuerySchema>;
export type CreatePresignedUploadBody = z.infer<
    typeof createPresignedUploadBodySchema
>;
export type CommitUploadedFileBody = z.infer<
    typeof commitUploadedFileBodySchema
>;
export type WorkerPresignIngestionPreviewUploadBody = z.infer<
    typeof workerPresignIngestionPreviewUploadBodySchema
>;
export type WorkerCompleteIngestionPreviewBody = z.infer<
    typeof workerCompleteIngestionPreviewBodySchema
>;
export type WorkerFailIngestionPreviewBody = z.infer<
    typeof workerFailIngestionPreviewBodySchema
>;
export type UpdateIngestionFileOverridesBody = z.infer<
    typeof updateIngestionFileOverridesBodySchema
>;
export type CreateIngestionItemBody = z.infer<typeof createIngestionItemBodySchema>;
export type AddIngestionItemFileBody = z.infer<typeof addIngestionItemFileBodySchema>;
export type ReorderIngestionItemFilesBody = z.infer<typeof reorderIngestionItemFilesBodySchema>;
export type ReorderIngestionItemsBody = z.infer<typeof reorderIngestionItemsBodySchema>;
export type UpdateIngestionItemBody = z.infer<typeof updateIngestionItemBodySchema>;
export type AccessLevel = z.infer<typeof accessLevelSchema>;
export type IngestionClassificationType = z.infer<typeof ingestionClassificationTypeSchema>;
export type IngestItemKind = z.infer<typeof ingestItemKindSchema>;
export type IngestionPipelinePreset = z.infer<
    typeof ingestionPipelinePresetSchema
>;
export type IngestionDto = z.infer<typeof ingestionDtoSchema>;
export type IngestionFileDto = z.infer<typeof ingestionFileDtoSchema>;
export type IngestionItemDto = z.infer<typeof ingestionItemDtoSchema>;
export type IngestionItemFileDto = z.infer<typeof ingestionItemFileDtoSchema>;
export type IngestionFileProcessingOverrides = z.infer<
    typeof ingestionFileProcessingOverridesSchema
>;
export type IngestionListResult = z.infer<typeof ingestionListResultSchema>;
export type CreateIngestionDraftResponse = z.infer<
    typeof createIngestionDraftResponseSchema
>;
export type UpdateIngestionResponse = z.infer<
    typeof updateIngestionResponseSchema
>;
export type GetIngestionResponse = z.infer<typeof getIngestionResponseSchema>;
export type CreatePresignedUploadResponse = z.infer<
    typeof createPresignedUploadResponseSchema
>;
export type CommitUploadedFileResponse = z.infer<
    typeof commitUploadedFileResponseSchema
>;
export type WorkerClaimIngestionPreviewResponse = z.infer<
    typeof workerClaimIngestionPreviewResponseSchema
>;
export type WorkerPresignIngestionPreviewUploadResponse = z.infer<
    typeof workerPresignIngestionPreviewUploadResponseSchema
>;
export type WorkerCompleteIngestionPreviewResponse = z.infer<
    typeof workerCompleteIngestionPreviewResponseSchema
>;
export type WorkerFailIngestionPreviewResponse = z.infer<
    typeof workerFailIngestionPreviewResponseSchema
>;
export type UpdateIngestionFileOverridesResponse = z.infer<
    typeof updateIngestionFileOverridesResponseSchema
>;
export type DeleteIngestionFileResponse = z.infer<
    typeof deleteIngestionFileResponseSchema
>;
export type DeleteIngestionResponse = z.infer<
    typeof deleteIngestionResponseSchema
>;
export type SubmitIngestionResponse = z.infer<
    typeof submitIngestionResponseSchema
>;
export type CancelIngestionResponse = z.infer<
    typeof cancelIngestionResponseSchema
>;
export type RestoreIngestionResponse = z.infer<
    typeof restoreIngestionResponseSchema
>;
export type RetryIngestionResponse = z.infer<
    typeof retryIngestionResponseSchema
>;
export type UploadFileBySignedTokenResponse = z.infer<
    typeof uploadFileBySignedTokenResponseSchema
>;
export type CreateIngestionItemResponse = z.infer<typeof createIngestionItemResponseSchema>;
export type ListIngestionItemsResponse = z.infer<typeof listIngestionItemsResponseSchema>;
export type AddIngestionItemFileResponse = z.infer<typeof addIngestionItemFileResponseSchema>;
export type ListIngestionItemFilesResponse = z.infer<typeof listIngestionItemFilesResponseSchema>;
export type ReorderIngestionItemFilesResponse = z.infer<typeof reorderIngestionItemFilesResponseSchema>;
export type ReorderIngestionItemsResponse = z.infer<typeof reorderIngestionItemsResponseSchema>;
export type UpdateIngestionItemResponse = z.infer<typeof updateIngestionItemResponseSchema>;
export type IngestionCapabilitiesResponse = z.infer<
    typeof ingestionCapabilitiesResponseSchema
>;

export function parseJsonObject(value: unknown): JsonObject {
    const parsed = jsonObjectSchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseIngestionFileProcessingOverrides(
    value: unknown,
): IngestionFileProcessingOverrides {
    const parsed = ingestionFileProcessingOverridesSchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseCreateIngestionBody(value: unknown): CreateIngestionBody {
    const parsed = createIngestionBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseUpdateIngestionBody(value: unknown): UpdateIngestionBody {
    const parsed = updateIngestionBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseIngestionCursorPayload(
    value: unknown,
): IngestionCursorPayload {
    const parsed = ingestionCursorPayloadSchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseIngestionListQuery(url: URL): IngestionListQuery {
    const parsed = ingestionListQuerySchema.safeParse({
        limit: url.searchParams.get("limit") ?? undefined,
        cursor: url.searchParams.get("cursor") ?? undefined,
    });

    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseIngestionIdParam(value: string): string {
    const parsed = ingestionIdParamSchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseIngestionFileIdParam(value: string): string {
    const parsed = ingestionFileIdParamSchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseIngestionItemIdParam(value: string): string {
    const parsed = ingestionItemIdParamSchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseUploadTokenParam(value: string): string {
    const parsed = uploadTokenParamSchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseCreatePresignedUploadBody(
    value: unknown,
): CreatePresignedUploadBody {
    const parsed = createPresignedUploadBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseCommitUploadedFileBody(
    value: unknown,
): CommitUploadedFileBody {
    const parsed = commitUploadedFileBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseWorkerPresignIngestionPreviewUploadBody(
    value: unknown,
): WorkerPresignIngestionPreviewUploadBody {
    const parsed = workerPresignIngestionPreviewUploadBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseWorkerCompleteIngestionPreviewBody(
    value: unknown,
): WorkerCompleteIngestionPreviewBody {
    const parsed = workerCompleteIngestionPreviewBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseWorkerFailIngestionPreviewBody(
    value: unknown,
): WorkerFailIngestionPreviewBody {
    const parsed = workerFailIngestionPreviewBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseUpdateIngestionFileOverridesBody(
    value: unknown,
): UpdateIngestionFileOverridesBody {
    const parsed = updateIngestionFileOverridesBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseCreateIngestionItemBody(value: unknown): CreateIngestionItemBody {
    const parsed = createIngestionItemBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseAddIngestionItemFileBody(value: unknown): AddIngestionItemFileBody {
    const parsed = addIngestionItemFileBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseReorderIngestionItemFilesBody(value: unknown): ReorderIngestionItemFilesBody {
    const parsed = reorderIngestionItemFilesBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseReorderIngestionItemsBody(value: unknown): ReorderIngestionItemsBody {
    const parsed = reorderIngestionItemsBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}

export function parseUpdateIngestionItemBody(value: unknown): UpdateIngestionItemBody {
    const parsed = updateIngestionItemBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}
