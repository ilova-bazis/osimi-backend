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

export const updateIngestionFileOverridesBodySchema = z.strictObject({
    processing_overrides: ingestionFileProcessingOverridesSchema,
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
    processing_overrides: ingestionFileProcessingOverridesSchema,
    error: jsonObjectSchema,
    created_at: z.string(),
    updated_at: z.string(),
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
export type UpdateIngestionFileOverridesBody = z.infer<
    typeof updateIngestionFileOverridesBodySchema
>;
export type AccessLevel = z.infer<typeof accessLevelSchema>;
export type IngestionClassificationType = z.infer<typeof ingestionClassificationTypeSchema>;
export type IngestItemKind = z.infer<typeof ingestItemKindSchema>;
export type IngestionPipelinePreset = z.infer<
    typeof ingestionPipelinePresetSchema
>;
export type IngestionDto = z.infer<typeof ingestionDtoSchema>;
export type IngestionFileDto = z.infer<typeof ingestionFileDtoSchema>;
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

export function parseUpdateIngestionFileOverridesBody(
    value: unknown,
): UpdateIngestionFileOverridesBody {
    const parsed = updateIngestionFileOverridesBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
    }

    return parsed.data;
}
