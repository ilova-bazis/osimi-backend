import { decodeCursor } from "../http/pagination.ts";
import { z } from "zod";

import type { JsonObject } from "./ingestion.ts";
import { jsonObjectSchema } from "./ingestion.ts";
import { mapZodErrorToValidation } from "./zod-errors.ts";

const OBJECT_ID_PATTERN = /^OBJ-[0-9]{8}-[A-Z0-9]+$/;

export const objectIdParamSchema = z.string().regex(OBJECT_ID_PATTERN, {
    message: "object_id must match format OBJ-YYYYMMDD-XXXXXX.",
});
export const artifactIdParamSchema = z.uuid();
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

export const requestedArtifactKindSchema = z.enum([
    "original",
    "pdf",
    "ocr_text",
    "thumbnail",
    "transcript",
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

export const patchObjectTitleBodySchema = z.strictObject({
    title: z.string().trim().min(1),
});

export const createObjectDownloadRequestBodySchema = z.strictObject({
    artifact_kind: requestedArtifactKindSchema,
});

export const updateAccessPolicyBodySchema = z.strictObject({
    access_level: accessLevelSchema,
    embargo_kind: embargoKindSchema,
    embargo_until: z.iso.datetime({ offset: true }).nullable().optional(),
    embargo_curation_state: curationStateSchema.nullable().optional(),
    rights_note: z.string().trim().min(1).nullable().optional(),
    sensitivity_note: z.string().trim().min(1).nullable().optional(),
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

export const objectDetailResponseSchema = z.object({
    object: objectDtoSchema.extend({
        ingest_manifest: jsonObjectSchema.nullable(),
        is_authorized: z.boolean(),
        is_deliverable: z.boolean(),
        can_download: z.boolean(),
        access_reason_code: objectListItemSchema.shape.access_reason_code,
    }),
});

export const objectArtifactSchema = z.object({
    id: z.string(),
    object_id: objectIdParamSchema,
    kind: artifactKindSchema,
    storage_key: z.string(),
    content_type: z.string(),
    size_bytes: z.number(),
    created_at: z.string(),
});

const objectDownloadRequestSchema = z.object({
    id: z.string(),
    object_id: objectIdParamSchema,
    requested_by: z.string(),
    artifact_kind: requestedArtifactKindSchema,
    status: objectDownloadRequestStatusSchema,
    failure_reason: z.string().nullable(),
    created_at: z.string(),
    updated_at: z.string(),
    completed_at: z.string().nullable(),
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
export type ObjectCursorPayload = z.infer<typeof objectCursorPayloadSchema>;
export type PatchObjectTitleBody = z.infer<typeof patchObjectTitleBodySchema>;
export type CreateObjectDownloadRequestBody = z.infer<
    typeof createObjectDownloadRequestBodySchema
>;
export type RequestedArtifactKind = z.infer<typeof requestedArtifactKindSchema>;
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
export type ObjectArtifactsResponse = z.infer<
    typeof objectArtifactsResponseSchema
>;
export type PatchObjectTitleResponse = z.infer<
    typeof patchObjectTitleResponseSchema
>;
export type CreateObjectDownloadRequestResponse = z.infer<
    typeof createObjectDownloadRequestResponseSchema
>;
export type ObjectArtifactDto = z.infer<typeof objectArtifactSchema>;
export type ObjectDownloadRequestDto = z.infer<
    typeof objectDownloadRequestSchema
>;
export type ListObjectDownloadRequestsResponse = z.infer<
    typeof listObjectDownloadRequestsResponseSchema
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

export function parsePatchObjectTitleBody(
    value: unknown,
): PatchObjectTitleBody {
    const parsed = patchObjectTitleBodySchema.safeParse(value);
    if (!parsed.success) {
        throw mapZodErrorToValidation(parsed.error);
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
