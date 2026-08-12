import type { UserRole } from "../auth/types.ts";
import { withExecutor, withSchemaClient } from "../db/client.ts";
import type { SqlExecutor } from "../db/client.ts";
import { escapeLikePattern } from "../db/like.ts";
import { toSafeNumberFromDbInt, type DbInt } from "../db/number.ts";
import type { JsonObject } from "../validation/ingestion.ts";

interface ObjectRow {
    object_id: string;
    tenant_id: string;
    type: "GENERIC" | "IMAGE" | "AUDIO" | "VIDEO" | "DOCUMENT";
    title: string;
    language_code: string | null;
    metadata: JsonObject;
    ingest_manifest: JsonObject | null;
    source_ingestion_id: string | null;
    source_ingestion_item_id: string | null;
    source_batch_label: string | null;
    processing_state:
        | "queued"
        | "ingesting"
        | "ingested"
        | "derivatives_running"
        | "derivatives_done"
        | "ocr_running"
        | "ocr_done"
        | "index_running"
        | "index_done"
        | "processing_failed"
        | "processing_skipped";
    curation_state:
        | "needs_review"
        | "review_in_progress"
        | "reviewed"
        | "curation_failed";
    availability_state:
        | "AVAILABLE"
        | "ARCHIVED"
        | "RESTORE_PENDING"
        | "RESTORING"
        | "UNAVAILABLE";
    access_level: "private" | "family" | "public";
    embargo_kind: "none" | "timed" | "curation_state";
    embargo_until: Date | null;
    embargo_curation_state:
        | "needs_review"
        | "review_in_progress"
        | "reviewed"
        | "curation_failed"
        | null;
    rights_note: string | null;
    sensitivity_note: string | null;
    created_at: Date;
    updated_at: Date;
    tags: string[] | null;
}

interface ObjectArtifactRow {
    id: string;
    object_id: string;
    kind: ArtifactKind;
    variant: string | null;
    storage_key: string;
    content_type: string;
    size_bytes: DbInt;
    created_at: Date;
}

interface ObjectArtifactSummaryRow {
    object_id: string;
    thumbnail_artifact_id: string | null;
    has_access_pdf: boolean;
    has_ocr: boolean;
}

export interface ObjectRecord {
    objectId: string;
    tenantId: string;
    type: "GENERIC" | "IMAGE" | "AUDIO" | "VIDEO" | "DOCUMENT";
    title: string;
    languageCode?: string;
    tags: string[];
    metadata: JsonObject;
    ingestManifest: JsonObject | null;
    sourceIngestionId?: string;
    sourceIngestionItemId?: string;
    sourceBatchLabel?: string;
    processingState: ObjectRow["processing_state"];
    curationState: ObjectRow["curation_state"];
    availabilityState: ObjectRow["availability_state"];
    accessLevel: ObjectRow["access_level"];
    embargoKind: ObjectRow["embargo_kind"];
    embargoUntil?: string;
    embargoCurationState?: ObjectRow["curation_state"];
    rightsNote?: string;
    sensitivityNote?: string;
    createdAt: Date;
    updatedAt: Date;
}

export interface ObjectArtifactRecord {
    id: string;
    objectId: string;
    kind: ArtifactKind;
    variant: string | null;
    storageKey: string;
    contentType: string;
    sizeBytes: number;
    createdAt: Date;
}

export interface ObjectArtifactSummary {
    thumbnailArtifactId: string | null;
    hasAccessPdf: boolean;
    hasOcr: boolean;
}

export type ArtifactKind =
    | "ingest_json"
    | "pipeline_json"
    | "catalog_json"
    | "original"
    | "preview"
    | "ocr"
    | "transcript"
    | "metadata"
    | "pdf"
    | "ocr_text"
    | "thumbnail"
    | "web_version"
    | "other";

export interface ListObjectsParams {
    tenantId: string;
    userId: string;
    role: UserRole;
    limit: number;
    sort: ObjectListSort;
    cursorCreatedAt?: string;
    cursorUpdatedAt?: string;
    cursorTitle?: string;
    cursorObjectId?: string;
    type?: ObjectRecord["type"];
    availabilityState?: ObjectRecord["availabilityState"];
    accessLevel?: ObjectRecord["accessLevel"];
    query?: string;
    language?: string;
    batchLabel?: string;
    fromCreatedAt?: string;
    toCreatedAt?: string;
    tag?: string;
}

export type ObjectListSort =
    | "created_at_desc"
    | "created_at_asc"
    | "updated_at_desc"
    | "updated_at_asc"
    | "title_asc"
    | "title_desc";

export interface ListObjectsResult {
    items: ObjectRecord[];
    totalCount: number;
    filteredCount: number;
}

interface CountRow {
    count: number;
}

function normalizeTags(input: unknown): string[] {
    if (!Array.isArray(input)) {
        return [];
    }

    const normalized = input
        .filter((value): value is string => typeof value === "string")
        .map((value) => value.trim().toLowerCase())
        .filter((value) => value.length > 0);

    return [...new Set(normalized)].sort((left, right) =>
        left.localeCompare(right),
    );
}

async function replaceObjectTags(
    sql: SqlExecutor,
    objectId: string,
    tags: string[],
): Promise<void> {
    await sql`
    DELETE FROM object_tags
    WHERE object_id = ${objectId}
  `;

    for (const tag of tags) {
        await sql`
      INSERT INTO tags (
        id,
        name_normalized,
        display_name
      )
      VALUES (
        ${crypto.randomUUID()},
        ${tag},
        ${tag}
      )
      ON CONFLICT (name_normalized)
      DO NOTHING
    `;

        const tagRows = await sql<{ id: string }[]>`
      SELECT id
      FROM tags
      WHERE name_normalized = ${tag}
      LIMIT 1
    `;

        const tagRow = tagRows[0];
        if (!tagRow) {
            continue;
        }

        await sql`
      INSERT INTO object_tags (
        object_id,
        tag_id
      )
      VALUES (
        ${objectId},
        ${tagRow.id}
      )
      ON CONFLICT (object_id, tag_id)
      DO NOTHING
    `;
    }
}

function mapObject(row: ObjectRow): ObjectRecord {
    return {
        objectId: row.object_id,
        tenantId: row.tenant_id,
        type: row.type,
        title: row.title,
        languageCode: row.language_code ?? undefined,
        tags: Array.isArray(row.tags) ? row.tags : [],
        metadata: row.metadata,
        ingestManifest: row.ingest_manifest ?? null,
        sourceIngestionId: row.source_ingestion_id ?? undefined,
        sourceIngestionItemId: row.source_ingestion_item_id ?? undefined,
        sourceBatchLabel: row.source_batch_label ?? undefined,
        processingState: row.processing_state,
        curationState: row.curation_state,
        availabilityState: row.availability_state,
        accessLevel: row.access_level,
        embargoKind: row.embargo_kind ?? "none",
        embargoUntil: row.embargo_until?.toISOString(),
        embargoCurationState: row.embargo_curation_state ?? undefined,
        rightsNote: row.rights_note ?? undefined,
        sensitivityNote: row.sensitivity_note ?? undefined,
        createdAt: new Date(row.created_at),
        updatedAt: new Date(row.updated_at),
    };
}

function mapArtifact(row: ObjectArtifactRow): ObjectArtifactRecord {
    return {
        id: row.id,
        objectId: row.object_id,
        kind: row.kind,
        variant: row.variant,
        storageKey: row.storage_key,
        contentType: row.content_type,
        sizeBytes: toSafeNumberFromDbInt(
            row.size_bytes,
            "object_artifacts.size_bytes",
        ),
        createdAt: new Date(row.created_at),
    };
}

export async function findObjectBySourceIngestion(params: {
    tenantId: string;
    ingestionId: string;
    executor?: SqlExecutor;
}): Promise<ObjectRecord | undefined> {
    const rows = await withExecutor(params.executor, async (sql) => {
        return await sql<ObjectRow[]>`
      SELECT
        obj.object_id,
        obj.tenant_id,
        obj.type,
        obj.title,
        obj.language_code,
        obj.metadata,
        obj.ingest_manifest,
        obj.source_ingestion_id,
        obj.source_ingestion_item_id,
        ing.batch_label AS source_batch_label,
        obj.availability_state,
        obj.access_level,
        obj.embargo_kind,
        obj.processing_state,
        obj.curation_state,
        obj.embargo_until,
        obj.embargo_curation_state,
        obj.rights_note,
        obj.sensitivity_note,
        obj.created_at,
        obj.updated_at,
        COALESCE((
          SELECT array_agg(tag.name_normalized ORDER BY tag.name_normalized)
          FROM object_tags otag
          INNER JOIN tags tag ON tag.id = otag.tag_id
          WHERE otag.object_id = obj.object_id
        ), ARRAY[]::text[]) AS tags
      FROM objects obj
      LEFT JOIN ingestions ing ON ing.id = obj.source_ingestion_id
      WHERE obj.tenant_id = ${params.tenantId}
        AND obj.source_ingestion_id = ${params.ingestionId}
      ORDER BY obj.created_at ASC
      LIMIT 1
    `;
    });

    const row = rows[0];
    return row ? mapObject(row) : undefined;
}

export async function findObjectBySourceIngestionItem(params: {
    tenantId: string;
    ingestionItemId: string;
    executor?: SqlExecutor;
}): Promise<ObjectRecord | undefined> {
    const rows = await withExecutor(params.executor, async (sql) => {
        return await sql<ObjectRow[]>`
      SELECT
        obj.object_id,
        obj.tenant_id,
        obj.type,
        obj.title,
        obj.language_code,
        obj.metadata,
        obj.ingest_manifest,
        obj.source_ingestion_id,
        obj.source_ingestion_item_id,
        ing.batch_label AS source_batch_label,
        obj.availability_state,
        obj.access_level,
        obj.embargo_kind,
        obj.processing_state,
        obj.curation_state,
        obj.embargo_until,
        obj.embargo_curation_state,
        obj.rights_note,
        obj.sensitivity_note,
        obj.created_at,
        obj.updated_at,
        COALESCE((
          SELECT array_agg(tag.name_normalized ORDER BY tag.name_normalized)
          FROM object_tags otag
          INNER JOIN tags tag ON tag.id = otag.tag_id
          WHERE otag.object_id = obj.object_id
        ), ARRAY[]::text[]) AS tags
      FROM objects obj
      LEFT JOIN ingestions ing ON ing.id = obj.source_ingestion_id
      WHERE obj.tenant_id = ${params.tenantId}
        AND obj.source_ingestion_item_id = ${params.ingestionItemId}
      ORDER BY obj.created_at ASC
      LIMIT 1
    `;
    });

    const row = rows[0];
    return row ? mapObject(row) : undefined;
}

export async function createObject(params: {
    objectId: string;
    tenantId: string;
    sourceIngestionId: string;
    sourceIngestionItemId?: string;
    type?: "GENERIC" | "IMAGE" | "AUDIO" | "VIDEO" | "DOCUMENT";
    title?: string;
    languageCode?: string;
    metadata?: JsonObject;
    tags?: string[];
}): Promise<ObjectRecord> {
    return await withSchemaClient(async (sql) => {
        const rows = await sql<ObjectRow[]>`
      INSERT INTO objects (
        object_id,
        tenant_id,
        type,
        title,
        language_code,
        metadata,
        source_ingestion_id,
        source_ingestion_item_id
      )
      VALUES (
        ${params.objectId},
        ${params.tenantId},
        ${params.type ?? "GENERIC"},
        ${params.title ?? ""},
        ${params.languageCode ?? null},
        ${params.metadata ?? {}},
        ${params.sourceIngestionId},
        ${params.sourceIngestionItemId ?? null}
      )
      RETURNING
        object_id,
        tenant_id,
        type,
        title,
        language_code,
        metadata,
        ingest_manifest,
        source_ingestion_id,
        source_ingestion_item_id,
        (SELECT ing.batch_label FROM ingestions ing WHERE ing.id = source_ingestion_id) AS source_batch_label,
        availability_state,
        access_level,
        embargo_kind,
        processing_state,
        curation_state,
        embargo_until,
        embargo_curation_state,
        rights_note,
        sensitivity_note,
        created_at,
        updated_at,
        COALESCE((
          SELECT array_agg(tag.name_normalized ORDER BY tag.name_normalized)
          FROM object_tags otag
          INNER JOIN tags tag ON tag.id = otag.tag_id
          WHERE otag.object_id = ${params.objectId}
        ), ARRAY[]::text[]) AS tags
    `;

        const tags = normalizeTags(params.tags ?? params.metadata?.tags);
        if (tags.length > 0) {
            await replaceObjectTags(sql, params.objectId, tags);
        }

        const mapped = mapObject(rows[0]!);
        return {
            ...mapped,
            tags,
        };
    });
}

export async function createOrGetObjectBySourceIngestion(params: {
    objectId: string;
    tenantId: string;
    sourceIngestionId: string;
    sourceIngestionItemId?: string;
    type?: "GENERIC" | "IMAGE" | "AUDIO" | "VIDEO" | "DOCUMENT";
    title?: string;
    languageCode?: string;
    accessLevel?: ObjectRow["access_level"];
    embargoKind?: ObjectRow["embargo_kind"];
    embargoUntil?: Date;
    rightsNote?: string;
    sensitivityNote?: string;
    metadata?: JsonObject;
    tags?: string[];
    executor?: SqlExecutor;
}): Promise<ObjectRecord> {
    return await withExecutor(params.executor, async (sql) => {
        const insertedRows = await sql<ObjectRow[]>`
      INSERT INTO objects (
        object_id,
        tenant_id,
        type,
        title,
        language_code,
        access_level,
        embargo_kind,
        embargo_until,
        rights_note,
        sensitivity_note,
        metadata,
        source_ingestion_id,
        source_ingestion_item_id
      )
      VALUES (
        ${params.objectId},
        ${params.tenantId},
        ${params.type ?? "GENERIC"},
        ${params.title ?? ""},
        ${params.languageCode ?? null},
        ${params.accessLevel ?? "private"}::object_access_level,
        ${params.embargoKind ?? "none"}::object_embargo_kind,
        ${params.embargoUntil ? params.embargoUntil.toISOString() : null},
        ${params.rightsNote ?? null},
        ${params.sensitivityNote ?? null},
        ${params.metadata ?? {}},
        ${params.sourceIngestionId},
        ${params.sourceIngestionItemId ?? null}
      )
      ON CONFLICT
      DO NOTHING
      RETURNING
        object_id,
        tenant_id,
        type,
        title,
        language_code,
        metadata,
        ingest_manifest,
        source_ingestion_id,
        source_ingestion_item_id,
        (SELECT ing.batch_label FROM ingestions ing WHERE ing.id = source_ingestion_id) AS source_batch_label,
        availability_state,
        access_level,
        embargo_kind,
        processing_state,
        curation_state,
        embargo_until,
        embargo_curation_state,
        rights_note,
        sensitivity_note,
        created_at,
        updated_at,
        ARRAY[]::text[] AS tags
    `;

        const inserted = insertedRows[0];
        if (inserted) {
            const tags = normalizeTags(params.tags ?? params.metadata?.tags);
            if (tags.length > 0) {
                await replaceObjectTags(sql, inserted.object_id, tags);
            }

            return {
                ...mapObject(inserted),
                tags,
            };
        }

        const existingRows = await sql<ObjectRow[]>`
      SELECT
        obj.object_id,
        obj.tenant_id,
        obj.type,
        obj.title,
        obj.language_code,
        obj.metadata,
        obj.ingest_manifest,
        obj.source_ingestion_id,
        obj.source_ingestion_item_id,
        ing.batch_label AS source_batch_label,
        obj.availability_state,
        obj.access_level,
        obj.embargo_kind,
        obj.processing_state,
        obj.curation_state,
        obj.embargo_until,
        obj.embargo_curation_state,
        obj.rights_note,
        obj.sensitivity_note,
        obj.created_at,
        obj.updated_at,
        COALESCE((
          SELECT array_agg(tag.name_normalized ORDER BY tag.name_normalized)
          FROM object_tags otag
          INNER JOIN tags tag ON tag.id = otag.tag_id
          WHERE otag.object_id = obj.object_id
        ), ARRAY[]::text[]) AS tags
      FROM objects obj
      LEFT JOIN ingestions ing ON ing.id = obj.source_ingestion_id
      WHERE obj.tenant_id = ${params.tenantId}
        AND obj.object_id = ${params.objectId}
      LIMIT 1
    `;

        const existingObject = mapObject(existingRows[0]!);

        return existingObject;
    });
}

export async function createObjectArtifact(params: {
    objectId: string;
    kind: ArtifactKind;
    variant?: string | null;
    storageKey: string;
    contentType: string;
    sizeBytes: number;
    executor?: SqlExecutor;
}): Promise<ObjectArtifactRecord> {
    const rows = await withExecutor(params.executor, async (sql) => {
        return await sql<ObjectArtifactRow[]>`
      INSERT INTO object_artifacts (
        id,
        object_id,
        kind,
        variant,
        storage_key,
        content_type,
        size_bytes
      )
      VALUES (
        ${crypto.randomUUID()},
        ${params.objectId},
        ${params.kind},
        ${params.variant ?? null},
        ${params.storageKey},
        ${params.contentType},
        ${params.sizeBytes}
      )
      RETURNING id, object_id, kind, variant, storage_key, content_type, size_bytes, created_at
    `;
    });

    return mapArtifact(rows[0]!);
}

export async function createOrFindObjectArtifactByStorageKey(params: {
    objectId: string;
    kind: ArtifactKind;
    variant?: string | null;
    storageKey: string;
    contentType: string;
    sizeBytes: number;
    executor: SqlExecutor;
}): Promise<ObjectArtifactRecord> {
    const rows = await params.executor<ObjectArtifactRow[]>`
      INSERT INTO object_artifacts (
        id, object_id, kind, variant, storage_key, content_type, size_bytes
      )
      VALUES (
        ${crypto.randomUUID()}, ${params.objectId}, ${params.kind},
        ${params.variant ?? null}, ${params.storageKey}, ${params.contentType},
        ${params.sizeBytes}
      )
      ON CONFLICT (storage_key) DO NOTHING
      RETURNING id, object_id, kind, variant, storage_key, content_type, size_bytes, created_at
    `;
    const inserted = rows[0];
    if (inserted) return mapArtifact(inserted);

    const existing = await findArtifactByStorageKey({
        objectId: params.objectId,
        storageKey: params.storageKey,
        executor: params.executor,
    });
    if (!existing) {
        throw new Error(`Artifact storage conflict could not be resolved for '${params.storageKey}'.`);
    }
    return existing;
}

export async function lockObjectForUpdate(params: {
    tenantId: string;
    objectId: string;
    executor: SqlExecutor;
}): Promise<boolean> {
    const rows = await params.executor<Array<{ object_id: string }>>`
      SELECT object_id
      FROM objects
      WHERE tenant_id = ${params.tenantId}
        AND object_id = ${params.objectId}
      FOR UPDATE
    `;
    return rows.length > 0;
}

export async function findLatestArtifactByKind(params: {
    tenantId: string;
    objectId: string;
    kind: ArtifactKind;
    variant: string | null;
    executor?: SqlExecutor;
}): Promise<ObjectArtifactRecord | undefined> {
    const rows = await withExecutor(params.executor, async (sql) => {
        return await sql<ObjectArtifactRow[]>`
      SELECT art.id, art.object_id, art.kind, art.variant, art.storage_key, art.content_type, art.size_bytes, art.created_at
      FROM object_artifacts art
      INNER JOIN objects obj ON obj.object_id = art.object_id
      WHERE obj.tenant_id = ${params.tenantId}
        AND obj.object_id = ${params.objectId}
        AND art.kind = ${params.kind}::artifact_kind
        AND art.variant IS NOT DISTINCT FROM ${params.variant}
      ORDER BY art.created_at DESC, art.id DESC
      LIMIT 1
    `;
    });

    const row = rows[0];
    return row ? mapArtifact(row) : undefined;
}

export async function listObjects(
    params: ListObjectsParams,
): Promise<ListObjectsResult> {
    return await withSchemaClient(async (sql) => {
        const queryPattern = params.query
            ? `%${escapeLikePattern(params.query)}%`
            : null;
        const likeEscape = "\\";
        const queryTime = new Date();

        const totalRows = await sql<CountRow[]>`
      SELECT COUNT(*)::int AS count
      FROM objects obj
      WHERE obj.tenant_id = ${params.tenantId}
    `;

        const filteredRows = await sql<CountRow[]>`
      SELECT COUNT(*)::int AS count
      FROM objects obj
      LEFT JOIN ingestions ing ON ing.id = obj.source_ingestion_id
      WHERE obj.tenant_id = ${params.tenantId}
        AND (${params.type ?? null}::object_type IS NULL OR obj.type = ${params.type ?? null}::object_type)
        AND (${params.availabilityState ?? null}::object_availability_state IS NULL OR obj.availability_state = ${params.availabilityState ?? null}::object_availability_state)
        AND (${params.accessLevel ?? null}::object_access_level IS NULL OR obj.access_level = ${params.accessLevel ?? null}::object_access_level)
        AND (${params.fromCreatedAt ?? null}::timestamptz IS NULL OR obj.created_at >= ${params.fromCreatedAt ?? null}::timestamptz)
        AND (${params.toCreatedAt ?? null}::timestamptz IS NULL OR obj.created_at <= ${params.toCreatedAt ?? null}::timestamptz)
        AND (${params.language ?? null}::text IS NULL OR lower(obj.language_code) = lower(${params.language ?? null}::text))
        AND (${params.batchLabel ?? null}::text IS NULL OR ing.batch_label ILIKE ${params.batchLabel ? `%${params.batchLabel}%` : null}::text)
        AND (
          ${queryPattern ?? null}::text IS NULL
          OR obj.title ILIKE ${queryPattern ?? null}::text ESCAPE ${likeEscape}
          OR obj.object_id ILIKE ${queryPattern ?? null}::text ESCAPE ${likeEscape}
          OR (
            obj.availability_state = 'AVAILABLE'::object_availability_state
            AND (
              ${params.role}::text = 'admin'
              OR obj.access_level = 'public'::object_access_level
              OR EXISTS (
                SELECT 1
                FROM object_access_assignments asg
                WHERE asg.object_id = obj.object_id
                  AND asg.tenant_id = obj.tenant_id
                  AND asg.user_id = ${params.userId}
                  AND (
                    (obj.access_level = 'family'::object_access_level AND asg.granted_level IN ('family', 'private'))
                    OR (obj.access_level = 'private'::object_access_level AND asg.granted_level = 'private')
                  )
              )
            )
            AND (
              obj.embargo_kind = 'none'::object_embargo_kind
              OR (obj.embargo_kind = 'timed'::object_embargo_kind AND (obj.embargo_until IS NULL OR obj.embargo_until <= ${queryTime}))
              OR (
                obj.embargo_kind = 'curation_state'::object_embargo_kind
                AND (obj.embargo_curation_state IS NULL OR obj.curation_state = obj.embargo_curation_state)
              )
            )
            AND (
              EXISTS (
                SELECT 1
                FROM object_artifacts art
                LEFT JOIN object_artifact_search_documents doc ON doc.artifact_id = art.id
                LEFT JOIN object_available_files file
                  ON file.id = doc.available_file_id
                  AND file.object_id = art.object_id
                  AND file.tenant_id = obj.tenant_id
                WHERE art.object_id = obj.object_id
                  AND (
                    art.id::text ILIKE ${queryPattern ?? null}::text ESCAPE ${likeEscape}
                    OR art.kind::text ILIKE ${queryPattern ?? null}::text ESCAPE ${likeEscape}
                    OR art.variant ILIKE ${queryPattern ?? null}::text ESCAPE ${likeEscape}
                    OR art.content_type ILIKE ${queryPattern ?? null}::text ESCAPE ${likeEscape}
                    OR (
                      art.kind IN ('ocr_text'::artifact_kind, 'transcript'::artifact_kind)
                      AND doc.text_content ILIKE ${queryPattern ?? null}::text ESCAPE ${likeEscape}
                    )
                    OR file.display_name ILIKE ${queryPattern ?? null}::text ESCAPE ${likeEscape}
                    OR file.archive_file_key ILIKE ${queryPattern ?? null}::text ESCAPE ${likeEscape}
                  )
              )
              OR EXISTS (
                SELECT 1
                FROM object_curated_document_pages page
                WHERE page.object_id = obj.object_id
                  AND page.curated_text ILIKE ${queryPattern ?? null}::text ESCAPE ${likeEscape}
              )
            )
          )
        )
        AND (
          (${params.tag ?? null}::text IS NULL)
          OR EXISTS (
            SELECT 1
            FROM object_tags otag
            INNER JOIN tags tag ON tag.id = otag.tag_id
            WHERE otag.object_id = obj.object_id
              AND tag.name_normalized = lower(${params.tag ?? null}::text)
          )
        )
    `;

        const rows = await sql<ObjectRow[]>`
        SELECT
          obj.object_id,
          obj.tenant_id,
          obj.type,
          obj.title,
          obj.language_code,
          obj.metadata,
          obj.ingest_manifest,
          obj.source_ingestion_id,
          obj.source_ingestion_item_id,
          ing.batch_label AS source_batch_label,
          obj.availability_state,
          obj.access_level,
          obj.embargo_kind,
          obj.processing_state,
          obj.curation_state,
          obj.embargo_until,
          obj.embargo_curation_state,
          obj.rights_note,
          obj.sensitivity_note,
          obj.created_at,
          obj.updated_at,
          COALESCE((
            SELECT array_agg(tag.name_normalized ORDER BY tag.name_normalized)
            FROM object_tags otag
            INNER JOIN tags tag ON tag.id = otag.tag_id
            WHERE otag.object_id = obj.object_id
          ), ARRAY[]::text[]) AS tags
        FROM objects obj
        LEFT JOIN ingestions ing ON ing.id = obj.source_ingestion_id
        WHERE obj.tenant_id = ${params.tenantId}
          AND (${params.type ?? null}::object_type IS NULL OR obj.type = ${params.type ?? null}::object_type)
          AND (${params.availabilityState ?? null}::object_availability_state IS NULL OR obj.availability_state = ${params.availabilityState ?? null}::object_availability_state)
        AND (${params.accessLevel ?? null}::object_access_level IS NULL OR obj.access_level = ${params.accessLevel ?? null}::object_access_level)
          AND (${params.fromCreatedAt ?? null}::timestamptz IS NULL OR obj.created_at >= ${params.fromCreatedAt ?? null}::timestamptz)
          AND (${params.toCreatedAt ?? null}::timestamptz IS NULL OR obj.created_at <= ${params.toCreatedAt ?? null}::timestamptz)
          AND (${params.language ?? null}::text IS NULL OR lower(obj.language_code) = lower(${params.language ?? null}::text))
          AND (${params.batchLabel ?? null}::text IS NULL OR ing.batch_label ILIKE ${params.batchLabel ? `%${params.batchLabel}%` : null}::text)
          AND (
            ${queryPattern ?? null}::text IS NULL
            OR obj.title ILIKE ${queryPattern ?? null}::text ESCAPE ${likeEscape}
            OR obj.object_id ILIKE ${queryPattern ?? null}::text ESCAPE ${likeEscape}
            OR (
              obj.availability_state = 'AVAILABLE'::object_availability_state
              AND (
                ${params.role}::text = 'admin'
                OR obj.access_level = 'public'::object_access_level
                OR EXISTS (
                  SELECT 1
                  FROM object_access_assignments asg
                  WHERE asg.object_id = obj.object_id
                    AND asg.tenant_id = obj.tenant_id
                    AND asg.user_id = ${params.userId}
                    AND (
                      (obj.access_level = 'family'::object_access_level AND asg.granted_level IN ('family', 'private'))
                      OR (obj.access_level = 'private'::object_access_level AND asg.granted_level = 'private')
                    )
                )
              )
              AND (
                obj.embargo_kind = 'none'::object_embargo_kind
                OR (obj.embargo_kind = 'timed'::object_embargo_kind AND (obj.embargo_until IS NULL OR obj.embargo_until <= ${queryTime}))
                OR (
                  obj.embargo_kind = 'curation_state'::object_embargo_kind
                  AND (obj.embargo_curation_state IS NULL OR obj.curation_state = obj.embargo_curation_state)
                )
              )
              AND (
                EXISTS (
                  SELECT 1
                  FROM object_artifacts art
                  LEFT JOIN object_artifact_search_documents doc ON doc.artifact_id = art.id
                  LEFT JOIN object_available_files file
                    ON file.id = doc.available_file_id
                    AND file.object_id = art.object_id
                    AND file.tenant_id = obj.tenant_id
                  WHERE art.object_id = obj.object_id
                    AND (
                      art.id::text ILIKE ${queryPattern ?? null}::text ESCAPE ${likeEscape}
                      OR art.kind::text ILIKE ${queryPattern ?? null}::text ESCAPE ${likeEscape}
                      OR art.variant ILIKE ${queryPattern ?? null}::text ESCAPE ${likeEscape}
                      OR art.content_type ILIKE ${queryPattern ?? null}::text ESCAPE ${likeEscape}
                      OR (
                        art.kind IN ('ocr_text'::artifact_kind, 'transcript'::artifact_kind)
                        AND doc.text_content ILIKE ${queryPattern ?? null}::text ESCAPE ${likeEscape}
                      )
                      OR file.display_name ILIKE ${queryPattern ?? null}::text ESCAPE ${likeEscape}
                      OR file.archive_file_key ILIKE ${queryPattern ?? null}::text ESCAPE ${likeEscape}
                    )
                )
                OR EXISTS (
                  SELECT 1
                  FROM object_curated_document_pages page
                  WHERE page.object_id = obj.object_id
                    AND page.curated_text ILIKE ${queryPattern ?? null}::text ESCAPE ${likeEscape}
                )
              )
            )
          )
          AND (
            (${params.tag ?? null}::text IS NULL)
            OR EXISTS (
              SELECT 1
              FROM object_tags otag
              INNER JOIN tags tag ON tag.id = otag.tag_id
              WHERE otag.object_id = obj.object_id
                AND tag.name_normalized = lower(${params.tag ?? null}::text)
            )
          )
          AND CASE ${params.sort}::text
            WHEN 'created_at_desc' THEN
              (${params.cursorCreatedAt ?? null}::timestamptz IS NULL OR ${params.cursorObjectId ?? null}::text IS NULL)
              OR (obj.created_at, obj.object_id) < (${params.cursorCreatedAt ?? null}::timestamptz, ${params.cursorObjectId ?? null}::text)
            WHEN 'created_at_asc' THEN
              (${params.cursorCreatedAt ?? null}::timestamptz IS NULL OR ${params.cursorObjectId ?? null}::text IS NULL)
              OR (obj.created_at, obj.object_id) > (${params.cursorCreatedAt ?? null}::timestamptz, ${params.cursorObjectId ?? null}::text)
            WHEN 'updated_at_desc' THEN
              (${params.cursorUpdatedAt ?? null}::timestamptz IS NULL OR ${params.cursorObjectId ?? null}::text IS NULL)
              OR (obj.updated_at, obj.object_id) < (${params.cursorUpdatedAt ?? null}::timestamptz, ${params.cursorObjectId ?? null}::text)
            WHEN 'updated_at_asc' THEN
              (${params.cursorUpdatedAt ?? null}::timestamptz IS NULL OR ${params.cursorObjectId ?? null}::text IS NULL)
              OR (obj.updated_at, obj.object_id) > (${params.cursorUpdatedAt ?? null}::timestamptz, ${params.cursorObjectId ?? null}::text)
            WHEN 'title_asc' THEN
              (${params.cursorTitle ?? null}::text IS NULL OR ${params.cursorObjectId ?? null}::text IS NULL)
              OR (obj.title, obj.object_id) > (${params.cursorTitle ?? null}::text, ${params.cursorObjectId ?? null}::text)
            ELSE
              (${params.cursorTitle ?? null}::text IS NULL OR ${params.cursorObjectId ?? null}::text IS NULL)
              OR (obj.title, obj.object_id) < (${params.cursorTitle ?? null}::text, ${params.cursorObjectId ?? null}::text)
          END
        ORDER BY
          CASE WHEN ${params.sort}::text = 'created_at_desc' THEN obj.created_at END DESC,
          CASE WHEN ${params.sort}::text = 'created_at_desc' THEN obj.object_id END DESC,
          CASE WHEN ${params.sort}::text = 'created_at_asc' THEN obj.created_at END ASC,
          CASE WHEN ${params.sort}::text = 'created_at_asc' THEN obj.object_id END ASC,
          CASE WHEN ${params.sort}::text = 'updated_at_desc' THEN obj.updated_at END DESC,
          CASE WHEN ${params.sort}::text = 'updated_at_desc' THEN obj.object_id END DESC,
          CASE WHEN ${params.sort}::text = 'updated_at_asc' THEN obj.updated_at END ASC,
          CASE WHEN ${params.sort}::text = 'updated_at_asc' THEN obj.object_id END ASC,
          CASE WHEN ${params.sort}::text = 'title_asc' THEN obj.title END ASC,
          CASE WHEN ${params.sort}::text = 'title_asc' THEN obj.object_id END ASC,
          CASE WHEN ${params.sort}::text = 'title_desc' THEN obj.title END DESC,
          CASE WHEN ${params.sort}::text = 'title_desc' THEN obj.object_id END DESC
        LIMIT ${params.limit}
      `;

        return {
            items: rows.map(mapObject),
            totalCount: totalRows[0]?.count ?? 0,
            filteredCount: filteredRows[0]?.count ?? 0,
        };
    });
}

export async function findObjectById(params: {
    tenantId: string;
    objectId: string;
    executor?: SqlExecutor;
}): Promise<ObjectRecord | undefined> {
    const rows = await withExecutor(params.executor, async (sql) => {
        return await sql<ObjectRow[]>`
      SELECT
        obj.object_id,
        obj.tenant_id,
        obj.type,
        obj.title,
        obj.language_code,
        obj.metadata,
        obj.ingest_manifest,
        obj.source_ingestion_id,
        obj.source_ingestion_item_id,
        ing.batch_label AS source_batch_label,
        obj.availability_state,
        obj.access_level,
        obj.embargo_kind,
        obj.processing_state,
        obj.curation_state,
        obj.embargo_until,
        obj.embargo_curation_state,
        obj.rights_note,
        obj.sensitivity_note,
        obj.created_at,
        obj.updated_at,
        COALESCE((
          SELECT array_agg(tag.name_normalized ORDER BY tag.name_normalized)
          FROM object_tags otag
          INNER JOIN tags tag ON tag.id = otag.tag_id
          WHERE otag.object_id = obj.object_id
        ), ARRAY[]::text[]) AS tags
      FROM objects obj
      LEFT JOIN ingestions ing ON ing.id = obj.source_ingestion_id
      WHERE obj.tenant_id = ${params.tenantId}
        AND obj.object_id = ${params.objectId}
      LIMIT 1
    `;
    });

    const row = rows[0];
    return row ? mapObject(row) : undefined;
}

export async function findObjectByIdUnscoped(params: {
    objectId: string;
}): Promise<ObjectRecord | undefined> {
    const rows = await withSchemaClient(async (sql) => {
        return await sql<ObjectRow[]>`
      SELECT
        obj.object_id,
        obj.tenant_id,
        obj.type,
        obj.title,
        obj.language_code,
        obj.metadata,
        obj.ingest_manifest,
        obj.source_ingestion_id,
        obj.source_ingestion_item_id,
        ing.batch_label AS source_batch_label,
        obj.availability_state,
        obj.access_level,
        obj.embargo_kind,
        obj.processing_state,
        obj.curation_state,
        obj.embargo_until,
        obj.embargo_curation_state,
        obj.rights_note,
        obj.sensitivity_note,
        obj.created_at,
        obj.updated_at,
        COALESCE((
          SELECT array_agg(tag.name_normalized ORDER BY tag.name_normalized)
          FROM object_tags otag
          INNER JOIN tags tag ON tag.id = otag.tag_id
          WHERE otag.object_id = obj.object_id
        ), ARRAY[]::text[]) AS tags
      FROM objects obj
      LEFT JOIN ingestions ing ON ing.id = obj.source_ingestion_id
      WHERE obj.object_id = ${params.objectId}
      LIMIT 1
    `;
    });

    const row = rows[0];
    return row ? mapObject(row) : undefined;
}

export async function updateObjectIngestManifest(params: {
    tenantId: string;
    objectId: string;
    ingestManifest: JsonObject;
    executor?: SqlExecutor;
}): Promise<ObjectRecord | undefined> {
    const rows = await withExecutor(params.executor, async (sql) => {
        return await sql<ObjectRow[]>`
      UPDATE objects
      SET ingest_manifest = ${params.ingestManifest},
          updated_at = now()
      WHERE tenant_id = ${params.tenantId}
        AND object_id = ${params.objectId}
      RETURNING
        object_id,
        tenant_id,
        type,
        title,
        language_code,
        metadata,
        ingest_manifest,
        source_ingestion_id,
        source_ingestion_item_id,
        (SELECT ing.batch_label FROM ingestions ing WHERE ing.id = source_ingestion_id) AS source_batch_label,
        availability_state,
        access_level,
        embargo_kind,
        processing_state,
        curation_state,
        embargo_until,
        embargo_curation_state,
        rights_note,
        sensitivity_note,
        created_at,
        updated_at,
        COALESCE((
          SELECT array_agg(tag.name_normalized ORDER BY tag.name_normalized)
          FROM object_tags otag
          INNER JOIN tags tag ON tag.id = otag.tag_id
          WHERE otag.object_id = ${params.objectId}
        ), ARRAY[]::text[]) AS tags
    `;
    });

    const row = rows[0];
    return row ? mapObject(row) : undefined;
}

export async function updateObjectProjectionState(params: {
    tenantId: string;
    objectId: string;
    processingState?: ObjectRecord["processingState"];
    curationState?: ObjectRecord["curationState"];
    availabilityState?: ObjectRecord["availabilityState"];
    accessLevel?: ObjectRecord["accessLevel"];
    embargoKind?: ObjectRecord["embargoKind"];
    embargoUntil?: string | null;
    embargoCurationState?: ObjectRecord["curationState"] | null;
    rightsNote?: string | null;
    sensitivityNote?: string | null;
    executor?: SqlExecutor;
}): Promise<ObjectRecord | undefined> {
    const rows = await withExecutor(params.executor, async (sql) => {
        const embargoKind = params.embargoKind ?? null;
        const embargoUntil =
            embargoKind === "timed" ? (params.embargoUntil ?? null) : null;
        const embargoCurationState =
            embargoKind === "curation_state"
                ? (params.embargoCurationState ?? null)
                : null;

        return await sql<ObjectRow[]>`
      UPDATE objects
      SET
        processing_state = COALESCE(${params.processingState ?? null}::object_processing_state, processing_state),
        curation_state = COALESCE(${params.curationState ?? null}::object_curation_state, curation_state),
        availability_state = COALESCE(${params.availabilityState ?? null}::object_availability_state, availability_state),
        access_level = COALESCE(${params.accessLevel ?? null}::object_access_level, access_level),
        embargo_kind = COALESCE(${embargoKind}::object_embargo_kind, embargo_kind),
        embargo_until = CASE
          WHEN ${embargoKind}::object_embargo_kind IS NULL THEN embargo_until
          ELSE ${embargoUntil}::timestamptz
        END,
        embargo_curation_state = CASE
          WHEN ${embargoKind}::object_embargo_kind IS NULL THEN embargo_curation_state
          ELSE ${embargoCurationState}::object_curation_state
        END,
        rights_note = COALESCE(${params.rightsNote ?? null}::text, rights_note),
        sensitivity_note = COALESCE(${params.sensitivityNote ?? null}::text, sensitivity_note),
        updated_at = now()
      WHERE tenant_id = ${params.tenantId}
        AND object_id = ${params.objectId}
      RETURNING
        object_id,
        tenant_id,
        type,
        title,
        language_code,
        metadata,
        ingest_manifest,
        source_ingestion_id,
        source_ingestion_item_id,
        (SELECT ing.batch_label FROM ingestions ing WHERE ing.id = source_ingestion_id) AS source_batch_label,
        availability_state,
        access_level,
        embargo_kind,
        processing_state,
        curation_state,
        embargo_until,
        embargo_curation_state,
        rights_note,
        sensitivity_note,
        created_at,
        updated_at,
        COALESCE((
          SELECT array_agg(tag.name_normalized ORDER BY tag.name_normalized)
          FROM object_tags otag
          INNER JOIN tags tag ON tag.id = otag.tag_id
          WHERE otag.object_id = ${params.objectId}
        ), ARRAY[]::text[]) AS tags
    `;
    });

    const row = rows[0];
    return row ? mapObject(row) : undefined;
}

export async function updateObjectAccessPolicy(params: {
    tenantId: string;
    objectId: string;
    accessLevel: ObjectRecord["accessLevel"];
    embargoKind: ObjectRecord["embargoKind"];
    embargoUntil?: string | null;
    embargoCurationState?: ObjectRecord["curationState"] | null;
    rightsNote?: string | null;
    sensitivityNote?: string | null;
}): Promise<ObjectRecord | undefined> {
    const embargoUntil =
        params.embargoKind === "timed" ? (params.embargoUntil ?? null) : null;
    const embargoCurationState =
        params.embargoKind === "curation_state"
            ? (params.embargoCurationState ?? null)
            : null;

    const rows = await withSchemaClient(async (sql) => {
        return await sql<ObjectRow[]>`
      UPDATE objects
      SET access_level = ${params.accessLevel}::object_access_level,
          embargo_kind = ${params.embargoKind}::object_embargo_kind,
          embargo_until = ${embargoUntil}::timestamptz,
          embargo_curation_state = ${embargoCurationState}::object_curation_state,
          rights_note = ${params.rightsNote ?? null}::text,
          sensitivity_note = ${params.sensitivityNote ?? null}::text,
          updated_at = now()
      WHERE tenant_id = ${params.tenantId}
        AND object_id = ${params.objectId}
      RETURNING
        object_id,
        tenant_id,
        type,
        title,
        language_code,
        metadata,
        ingest_manifest,
        source_ingestion_id,
        source_ingestion_item_id,
        (SELECT ing.batch_label FROM ingestions ing WHERE ing.id = source_ingestion_id) AS source_batch_label,
        availability_state,
        access_level,
        embargo_kind,
        processing_state,
        curation_state,
        embargo_until,
        embargo_curation_state,
        rights_note,
        sensitivity_note,
        created_at,
        updated_at,
        COALESCE((
          SELECT array_agg(tag.name_normalized ORDER BY tag.name_normalized)
          FROM object_tags otag
          INNER JOIN tags tag ON tag.id = otag.tag_id
          WHERE otag.object_id = ${params.objectId}
        ), ARRAY[]::text[]) AS tags
    `;
    });

    const row = rows[0];
    return row ? mapObject(row) : undefined;
}

export async function updateObjectMetadataPages(params: {
    tenantId: string;
    objectId: string;
    pages: Array<{
        page_number: number;
        label: string | null;
        image_artifact_id?: string | null;
        ocr_text_artifact_id?: string | null;
    }>;
    executor?: SqlExecutor;
}): Promise<ObjectRecord | undefined> {
    const rows = await withExecutor(params.executor, async (sql) => {
        return await sql<ObjectRow[]>`
      UPDATE objects
      SET metadata = metadata || ${{
          pages: params.pages,
          page_count: params.pages.length,
      }},
          updated_at = now()
      WHERE tenant_id = ${params.tenantId}
        AND object_id = ${params.objectId}
      RETURNING
        object_id,
        tenant_id,
        type,
        title,
        language_code,
        metadata,
        ingest_manifest,
        source_ingestion_id,
        source_ingestion_item_id,
        (SELECT ing.batch_label FROM ingestions ing WHERE ing.id = source_ingestion_id) AS source_batch_label,
        availability_state,
        access_level,
        embargo_kind,
        processing_state,
        curation_state,
        embargo_until,
        embargo_curation_state,
        rights_note,
        sensitivity_note,
        created_at,
        updated_at,
        COALESCE((
          SELECT array_agg(tag.name_normalized ORDER BY tag.name_normalized)
          FROM object_tags otag
          INNER JOIN tags tag ON tag.id = otag.tag_id
          WHERE otag.object_id = ${params.objectId}
        ), ARRAY[]::text[]) AS tags
    `;
    });

    const row = rows[0];
    return row ? mapObject(row) : undefined;
}

export async function listArtifactsByObjectId(params: {
    tenantId: string;
    objectId: string;
}): Promise<ObjectArtifactRecord[]> {
    const rows = await withSchemaClient(async (sql) => {
        return await sql<ObjectArtifactRow[]>`
      SELECT art.id, art.object_id, art.kind, art.variant, art.storage_key, art.content_type, art.size_bytes, art.created_at
      FROM object_artifacts art
      INNER JOIN objects obj ON obj.object_id = art.object_id
      WHERE obj.tenant_id = ${params.tenantId}
        AND obj.object_id = ${params.objectId}
      ORDER BY art.created_at ASC, art.id ASC
    `;
    });

    return rows.map(mapArtifact);
}

export async function findPreferredThumbnailArtifactIdByObjectId(params: {
    tenantId: string;
    objectId: string;
}): Promise<string | undefined> {
    const rows = await withSchemaClient(async (sql) => {
        return await sql<{ artifact_id: string }[]>`
      SELECT art.id AS artifact_id
      FROM object_artifacts art
      INNER JOIN objects obj ON obj.object_id = art.object_id
      WHERE obj.tenant_id = ${params.tenantId}
        AND obj.object_id = ${params.objectId}
        AND art.kind = ${"thumbnail"}::artifact_kind
      ORDER BY
        CASE WHEN art.variant IS NULL THEN 0 ELSE 1 END ASC,
        art.created_at DESC,
        art.id DESC
      LIMIT 1
    `;
    });

    return rows[0]?.artifact_id;
}

export async function listObjectArtifactSummariesByObjectIds(params: {
    tenantId: string;
    objectIds: string[];
}): Promise<Map<string, ObjectArtifactSummary>> {
    if (params.objectIds.length === 0) {
        return new Map();
    }

    const rows = await withSchemaClient(async (sql) => {
        return await sql<ObjectArtifactSummaryRow[]>`
      WITH artifacts AS (
        SELECT art.object_id, art.id, art.kind, art.variant, art.created_at
        FROM object_artifacts art
        INNER JOIN objects obj ON obj.object_id = art.object_id
        WHERE obj.tenant_id = ${params.tenantId}
          AND art.object_id IN ${sql(params.objectIds)}
          AND art.kind IN (
            ${"thumbnail"}::artifact_kind,
            ${"pdf"}::artifact_kind,
            ${"ocr_text"}::artifact_kind
          )
      ),
      preferred_thumbnails AS (
        SELECT DISTINCT ON (object_id) object_id, id AS thumbnail_artifact_id
        FROM artifacts
        WHERE kind = ${"thumbnail"}::artifact_kind
        ORDER BY
          object_id ASC,
          CASE WHEN variant IS NULL THEN 0 ELSE 1 END ASC,
          created_at DESC,
          id DESC
      )
      SELECT
        art.object_id,
        thumbnail.thumbnail_artifact_id,
        BOOL_OR(art.kind = ${"pdf"}::artifact_kind) AS has_access_pdf,
        BOOL_OR(art.kind = ${"ocr_text"}::artifact_kind) AS has_ocr
      FROM artifacts art
      LEFT JOIN preferred_thumbnails thumbnail ON thumbnail.object_id = art.object_id
      GROUP BY art.object_id, thumbnail.thumbnail_artifact_id
    `;
    });

    return new Map(
        rows.map((row) => [
            row.object_id,
            {
                thumbnailArtifactId: row.thumbnail_artifact_id,
                hasAccessPdf: row.has_access_pdf,
                hasOcr: row.has_ocr,
            },
        ]),
    );
}

export async function findArtifactById(params: {
    tenantId: string;
    objectId: string;
    artifactId: string;
}): Promise<ObjectArtifactRecord | undefined> {
    const rows = await withSchemaClient(async (sql) => {
        return await sql<ObjectArtifactRow[]>`
      SELECT art.id, art.object_id, art.kind, art.variant, art.storage_key, art.content_type, art.size_bytes, art.created_at
      FROM object_artifacts art
      INNER JOIN objects obj ON obj.object_id = art.object_id
      WHERE obj.tenant_id = ${params.tenantId}
        AND obj.object_id = ${params.objectId}
        AND art.id = ${params.artifactId}
      LIMIT 1
    `;
    });

    const row = rows[0];
    return row ? mapArtifact(row) : undefined;
}

export async function findArtifactByStorageKey(params: {
    objectId: string;
    storageKey: string;
    executor?: SqlExecutor;
}): Promise<ObjectArtifactRecord | undefined> {
    const rows = await withExecutor(params.executor, async (sql) => {
        return await sql<ObjectArtifactRow[]>`
      SELECT id, object_id, kind, variant, storage_key, content_type, size_bytes, created_at
      FROM object_artifacts
      WHERE object_id = ${params.objectId}
        AND storage_key = ${params.storageKey}
      LIMIT 1
    `;
    });

    const row = rows[0];
    return row ? mapArtifact(row) : undefined;
}
