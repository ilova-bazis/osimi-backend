import { withSchemaClient } from "../db/client.ts";
import type { DbClient } from "../db/client.ts";

interface ObjectRow {
  object_id: string;
  tenant_id: string;
  type: "GENERIC" | "IMAGE" | "AUDIO" | "VIDEO" | "DOCUMENT";
  title: string;
  language_code: string | null;
  metadata: unknown;
  ingest_manifest: unknown;
  source_ingestion_id: string | null;
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
  embargo_curation_state: "needs_review" | "review_in_progress" | "reviewed" | "curation_failed" | null;
  rights_note: string | null;
  sensitivity_note: string | null;
  created_at: Date;
  updated_at: Date;
  tags: string[] | null;
}

interface ObjectArtifactRow {
  id: string;
  object_id: string;
  kind: string;
  storage_key: string;
  content_type: string;
  size_bytes: number;
  created_at: Date;
}

export interface ObjectRecord {
  objectId: string;
  tenantId: string;
  type: "GENERIC" | "IMAGE" | "AUDIO" | "VIDEO" | "DOCUMENT";
  title: string;
  languageCode?: string;
  tags: string[];
  metadata: unknown;
  ingestManifest?: Record<string, unknown>;
  sourceIngestionId?: string;
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
  kind: string;
  storageKey: string;
  contentType: string;
  sizeBytes: number;
  createdAt: Date;
}

export interface ListObjectsParams {
  tenantId: string;
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
  sql: DbClient,
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
    ingestManifest:
      row.ingest_manifest &&
      typeof row.ingest_manifest === "object" &&
      !Array.isArray(row.ingest_manifest)
        ? (row.ingest_manifest as Record<string, unknown>)
        : undefined,
    sourceIngestionId: row.source_ingestion_id ?? undefined,
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
    storageKey: row.storage_key,
    contentType: row.content_type,
    sizeBytes: Number(row.size_bytes),
    createdAt: new Date(row.created_at),
  };
}

export async function findObjectBySourceIngestion(params: {
  tenantId: string;
  ingestionId: string;
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

export async function createObject(params: {
  objectId: string;
  tenantId: string;
  sourceIngestionId: string;
  type?: "GENERIC" | "IMAGE" | "AUDIO" | "VIDEO" | "DOCUMENT";
  title?: string;
  languageCode?: string;
  metadata?: Record<string, unknown>;
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
        source_ingestion_id
      )
      VALUES (
        ${params.objectId},
        ${params.tenantId},
        ${params.type ?? "GENERIC"},
        ${params.title ?? ""},
        ${params.languageCode ?? null},
        ${params.metadata ?? {}},
        ${params.sourceIngestionId}
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
  type?: "GENERIC" | "IMAGE" | "AUDIO" | "VIDEO" | "DOCUMENT";
  title?: string;
  languageCode?: string;
  metadata?: Record<string, unknown>;
  tags?: string[];
}): Promise<ObjectRecord> {
  return await withSchemaClient(async (sql) => {
    const insertedRows = await sql<ObjectRow[]>`
      INSERT INTO objects (
        object_id,
        tenant_id,
        type,
        title,
        language_code,
        metadata,
        source_ingestion_id
      )
      VALUES (
        ${params.objectId},
        ${params.tenantId},
        ${params.type ?? "GENERIC"},
        ${params.title ?? ""},
        ${params.languageCode ?? null},
        ${params.metadata ?? {}},
        ${params.sourceIngestionId}
      )
      ON CONFLICT (source_ingestion_id)
      WHERE source_ingestion_id IS NOT NULL
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
        AND obj.source_ingestion_id = ${params.sourceIngestionId}
      LIMIT 1
    `;

    const tags = normalizeTags(params.tags ?? params.metadata?.tags);
    const existingObject = mapObject(existingRows[0]!);

    if (tags.length > 0) {
      await replaceObjectTags(sql, existingObject.objectId, tags);
      return {
        ...existingObject,
        tags,
      };
    }

    return existingObject;
  });
}

export async function createObjectArtifact(params: {
  objectId: string;
  kind:
    | "ingest_json"
    | "original"
    | "preview"
    | "ocr"
    | "transcript"
    | "metadata"
    | "other";
  storageKey: string;
  contentType: string;
  sizeBytes: number;
}): Promise<ObjectArtifactRecord> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<ObjectArtifactRow[]>`
      INSERT INTO object_artifacts (
        id,
        object_id,
        kind,
        storage_key,
        content_type,
        size_bytes
      )
      VALUES (
        ${crypto.randomUUID()},
        ${params.objectId},
        ${params.kind},
        ${params.storageKey},
        ${params.contentType},
        ${params.sizeBytes}
      )
      RETURNING id, object_id, kind, storage_key, content_type, size_bytes, created_at
    `;
  });

  return mapArtifact(rows[0]!);
}

export async function listObjects(
  params: ListObjectsParams,
): Promise<ListObjectsResult> {
  return await withSchemaClient(async (sql) => {
    const queryPattern = params.query ? `%${params.query}%` : null;

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
        AND (${queryPattern ?? null}::text IS NULL OR obj.title ILIKE ${queryPattern ?? null}::text OR obj.object_id ILIKE ${queryPattern ?? null}::text)
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

    let rows: ObjectRow[];

    if (params.sort === "created_at_desc") {
      rows = await sql<ObjectRow[]>`
        SELECT
          obj.object_id,
          obj.tenant_id,
          obj.type,
          obj.title,
          obj.language_code,
          obj.metadata,
          obj.ingest_manifest,
          obj.source_ingestion_id,
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
          AND (${queryPattern ?? null}::text IS NULL OR obj.title ILIKE ${queryPattern ?? null}::text OR obj.object_id ILIKE ${queryPattern ?? null}::text)
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
          AND (
            (${params.cursorCreatedAt ?? null}::timestamptz IS NULL OR ${params.cursorObjectId ?? null}::text IS NULL)
            OR (obj.created_at, obj.object_id) < (${params.cursorCreatedAt ?? null}::timestamptz, ${params.cursorObjectId ?? null}::text)
          )
        ORDER BY obj.created_at DESC, obj.object_id DESC
        LIMIT ${params.limit}
      `;
    } else if (params.sort === "created_at_asc") {
      rows = await sql<ObjectRow[]>`
        SELECT
          obj.object_id,
          obj.tenant_id,
          obj.type,
          obj.title,
          obj.language_code,
          obj.metadata,
          obj.ingest_manifest,
          obj.source_ingestion_id,
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
          AND (${queryPattern ?? null}::text IS NULL OR obj.title ILIKE ${queryPattern ?? null}::text OR obj.object_id ILIKE ${queryPattern ?? null}::text)
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
          AND (
            (${params.cursorCreatedAt ?? null}::timestamptz IS NULL OR ${params.cursorObjectId ?? null}::text IS NULL)
            OR (obj.created_at, obj.object_id) > (${params.cursorCreatedAt ?? null}::timestamptz, ${params.cursorObjectId ?? null}::text)
          )
        ORDER BY obj.created_at ASC, obj.object_id ASC
        LIMIT ${params.limit}
      `;
    } else if (params.sort === "updated_at_desc") {
      rows = await sql<ObjectRow[]>`
        SELECT
          obj.object_id,
          obj.tenant_id,
          obj.type,
          obj.title,
          obj.language_code,
          obj.metadata,
          obj.ingest_manifest,
          obj.source_ingestion_id,
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
          AND (${queryPattern ?? null}::text IS NULL OR obj.title ILIKE ${queryPattern ?? null}::text OR obj.object_id ILIKE ${queryPattern ?? null}::text)
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
          AND (
            (${params.cursorUpdatedAt ?? null}::timestamptz IS NULL OR ${params.cursorObjectId ?? null}::text IS NULL)
            OR (obj.updated_at, obj.object_id) < (${params.cursorUpdatedAt ?? null}::timestamptz, ${params.cursorObjectId ?? null}::text)
          )
        ORDER BY obj.updated_at DESC, obj.object_id DESC
        LIMIT ${params.limit}
      `;
    } else if (params.sort === "updated_at_asc") {
      rows = await sql<ObjectRow[]>`
        SELECT
          obj.object_id,
          obj.tenant_id,
          obj.type,
          obj.title,
          obj.language_code,
          obj.metadata,
          obj.ingest_manifest,
          obj.source_ingestion_id,
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
          AND (${queryPattern ?? null}::text IS NULL OR obj.title ILIKE ${queryPattern ?? null}::text OR obj.object_id ILIKE ${queryPattern ?? null}::text)
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
          AND (
            (${params.cursorUpdatedAt ?? null}::timestamptz IS NULL OR ${params.cursorObjectId ?? null}::text IS NULL)
            OR (obj.updated_at, obj.object_id) > (${params.cursorUpdatedAt ?? null}::timestamptz, ${params.cursorObjectId ?? null}::text)
          )
        ORDER BY obj.updated_at ASC, obj.object_id ASC
        LIMIT ${params.limit}
      `;
    } else if (params.sort === "title_asc") {
      rows = await sql<ObjectRow[]>`
        SELECT
          obj.object_id,
          obj.tenant_id,
          obj.type,
          obj.title,
          obj.language_code,
          obj.metadata,
          obj.ingest_manifest,
          obj.source_ingestion_id,
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
          AND (${queryPattern ?? null}::text IS NULL OR obj.title ILIKE ${queryPattern ?? null}::text OR obj.object_id ILIKE ${queryPattern ?? null}::text)
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
          AND (
            (${params.cursorTitle ?? null}::text IS NULL OR ${params.cursorObjectId ?? null}::text IS NULL)
            OR (obj.title, obj.object_id) > (${params.cursorTitle ?? null}::text, ${params.cursorObjectId ?? null}::text)
          )
        ORDER BY obj.title ASC, obj.object_id ASC
        LIMIT ${params.limit}
      `;
    } else {
      rows = await sql<ObjectRow[]>`
        SELECT
          obj.object_id,
          obj.tenant_id,
          obj.type,
          obj.title,
          obj.language_code,
          obj.metadata,
          obj.ingest_manifest,
          obj.source_ingestion_id,
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
          AND (${queryPattern ?? null}::text IS NULL OR obj.title ILIKE ${queryPattern ?? null}::text OR obj.object_id ILIKE ${queryPattern ?? null}::text)
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
          AND (
            (${params.cursorTitle ?? null}::text IS NULL OR ${params.cursorObjectId ?? null}::text IS NULL)
            OR (obj.title, obj.object_id) < (${params.cursorTitle ?? null}::text, ${params.cursorObjectId ?? null}::text)
          )
        ORDER BY obj.title DESC, obj.object_id DESC
        LIMIT ${params.limit}
      `;
    }

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

export async function updateObjectTitle(params: {
  tenantId: string;
  objectId: string;
  title: string;
}): Promise<ObjectRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<ObjectRow[]>`
      UPDATE objects
      SET title = ${params.title},
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

export async function updateObjectIngestManifest(params: {
  tenantId: string;
  objectId: string;
  ingestManifest: Record<string, unknown>;
}): Promise<ObjectRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
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
}): Promise<ObjectRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    const embargoKind = params.embargoKind ?? null;
    const embargoUntil =
      embargoKind === "timed" ? params.embargoUntil ?? null : null;
    const embargoCurationState =
      embargoKind === "curation_state"
        ? params.embargoCurationState ?? null
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
    params.embargoKind === "timed" ? params.embargoUntil ?? null : null;
  const embargoCurationState =
    params.embargoKind === "curation_state"
      ? params.embargoCurationState ?? null
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
      SELECT art.id, art.object_id, art.kind, art.storage_key, art.content_type, art.size_bytes, art.created_at
      FROM object_artifacts art
      INNER JOIN objects obj ON obj.object_id = art.object_id
      WHERE obj.tenant_id = ${params.tenantId}
        AND obj.object_id = ${params.objectId}
      ORDER BY art.created_at ASC, art.id ASC
    `;
  });

  return rows.map(mapArtifact);
}

export async function findArtifactById(params: {
  tenantId: string;
  objectId: string;
  artifactId: string;
}): Promise<ObjectArtifactRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<ObjectArtifactRow[]>`
      SELECT art.id, art.object_id, art.kind, art.storage_key, art.content_type, art.size_bytes, art.created_at
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
}): Promise<ObjectArtifactRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<ObjectArtifactRow[]>`
      SELECT id, object_id, kind, storage_key, content_type, size_bytes, created_at
      FROM object_artifacts
      WHERE object_id = ${params.objectId}
        AND storage_key = ${params.storageKey}
      LIMIT 1
    `;
  });

  const row = rows[0];
  return row ? mapArtifact(row) : undefined;
}
