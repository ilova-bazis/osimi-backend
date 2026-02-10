import { db, qualifiedTableName } from "../db/runtime.ts";

interface ObjectRow {
  object_id: string;
  tenant_id: string;
  type: "GENERIC" | "IMAGE" | "AUDIO" | "VIDEO" | "DOCUMENT";
  title: string;
  metadata: unknown;
  source_ingestion_id: string | null;
  status: "ACTIVE" | "ARCHIVED" | "DELETED";
  created_at: Date;
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
  metadata: unknown;
  sourceIngestionId?: string;
  status: "ACTIVE" | "ARCHIVED" | "DELETED";
  createdAt: Date;
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
  cursorCreatedAt?: string;
  cursorObjectId?: string;
  type?: ObjectRecord["type"];
  fromCreatedAt?: string;
  toCreatedAt?: string;
  tag?: string;
}

function mapObject(row: ObjectRow): ObjectRecord {
  return {
    objectId: row.object_id,
    tenantId: row.tenant_id,
    type: row.type,
    title: row.title,
    metadata: row.metadata,
    sourceIngestionId: row.source_ingestion_id ?? undefined,
    status: row.status,
    createdAt: new Date(row.created_at),
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
  const sql = db();
  const objectsTable = qualifiedTableName("objects");

  const rows = (await sql.unsafe(
    `
      SELECT object_id, tenant_id, type, title, metadata, source_ingestion_id, status, created_at
      FROM ${objectsTable}
      WHERE tenant_id = $1
        AND source_ingestion_id = $2
      ORDER BY created_at ASC
      LIMIT 1
    `,
    [params.tenantId, params.ingestionId],
  )) as ObjectRow[];

  const row = rows[0];
  return row ? mapObject(row) : undefined;
}

export async function createObject(params: {
  objectId: string;
  tenantId: string;
  sourceIngestionId: string;
  type?: "GENERIC" | "IMAGE" | "AUDIO" | "VIDEO" | "DOCUMENT";
  title?: string;
  metadata?: Record<string, unknown>;
}): Promise<ObjectRecord> {
  const sql = db();
  const objectsTable = qualifiedTableName("objects");

  const rows = (await sql.unsafe(
    `
      INSERT INTO ${objectsTable} (
        object_id,
        tenant_id,
        type,
        title,
        metadata,
        source_ingestion_id,
        status
      )
      VALUES ($1, $2, $3, $4, $5::jsonb, $6, 'ACTIVE')
      RETURNING object_id, tenant_id, type, title, metadata, source_ingestion_id, status, created_at
    `,
    [
      params.objectId,
      params.tenantId,
      params.type ?? "GENERIC",
      params.title ?? "",
      JSON.stringify(params.metadata ?? {}),
      params.sourceIngestionId,
    ],
  )) as ObjectRow[];

  return mapObject(rows[0]!);
}

export async function createObjectArtifact(params: {
  objectId: string;
  kind: "ingest_json" | "original" | "preview" | "ocr" | "transcript" | "metadata" | "other";
  storageKey: string;
  contentType: string;
  sizeBytes: number;
}): Promise<ObjectArtifactRecord> {
  const sql = db();
  const artifactsTable = qualifiedTableName("object_artifacts");

  const rows = (await sql.unsafe(
    `
      INSERT INTO ${artifactsTable} (
        id,
        object_id,
        kind,
        storage_key,
        content_type,
        size_bytes
      )
      VALUES ($1, $2, $3, $4, $5, $6)
      RETURNING id, object_id, kind, storage_key, content_type, size_bytes, created_at
    `,
    [crypto.randomUUID(), params.objectId, params.kind, params.storageKey, params.contentType, params.sizeBytes],
  )) as ObjectArtifactRow[];

  return mapArtifact(rows[0]!);
}

export async function listObjects(params: ListObjectsParams): Promise<ObjectRecord[]> {
  const sql = db();
  const objectsTable = qualifiedTableName("objects");

  const values: Array<string | number> = [params.tenantId, params.limit];
  let nextIndex = 3;
  let filters = "";

  if (params.type) {
    filters += ` AND type = $${nextIndex}`;
    values.push(params.type);
    nextIndex += 1;
  }

  if (params.fromCreatedAt) {
    filters += ` AND created_at >= $${nextIndex}::timestamptz`;
    values.push(params.fromCreatedAt);
    nextIndex += 1;
  }

  if (params.toCreatedAt) {
    filters += ` AND created_at <= $${nextIndex}::timestamptz`;
    values.push(params.toCreatedAt);
    nextIndex += 1;
  }

  if (params.tag) {
    filters += ` AND (metadata->'tags') ? $${nextIndex}`;
    values.push(params.tag);
    nextIndex += 1;
  }

  if (params.cursorCreatedAt && params.cursorObjectId) {
    filters += ` AND (created_at, object_id) < ($${nextIndex}::timestamptz, $${nextIndex + 1})`;
    values.push(params.cursorCreatedAt, params.cursorObjectId);
    nextIndex += 2;
  }

  const rows = (await sql.unsafe(
    `
      SELECT object_id, tenant_id, type, title, metadata, source_ingestion_id, status, created_at
      FROM ${objectsTable}
      WHERE tenant_id = $1
      ${filters}
      ORDER BY created_at DESC, object_id DESC
      LIMIT $2
    `,
    values,
  )) as ObjectRow[];

  return rows.map(mapObject);
}

export async function findObjectById(params: {
  tenantId: string;
  objectId: string;
}): Promise<ObjectRecord | undefined> {
  const sql = db();
  const objectsTable = qualifiedTableName("objects");

  const rows = (await sql.unsafe(
    `
      SELECT object_id, tenant_id, type, title, metadata, source_ingestion_id, status, created_at
      FROM ${objectsTable}
      WHERE tenant_id = $1
        AND object_id = $2
      LIMIT 1
    `,
    [params.tenantId, params.objectId],
  )) as ObjectRow[];

  const row = rows[0];
  return row ? mapObject(row) : undefined;
}

export async function updateObjectTitle(params: {
  tenantId: string;
  objectId: string;
  title: string;
}): Promise<ObjectRecord | undefined> {
  const sql = db();
  const objectsTable = qualifiedTableName("objects");

  const rows = (await sql.unsafe(
    `
      UPDATE ${objectsTable}
      SET title = $3
      WHERE tenant_id = $1
        AND object_id = $2
      RETURNING object_id, tenant_id, type, title, metadata, source_ingestion_id, status, created_at
    `,
    [params.tenantId, params.objectId, params.title],
  )) as ObjectRow[];

  const row = rows[0];
  return row ? mapObject(row) : undefined;
}

export async function listArtifactsByObjectId(params: {
  tenantId: string;
  objectId: string;
}): Promise<ObjectArtifactRecord[]> {
  const sql = db();
  const objectsTable = qualifiedTableName("objects");
  const artifactsTable = qualifiedTableName("object_artifacts");

  const rows = (await sql.unsafe(
    `
      SELECT a.id, a.object_id, a.kind, a.storage_key, a.content_type, a.size_bytes, a.created_at
      FROM ${artifactsTable} a
      INNER JOIN ${objectsTable} o ON o.object_id = a.object_id
      WHERE o.tenant_id = $1
        AND o.object_id = $2
      ORDER BY a.created_at ASC, a.id ASC
    `,
    [params.tenantId, params.objectId],
  )) as ObjectArtifactRow[];

  return rows.map(mapArtifact);
}

export async function findArtifactById(params: {
  tenantId: string;
  objectId: string;
  artifactId: string;
}): Promise<ObjectArtifactRecord | undefined> {
  const sql = db();
  const objectsTable = qualifiedTableName("objects");
  const artifactsTable = qualifiedTableName("object_artifacts");

  const rows = (await sql.unsafe(
    `
      SELECT a.id, a.object_id, a.kind, a.storage_key, a.content_type, a.size_bytes, a.created_at
      FROM ${artifactsTable} a
      INNER JOIN ${objectsTable} o ON o.object_id = a.object_id
      WHERE o.tenant_id = $1
        AND o.object_id = $2
        AND a.id = $3
      LIMIT 1
    `,
    [params.tenantId, params.objectId, params.artifactId],
  )) as ObjectArtifactRow[];

  const row = rows[0];
  return row ? mapArtifact(row) : undefined;
}

export async function findArtifactByStorageKey(params: {
  objectId: string;
  storageKey: string;
}): Promise<ObjectArtifactRecord | undefined> {
  const sql = db();
  const artifactsTable = qualifiedTableName("object_artifacts");

  const rows = (await sql.unsafe(
    `
      SELECT id, object_id, kind, storage_key, content_type, size_bytes, created_at
      FROM ${artifactsTable}
      WHERE object_id = $1
        AND storage_key = $2
      LIMIT 1
    `,
    [params.objectId, params.storageKey],
  )) as ObjectArtifactRow[];

  const row = rows[0];
  return row ? mapArtifact(row) : undefined;
}
