import { withSchemaClient } from "../db/client.ts";

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
  const rows = await withSchemaClient(async (sql) => {
    return await sql<ObjectRow[]>`
      SELECT object_id, tenant_id, type, title, metadata, source_ingestion_id, status, created_at
      FROM objects
      WHERE tenant_id = ${params.tenantId}
        AND source_ingestion_id = ${params.ingestionId}
      ORDER BY created_at ASC
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
  metadata?: Record<string, unknown>;
}): Promise<ObjectRecord> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<ObjectRow[]>`
      INSERT INTO objects (
        object_id,
        tenant_id,
        type,
        title,
        metadata,
        source_ingestion_id,
        status
      )
      VALUES (
        ${params.objectId},
        ${params.tenantId},
        ${params.type ?? "GENERIC"},
        ${params.title ?? ""},
        CAST(${JSON.stringify(params.metadata ?? {})} AS jsonb),
        ${params.sourceIngestionId},
        'ACTIVE'
      )
      RETURNING object_id, tenant_id, type, title, metadata, source_ingestion_id, status, created_at
    `;
  });

  return mapObject(rows[0]!);
}

export async function createOrGetObjectBySourceIngestion(params: {
  objectId: string;
  tenantId: string;
  sourceIngestionId: string;
  type?: "GENERIC" | "IMAGE" | "AUDIO" | "VIDEO" | "DOCUMENT";
  title?: string;
  metadata?: Record<string, unknown>;
}): Promise<ObjectRecord> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<ObjectRow[]>`
      WITH inserted AS (
        INSERT INTO objects (
          object_id,
          tenant_id,
          type,
          title,
          metadata,
          source_ingestion_id,
          status
        )
        VALUES (
          ${params.objectId},
          ${params.tenantId},
          ${params.type ?? "GENERIC"},
          ${params.title ?? ""},
          CAST(${JSON.stringify(params.metadata ?? {})} AS jsonb),
          ${params.sourceIngestionId},
          'ACTIVE'
        )
        ON CONFLICT (source_ingestion_id)
        WHERE source_ingestion_id IS NOT NULL
        DO NOTHING
        RETURNING object_id, tenant_id, type, title, metadata, source_ingestion_id, status, created_at
      )
      SELECT object_id, tenant_id, type, title, metadata, source_ingestion_id, status, created_at
      FROM inserted
      UNION ALL
      SELECT object_id, tenant_id, type, title, metadata, source_ingestion_id, status, created_at
      FROM objects
      WHERE tenant_id = ${params.tenantId}
        AND source_ingestion_id = ${params.sourceIngestionId}
      LIMIT 1
    `;
  });

  return mapObject(rows[0]!);
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
): Promise<ObjectRecord[]> {
  const rows = await withSchemaClient(async (sql) => {
    if (params.cursorCreatedAt && params.cursorObjectId) {
      return await sql<ObjectRow[]>`
        SELECT object_id, tenant_id, type, title, metadata, source_ingestion_id, status, created_at
        FROM objects
        WHERE tenant_id = ${params.tenantId}
          AND (${params.type ?? null}::object_type IS NULL OR type = ${params.type ?? null}::object_type)
          AND (${params.fromCreatedAt ?? null}::timestamptz IS NULL OR created_at >= ${params.fromCreatedAt ?? null}::timestamptz)
          AND (${params.toCreatedAt ?? null}::timestamptz IS NULL OR created_at <= ${params.toCreatedAt ?? null}::timestamptz)
          AND (${params.tag ?? null}::text IS NULL OR (metadata->'tags') ? (${params.tag ?? null}::text))
          AND (created_at, object_id) < (${params.cursorCreatedAt}::timestamptz, ${params.cursorObjectId}::text)
        ORDER BY created_at DESC, object_id DESC
        LIMIT ${params.limit}
      `;
    }

    return await sql<ObjectRow[]>`
      SELECT object_id, tenant_id, type, title, metadata, source_ingestion_id, status, created_at
      FROM objects
      WHERE tenant_id = ${params.tenantId}
        AND (${params.type ?? null}::object_type IS NULL OR type = ${params.type ?? null}::object_type)
        AND (${params.fromCreatedAt ?? null}::timestamptz IS NULL OR created_at >= ${params.fromCreatedAt ?? null}::timestamptz)
        AND (${params.toCreatedAt ?? null}::timestamptz IS NULL OR created_at <= ${params.toCreatedAt ?? null}::timestamptz)
        AND (${params.tag ?? null}::text IS NULL OR (metadata->'tags') ? (${params.tag ?? null}::text))
      ORDER BY created_at DESC, object_id DESC
      LIMIT ${params.limit}
    `;
  });

  return rows.map(mapObject);
}

export async function findObjectById(params: {
  tenantId: string;
  objectId: string;
}): Promise<ObjectRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<ObjectRow[]>`
      SELECT object_id, tenant_id, type, title, metadata, source_ingestion_id, status, created_at
      FROM objects
      WHERE tenant_id = ${params.tenantId}
        AND object_id = ${params.objectId}
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
      SET title = ${params.title}
      WHERE tenant_id = ${params.tenantId}
        AND object_id = ${params.objectId}
      RETURNING object_id, tenant_id, type, title, metadata, source_ingestion_id, status, created_at
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
      SELECT a.id, a.object_id, a.kind, a.storage_key, a.content_type, a.size_bytes, a.created_at
      FROM object_artifacts a
      INNER JOIN objects o ON o.object_id = a.object_id
      WHERE o.tenant_id = ${params.tenantId}
        AND o.object_id = ${params.objectId}
      ORDER BY a.created_at ASC, a.id ASC
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
      SELECT a.id, a.object_id, a.kind, a.storage_key, a.content_type, a.size_bytes, a.created_at
      FROM object_artifacts a
      INNER JOIN objects o ON o.object_id = a.object_id
      WHERE o.tenant_id = ${params.tenantId}
        AND o.object_id = ${params.objectId}
        AND a.id = ${params.artifactId}
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
