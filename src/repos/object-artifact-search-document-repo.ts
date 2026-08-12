import { withExecutor, withSchemaClient, type SqlExecutor } from "../db/client.ts";
import { toSafeNumberFromDbInt, type DbInt } from "../db/number.ts";
import type { ObjectArtifactRecord } from "./object-repo.ts";

interface ObjectArtifactSearchDocumentRow {
  artifact_id: string;
  available_file_id: string | null;
  text_content: string | null;
  indexed_at: Date | null;
  updated_at: Date;
}

interface ArtifactSearchBackfillCandidateRow {
  tenant_id: string;
  id: string;
  object_id: string;
  kind: ObjectArtifactRecord["kind"];
  variant: string | null;
  storage_key: string;
  content_type: string;
  size_bytes: DbInt;
  created_at: Date;
}

export interface ObjectArtifactSearchDocumentRecord {
  artifactId: string;
  availableFileId: string | null;
  textContent: string | null;
  indexedAt: Date | null;
  updatedAt: Date;
}

export interface ArtifactSearchBackfillCandidate {
  tenantId: string;
  artifact: ObjectArtifactRecord;
}

function mapSearchDocument(
  row: ObjectArtifactSearchDocumentRow,
): ObjectArtifactSearchDocumentRecord {
  return {
    artifactId: row.artifact_id,
    availableFileId: row.available_file_id,
    textContent: row.text_content,
    indexedAt: row.indexed_at,
    updatedAt: row.updated_at,
  };
}

function mapBackfillCandidate(
  row: ArtifactSearchBackfillCandidateRow,
): ArtifactSearchBackfillCandidate {
  return {
    tenantId: row.tenant_id,
    artifact: {
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
    },
  };
}

export async function listArtifactSearchBackfillCandidates(params: {
  afterArtifactId?: string;
  limit: number;
}): Promise<ArtifactSearchBackfillCandidate[]> {
  const afterArtifactId = params.afterArtifactId ?? null;
  const rows = await withSchemaClient(async (sql) => {
    return await sql<ArtifactSearchBackfillCandidateRow[]>`
      SELECT obj.tenant_id, art.id, art.object_id, art.kind, art.variant,
             art.storage_key, art.content_type, art.size_bytes, art.created_at
      FROM object_artifacts art
      INNER JOIN objects obj ON obj.object_id = art.object_id
      LEFT JOIN object_artifact_search_documents doc ON doc.artifact_id = art.id
      WHERE art.kind IN ('ocr_text'::artifact_kind, 'transcript'::artifact_kind)
        AND doc.text_content IS NULL
        AND (${afterArtifactId}::uuid IS NULL OR art.id > ${afterArtifactId}::uuid)
      ORDER BY art.id ASC
      LIMIT ${params.limit}
    `;
  });

  return rows.map(mapBackfillCandidate);
}

export async function upsertArtifactSearchText(params: {
  tenantId: string;
  objectId: string;
  artifactId: string;
  textContent: string;
  indexedAt: Date;
  availableFileId?: string | null;
  executor?: SqlExecutor;
}): Promise<ObjectArtifactSearchDocumentRecord | undefined> {
  const updateProvenance = params.availableFileId !== undefined;
  const availableFileId = params.availableFileId ?? null;
  const rows = await withExecutor(params.executor, async (sql) => {
    return await sql<ObjectArtifactSearchDocumentRow[]>`
      INSERT INTO object_artifact_search_documents (
        artifact_id,
        available_file_id,
        text_content,
        indexed_at,
        updated_at
      )
      SELECT art.id, ${availableFileId}, ${params.textContent}, ${params.indexedAt}, now()
      FROM object_artifacts art
      INNER JOIN objects obj ON obj.object_id = art.object_id
      WHERE obj.tenant_id = ${params.tenantId}
        AND obj.object_id = ${params.objectId}
        AND art.id = ${params.artifactId}
        AND (
          ${availableFileId}::uuid IS NULL
          OR EXISTS (
            SELECT 1
            FROM object_available_files file
            WHERE file.id = ${availableFileId}
              AND file.tenant_id = ${params.tenantId}
              AND file.object_id = ${params.objectId}
          )
        )
      ON CONFLICT (artifact_id)
      DO UPDATE SET
        available_file_id = CASE
          WHEN ${updateProvenance} THEN EXCLUDED.available_file_id
          ELSE object_artifact_search_documents.available_file_id
        END,
        text_content = EXCLUDED.text_content,
        indexed_at = EXCLUDED.indexed_at,
        updated_at = now()
      RETURNING artifact_id, available_file_id, text_content, indexed_at, updated_at
    `;
  });

  const row = rows[0];
  return row ? mapSearchDocument(row) : undefined;
}

export async function upsertArtifactSearchProvenance(params: {
  tenantId: string;
  objectId: string;
  artifactId: string;
  availableFileId: string;
  executor?: SqlExecutor;
}): Promise<ObjectArtifactSearchDocumentRecord | undefined> {
  const rows = await withExecutor(params.executor, async (sql) => {
    return await sql<ObjectArtifactSearchDocumentRow[]>`
      INSERT INTO object_artifact_search_documents (
        artifact_id,
        available_file_id,
        updated_at
      )
      SELECT art.id, file.id, now()
      FROM object_artifacts art
      INNER JOIN objects obj ON obj.object_id = art.object_id
      INNER JOIN object_available_files file
        ON file.id = ${params.availableFileId}
        AND file.tenant_id = obj.tenant_id
        AND file.object_id = obj.object_id
      WHERE obj.tenant_id = ${params.tenantId}
        AND obj.object_id = ${params.objectId}
        AND art.id = ${params.artifactId}
      ON CONFLICT (artifact_id)
      DO UPDATE SET
        available_file_id = EXCLUDED.available_file_id,
        updated_at = now()
      RETURNING artifact_id, available_file_id, text_content, indexed_at, updated_at
    `;
  });

  const row = rows[0];
  return row ? mapSearchDocument(row) : undefined;
}

export async function findArtifactSearchDocument(params: {
  tenantId: string;
  objectId: string;
  artifactId: string;
}): Promise<ObjectArtifactSearchDocumentRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<ObjectArtifactSearchDocumentRow[]>`
      SELECT doc.artifact_id, doc.available_file_id, doc.text_content,
             doc.indexed_at, doc.updated_at
      FROM object_artifact_search_documents doc
      INNER JOIN object_artifacts art ON art.id = doc.artifact_id
      INNER JOIN objects obj ON obj.object_id = art.object_id
      WHERE obj.tenant_id = ${params.tenantId}
        AND obj.object_id = ${params.objectId}
        AND art.id = ${params.artifactId}
      LIMIT 1
    `;
  });

  const row = rows[0];
  return row ? mapSearchDocument(row) : undefined;
}
