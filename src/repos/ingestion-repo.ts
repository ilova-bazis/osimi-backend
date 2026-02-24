import { withSchemaClient } from "../db/client.ts";
import type { IngestionStatus } from "../domain/ingestions/state-machine.ts";
import type {
  AccessLevel,
  IngestionDocumentType,
  IngestionFileProcessingOverrides,
  IngestionPipelinePreset,
  JsonObject,
} from "../validation/ingestion.ts";
import type { IngestionSummary } from "../validation/catalog.ts";

type IngestionFileStatus = "PENDING" | "UPLOADED" | "VALIDATED" | "FAILED";

interface IngestionRow {
  id: string;
  batch_label: string;
  tenant_id: string;
  status: IngestionStatus;
  created_by: string;
  schema_version: string;
  document_type: IngestionDocumentType;
  language_code: string;
  pipeline_preset: IngestionPipelinePreset;
  access_level: AccessLevel;
  embargo_until: Date | null;
  rights_note: string | null;
  sensitivity_note: string | null;
  summary: IngestionSummary;
  error_summary: JsonObject;
  created_at: Date;
  updated_at: Date;
}

interface IngestionFileRow {
  id: string;
  ingestion_id: string;
  filename: string;
  content_type: string;
  size_bytes: number;
  storage_key: string;
  status: IngestionFileStatus;
  checksum_sha256: string | null;
  processing_overrides: IngestionFileProcessingOverrides;
  error: JsonObject;
  created_at: Date;
  updated_at: Date;
}

export interface IngestionRecord {
  id: string;
  batchLabel: string;
  tenantId: string;
  status: IngestionStatus;
  createdBy: string;
  schemaVersion: string;
  documentType: IngestionDocumentType;
  languageCode: string;
  pipelinePreset: IngestionPipelinePreset;
  accessLevel: AccessLevel;
  embargoUntil?: Date;
  rightsNote?: string;
  sensitivityNote?: string;
  summary: IngestionSummary;
  errorSummary: JsonObject;
  createdAt: Date;
  updatedAt: Date;
}

export interface IngestionWithCreatorRecord extends IngestionRecord {
  createdByUsername?: string;
}

export interface IngestionFileRecord {
  id: string;
  ingestionId: string;
  filename: string;
  contentType: string;
  sizeBytes: number;
  storageKey: string;
  status: IngestionFileStatus;
  checksumSha256?: string;
  processingOverrides: IngestionFileProcessingOverrides;
  error: JsonObject;
  createdAt: Date;
  updatedAt: Date;
}

export interface StagingCleanupCandidate {
  ingestionId: string;
  tenantId: string;
  status: IngestionStatus;
  updatedAt: Date;
  storageKey: string;
}

export interface StuckIngestionRecord {
  ingestionId: string;
  tenantId: string;
  status: IngestionStatus;
  updatedAt: Date;
  createdBy: string;
}

function mapIngestion(row: IngestionRow): IngestionRecord {
  return {
    id: row.id,
    batchLabel: row.batch_label,
    tenantId: row.tenant_id,
    status: row.status,
    createdBy: row.created_by,
    schemaVersion: row.schema_version,
    documentType: row.document_type,
    languageCode: row.language_code,
    pipelinePreset: row.pipeline_preset,
    accessLevel: row.access_level,
    embargoUntil: row.embargo_until ? new Date(row.embargo_until) : undefined,
    rightsNote: row.rights_note ?? undefined,
    sensitivityNote: row.sensitivity_note ?? undefined,
    summary: row.summary,
    errorSummary: row.error_summary,
    createdAt: new Date(row.created_at),
    updatedAt: new Date(row.updated_at),
  };
}

function mapIngestionWithCreator(row: IngestionRow & { created_by_username: string | null }): IngestionWithCreatorRecord {
  return {
    ...mapIngestion(row),
    createdByUsername: row.created_by_username ?? undefined,
  };
}

function mapIngestionFile(row: IngestionFileRow): IngestionFileRecord {
  return {
    id: row.id,
    ingestionId: row.ingestion_id,
    filename: row.filename,
    contentType: row.content_type,
    sizeBytes: Number(row.size_bytes),
    storageKey: row.storage_key,
    status: row.status,
    checksumSha256: row.checksum_sha256 ?? undefined,
    processingOverrides: row.processing_overrides,
    error: row.error,
    createdAt: new Date(row.created_at),
    updatedAt: new Date(row.updated_at),
  };
}

function mapStagingCleanupCandidate(row: {
  ingestion_id: string;
  tenant_id: string;
  status: IngestionStatus;
  updated_at: Date;
  storage_key: string;
}): StagingCleanupCandidate {
  return {
    ingestionId: row.ingestion_id,
    tenantId: row.tenant_id,
    status: row.status,
    updatedAt: new Date(row.updated_at),
    storageKey: row.storage_key,
  };
}

function mapStuckIngestion(row: {
  id: string;
  tenant_id: string;
  status: IngestionStatus;
  updated_at: Date;
  created_by: string;
}): StuckIngestionRecord {
  return {
    ingestionId: row.id,
    tenantId: row.tenant_id,
    status: row.status,
    updatedAt: new Date(row.updated_at),
    createdBy: row.created_by,
  };
}

export async function createIngestion(params: {
  id: string;
  batchLabel: string;
  tenantId: string;
  createdBy: string;
  schemaVersion: string;
  documentType: IngestionDocumentType;
  languageCode: string;
  pipelinePreset: IngestionPipelinePreset;
  accessLevel: AccessLevel;
  embargoUntil?: Date;
  rightsNote?: string;
  sensitivityNote?: string;
  summary?: IngestionSummary;
}): Promise<IngestionRecord> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<IngestionRow[]>`
      INSERT INTO ingestions (
        id,
        batch_label,
        tenant_id,
        status,
        created_by,
        schema_version,
        document_type,
        language_code,
        pipeline_preset,
        access_level,
        embargo_until,
        rights_note,
        sensitivity_note,
        summary
      )
      VALUES (
        ${params.id},
        ${params.batchLabel},
        ${params.tenantId},
        'DRAFT',
        ${params.createdBy},
        ${params.schemaVersion},
        ${params.documentType},
        ${params.languageCode},
        ${params.pipelinePreset},
        ${params.accessLevel},
        ${params.embargoUntil ? params.embargoUntil.toISOString() : null},
        ${params.rightsNote ?? null},
        ${params.sensitivityNote ?? null},
        ${params.summary ?? {}}
      )
      RETURNING id, batch_label, tenant_id, status, created_by, schema_version, document_type, language_code,
        pipeline_preset, access_level, embargo_until, rights_note, sensitivity_note, summary, error_summary,
        created_at, updated_at
    `;
  });

  return mapIngestion(rows[0]!);
}

export async function findIngestionById(
  tenantId: string,
  ingestionId: string,
): Promise<IngestionRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<IngestionRow[]>`
      SELECT id, batch_label, tenant_id, status, created_by, schema_version, document_type, language_code,
        pipeline_preset, access_level, embargo_until, rights_note, sensitivity_note, summary, error_summary,
        created_at, updated_at
      FROM ingestions
      WHERE id = ${ingestionId}
        AND tenant_id = ${tenantId}
      LIMIT 1
    `;
  });

  const row = rows[0];
  return row ? mapIngestion(row) : undefined;
}

export async function listIngestions(params: {
  tenantId: string;
  limit: number;
  cursorCreatedAt?: string;
  cursorId?: string;
}): Promise<IngestionRecord[]> {
  const rows = await withSchemaClient(async (sql) => {
    if (params.cursorCreatedAt && params.cursorId) {
      return await sql<IngestionRow[]>`
        SELECT id, batch_label, tenant_id, status, created_by, schema_version, document_type, language_code,
          pipeline_preset, access_level, embargo_until, rights_note, sensitivity_note, summary, error_summary,
          created_at, updated_at
        FROM ingestions
        WHERE tenant_id = ${params.tenantId}
          AND (created_at, id) < (${params.cursorCreatedAt}::timestamptz, ${params.cursorId}::uuid)
        ORDER BY created_at DESC, id DESC
        LIMIT ${params.limit}
      `;
    }

    return await sql<IngestionRow[]>`
      SELECT id, batch_label, tenant_id, status, created_by, schema_version, document_type, language_code,
        pipeline_preset, access_level, embargo_until, rights_note, sensitivity_note, summary, error_summary,
        created_at, updated_at
      FROM ingestions
      WHERE tenant_id = ${params.tenantId}
      ORDER BY created_at DESC, id DESC
      LIMIT ${params.limit}
    `;
  });

  return rows.map(mapIngestion);
}

export async function updateIngestionStatus(params: {
  ingestionId: string;
  tenantId: string;
  fromStatus: IngestionStatus;
  toStatus: IngestionStatus;
}): Promise<IngestionRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<IngestionRow[]>`
      UPDATE ingestions
      SET status = ${params.toStatus},
          updated_at = now()
      WHERE id = ${params.ingestionId}
        AND tenant_id = ${params.tenantId}
        AND status = ${params.fromStatus}
      RETURNING id, batch_label, tenant_id, status, created_by, schema_version, document_type, language_code,
        pipeline_preset, access_level, embargo_until, rights_note, sensitivity_note, summary, error_summary,
        created_at, updated_at
    `;
  });

  const row = rows[0];
  return row ? mapIngestion(row) : undefined;
}

export async function createIngestionFile(params: {
  id: string;
  ingestionId: string;
  filename: string;
  contentType: string;
  sizeBytes: number;
  storageKey: string;
}): Promise<IngestionFileRecord> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<IngestionFileRow[]>`
      INSERT INTO ingestion_files (
        id,
        ingestion_id,
        filename,
        content_type,
        size_bytes,
        storage_key,
        status
      )
      VALUES (${params.id}, ${params.ingestionId}, ${params.filename}, ${params.contentType}, ${params.sizeBytes}, ${params.storageKey}, 'PENDING')
      RETURNING id, ingestion_id, filename, content_type, size_bytes, storage_key, status, checksum_sha256,
        processing_overrides, error, created_at, updated_at
    `;
  });

  return mapIngestionFile(rows[0]!);
}

export async function findIngestionFileById(params: {
  tenantId: string;
  ingestionId: string;
  fileId: string;
}): Promise<IngestionFileRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<IngestionFileRow[]>`
      SELECT
        file.id,
        file.ingestion_id,
        file.filename,
        file.content_type,
        file.size_bytes,
        file.storage_key,
        file.status,
        file.checksum_sha256,
        file.processing_overrides,
        file.error,
        file.created_at,
        file.updated_at
      FROM ingestion_files file
      INNER JOIN ingestions ing ON ing.id = file.ingestion_id
      WHERE file.id = ${params.fileId}
        AND file.ingestion_id = ${params.ingestionId}
        AND ing.tenant_id = ${params.tenantId}
      LIMIT 1
    `;
  });

  const row = rows[0];
  return row ? mapIngestionFile(row) : undefined;
}

export async function listIngestionFiles(params: {
  tenantId: string;
  ingestionId: string;
}): Promise<IngestionFileRecord[]> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<IngestionFileRow[]>`
      SELECT
        file.id,
        file.ingestion_id,
        file.filename,
        file.content_type,
        file.size_bytes,
        file.storage_key,
        file.status,
        file.checksum_sha256,
        file.processing_overrides,
        file.error,
        file.created_at,
        file.updated_at
      FROM ingestion_files file
      INNER JOIN ingestions ing ON ing.id = file.ingestion_id
      WHERE file.ingestion_id = ${params.ingestionId}
        AND ing.tenant_id = ${params.tenantId}
      ORDER BY file.created_at ASC, file.id ASC
    `;
  });

  return rows.map(mapIngestionFile);
}

export async function markIngestionFileUploaded(params: {
  fileId: string;
  ingestionId: string;
  checksumSha256: string;
}): Promise<IngestionFileRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<IngestionFileRow[]>`
      UPDATE ingestion_files
      SET status = 'UPLOADED',
          checksum_sha256 = ${params.checksumSha256},
          updated_at = now()
      WHERE id = ${params.fileId}
        AND ingestion_id = ${params.ingestionId}
        AND status = 'PENDING'
      RETURNING id, ingestion_id, filename, content_type, size_bytes, storage_key, status, checksum_sha256,
        processing_overrides, error, created_at, updated_at
    `;
  });

  const row = rows[0];
  return row ? mapIngestionFile(row) : undefined;
}

export async function deleteIngestionFile(params: {
  tenantId: string;
  ingestionId: string;
  fileId: string;
}): Promise<boolean> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<Array<{ id: string }>>`
      DELETE FROM ingestion_files file
      USING ingestions ing
      WHERE file.id = ${params.fileId}
        AND file.ingestion_id = ${params.ingestionId}
        AND ing.id = file.ingestion_id
        AND ing.tenant_id = ${params.tenantId}
      RETURNING file.id
    `;
  });

  return rows.length > 0;
}

export async function updateIngestionFileProcessingOverrides(params: {
  tenantId: string;
  ingestionId: string;
  fileId: string;
  processingOverrides: IngestionFileProcessingOverrides;
}): Promise<IngestionFileRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<IngestionFileRow[]>`
      UPDATE ingestion_files file
      SET processing_overrides = ${params.processingOverrides},
          updated_at = now()
      FROM ingestions ing
      WHERE file.id = ${params.fileId}
        AND file.ingestion_id = ${params.ingestionId}
        AND ing.id = file.ingestion_id
        AND ing.tenant_id = ${params.tenantId}
      RETURNING file.id, file.ingestion_id, file.filename, file.content_type, file.size_bytes,
        file.storage_key, file.status, file.checksum_sha256, file.processing_overrides,
        file.error, file.created_at, file.updated_at
    `;
  });

  const row = rows[0];
  return row ? mapIngestionFile(row) : undefined;
}

export async function deleteIngestion(params: {
  tenantId: string;
  ingestionId: string;
}): Promise<boolean> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<Array<{ id: string }>>`
      DELETE FROM ingestions
      WHERE id = ${params.ingestionId}
        AND tenant_id = ${params.tenantId}
      RETURNING id
    `;
  });

  return rows.length > 0;
}

export async function listStagingCleanupCandidates(params: {
  completedRetentionDays: number;
  failedCanceledRetentionDays: number;
}): Promise<StagingCleanupCandidate[]> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<
      Array<{
        ingestion_id: string;
        tenant_id: string;
        status: IngestionStatus;
        updated_at: Date;
        storage_key: string;
      }>
    >`
      SELECT
        ing.id AS ingestion_id,
        ing.tenant_id,
        ing.status,
        ing.updated_at,
        file.storage_key
      FROM ingestions ing
      INNER JOIN ingestion_files file ON file.ingestion_id = ing.id
      WHERE (
        ing.status = 'COMPLETED'
        AND ing.updated_at <= now() - (${params.completedRetentionDays}::int * interval '1 day')
      )
      OR (
        ing.status IN ('FAILED', 'CANCELED')
        AND ing.updated_at <= now() - (${params.failedCanceledRetentionDays}::int * interval '1 day')
      )
    `;
  });

  return rows.map(mapStagingCleanupCandidate);
}

export async function listStuckIngestions(params: {
  thresholdMinutes: number;
}): Promise<StuckIngestionRecord[]> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<
      Array<{
        id: string;
        tenant_id: string;
        status: IngestionStatus;
        updated_at: Date;
        created_by: string;
      }>
    >`
      SELECT id, tenant_id, status, updated_at, created_by
      FROM ingestions
      WHERE status IN ('UPLOADING', 'PROCESSING')
        AND updated_at <= now() - (${params.thresholdMinutes}::int * interval '1 minute')
      ORDER BY updated_at ASC
    `;
  });

  return rows.map(mapStuckIngestion);
}

export async function findIngestionWithCreator(params: {
  tenantId: string;
  ingestionId: string;
}): Promise<IngestionWithCreatorRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<
      Array<IngestionRow & { created_by_username: string | null }>
    >`
      SELECT
        ing.id,
        ing.batch_label,
        ing.tenant_id,
        ing.status,
        ing.created_by,
        ing.schema_version,
        ing.document_type,
        ing.language_code,
        ing.pipeline_preset,
        ing.access_level,
        ing.embargo_until,
        ing.rights_note,
        ing.sensitivity_note,
        ing.summary,
        ing.error_summary,
        ing.created_at,
        ing.updated_at,
        usr.username AS created_by_username
      FROM ingestions ing
      LEFT JOIN users usr ON usr.id = ing.created_by
      WHERE ing.id = ${params.ingestionId}
        AND ing.tenant_id = ${params.tenantId}
      LIMIT 1
    `;
  });

  const row = rows[0];
  return row ? mapIngestionWithCreator(row) : undefined;
}
