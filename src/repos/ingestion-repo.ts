import { withExecutor, withSchemaClient } from "../db/client.ts";
import type { SqlExecutor } from "../db/client.ts";
import { toSafeNumberFromDbInt, type DbInt } from "../db/number.ts";
import type { IngestionStatus } from "../domain/ingestions/state-machine.ts";
import type {
  AccessLevel,
  IngestionClassificationType,
  IngestItemKind,
  IngestionFileProcessingOverrides,
  JsonObject,
  IngestionPipelinePreset,
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
  classification_type: IngestionClassificationType;
  item_kind: IngestItemKind;
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
  size_bytes: DbInt;
  storage_key: string;
  status: IngestionFileStatus;
  checksum_sha256: string | null;
  upload_token_id?: string | null;
  upload_checksum_sha256?: string | null;
  preview_upload_token_id?: string | null;
  preview_status:
    | "pending"
    | "processing"
    | "ready"
    | "failed"
    | "unsupported"
    | null;
  preview_claimed_by: string | null;
  preview_claimed_at: Date | null;
  preview_storage_key: string | null;
  preview_content_type: string | null;
  preview_size_bytes: DbInt | null;
  preview_width: number | null;
  preview_height: number | null;
  preview_error: JsonObject | null;
  preview_generated_at: Date | null;
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
  classificationType: IngestionClassificationType;
  itemKind: IngestItemKind;
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
  uploadTokenId?: string;
  uploadChecksumSha256?: string;
  previewUploadTokenId?: string;
  previewStatus?: "pending" | "processing" | "ready" | "failed" | "unsupported";
  previewClaimedBy?: string;
  previewClaimedAt?: Date;
  previewStorageKey?: string;
  previewContentType?: string;
  previewSizeBytes?: number;
  previewWidth?: number;
  previewHeight?: number;
  previewError?: JsonObject;
  previewGeneratedAt?: Date;
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
  previewStorageKey?: string;
}

export interface ClaimedIngestionPreviewRecord {
  ingestionId: string;
  tenantId: string;
  batchLabel: string;
  fileId: string;
  filename: string;
  contentType: string;
  sizeBytes: number;
  storageKey: string;
  claimedBy?: string;
  claimedAt?: Date;
  previewUploadTokenId?: string;
}

function mapClaimedIngestionPreview(row: {
  ingestion_id: string;
  tenant_id: string;
  batch_label: string;
  file_id: string;
  filename: string;
  content_type: string;
  size_bytes: DbInt;
  storage_key: string;
  preview_claimed_by: string | null;
  preview_claimed_at: Date | null;
  preview_upload_token_id?: string | null;
}): ClaimedIngestionPreviewRecord {
  return {
    ingestionId: row.ingestion_id,
    tenantId: row.tenant_id,
    batchLabel: row.batch_label,
    fileId: row.file_id,
    filename: row.filename,
    contentType: row.content_type,
    sizeBytes: toSafeNumberFromDbInt(row.size_bytes, "ingestion_files.size_bytes"),
    storageKey: row.storage_key,
    claimedBy: row.preview_claimed_by ?? undefined,
    claimedAt: row.preview_claimed_at ? new Date(row.preview_claimed_at) : undefined,
    previewUploadTokenId: row.preview_upload_token_id ?? undefined,
  };
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
    classificationType: row.classification_type,
    itemKind: row.item_kind,
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
    sizeBytes: toSafeNumberFromDbInt(row.size_bytes, "ingestion_files.size_bytes"),
    storageKey: row.storage_key,
    status: row.status,
    checksumSha256: row.checksum_sha256 ?? undefined,
    uploadTokenId: row.upload_token_id ?? undefined,
    uploadChecksumSha256: row.upload_checksum_sha256 ?? undefined,
    previewUploadTokenId: row.preview_upload_token_id ?? undefined,
    previewStatus: row.preview_status ?? undefined,
    previewClaimedBy: row.preview_claimed_by ?? undefined,
    previewClaimedAt: row.preview_claimed_at ? new Date(row.preview_claimed_at) : undefined,
    previewStorageKey: row.preview_storage_key ?? undefined,
    previewContentType: row.preview_content_type ?? undefined,
    previewSizeBytes:
      row.preview_size_bytes === null
        ? undefined
        : toSafeNumberFromDbInt(row.preview_size_bytes, "ingestion_files.preview_size_bytes"),
    previewWidth: row.preview_width ?? undefined,
    previewHeight: row.preview_height ?? undefined,
    previewError: row.preview_error ?? undefined,
    previewGeneratedAt: row.preview_generated_at ? new Date(row.preview_generated_at) : undefined,
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
  preview_storage_key: string | null;
}): StagingCleanupCandidate {
  return {
    ingestionId: row.ingestion_id,
    tenantId: row.tenant_id,
    status: row.status,
    updatedAt: new Date(row.updated_at),
    storageKey: row.storage_key,
    previewStorageKey: row.preview_storage_key ?? undefined,
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
  classificationType: IngestionClassificationType;
  itemKind: IngestItemKind;
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
        classification_type,
        item_kind,
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
        ${params.classificationType},
        ${params.itemKind},
        ${params.languageCode},
        ${params.pipelinePreset},
        ${params.accessLevel},
        ${params.embargoUntil ? params.embargoUntil.toISOString() : null},
        ${params.rightsNote ?? null},
        ${params.sensitivityNote ?? null},
        ${params.summary ?? {}}
      )
      RETURNING id, batch_label, tenant_id, status, created_by, schema_version, classification_type, item_kind, language_code,
        pipeline_preset, access_level, embargo_until, rights_note, sensitivity_note, summary, error_summary,
        created_at, updated_at
    `;
  });

  return mapIngestion(rows[0]!);
}

export async function findIngestionById(
  tenantId: string,
  ingestionId: string,
  executor?: SqlExecutor,
): Promise<IngestionRecord | undefined> {
  const rows = await withExecutor(executor, async (sql) => {
    return await sql<IngestionRow[]>`
      SELECT id, batch_label, tenant_id, status, created_by, schema_version, classification_type, item_kind, language_code,
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

export async function findIngestionByIdForUpdate(
  tenantId: string,
  ingestionId: string,
  executor: SqlExecutor,
): Promise<IngestionRecord | undefined> {
  const rows = await executor<IngestionRow[]>`
    SELECT id, batch_label, tenant_id, status, created_by, schema_version, classification_type, item_kind, language_code,
      pipeline_preset, access_level, embargo_until, rights_note, sensitivity_note, summary, error_summary,
      created_at, updated_at
    FROM ingestions
    WHERE id = ${ingestionId}
      AND tenant_id = ${tenantId}
    FOR UPDATE
  `;

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
        SELECT id, batch_label, tenant_id, status, created_by, schema_version, classification_type, item_kind, language_code,
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
      SELECT id, batch_label, tenant_id, status, created_by, schema_version, classification_type, item_kind, language_code,
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
  executor?: SqlExecutor;
}): Promise<IngestionRecord | undefined> {
  const rows = await withExecutor(params.executor, async (sql) => {
    return await sql<IngestionRow[]>`
      UPDATE ingestions
      SET status = ${params.toStatus},
          updated_at = now()
      WHERE id = ${params.ingestionId}
        AND tenant_id = ${params.tenantId}
        AND status = ${params.fromStatus}
      RETURNING id, batch_label, tenant_id, status, created_by, schema_version, classification_type, item_kind, language_code,
        pipeline_preset, access_level, embargo_until, rights_note, sensitivity_note, summary, error_summary,
        created_at, updated_at
    `;
  });

  const row = rows[0];
  return row ? mapIngestion(row) : undefined;
}

export async function updateIngestionDetails(params: {
  ingestionId: string;
  tenantId: string;
  batchLabel?: string;
  classificationType?: IngestionClassificationType;
  itemKind?: IngestItemKind;
  languageCode?: string;
  pipelinePreset?: IngestionPipelinePreset;
  accessLevel?: AccessLevel;
  summary?: IngestionSummary;
  embargoUntil?: string | null;
  rightsNote?: string | null;
  sensitivityNote?: string | null;
  hasBatchLabel: boolean;
  hasClassificationType: boolean;
  hasItemKind: boolean;
  hasLanguageCode: boolean;
  hasPipelinePreset: boolean;
  hasAccessLevel: boolean;
  hasSummary: boolean;
  hasEmbargoUntil: boolean;
  hasRightsNote: boolean;
  hasSensitivityNote: boolean;
}): Promise<IngestionRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<IngestionRow[]>`
      UPDATE ingestions
      SET
        batch_label = CASE
          WHEN ${params.hasBatchLabel} THEN ${params.batchLabel ?? null}::text
          ELSE batch_label
        END,
        classification_type = CASE
          WHEN ${params.hasClassificationType}
            THEN ${params.classificationType ?? null}::ingestion_classification_type
          ELSE classification_type
        END,
        item_kind = CASE
          WHEN ${params.hasItemKind}
            THEN ${params.itemKind ?? null}::ingest_item_kind
          ELSE item_kind
        END,
        language_code = CASE
          WHEN ${params.hasLanguageCode} THEN ${params.languageCode ?? null}::text
          ELSE language_code
        END,
        pipeline_preset = CASE
          WHEN ${params.hasPipelinePreset}
            THEN ${params.pipelinePreset ?? null}::ingestion_pipeline_preset
          ELSE pipeline_preset
        END,
        access_level = CASE
          WHEN ${params.hasAccessLevel}
            THEN ${params.accessLevel ?? null}::object_access_level
          ELSE access_level
        END,
        summary = CASE
          WHEN ${params.hasSummary} THEN ${params.summary ?? {}}
          ELSE summary
        END,
        embargo_until = CASE
          WHEN ${params.hasEmbargoUntil} THEN ${params.embargoUntil ?? null}::timestamptz
          ELSE embargo_until
        END,
        rights_note = CASE
          WHEN ${params.hasRightsNote} THEN ${params.rightsNote ?? null}::text
          ELSE rights_note
        END,
        sensitivity_note = CASE
          WHEN ${params.hasSensitivityNote} THEN ${params.sensitivityNote ?? null}::text
          ELSE sensitivity_note
        END,
        updated_at = now()
      WHERE id = ${params.ingestionId}
        AND tenant_id = ${params.tenantId}
      RETURNING id, batch_label, tenant_id, status, created_by, schema_version, classification_type, item_kind, language_code,
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
  uploadTokenId: string;
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
        upload_token_id,
        status
      )
      VALUES (${params.id}, ${params.ingestionId}, ${params.filename}, ${params.contentType}, ${params.sizeBytes}, ${params.storageKey}, ${params.uploadTokenId}, 'PENDING')
      RETURNING id, ingestion_id, filename, content_type, size_bytes, storage_key, status, checksum_sha256, upload_token_id, upload_checksum_sha256,
        preview_status, preview_claimed_by, preview_claimed_at, preview_storage_key, preview_content_type, preview_size_bytes, preview_width,
        preview_height, preview_error, preview_generated_at, processing_overrides, error, created_at, updated_at
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
        file.upload_token_id,
        file.upload_checksum_sha256,
        file.preview_upload_token_id,
        file.preview_status,
        file.preview_claimed_by,
        file.preview_claimed_at,
        file.preview_storage_key,
        file.preview_content_type,
        file.preview_size_bytes,
        file.preview_width,
        file.preview_height,
        file.preview_error,
        file.preview_generated_at,
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
        file.upload_token_id,
        file.upload_checksum_sha256,
        file.preview_status,
        file.preview_claimed_by,
        file.preview_claimed_at,
        file.preview_storage_key,
        file.preview_content_type,
        file.preview_size_bytes,
        file.preview_width,
        file.preview_height,
        file.preview_error,
        file.preview_generated_at,
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
        AND upload_checksum_sha256 = ${params.checksumSha256}
      RETURNING id, ingestion_id, filename, content_type, size_bytes, storage_key, status, checksum_sha256, upload_token_id, upload_checksum_sha256,
        preview_status, preview_claimed_by, preview_claimed_at, preview_storage_key, preview_content_type, preview_size_bytes, preview_width,
        preview_height, preview_error, preview_generated_at, processing_overrides, error, created_at, updated_at
    `;
  });

  const row = rows[0];
  return row ? mapIngestionFile(row) : undefined;
}

export async function prepareIngestionFileUpload(params: {
  tenantId: string;
  ingestionId: string;
  fileId: string;
  storageKey: string;
  uploadTokenId: string;
}): Promise<IngestionFileRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<IngestionFileRow[]>`
      UPDATE ingestion_files file
      SET storage_key = ${params.storageKey},
          upload_token_id = ${params.uploadTokenId},
          upload_checksum_sha256 = NULL,
          updated_at = now()
      FROM ingestions ing
      WHERE file.id = ${params.fileId}
        AND file.ingestion_id = ${params.ingestionId}
        AND ing.id = file.ingestion_id
        AND ing.tenant_id = ${params.tenantId}
        AND file.status = 'PENDING'
      RETURNING file.id, file.ingestion_id, file.filename, file.content_type, file.size_bytes, file.storage_key,
        file.status, file.checksum_sha256, file.upload_token_id, file.upload_checksum_sha256,
        file.preview_status, file.preview_claimed_by, file.preview_claimed_at, file.preview_storage_key, file.preview_content_type,
        file.preview_size_bytes, file.preview_width, file.preview_height, file.preview_error, file.preview_generated_at,
        file.processing_overrides, file.error, file.created_at, file.updated_at
    `;
  });

  const row = rows[0];
  return row ? mapIngestionFile(row) : undefined;
}

export async function recordIngestionFileUpload(params: {
  tenantId: string;
  ingestionId: string;
  fileId: string;
  storageKey: string;
  uploadTokenId: string;
  checksumSha256: string;
}): Promise<IngestionFileRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<IngestionFileRow[]>`
      UPDATE ingestion_files file
      SET upload_checksum_sha256 = ${params.checksumSha256},
          updated_at = now()
      FROM ingestions ing
      WHERE file.id = ${params.fileId}
        AND file.ingestion_id = ${params.ingestionId}
        AND ing.id = file.ingestion_id
        AND ing.tenant_id = ${params.tenantId}
        AND file.status = 'PENDING'
        AND file.storage_key = ${params.storageKey}
        AND file.upload_token_id = ${params.uploadTokenId}
      RETURNING file.id, file.ingestion_id, file.filename, file.content_type, file.size_bytes, file.storage_key,
        file.status, file.checksum_sha256, file.upload_token_id, file.upload_checksum_sha256,
        file.preview_status, file.preview_claimed_by, file.preview_claimed_at, file.preview_storage_key, file.preview_content_type,
        file.preview_size_bytes, file.preview_width, file.preview_height, file.preview_error, file.preview_generated_at,
        file.processing_overrides, file.error, file.created_at, file.updated_at
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

export async function markIngestionFilePreviewPending(params: {
  ingestionId: string;
  fileId: string;
}): Promise<IngestionFileRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<IngestionFileRow[]>`
      UPDATE ingestion_files
      SET preview_status = 'pending',
          preview_claimed_by = null,
          preview_claimed_at = null,
          preview_storage_key = null,
          preview_content_type = null,
          preview_size_bytes = null,
          preview_width = null,
          preview_height = null,
          preview_error = null,
          preview_generated_at = null,
          updated_at = now()
      WHERE id = ${params.fileId}
        AND ingestion_id = ${params.ingestionId}
      RETURNING id, ingestion_id, filename, content_type, size_bytes, storage_key, status, checksum_sha256,
        preview_status, preview_claimed_by, preview_claimed_at, preview_storage_key, preview_content_type, preview_size_bytes, preview_width,
        preview_height, preview_error, preview_generated_at, processing_overrides, error, created_at, updated_at
    `;
  });

  const row = rows[0];
  return row ? mapIngestionFile(row) : undefined;
}

export async function markIngestionFilePreviewUnsupported(params: {
  ingestionId: string;
  fileId: string;
}): Promise<IngestionFileRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<IngestionFileRow[]>`
      UPDATE ingestion_files
      SET preview_status = 'unsupported',
          preview_claimed_by = null,
          preview_claimed_at = null,
          preview_storage_key = null,
          preview_content_type = null,
          preview_size_bytes = null,
          preview_width = null,
          preview_height = null,
          preview_error = null,
          preview_generated_at = null,
          updated_at = now()
      WHERE id = ${params.fileId}
        AND ingestion_id = ${params.ingestionId}
      RETURNING id, ingestion_id, filename, content_type, size_bytes, storage_key, status, checksum_sha256,
        preview_status, preview_claimed_by, preview_claimed_at, preview_storage_key, preview_content_type, preview_size_bytes, preview_width,
        preview_height, preview_error, preview_generated_at, processing_overrides, error, created_at, updated_at
    `;
  });

  const row = rows[0];
  return row ? mapIngestionFile(row) : undefined;
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
        file.storage_key, file.status, file.checksum_sha256, file.preview_status,
        file.preview_claimed_by, file.preview_claimed_at, file.preview_storage_key, file.preview_content_type, file.preview_size_bytes,
        file.preview_width, file.preview_height, file.preview_error, file.preview_generated_at,
        file.processing_overrides, file.error, file.created_at, file.updated_at
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
        preview_storage_key: string | null;
      }>
    >`
      SELECT
        ing.id AS ingestion_id,
        ing.tenant_id,
        ing.status,
        ing.updated_at,
        file.storage_key,
        file.preview_storage_key
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
  executor?: SqlExecutor;
}): Promise<IngestionWithCreatorRecord | undefined> {
  const rows = await withExecutor(params.executor, async (sql) => {
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
        ing.classification_type,
        ing.item_kind,
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
export async function claimNextPendingIngestionPreview(params: {
  workerId?: string;
  claimTimeoutMinutes: number;
}): Promise<ClaimedIngestionPreviewRecord | undefined> {
  const claimedBy = params.workerId?.trim() || "worker";
  const rows = await withSchemaClient(async (sql) => {
    return await sql<
      Array<{
        ingestion_id: string;
        tenant_id: string;
        batch_label: string;
        file_id: string;
        filename: string;
        content_type: string;
        size_bytes: DbInt;
        storage_key: string;
        preview_claimed_by: string | null;
        preview_claimed_at: Date | null;
        preview_upload_token_id: string | null;
      }>
    >`
      WITH candidate AS (
        SELECT file.id
        FROM ingestion_files file
        INNER JOIN ingestions ing ON ing.id = file.ingestion_id
        WHERE file.status = 'UPLOADED'
          AND file.preview_status IN ('pending', 'processing')
          AND ing.status IN ('DRAFT', 'UPLOADING', 'CANCELED')
          AND (
            file.preview_status = 'pending'
            OR file.preview_claimed_at IS NULL
            OR file.preview_claimed_at <= now() - (${params.claimTimeoutMinutes}::int * interval '1 minute')
          )
        ORDER BY file.updated_at ASC, file.id ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      )
      UPDATE ingestion_files file
      SET preview_status = 'processing',
          preview_claimed_by = ${claimedBy},
          preview_claimed_at = now(),
          updated_at = now()
      FROM candidate, ingestions ing
      WHERE file.id = candidate.id
        AND ing.id = file.ingestion_id
      RETURNING file.ingestion_id, ing.tenant_id, ing.batch_label, file.id AS file_id,
        file.filename, file.content_type, file.size_bytes, file.storage_key,
        file.preview_claimed_by, file.preview_claimed_at, file.preview_upload_token_id
    `;
  });

  const row = rows[0];
  return row ? mapClaimedIngestionPreview(row) : undefined;
}

export async function findClaimedIngestionPreview(params: {
  ingestionId: string;
  fileId: string;
  workerId?: string;
}): Promise<ClaimedIngestionPreviewRecord | undefined> {
  const claimedBy = params.workerId?.trim() || "worker";
  const rows = await withSchemaClient(async (sql) => {
    return await sql<
      Array<{
        ingestion_id: string;
        tenant_id: string;
        batch_label: string;
        file_id: string;
        filename: string;
        content_type: string;
        size_bytes: DbInt;
        storage_key: string;
        preview_claimed_by: string | null;
        preview_claimed_at: Date | null;
        preview_upload_token_id: string | null;
      }>
    >`
      SELECT file.ingestion_id, ing.tenant_id, ing.batch_label, file.id AS file_id,
        file.filename, file.content_type, file.size_bytes, file.storage_key,
        file.preview_claimed_by, file.preview_claimed_at, file.preview_upload_token_id
      FROM ingestion_files file
      INNER JOIN ingestions ing ON ing.id = file.ingestion_id
      WHERE file.ingestion_id = ${params.ingestionId}
        AND file.id = ${params.fileId}
        AND file.preview_status = 'processing'
        AND file.preview_claimed_by = ${claimedBy}
      LIMIT 1
    `;
  });

  const row = rows[0];
  return row ? mapClaimedIngestionPreview(row) : undefined;
}

export async function updateIngestionPreviewUpload(params: {
  ingestionId: string;
  fileId: string;
  workerId?: string;
  storageKey: string;
  contentType: string;
  uploadTokenId: string;
}): Promise<IngestionFileRecord | undefined> {
  const claimedBy = params.workerId?.trim() || "worker";
  const rows = await withSchemaClient(async (sql) => {
    return await sql<IngestionFileRow[]>`
      UPDATE ingestion_files
      SET preview_storage_key = ${params.storageKey},
          preview_content_type = ${params.contentType},
          preview_upload_token_id = ${params.uploadTokenId},
          updated_at = now()
      WHERE id = ${params.fileId}
        AND ingestion_id = ${params.ingestionId}
        AND preview_status = 'processing'
        AND preview_claimed_by = ${claimedBy}
      RETURNING id, ingestion_id, filename, content_type, size_bytes, storage_key, status, checksum_sha256,
        preview_status, preview_claimed_by, preview_claimed_at, preview_storage_key, preview_content_type, preview_size_bytes, preview_width,
        preview_height, preview_error, preview_generated_at, processing_overrides, error, created_at, updated_at
    `;
  });

  const row = rows[0];
  return row ? mapIngestionFile(row) : undefined;
}

export async function markIngestionFilePreviewReady(params: {
  ingestionId: string;
  fileId: string;
  workerId?: string;
  storageKey: string;
  contentType: string;
  sizeBytes: number;
  uploadTokenId: string;
  width?: number;
  height?: number;
}): Promise<IngestionFileRecord | undefined> {
  const claimedBy = params.workerId?.trim() || "worker";
  const rows = await withSchemaClient(async (sql) => {
    return await sql<IngestionFileRow[]>`
      UPDATE ingestion_files
      SET preview_status = 'ready',
          preview_claimed_by = null,
          preview_claimed_at = null,
          preview_storage_key = ${params.storageKey},
          preview_content_type = ${params.contentType},
          preview_size_bytes = ${params.sizeBytes},
          preview_width = ${params.width ?? null},
          preview_height = ${params.height ?? null},
          preview_error = null,
          preview_generated_at = now(),
          updated_at = now()
      WHERE id = ${params.fileId}
        AND ingestion_id = ${params.ingestionId}
        AND preview_status = 'processing'
        AND preview_claimed_by = ${claimedBy}
        AND preview_upload_token_id = ${params.uploadTokenId}
      RETURNING id, ingestion_id, filename, content_type, size_bytes, storage_key, status, checksum_sha256,
        preview_status, preview_claimed_by, preview_claimed_at, preview_storage_key, preview_content_type, preview_size_bytes, preview_width,
        preview_height, preview_error, preview_generated_at, processing_overrides, error, created_at, updated_at
    `;
  });

  const row = rows[0];
  return row ? mapIngestionFile(row) : undefined;
}

export async function markIngestionFilePreviewFailed(params: {
  ingestionId: string;
  fileId: string;
  workerId?: string;
  error: JsonObject;
}): Promise<IngestionFileRecord | undefined> {
  const claimedBy = params.workerId?.trim() || "worker";
  const rows = await withSchemaClient(async (sql) => {
    return await sql<IngestionFileRow[]>`
      UPDATE ingestion_files
      SET preview_status = 'failed',
          preview_claimed_by = null,
          preview_claimed_at = null,
          preview_upload_token_id = null,
          preview_storage_key = null,
          preview_content_type = null,
          preview_size_bytes = null,
          preview_width = null,
          preview_height = null,
          preview_error = ${params.error},
          preview_generated_at = null,
          updated_at = now()
      WHERE id = ${params.fileId}
        AND ingestion_id = ${params.ingestionId}
        AND preview_status = 'processing'
        AND preview_claimed_by = ${claimedBy}
      RETURNING id, ingestion_id, filename, content_type, size_bytes, storage_key, status, checksum_sha256,
        preview_status, preview_claimed_by, preview_claimed_at, preview_storage_key, preview_content_type, preview_size_bytes, preview_width,
        preview_height, preview_error, preview_generated_at, processing_overrides, error, created_at, updated_at
    `;
  });

  const row = rows[0];
  return row ? mapIngestionFile(row) : undefined;
}
