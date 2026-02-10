import { db, qualifiedTableName } from "../db/runtime.ts";
import type { IngestionStatus } from "../domain/ingestions/state-machine.ts";

type IngestionFileStatus = "PENDING" | "UPLOADED" | "VALIDATED" | "FAILED";

interface IngestionRow {
  id: string;
  upload_id: string;
  tenant_id: string;
  status: IngestionStatus;
  created_by: string;
  summary: unknown;
  error_summary: unknown;
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
  error: unknown;
  created_at: Date;
  updated_at: Date;
}

export interface IngestionRecord {
  id: string;
  uploadId: string;
  tenantId: string;
  status: IngestionStatus;
  createdBy: string;
  summary: unknown;
  errorSummary: unknown;
  createdAt: Date;
  updatedAt: Date;
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
  error: unknown;
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
    uploadId: row.upload_id,
    tenantId: row.tenant_id,
    status: row.status,
    createdBy: row.created_by,
    summary: row.summary,
    errorSummary: row.error_summary,
    createdAt: new Date(row.created_at),
    updatedAt: new Date(row.updated_at),
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
  uploadId: string;
  tenantId: string;
  createdBy: string;
}): Promise<IngestionRecord> {
  const sql = db();
  const ingestionsTable = qualifiedTableName("ingestions");

  const rows = (await sql.unsafe(
    `
      INSERT INTO ${ingestionsTable} (
        id,
        upload_id,
        tenant_id,
        status,
        created_by
      )
      VALUES ($1, $2, $3, 'DRAFT', $4)
      RETURNING id, upload_id, tenant_id, status, created_by, summary, error_summary, created_at, updated_at
    `,
    [params.id, params.uploadId, params.tenantId, params.createdBy],
  )) as IngestionRow[];

  return mapIngestion(rows[0]!);
}

export async function findIngestionById(tenantId: string, ingestionId: string): Promise<IngestionRecord | undefined> {
  const sql = db();
  const ingestionsTable = qualifiedTableName("ingestions");

  const rows = (await sql.unsafe(
    `
      SELECT id, upload_id, tenant_id, status, created_by, summary, error_summary, created_at, updated_at
      FROM ${ingestionsTable}
      WHERE id = $1
        AND tenant_id = $2
      LIMIT 1
    `,
    [ingestionId, tenantId],
  )) as IngestionRow[];

  const row = rows[0];
  return row ? mapIngestion(row) : undefined;
}

export async function listIngestions(params: {
  tenantId: string;
  limit: number;
  cursorCreatedAt?: string;
  cursorId?: string;
}): Promise<IngestionRecord[]> {
  const sql = db();
  const ingestionsTable = qualifiedTableName("ingestions");

  const values: Array<string | number> = [params.tenantId, params.limit];
  let cursorClause = "";

  if (params.cursorCreatedAt && params.cursorId) {
    values.push(params.cursorCreatedAt, params.cursorId);
    cursorClause = "AND (created_at, id) < ($3::timestamptz, $4::uuid)";
  }

  const rows = (await sql.unsafe(
    `
      SELECT id, upload_id, tenant_id, status, created_by, summary, error_summary, created_at, updated_at
      FROM ${ingestionsTable}
      WHERE tenant_id = $1
        ${cursorClause}
      ORDER BY created_at DESC, id DESC
      LIMIT $2
    `,
    values,
  )) as IngestionRow[];

  return rows.map(mapIngestion);
}

export async function updateIngestionStatus(params: {
  ingestionId: string;
  tenantId: string;
  status: IngestionStatus;
}): Promise<IngestionRecord | undefined> {
  const sql = db();
  const ingestionsTable = qualifiedTableName("ingestions");

  const rows = (await sql.unsafe(
    `
      UPDATE ${ingestionsTable}
      SET status = $1,
          updated_at = now()
      WHERE id = $2
        AND tenant_id = $3
      RETURNING id, upload_id, tenant_id, status, created_by, summary, error_summary, created_at, updated_at
    `,
    [params.status, params.ingestionId, params.tenantId],
  )) as IngestionRow[];

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
  const sql = db();
  const filesTable = qualifiedTableName("ingestion_files");

  const rows = (await sql.unsafe(
    `
      INSERT INTO ${filesTable} (
        id,
        ingestion_id,
        filename,
        content_type,
        size_bytes,
        storage_key,
        status
      )
      VALUES ($1, $2, $3, $4, $5, $6, 'PENDING')
      RETURNING id, ingestion_id, filename, content_type, size_bytes, storage_key, status, checksum_sha256, error, created_at, updated_at
    `,
    [params.id, params.ingestionId, params.filename, params.contentType, params.sizeBytes, params.storageKey],
  )) as IngestionFileRow[];

  return mapIngestionFile(rows[0]!);
}

export async function findIngestionFileById(params: {
  tenantId: string;
  ingestionId: string;
  fileId: string;
}): Promise<IngestionFileRecord | undefined> {
  const sql = db();
  const filesTable = qualifiedTableName("ingestion_files");
  const ingestionsTable = qualifiedTableName("ingestions");

  const rows = (await sql.unsafe(
    `
      SELECT
        f.id,
        f.ingestion_id,
        f.filename,
        f.content_type,
        f.size_bytes,
        f.storage_key,
        f.status,
        f.checksum_sha256,
        f.error,
        f.created_at,
        f.updated_at
      FROM ${filesTable} f
      INNER JOIN ${ingestionsTable} i ON i.id = f.ingestion_id
      WHERE f.id = $1
        AND f.ingestion_id = $2
        AND i.tenant_id = $3
      LIMIT 1
    `,
    [params.fileId, params.ingestionId, params.tenantId],
  )) as IngestionFileRow[];

  const row = rows[0];
  return row ? mapIngestionFile(row) : undefined;
}

export async function listIngestionFiles(params: {
  tenantId: string;
  ingestionId: string;
}): Promise<IngestionFileRecord[]> {
  const sql = db();
  const filesTable = qualifiedTableName("ingestion_files");
  const ingestionsTable = qualifiedTableName("ingestions");

  const rows = (await sql.unsafe(
    `
      SELECT
        f.id,
        f.ingestion_id,
        f.filename,
        f.content_type,
        f.size_bytes,
        f.storage_key,
        f.status,
        f.checksum_sha256,
        f.error,
        f.created_at,
        f.updated_at
      FROM ${filesTable} f
      INNER JOIN ${ingestionsTable} i ON i.id = f.ingestion_id
      WHERE f.ingestion_id = $1
        AND i.tenant_id = $2
      ORDER BY f.created_at ASC, f.id ASC
    `,
    [params.ingestionId, params.tenantId],
  )) as IngestionFileRow[];

  return rows.map(mapIngestionFile);
}

export async function markIngestionFileUploaded(params: {
  fileId: string;
  checksumSha256: string;
}): Promise<IngestionFileRecord | undefined> {
  const sql = db();
  const filesTable = qualifiedTableName("ingestion_files");

  const rows = (await sql.unsafe(
    `
      UPDATE ${filesTable}
      SET status = 'UPLOADED',
          checksum_sha256 = $2,
          updated_at = now()
      WHERE id = $1
      RETURNING id, ingestion_id, filename, content_type, size_bytes, storage_key, status, checksum_sha256, error, created_at, updated_at
    `,
    [params.fileId, params.checksumSha256],
  )) as IngestionFileRow[];

  const row = rows[0];
  return row ? mapIngestionFile(row) : undefined;
}

export async function listStagingCleanupCandidates(params: {
  completedRetentionDays: number;
  failedCanceledRetentionDays: number;
}): Promise<StagingCleanupCandidate[]> {
  const sql = db();
  const ingestionsTable = qualifiedTableName("ingestions");
  const filesTable = qualifiedTableName("ingestion_files");

  const rows = (await sql.unsafe(
    `
      SELECT
        i.id AS ingestion_id,
        i.tenant_id,
        i.status,
        i.updated_at,
        f.storage_key
      FROM ${ingestionsTable} i
      INNER JOIN ${filesTable} f ON f.ingestion_id = i.id
      WHERE (
        i.status = 'COMPLETED'
        AND i.updated_at <= now() - ($1::int * interval '1 day')
      )
      OR (
        i.status IN ('FAILED', 'CANCELED')
        AND i.updated_at <= now() - ($2::int * interval '1 day')
      )
    `,
    [params.completedRetentionDays, params.failedCanceledRetentionDays],
  )) as Array<{
    ingestion_id: string;
    tenant_id: string;
    status: IngestionStatus;
    updated_at: Date;
    storage_key: string;
  }>;

  return rows.map(mapStagingCleanupCandidate);
}

export async function listStuckIngestions(params: {
  thresholdMinutes: number;
}): Promise<StuckIngestionRecord[]> {
  const sql = db();
  const ingestionsTable = qualifiedTableName("ingestions");

  const rows = (await sql.unsafe(
    `
      SELECT id, tenant_id, status, updated_at, created_by
      FROM ${ingestionsTable}
      WHERE status IN ('UPLOADING', 'PROCESSING')
        AND updated_at <= now() - ($1::int * interval '1 minute')
      ORDER BY updated_at ASC
    `,
    [params.thresholdMinutes],
  )) as Array<{
    id: string;
    tenant_id: string;
    status: IngestionStatus;
    updated_at: Date;
    created_by: string;
  }>;

  return rows.map(mapStuckIngestion);
}
