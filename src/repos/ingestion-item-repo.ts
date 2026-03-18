import { withSchemaClient } from "../db/client.ts";
import { toSafeNumberFromDbInt, type DbInt } from "../db/number.ts";
import type {
  IngestionClassificationType,
  IngestItemKind,
  JsonObject,
} from "../validation/ingestion.ts";

type IngestionItemStatus =
  | "PENDING"
  | "READY"
  | "PROCESSING"
  | "COMPLETED"
  | "FAILED"
  | "SKIPPED";

type IngestionItemFileRole =
  | "primary"
  | "front"
  | "back"
  | "page"
  | "attachment"
  | "transcript_source"
  | "side_a"
  | "side_b"
  | "other";

interface IngestionItemRow {
  id: string;
  ingestion_id: string;
  item_index: number;
  status: IngestionItemStatus;
  classification_type: IngestionClassificationType | null;
  item_kind: IngestItemKind | null;
  language_code: string | null;
  title: string | null;
  summary: JsonObject;
  error_summary: JsonObject;
  object_id: string | null;
  created_at: Date;
  updated_at: Date;
}

interface IngestionItemFileRow {
  id: string;
  ingestion_item_id: string;
  ingestion_file_id: string;
  ingestion_id: string;
  role: IngestionItemFileRole;
  sort_order: number;
  page_number: number | null;
  is_primary: boolean;
  logical_label: string | null;
  created_at: Date;
}

interface IngestionItemSummaryRow {
  total_count: number;
  completed_count: number;
  failed_count: number;
  skipped_count: number;
}

interface LeasedIngestionFileRow {
  id: string;
  filename: string;
  storage_key: string;
  content_type: string;
  size_bytes: DbInt;
  checksum_sha256: string | null;
  processing_overrides: Record<string, unknown>;
  status: "PENDING" | "UPLOADED" | "VALIDATED" | "FAILED";
  ingestion_item_id: string | null;
  item_index: number | null;
  sort_order: number | null;
}

export interface IngestionItemRecord {
  id: string;
  ingestionId: string;
  itemIndex: number;
  status: IngestionItemStatus;
  classificationType?: IngestionClassificationType;
  itemKind?: IngestItemKind;
  languageCode?: string;
  title?: string;
  summary: JsonObject;
  errorSummary: JsonObject;
  objectId?: string;
  createdAt: Date;
  updatedAt: Date;
}

export interface IngestionItemFileRecord {
  id: string;
  ingestionItemId: string;
  ingestionFileId: string;
  ingestionId: string;
  role: IngestionItemFileRole;
  sortOrder: number;
  pageNumber?: number;
  isPrimary: boolean;
  logicalLabel?: string;
  createdAt: Date;
}

export interface IngestionItemSummaryRecord {
  totalCount: number;
  completedCount: number;
  failedCount: number;
  skippedCount: number;
}

export interface LeasedIngestionFileRecord {
  id: string;
  filename: string;
  storageKey: string;
  contentType: string;
  sizeBytes: number;
  checksumSha256: string | null;
  processingOverrides: Record<string, unknown>;
  status: "PENDING" | "UPLOADED" | "VALIDATED" | "FAILED";
  ingestionItemId?: string;
  itemIndex?: number;
  sortOrder?: number;
}

function mapIngestionItem(row: IngestionItemRow): IngestionItemRecord {
  return {
    id: row.id,
    ingestionId: row.ingestion_id,
    itemIndex: row.item_index,
    status: row.status,
    classificationType: row.classification_type ?? undefined,
    itemKind: row.item_kind ?? undefined,
    languageCode: row.language_code ?? undefined,
    title: row.title ?? undefined,
    summary: row.summary,
    errorSummary: row.error_summary,
    objectId: row.object_id ?? undefined,
    createdAt: new Date(row.created_at),
    updatedAt: new Date(row.updated_at),
  };
}

function mapIngestionItemFile(row: IngestionItemFileRow): IngestionItemFileRecord {
  return {
    id: row.id,
    ingestionItemId: row.ingestion_item_id,
    ingestionFileId: row.ingestion_file_id,
    ingestionId: row.ingestion_id,
    role: row.role,
    sortOrder: row.sort_order,
    pageNumber: row.page_number ?? undefined,
    isPrimary: row.is_primary,
    logicalLabel: row.logical_label ?? undefined,
    createdAt: new Date(row.created_at),
  };
}

function mapLeasedIngestionFile(row: LeasedIngestionFileRow): LeasedIngestionFileRecord {
  return {
    id: row.id,
    filename: row.filename,
    storageKey: row.storage_key,
    contentType: row.content_type,
    sizeBytes: toSafeNumberFromDbInt(row.size_bytes, "ingestion_files.size_bytes"),
    checksumSha256: row.checksum_sha256,
    processingOverrides: row.processing_overrides,
    status: row.status,
    ingestionItemId: row.ingestion_item_id ?? undefined,
    itemIndex: row.item_index ?? undefined,
    sortOrder: row.sort_order ?? undefined,
  };
}

export async function createIngestionItem(params: {
  id: string;
  tenantId: string;
  ingestionId: string;
  itemIndex: number;
  classificationType?: IngestionClassificationType;
  itemKind?: IngestItemKind;
  languageCode?: string;
  title?: string;
  summary?: JsonObject;
}): Promise<IngestionItemRecord> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<IngestionItemRow[]>`
      INSERT INTO ingestion_items (
        id,
        ingestion_id,
        item_index,
        classification_type,
        item_kind,
        language_code,
        title,
        summary
      )
      SELECT
        ${params.id},
        ${params.ingestionId},
        ${params.itemIndex},
        ${params.classificationType ?? null}::ingestion_classification_type,
        ${params.itemKind ?? null}::ingest_item_kind,
        ${params.languageCode ?? null},
        ${params.title ?? null},
        ${params.summary ?? {}}
      WHERE EXISTS (
        SELECT 1
        FROM ingestions ing
        WHERE ing.id = ${params.ingestionId}
          AND ing.tenant_id = ${params.tenantId}
      )
      RETURNING
        id,
        ingestion_id,
        item_index,
        status,
        classification_type,
        item_kind,
        language_code,
        title,
        summary,
        error_summary,
        object_id,
        created_at,
        updated_at
    `;
  });

  return mapIngestionItem(rows[0]!);
}

export async function findIngestionItemById(params: {
  tenantId: string;
  ingestionId: string;
  ingestionItemId: string;
}): Promise<IngestionItemRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<IngestionItemRow[]>`
      SELECT
        item.id,
        item.ingestion_id,
        item.item_index,
        item.status,
        item.classification_type,
        item.item_kind,
        item.language_code,
        item.title,
        item.summary,
        item.error_summary,
        item.object_id,
        item.created_at,
        item.updated_at
      FROM ingestion_items item
      INNER JOIN ingestions ing ON ing.id = item.ingestion_id
      WHERE item.id = ${params.ingestionItemId}
        AND item.ingestion_id = ${params.ingestionId}
        AND ing.tenant_id = ${params.tenantId}
      LIMIT 1
    `;
  });

  const row = rows[0];
  return row ? mapIngestionItem(row) : undefined;
}

export async function listIngestionItems(params: {
  tenantId: string;
  ingestionId: string;
}): Promise<IngestionItemRecord[]> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<IngestionItemRow[]>`
      SELECT
        item.id,
        item.ingestion_id,
        item.item_index,
        item.status,
        item.classification_type,
        item.item_kind,
        item.language_code,
        item.title,
        item.summary,
        item.error_summary,
        item.object_id,
        item.created_at,
        item.updated_at
      FROM ingestion_items item
      INNER JOIN ingestions ing ON ing.id = item.ingestion_id
      WHERE item.ingestion_id = ${params.ingestionId}
        AND ing.tenant_id = ${params.tenantId}
      ORDER BY item.item_index ASC, item.id ASC
    `;
  });

  return rows.map(mapIngestionItem);
}

export async function updateIngestionItem(params: {
  tenantId: string;
  ingestionId: string;
  ingestionItemId: string;
  classificationType?: IngestionClassificationType;
  itemKind?: IngestItemKind;
  languageCode?: string;
  title?: string | null;
  summary?: JsonObject;
}): Promise<IngestionItemRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<IngestionItemRow[]>`
      UPDATE ingestion_items item
      SET classification_type = COALESCE(
            ${params.classificationType ?? null}::ingestion_classification_type,
            item.classification_type
          ),
          item_kind = COALESCE(
            ${params.itemKind ?? null}::ingest_item_kind,
            item.item_kind
          ),
          language_code = COALESCE(${params.languageCode ?? null}, item.language_code),
          title = CASE
            WHEN ${params.title === null}::boolean THEN NULL
            ELSE COALESCE(${params.title ?? null}, item.title)
          END,
          summary = COALESCE(${params.summary ?? null}, item.summary),
          updated_at = now()
      FROM ingestions ing
      WHERE item.id = ${params.ingestionItemId}
        AND item.ingestion_id = ${params.ingestionId}
        AND ing.id = item.ingestion_id
        AND ing.tenant_id = ${params.tenantId}
      RETURNING
        item.id,
        item.ingestion_id,
        item.item_index,
        item.status,
        item.classification_type,
        item.item_kind,
        item.language_code,
        item.title,
        item.summary,
        item.error_summary,
        item.object_id,
        item.created_at,
        item.updated_at
    `;
  });

  const row = rows[0];
  return row ? mapIngestionItem(row) : undefined;
}

export async function reorderIngestionItems(params: {
  tenantId: string;
  ingestionId: string;
  items: Array<{
    ingestionItemId: string;
    itemIndex: number;
  }>;
}): Promise<IngestionItemRecord[]> {
  return withSchemaClient(async (sql) => {
    return sql.begin(async (transaction) => {
      const existingRows = await transaction<Array<{ id: string }>>`
        SELECT item.id
        FROM ingestion_items item
        INNER JOIN ingestions ing ON ing.id = item.ingestion_id
        WHERE item.ingestion_id = ${params.ingestionId}
          AND ing.tenant_id = ${params.tenantId}
      `;

      const existingIds = new Set(existingRows.map((row) => row.id));
      const providedIds = new Set(params.items.map((row) => row.ingestionItemId));

      if (existingIds.size !== providedIds.size) {
        return [];
      }

      for (const existingId of existingIds) {
        if (!providedIds.has(existingId)) {
          return [];
        }
      }

      await transaction`
        UPDATE ingestion_items
        SET item_index = item_index + 1000000
        WHERE ingestion_id = ${params.ingestionId}
      `;

      for (const item of params.items) {
        await transaction`
          UPDATE ingestion_items
          SET item_index = ${item.itemIndex}
          WHERE ingestion_id = ${params.ingestionId}
            AND id = ${item.ingestionItemId}
        `;
      }

      const rows = await transaction<IngestionItemRow[]>`
        SELECT
          item.id,
          item.ingestion_id,
          item.item_index,
          item.status,
          item.classification_type,
          item.item_kind,
          item.language_code,
          item.title,
          item.summary,
          item.error_summary,
          item.object_id,
          item.created_at,
          item.updated_at
        FROM ingestion_items item
        INNER JOIN ingestions ing ON ing.id = item.ingestion_id
        WHERE item.ingestion_id = ${params.ingestionId}
          AND ing.tenant_id = ${params.tenantId}
        ORDER BY item.item_index ASC, item.id ASC
      `;

      return rows.map(mapIngestionItem);
    });
  });
}

export async function updateIngestionItemStatus(params: {
  tenantId: string;
  ingestionId: string;
  ingestionItemId: string;
  fromStatus: IngestionItemStatus;
  toStatus: IngestionItemStatus;
}): Promise<IngestionItemRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<IngestionItemRow[]>`
      UPDATE ingestion_items item
      SET status = ${params.toStatus}::ingestion_item_status,
          updated_at = now()
      FROM ingestions ing
      WHERE item.id = ${params.ingestionItemId}
        AND item.ingestion_id = ${params.ingestionId}
        AND item.status = ${params.fromStatus}::ingestion_item_status
        AND ing.id = item.ingestion_id
        AND ing.tenant_id = ${params.tenantId}
      RETURNING
        item.id,
        item.ingestion_id,
        item.item_index,
        item.status,
        item.classification_type,
        item.item_kind,
        item.language_code,
        item.title,
        item.summary,
        item.error_summary,
        item.object_id,
        item.created_at,
        item.updated_at
    `;
  });

  const row = rows[0];
  return row ? mapIngestionItem(row) : undefined;
}

export async function setIngestionItemStatus(params: {
  tenantId: string;
  ingestionId: string;
  ingestionItemId: string;
  toStatus: IngestionItemStatus;
  objectId?: string;
}): Promise<IngestionItemRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<IngestionItemRow[]>`
      UPDATE ingestion_items item
      SET status = ${params.toStatus}::ingestion_item_status,
          object_id = COALESCE(${params.objectId ?? null}, item.object_id),
          updated_at = now()
      FROM ingestions ing
      WHERE item.id = ${params.ingestionItemId}
        AND item.ingestion_id = ${params.ingestionId}
        AND ing.id = item.ingestion_id
        AND ing.tenant_id = ${params.tenantId}
      RETURNING
        item.id,
        item.ingestion_id,
        item.item_index,
        item.status,
        item.classification_type,
        item.item_kind,
        item.language_code,
        item.title,
        item.summary,
        item.error_summary,
        item.object_id,
        item.created_at,
        item.updated_at
    `;
  });

  const row = rows[0];
  return row ? mapIngestionItem(row) : undefined;
}

export async function summarizeIngestionItems(params: {
  tenantId: string;
  ingestionId: string;
}): Promise<IngestionItemSummaryRecord> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<IngestionItemSummaryRow[]>`
      SELECT
        COUNT(*)::int AS total_count,
        COUNT(*) FILTER (WHERE item.status = 'COMPLETED')::int AS completed_count,
        COUNT(*) FILTER (WHERE item.status = 'FAILED')::int AS failed_count,
        COUNT(*) FILTER (WHERE item.status = 'SKIPPED')::int AS skipped_count
      FROM ingestion_items item
      INNER JOIN ingestions ing ON ing.id = item.ingestion_id
      WHERE item.ingestion_id = ${params.ingestionId}
        AND ing.tenant_id = ${params.tenantId}
    `;
  });

  const row = rows[0] ?? {
    total_count: 0,
    completed_count: 0,
    failed_count: 0,
    skipped_count: 0,
  };

  return {
    totalCount: row.total_count,
    completedCount: row.completed_count,
    failedCount: row.failed_count,
    skippedCount: row.skipped_count,
  };
}

export async function createIngestionItemFile(params: {
  id: string;
  tenantId: string;
  ingestionId: string;
  ingestionItemId: string;
  ingestionFileId: string;
  sortOrder: number;
  role?: IngestionItemFileRole;
  pageNumber?: number;
  isPrimary?: boolean;
  logicalLabel?: string;
}): Promise<IngestionItemFileRecord> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<IngestionItemFileRow[]>`
      INSERT INTO ingestion_item_files (
        id,
        ingestion_item_id,
        ingestion_file_id,
        ingestion_id,
        role,
        sort_order,
        page_number,
        is_primary,
        logical_label
      )
      SELECT
        ${params.id},
        ${params.ingestionItemId},
        ${params.ingestionFileId},
        ${params.ingestionId},
        ${params.role ?? "primary"}::ingestion_item_file_role,
        ${params.sortOrder},
        ${params.pageNumber ?? null},
        ${params.isPrimary ?? false},
        ${params.logicalLabel ?? null}
      WHERE EXISTS (
        SELECT 1
        FROM ingestions ing
        WHERE ing.id = ${params.ingestionId}
          AND ing.tenant_id = ${params.tenantId}
      )
      RETURNING
        id,
        ingestion_item_id,
        ingestion_file_id,
        ingestion_id,
        role,
        sort_order,
        page_number,
        is_primary,
        logical_label,
        created_at
    `;
  });

  return mapIngestionItemFile(rows[0]!);
}

export async function listIngestionItemFiles(params: {
  tenantId: string;
  ingestionId: string;
  ingestionItemId: string;
}): Promise<IngestionItemFileRecord[]> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<IngestionItemFileRow[]>`
      SELECT
        link.id,
        link.ingestion_item_id,
        link.ingestion_file_id,
        link.ingestion_id,
        link.role,
        link.sort_order,
        link.page_number,
        link.is_primary,
        link.logical_label,
        link.created_at
      FROM ingestion_item_files link
      INNER JOIN ingestions ing ON ing.id = link.ingestion_id
      WHERE link.ingestion_id = ${params.ingestionId}
        AND link.ingestion_item_id = ${params.ingestionItemId}
        AND ing.tenant_id = ${params.tenantId}
      ORDER BY link.sort_order ASC, link.id ASC
    `;
  });

  return rows.map(mapIngestionItemFile);
}

export async function reorderIngestionItemFiles(params: {
  tenantId: string;
  ingestionId: string;
  ingestionItemId: string;
  files: Array<{
    ingestionFileId: string;
    sortOrder: number;
  }>;
}): Promise<IngestionItemFileRecord[]> {
  return withSchemaClient(async (sql) => {
    return sql.begin(async (transaction) => {
      const existingRows = await transaction<Array<{ ingestion_file_id: string }>>`
        SELECT link.ingestion_file_id
        FROM ingestion_item_files link
        INNER JOIN ingestions ing ON ing.id = link.ingestion_id
        WHERE link.ingestion_id = ${params.ingestionId}
          AND link.ingestion_item_id = ${params.ingestionItemId}
          AND ing.tenant_id = ${params.tenantId}
      `;

      const existingIds = new Set(existingRows.map((row) => row.ingestion_file_id));
      const providedIds = new Set(params.files.map((row) => row.ingestionFileId));

      if (existingIds.size !== providedIds.size) {
        return [];
      }

      for (const existingId of existingIds) {
        if (!providedIds.has(existingId)) {
          return [];
        }
      }

      await transaction`
        UPDATE ingestion_item_files
        SET sort_order = sort_order + 1000000
        WHERE ingestion_id = ${params.ingestionId}
          AND ingestion_item_id = ${params.ingestionItemId}
      `;

      for (const file of params.files) {
        await transaction`
          UPDATE ingestion_item_files
          SET sort_order = ${file.sortOrder}
          WHERE ingestion_id = ${params.ingestionId}
            AND ingestion_item_id = ${params.ingestionItemId}
            AND ingestion_file_id = ${file.ingestionFileId}
        `;
      }

      const rows = await transaction<IngestionItemFileRow[]>`
        SELECT
          link.id,
          link.ingestion_item_id,
          link.ingestion_file_id,
          link.ingestion_id,
          link.role,
          link.sort_order,
          link.page_number,
          link.is_primary,
          link.logical_label,
          link.created_at
        FROM ingestion_item_files link
        INNER JOIN ingestions ing ON ing.id = link.ingestion_id
        WHERE link.ingestion_id = ${params.ingestionId}
          AND link.ingestion_item_id = ${params.ingestionItemId}
          AND ing.tenant_id = ${params.tenantId}
        ORDER BY link.sort_order ASC, link.id ASC
      `;

      return rows.map(mapIngestionItemFile);
    });
  });
}

export async function listLeasedIngestionFiles(params: {
  tenantId: string;
  ingestionId: string;
}): Promise<LeasedIngestionFileRecord[]> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<LeasedIngestionFileRow[]>`
      SELECT
        file.id,
        file.filename,
        file.storage_key,
        file.content_type,
        file.size_bytes,
        file.checksum_sha256,
        file.processing_overrides,
        file.status,
        link.ingestion_item_id,
        item.item_index,
        link.sort_order
      FROM ingestion_files file
      INNER JOIN ingestions ing ON ing.id = file.ingestion_id
      LEFT JOIN ingestion_item_files link ON link.ingestion_file_id = file.id
      LEFT JOIN ingestion_items item ON item.id = link.ingestion_item_id
      WHERE file.ingestion_id = ${params.ingestionId}
        AND ing.tenant_id = ${params.tenantId}
      ORDER BY
        item.item_index ASC NULLS LAST,
        link.sort_order ASC NULLS LAST,
        file.filename ASC,
        file.id ASC
    `;
  });

  return rows.map(mapLeasedIngestionFile);
}
