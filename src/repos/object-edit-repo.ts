import { withSchemaClient } from "../db/client.ts";
import type { UserRole } from "../auth/types.ts";
import { isObjectAccessAuthorized } from "../domain/objects/access-policy.ts";
import {
  type ArchiveRequestSqlExecutor,
  findActiveCurationApplyByObjectWithExecutor,
  findActiveArchiveRequestByDedupeKeyWithExecutor,
  tryCreateCurationApplyArchiveRequestWithExecutor,
} from "./archive-request-repo.ts";
import type { JsonObject } from "../validation/ingestion.ts";
import {
  createCurationPublicationWithExecutor,
  findCurationPublicationByIdentity,
} from "./curation-publication-repo.ts";

interface ObjectEditRow {
  object_id: string;
  tenant_id: string;
  type: "GENERIC" | "IMAGE" | "AUDIO" | "VIDEO" | "DOCUMENT";
  curation_state:
    | "needs_review"
    | "review_in_progress"
    | "reviewed"
    | "curation_failed";
  title: string;
  language_code: string | null;
  publication_date: string;
  date_precision: "none" | "year" | "month" | "day";
  date_approximate: boolean;
  description: string | null;
  rights_note: string | null;
  sensitivity_note: string | null;
  access_level: "private" | "family" | "public";
  metadata: JsonObject;
  updated_at: Date;
  tags: string[] | null;
  people: string[] | null;
  revision: number | null;
  locked_by: string | null;
  locked_until: Date | null;
  edit_updated_at: Date | null;
  edit_updated_by: string | null;
}

interface ObjectEditEventRow {
  id: string;
  object_id: string;
  type:
    | "METADATA_UPDATED"
    | "RIGHTS_UPDATED"
    | "DOCUMENT_PAGE_UPDATED"
    | "CURATION_SUBMITTED";
  actor_user_id: string | null;
  revision_before: number | null;
  revision_after: number | null;
  payload: JsonObject;
  created_at: Date;
}

export interface ObjectEditRecord {
  objectId: string;
  tenantId: string;
  type: ObjectEditRow["type"];
  curationState: ObjectEditRow["curation_state"];
  title: string;
  languageCode: string | null;
  publicationDate: string;
  datePrecision: ObjectEditRow["date_precision"];
  dateApproximate: boolean;
  description: string | null;
  rightsNote: string | null;
  sensitivityNote: string | null;
  accessLevel: ObjectEditRow["access_level"];
  metadata: JsonObject;
  updatedAt: Date;
  tags: string[];
  people: string[];
  revision: number;
  lockedBy: string | null;
  lockedUntil: Date | null;
  editUpdatedAt: Date | null;
  editUpdatedBy: string | null;
}

export interface ObjectEditEventRecord {
  id: string;
  objectId: string;
  type: ObjectEditEventRow["type"];
  actorUserId: string | null;
  revisionBefore: number | null;
  revisionAfter: number | null;
  payload: JsonObject;
  createdAt: Date;
}

interface CuratedDocumentPageRow {
  page_number: number;
  curated_text: string;
  updated_at: Date;
  updated_by: string | null;
}

export interface CuratedDocumentPageRecord {
  pageNumber: number;
  curatedText: string;
  updatedAt: Date;
  updatedBy: string | null;
}

export interface SubmittedCurationRequestRecord {
  id: string;
  actionType: "curation_apply";
  status: "PENDING" | "PROCESSING" | "COMPLETED" | "FAILED" | "CANCELED";
  createdAt: Date;
  requestedBy: string;
}

export interface ListObjectEditEventsResult {
  events: ObjectEditEventRecord[];
}

export type UpdateCuratedDocumentPagesResult =
  | { status: "not_found" }
  | { status: "unauthorized" }
  | { status: "locked"; lockedBy: string; lockedUntil: Date }
  | { status: "invalid_media_type" }
  | { status: "revision_conflict"; latestRevision: number }
  | { status: "invalid_page_numbers"; invalidPageNumbers: number[] }
  | { status: "updated"; record: ObjectEditRecord; updatedCount: number };

export type SubmitDocumentCurationResult =
  | { status: "not_found" }
  | { status: "unauthorized" }
  | { status: "locked"; lockedBy: string; lockedUntil: Date }
  | { status: "invalid_media_type" }
  | { status: "revision_conflict"; latestRevision: number }
  | { status: "deduped"; record: ObjectEditRecord; request: SubmittedCurationRequestRecord }
  | { status: "publication_active"; request: SubmittedCurationRequestRecord }
  | { status: "submitted"; record: ObjectEditRecord; request: SubmittedCurationRequestRecord };

export type UpdateObjectEditMetadataResult =
  | { status: "not_found" }
  | { status: "unauthorized" }
  | { status: "locked"; lockedBy: string; lockedUntil: Date }
  | { status: "revision_conflict"; latestRevision: number }
  | { status: "updated"; record: ObjectEditRecord };

export type AcquireObjectEditLockResult =
  | { status: "not_found" }
  | { status: "acquired"; record: ObjectEditRecord }
  | { status: "extended"; record: ObjectEditRecord }
  | { status: "locked"; record: ObjectEditRecord };

export type ReleaseObjectEditLockResult =
  | { status: "not_found" }
  | { status: "released" }
  | { status: "not_owner" };

function normalizeList(values: string[]): string[] {
  return [...new Set(values.map((value) => value.trim()).filter((value) => value.length > 0))];
}

function getActiveLockHeldByAnotherUser(
  row: ObjectEditRow,
  userId: string,
): { lockedBy: string; lockedUntil: Date } | undefined {
  if (
    row.locked_by &&
    row.locked_until &&
    row.locked_until > new Date() &&
    row.locked_by !== userId
  ) {
    return {
      lockedBy: row.locked_by,
      lockedUntil: row.locked_until,
    };
  }
}

async function isObjectEditAuthorized(
  sql: ArchiveRequestSqlExecutor,
  params: { tenantId: string; objectId: string; userId: string; role: UserRole; accessLevel: ObjectEditRow["access_level"] },
): Promise<boolean> {
  const assignments = await sql<{ granted_level: "family" | "private" }[]>`
    SELECT granted_level
    FROM object_access_assignments
    WHERE tenant_id = ${params.tenantId}
      AND object_id = ${params.objectId}
      AND user_id = ${params.userId}
    FOR KEY SHARE
  `;

  return isObjectAccessAuthorized({
    role: params.role,
    accessLevel: params.accessLevel,
    assignmentLevel: assignments[0]?.granted_level,
  });
}

function mapObjectEdit(row: ObjectEditRow): ObjectEditRecord {
  return {
    objectId: row.object_id,
    tenantId: row.tenant_id,
    type: row.type,
    curationState: row.curation_state,
    title: row.title,
    languageCode: row.language_code,
    publicationDate: row.publication_date,
    datePrecision: row.date_precision,
    dateApproximate: row.date_approximate,
    description: row.description,
    rightsNote: row.rights_note,
    sensitivityNote: row.sensitivity_note,
    accessLevel: row.access_level,
    metadata: row.metadata,
    updatedAt: row.updated_at,
    tags: Array.isArray(row.tags) ? row.tags : [],
    people: Array.isArray(row.people) ? row.people : [],
    revision: row.revision ?? 0,
    lockedBy: row.locked_by,
    lockedUntil: row.locked_until,
    editUpdatedAt: row.edit_updated_at,
    editUpdatedBy: row.edit_updated_by,
  };
}

function mapObjectEditEvent(row: ObjectEditEventRow): ObjectEditEventRecord {
  return {
    id: row.id,
    objectId: row.object_id,
    type: row.type,
    actorUserId: row.actor_user_id,
    revisionBefore: row.revision_before,
    revisionAfter: row.revision_after,
    payload: row.payload,
    createdAt: row.created_at,
  };
}

function mapCuratedDocumentPage(
  row: CuratedDocumentPageRow,
): CuratedDocumentPageRecord {
  return {
    pageNumber: row.page_number,
    curatedText: row.curated_text,
    updatedAt: row.updated_at,
    updatedBy: row.updated_by,
  };
}

async function replaceObjectTags(
  sql: ArchiveRequestSqlExecutor,
  objectId: string,
  tags: string[],
): Promise<void> {
  await sql`
    DELETE FROM object_tags
    WHERE object_id = ${objectId}
  `;

  for (const tag of tags) {
    const normalizedTag = tag.trim().toLowerCase();

    await sql`
      INSERT INTO tags (
        id,
        name_normalized,
        display_name
      )
      VALUES (
        ${crypto.randomUUID()},
        ${normalizedTag},
        ${normalizedTag}
      )
      ON CONFLICT (name_normalized)
      DO NOTHING
    `;

    const tagRows = await sql<{ id: string }[]>`
      SELECT id
      FROM tags
      WHERE name_normalized = ${normalizedTag}
      LIMIT 1
    `;

    const tagId = tagRows[0]?.id;
    if (!tagId) {
      continue;
    }

    await sql`
      INSERT INTO object_tags (object_id, tag_id)
      VALUES (${objectId}, ${tagId})
      ON CONFLICT (object_id, tag_id)
      DO NOTHING
    `;
  }
}

async function replaceObjectPeople(
  sql: ArchiveRequestSqlExecutor,
  objectId: string,
  people: string[],
): Promise<void> {
  await sql`
    DELETE FROM object_people
    WHERE object_id = ${objectId}
  `;

  const normalizedPeople = normalizeList(people);
  for (const [index, person] of normalizedPeople.entries()) {
    await sql`
      INSERT INTO object_people (
        object_id,
        person_name,
        sort_order
      )
      VALUES (
        ${objectId},
        ${person},
        ${index}
      )
    `;
  }
}

function buildEditableMetadata(params: {
  existing: JsonObject;
  title: string;
  publicationDate: string;
  datePrecision: "none" | "year" | "month" | "day";
  dateApproximate: boolean;
  language: string | null;
  tags: string[];
  people: string[];
  description: string | null;
}): JsonObject {
  return {
    ...params.existing,
    title: params.title,
    publication_date: params.publicationDate,
    date_precision: params.datePrecision,
    date_approximate: params.dateApproximate,
    language: params.language,
    tags: params.tags,
    people: params.people,
    description: params.description,
  };
}

function getMetadataDocumentPageNumbers(metadata: JsonObject): number[] {
  const pages = metadata.pages;
  if (!Array.isArray(pages)) {
    return [];
  }

  return pages
    .map((entry) => {
      if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
        return undefined;
      }

      const pageNumber = (entry as Record<string, unknown>).page_number;
      return typeof pageNumber === "number" && Number.isInteger(pageNumber) && pageNumber > 0
        ? pageNumber
        : undefined;
    })
    .filter((pageNumber): pageNumber is number => pageNumber !== undefined)
    .sort((left, right) => left - right);
}

async function selectObjectEditRow(
  sql: ArchiveRequestSqlExecutor,
  params: {
    tenantId: string;
    objectId: string;
    forUpdate?: boolean;
  },
): Promise<ObjectEditRow | undefined> {
  const rows = params.forUpdate
    ? await sql<ObjectEditRow[]>`
        SELECT
          obj.object_id,
          obj.tenant_id,
          obj.type,
          obj.curation_state,
          obj.title,
          obj.language_code,
          obj.publication_date,
          obj.date_precision,
          obj.date_approximate,
          obj.description,
          obj.rights_note,
          obj.sensitivity_note,
          obj.access_level,
          obj.metadata,
          obj.updated_at,
          COALESCE((
            SELECT array_agg(tag.name_normalized ORDER BY tag.name_normalized)
            FROM object_tags otag
            INNER JOIN tags tag ON tag.id = otag.tag_id
            WHERE otag.object_id = obj.object_id
          ), ARRAY[]::text[]) AS tags,
          COALESCE((
            SELECT array_agg(op.person_name ORDER BY op.sort_order, op.person_name)
            FROM object_people op
            WHERE op.object_id = obj.object_id
          ), ARRAY[]::text[]) AS people,
          rev.revision,
          rev.locked_by,
          rev.locked_until,
          rev.updated_at AS edit_updated_at,
          rev.updated_by::text AS edit_updated_by
        FROM objects obj
        LEFT JOIN object_edits rev ON rev.object_id = obj.object_id
        WHERE obj.tenant_id = ${params.tenantId}
          AND obj.object_id = ${params.objectId}
        LIMIT 1
        FOR UPDATE OF obj
      `
    : await sql<ObjectEditRow[]>`
        SELECT
          obj.object_id,
          obj.tenant_id,
          obj.type,
          obj.curation_state,
          obj.title,
          obj.language_code,
          obj.publication_date,
          obj.date_precision,
          obj.date_approximate,
          obj.description,
          obj.rights_note,
          obj.sensitivity_note,
          obj.access_level,
          obj.metadata,
          obj.updated_at,
          COALESCE((
            SELECT array_agg(tag.name_normalized ORDER BY tag.name_normalized)
            FROM object_tags otag
            INNER JOIN tags tag ON tag.id = otag.tag_id
            WHERE otag.object_id = obj.object_id
          ), ARRAY[]::text[]) AS tags,
          COALESCE((
            SELECT array_agg(op.person_name ORDER BY op.sort_order, op.person_name)
            FROM object_people op
            WHERE op.object_id = obj.object_id
          ), ARRAY[]::text[]) AS people,
          rev.revision,
          rev.locked_by,
          rev.locked_until,
          rev.updated_at AS edit_updated_at,
          rev.updated_by::text AS edit_updated_by
        FROM objects obj
        LEFT JOIN object_edits rev ON rev.object_id = obj.object_id
        WHERE obj.tenant_id = ${params.tenantId}
          AND obj.object_id = ${params.objectId}
        LIMIT 1
      `;

  return rows[0];
}

export async function findObjectEditById(params: {
  tenantId: string;
  objectId: string;
}): Promise<ObjectEditRecord | undefined> {
  const row = await withSchemaClient(async (sql) => {
    return await selectObjectEditRow(sql, params);
  });

  return row ? mapObjectEdit(row) : undefined;
}

export async function acquireObjectEditLock(params: {
  tenantId: string;
  objectId: string;
  userId: string;
  durationMinutes: number;
}): Promise<AcquireObjectEditLockResult> {
  return await withSchemaClient(async (sql) => {
    return await sql.begin(async (transaction) => {
      const currentRow = await selectObjectEditRow(transaction, {
        tenantId: params.tenantId,
        objectId: params.objectId,
        forUpdate: true,
      });

      if (!currentRow) {
        return { status: "not_found" };
      }

      const existingLock = currentRow.locked_by;
      const existingLockUntil = currentRow.locked_until;
      const now = new Date();

      if (existingLock && existingLockUntil && existingLockUntil > now && existingLock !== params.userId) {
        return { status: "locked", record: mapObjectEdit(currentRow) };
      }

      const lockUntil = new Date(now.getTime() + params.durationMinutes * 60_000);

      await transaction`
        INSERT INTO object_edits (object_id, revision, locked_by, locked_until)
        VALUES (${params.objectId}, 0, ${params.userId}, ${lockUntil})
        ON CONFLICT (object_id)
        DO UPDATE SET
          locked_by = EXCLUDED.locked_by,
          locked_until = EXCLUDED.locked_until
      `;

      const updatedRow = await selectObjectEditRow(transaction, {
        tenantId: params.tenantId,
        objectId: params.objectId,
      });

      const isExtended = existingLock === params.userId && existingLockUntil && existingLockUntil > now;

      return {
        status: isExtended ? "extended" : "acquired",
        record: mapObjectEdit(updatedRow!),
      };
    });
  });
}

export async function releaseObjectEditLock(params: {
  tenantId: string;
  objectId: string;
  userId: string;
}): Promise<ReleaseObjectEditLockResult> {
  return await withSchemaClient(async (sql) => {
    return await sql.begin(async (transaction) => {
      const currentRow = await selectObjectEditRow(transaction, {
        tenantId: params.tenantId,
        objectId: params.objectId,
        forUpdate: true,
      });

      if (!currentRow) {
        return { status: "not_found" };
      }

      if (currentRow.locked_by !== params.userId) {
        return { status: "not_owner" };
      }

      await transaction`
        UPDATE object_edits
        SET locked_by = NULL,
            locked_until = NULL
        WHERE object_id = ${params.objectId}
      `;

      return { status: "released" };
    });
  });
}

export async function updateObjectEditMetadata(params: {
  tenantId: string;
  objectId: string;
  actorUserId: string;
  actorRole: UserRole;
  revision: number;
  title: string;
  publicationDate: string;
  datePrecision: "none" | "year" | "month" | "day";
  dateApproximate: boolean;
  language: string | null;
  tags: string[];
  people: string[];
  description: string | null;
  rightsNote: string | null;
  sensitivityNote: string | null;
}): Promise<UpdateObjectEditMetadataResult> {
  return await withSchemaClient(async (sql) => {
    return await sql.begin(async (transaction) => {
      const currentRow = await selectObjectEditRow(transaction, {
        tenantId: params.tenantId,
        objectId: params.objectId,
        forUpdate: true,
      });

      if (!currentRow) {
        return { status: "not_found" };
      }

      if (!await isObjectEditAuthorized(transaction, {
        tenantId: params.tenantId,
        objectId: params.objectId,
        userId: params.actorUserId,
        role: params.actorRole,
        accessLevel: currentRow.access_level,
      })) {
        return { status: "unauthorized" };
      }

      const activeLock = getActiveLockHeldByAnotherUser(currentRow, params.actorUserId);
      if (activeLock) {
        return { status: "locked", ...activeLock };
      }

      await transaction`
        INSERT INTO object_edits (object_id, revision)
        VALUES (${params.objectId}, 0)
        ON CONFLICT (object_id) DO NOTHING
      `;

      const revisionRows = await transaction<{ revision: number }[]>`
        SELECT revision
        FROM object_edits
        WHERE object_id = ${params.objectId}
        LIMIT 1
      `;

      const currentRevision = revisionRows[0]?.revision ?? 0;
      if (currentRevision !== params.revision) {
        return {
          status: "revision_conflict",
          latestRevision: currentRevision,
        };
      }
      const nextRevision = currentRevision + 1;

      const normalizedTags = normalizeList(params.tags).map((value) => value.toLowerCase());
      const normalizedPeople = normalizeList(params.people);
      const mergedMetadata = buildEditableMetadata({
        existing: currentRow.metadata,
        title: params.title,
        publicationDate: params.publicationDate,
        datePrecision: params.datePrecision,
        dateApproximate: params.dateApproximate,
        language: params.language,
        tags: normalizedTags,
        people: normalizedPeople,
        description: params.description,
      });

      await transaction`
        UPDATE objects
        SET title = ${params.title},
            language_code = ${params.language},
            publication_date = ${params.publicationDate},
            date_precision = ${params.datePrecision}::object_date_precision,
            date_approximate = ${params.dateApproximate},
            description = ${params.description},
            rights_note = ${params.rightsNote},
            sensitivity_note = ${params.sensitivityNote},
            metadata = ${mergedMetadata},
            updated_at = now()
        WHERE object_id = ${params.objectId}
      `;

      await replaceObjectTags(transaction, params.objectId, normalizedTags);
      await replaceObjectPeople(transaction, params.objectId, normalizedPeople);

      await transaction`
        UPDATE object_edits
        SET revision = ${nextRevision},
            updated_at = now(),
            updated_by = ${params.actorUserId}
        WHERE object_id = ${params.objectId}
      `;

      const metadataChanged =
        currentRow.title !== params.title ||
        (currentRow.language_code ?? null) !== params.language ||
        currentRow.publication_date !== params.publicationDate ||
        currentRow.date_precision !== params.datePrecision ||
        currentRow.date_approximate !== params.dateApproximate ||
        (currentRow.description ?? null) !== params.description ||
        JSON.stringify(currentRow.tags ?? []) !== JSON.stringify(normalizedTags) ||
        JSON.stringify(currentRow.people ?? []) !== JSON.stringify(normalizedPeople);
      const rightsChanged =
        (currentRow.rights_note ?? null) !== params.rightsNote ||
        (currentRow.sensitivity_note ?? null) !== params.sensitivityNote;

      if (metadataChanged) {
        await transaction`
          INSERT INTO object_edit_events (
            id,
            object_id,
            tenant_id,
            type,
            actor_user_id,
            revision_before,
            revision_after,
            payload
          )
          VALUES (
            ${crypto.randomUUID()},
            ${params.objectId},
            ${params.tenantId},
            ${"METADATA_UPDATED"},
            ${params.actorUserId},
            ${currentRevision},
            ${nextRevision},
            ${<JsonObject>{
              fields: [
                "title",
                "publication_date",
                "date_precision",
                "date_approximate",
                "language",
                "tags",
                "people",
                "description",
              ],
            }}
          )
        `;
      }

      if (rightsChanged) {
        await transaction`
          INSERT INTO object_edit_events (
            id,
            object_id,
            tenant_id,
            type,
            actor_user_id,
            revision_before,
            revision_after,
            payload
          )
          VALUES (
            ${crypto.randomUUID()},
            ${params.objectId},
            ${params.tenantId},
            ${"RIGHTS_UPDATED"},
            ${params.actorUserId},
            ${currentRevision},
            ${nextRevision},
            ${<JsonObject>{
              fields: ["rights_note", "sensitivity_note"],
            }}
          )
        `;
      }

      const updatedRow = await selectObjectEditRow(transaction, {
        tenantId: params.tenantId,
        objectId: params.objectId,
      });

      return {
        status: "updated",
        record: mapObjectEdit(updatedRow!),
      };
    });
  });
}

export async function listObjectEditEvents(params: {
  tenantId: string;
  objectId: string;
  limit: number;
  cursorCreatedAt?: string;
  cursorEventId?: string;
}): Promise<ListObjectEditEventsResult> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<ObjectEditEventRow[]>`
      SELECT evt.id, evt.object_id, evt.type, evt.actor_user_id::text, evt.revision_before,
             evt.revision_after, evt.payload, evt.created_at
      FROM object_edit_events evt
      INNER JOIN objects obj ON obj.object_id = evt.object_id
      WHERE obj.tenant_id = ${params.tenantId}
        AND evt.object_id = ${params.objectId}
        AND (
          (${params.cursorCreatedAt ?? null}::timestamptz IS NULL OR ${params.cursorEventId ?? null}::uuid IS NULL)
          OR (evt.created_at, evt.id) < (${params.cursorCreatedAt ?? null}::timestamptz, ${params.cursorEventId ?? null}::uuid)
        )
      ORDER BY evt.created_at DESC, evt.id DESC
      LIMIT ${params.limit}
    `;
  });

  return {
    events: rows.map(mapObjectEditEvent),
  };
}

export async function listCuratedDocumentPages(params: {
  tenantId: string;
  objectId: string;
}): Promise<CuratedDocumentPageRecord[]> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<CuratedDocumentPageRow[]>`
      SELECT page.page_number, page.curated_text, page.updated_at, page.updated_by::text
      FROM object_curated_document_pages page
      INNER JOIN objects obj ON obj.object_id = page.object_id
      WHERE obj.tenant_id = ${params.tenantId}
        AND page.object_id = ${params.objectId}
      ORDER BY page.page_number ASC
    `;
  });

  return rows.map(mapCuratedDocumentPage);
}

export async function updateCuratedDocumentPages(params: {
  tenantId: string;
  objectId: string;
  actorUserId: string;
  actorRole: UserRole;
  revision: number;
  pages: Array<{ pageNumber: number; curatedText: string }>;
}): Promise<UpdateCuratedDocumentPagesResult> {
  return await withSchemaClient(async (sql) => {
    return await sql.begin(async (transaction) => {
      const currentRow = await selectObjectEditRow(transaction, {
        tenantId: params.tenantId,
        objectId: params.objectId,
        forUpdate: true,
      });

      if (!currentRow) {
        return { status: "not_found" };
      }

      if (!await isObjectEditAuthorized(transaction, {
        tenantId: params.tenantId,
        objectId: params.objectId,
        userId: params.actorUserId,
        role: params.actorRole,
        accessLevel: currentRow.access_level,
      })) {
        return { status: "unauthorized" };
      }

      const activeLock = getActiveLockHeldByAnotherUser(currentRow, params.actorUserId);
      if (activeLock) {
        return { status: "locked", ...activeLock };
      }

      if (currentRow.type !== "DOCUMENT") {
        return { status: "invalid_media_type" };
      }

      await transaction`
        INSERT INTO object_edits (object_id, revision)
        VALUES (${params.objectId}, 0)
        ON CONFLICT (object_id) DO NOTHING
      `;

      const revisionRows = await transaction<{ revision: number }[]>`
        SELECT revision
        FROM object_edits
        WHERE object_id = ${params.objectId}
        LIMIT 1
      `;
      const currentRevision = revisionRows[0]?.revision ?? 0;
      if (currentRevision !== params.revision) {
        return {
          status: "revision_conflict",
          latestRevision: currentRevision,
        };
      }
      const nextRevision = currentRevision + 1;

      const existingPageNumbers = getMetadataDocumentPageNumbers(currentRow.metadata);
      const invalidPageNumbers = params.pages
        .map((page) => page.pageNumber)
        .filter((pageNumber) => !existingPageNumbers.includes(pageNumber));
      if (invalidPageNumbers.length > 0) {
        return {
          status: "invalid_page_numbers",
          invalidPageNumbers: [...new Set(invalidPageNumbers)].sort((left, right) => left - right),
        };
      }

      for (const page of params.pages) {
        await transaction`
          INSERT INTO object_curated_document_pages (
            object_id,
            page_number,
            curated_text,
            updated_at,
            updated_by
          )
          VALUES (
            ${params.objectId},
            ${page.pageNumber},
            ${page.curatedText},
            now(),
            ${params.actorUserId}
          )
          ON CONFLICT (object_id, page_number)
          DO UPDATE SET curated_text = EXCLUDED.curated_text,
                        updated_at = now(),
                        updated_by = EXCLUDED.updated_by
        `;
      }

      await transaction`
        UPDATE object_edits
        SET revision = ${nextRevision},
            updated_at = now(),
            updated_by = ${params.actorUserId}
        WHERE object_id = ${params.objectId}
      `;

      await transaction`
        INSERT INTO object_edit_events (
          id,
          object_id,
          tenant_id,
          type,
          actor_user_id,
          revision_before,
          revision_after,
          payload
        )
        VALUES (
          ${crypto.randomUUID()},
          ${params.objectId},
          ${params.tenantId},
          ${"DOCUMENT_PAGE_UPDATED"},
          ${params.actorUserId},
          ${currentRevision},
          ${nextRevision},
          ${<JsonObject>{
            page_numbers: params.pages.map((page) => page.pageNumber).sort((left, right) => left - right),
          }}
        )
      `;

      const updatedRow = await selectObjectEditRow(transaction, {
        tenantId: params.tenantId,
        objectId: params.objectId,
      });

      return {
        status: "updated",
        record: mapObjectEdit(updatedRow!),
        updatedCount: params.pages.length,
      };
    });
  });
}

export async function submitDocumentCuration(params: {
  tenantId: string;
  objectId: string;
  actorUserId: string;
  actorRole: UserRole;
  revision: number;
  requestId: string;
  idempotencyKey: string;
  publicationRevision: number;
  targetVersion: string;
  source: {
    storageKey: string;
    contentType: string;
    sizeBytes: number;
    checksumSha256: string;
  };
  actionPayload: JsonObject;
  reviewNote: string | null;
}): Promise<SubmitDocumentCurationResult> {
  return await withSchemaClient(async (sql) => {
    return await sql.begin(async (transaction) => {
      const currentRow = await selectObjectEditRow(transaction, {
        tenantId: params.tenantId,
        objectId: params.objectId,
        forUpdate: true,
      });

      if (!currentRow) {
        return { status: "not_found" };
      }

      if (!await isObjectEditAuthorized(transaction, {
        tenantId: params.tenantId,
        objectId: params.objectId,
        userId: params.actorUserId,
        role: params.actorRole,
        accessLevel: currentRow.access_level,
      })) {
        return { status: "unauthorized" };
      }

      const activeLock = getActiveLockHeldByAnotherUser(currentRow, params.actorUserId);
      if (activeLock) {
        return { status: "locked", ...activeLock };
      }

      if (currentRow.type !== "DOCUMENT") {
        return { status: "invalid_media_type" };
      }

      await transaction`
        INSERT INTO object_edits (object_id, revision)
        VALUES (${params.objectId}, 0)
        ON CONFLICT (object_id) DO NOTHING
      `;

      const exactPublication = await findCurationPublicationByIdentity({
        tenantId: params.tenantId,
        objectId: params.objectId,
        curatedKind: "ocr_curated",
        publicationRevision: params.publicationRevision,
        executor: transaction,
      });
      if (
        exactPublication?.requestStatus &&
        exactPublication.requestedBy &&
        exactPublication.requestCreatedAt
      ) {
        const row = await selectObjectEditRow(transaction, {
          tenantId: params.tenantId,
          objectId: params.objectId,
        });
        return {
          status: "deduped",
          record: mapObjectEdit(row!),
          request: {
            id: exactPublication.requestId,
            actionType: "curation_apply",
            status: exactPublication.requestStatus,
            createdAt: exactPublication.requestCreatedAt,
            requestedBy: exactPublication.requestedBy,
          },
        };
      }

      const existing = await findActiveArchiveRequestByDedupeKeyWithExecutor(
        transaction,
        {
          tenantId: params.tenantId,
          actionType: "curation_apply",
          dedupeKey: params.idempotencyKey,
        },
      );
      if (existing) {
        const row = await selectObjectEditRow(transaction, {
          tenantId: params.tenantId,
          objectId: params.objectId,
        });

        return {
          status: "deduped",
          record: mapObjectEdit(row!),
          request: {
            id: existing.id,
            actionType: "curation_apply",
            status: existing.status,
            createdAt: existing.createdAt,
            requestedBy: existing.requestedBy,
          },
        };
      }

      const activePublication = await findActiveCurationApplyByObjectWithExecutor(
        transaction,
        {
          tenantId: params.tenantId,
          objectId: params.objectId,
        },
      );
      if (activePublication) {
        return {
          status: "publication_active",
          request: {
            id: activePublication.id,
            actionType: "curation_apply",
            status: activePublication.status,
            createdAt: activePublication.createdAt,
            requestedBy: activePublication.requestedBy,
          },
        };
      }

      const revisionRows = await transaction<{ revision: number }[]>`
        SELECT revision
        FROM object_edits
        WHERE object_id = ${params.objectId}
        LIMIT 1
      `;
      const currentRevision = revisionRows[0]?.revision ?? 0;
      if (currentRevision !== params.revision) {
        return {
          status: "revision_conflict",
          latestRevision: currentRevision,
        };
      }
      const nextRevision = currentRevision + 1;
      const nextCurationState =
        currentRow.curation_state === "needs_review"
          ? "review_in_progress"
          : currentRow.curation_state === "review_in_progress"
            ? "review_in_progress"
            : currentRow.curation_state;

      const createdRequest = await tryCreateCurationApplyArchiveRequestWithExecutor(transaction, {
        requestId: params.requestId,
        tenantId: params.tenantId,
        targetType: "object",
        targetId: params.objectId,
        actionType: "curation_apply",
        actionPayload: params.actionPayload,
        requestedBy: params.actorUserId,
        dedupeKey: params.idempotencyKey,
      });

      if (!createdRequest) {
        const concurrentPublication = await findActiveCurationApplyByObjectWithExecutor(
          transaction,
          {
            tenantId: params.tenantId,
            objectId: params.objectId,
          },
        );
        if (!concurrentPublication) {
          throw new Error("Active curation publication conflict did not resolve to a request.");
        }

        return {
          status: "publication_active",
          request: {
            id: concurrentPublication.id,
            actionType: "curation_apply",
            status: concurrentPublication.status,
            createdAt: concurrentPublication.createdAt,
            requestedBy: concurrentPublication.requestedBy,
          },
        };
      }

      await createCurationPublicationWithExecutor(transaction, {
        requestId: createdRequest.id,
        tenantId: params.tenantId,
        objectId: params.objectId,
        curatedKind: "ocr_curated",
        publicationRevision: params.publicationRevision,
        targetVersion: params.targetVersion,
        storageKey: params.source.storageKey,
        contentType: params.source.contentType,
        sizeBytes: params.source.sizeBytes,
        checksumSha256: params.source.checksumSha256,
      });

      await transaction`
        UPDATE objects
        SET curation_state = ${nextCurationState}::object_curation_state,
            updated_at = now()
        WHERE object_id = ${params.objectId}
      `;

      await transaction`
        UPDATE object_edits
        SET revision = ${nextRevision},
            updated_at = now(),
            updated_by = ${params.actorUserId}
        WHERE object_id = ${params.objectId}
      `;

      await transaction`
        INSERT INTO object_edit_events (
          id,
          object_id,
          tenant_id,
          type,
          actor_user_id,
          revision_before,
          revision_after,
          payload
        )
        VALUES (
          ${crypto.randomUUID()},
          ${params.objectId},
          ${params.tenantId},
          ${"CURATION_SUBMITTED"},
          ${params.actorUserId},
          ${currentRevision},
          ${nextRevision},
          ${<JsonObject>{
            review_note: params.reviewNote,
            request_id: params.requestId,
            archive_curated_kind: "ocr_curated",
            archive_target_version: (params.actionPayload.target_version as string | undefined) ?? null,
            archive_idempotency_key: params.idempotencyKey,
          }}
        )
      `;

      const updatedRow = await selectObjectEditRow(transaction, {
        tenantId: params.tenantId,
        objectId: params.objectId,
      });

      return {
        status: "submitted",
        record: mapObjectEdit(updatedRow!),
        request: {
          id: createdRequest.id,
          actionType: "curation_apply",
          status: createdRequest.status,
          createdAt: createdRequest.createdAt,
          requestedBy: createdRequest.requestedBy,
        },
      };
    });
  });
}
