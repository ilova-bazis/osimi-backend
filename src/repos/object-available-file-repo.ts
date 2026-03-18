import { withSchemaClient } from "../db/client.ts";
import {
    toNullableSafeNumberFromDbInt,
    type DbInt,
} from "../db/number.ts";
import type { ArtifactKind } from "./object-repo.ts";
import type { JsonObject } from "../validation/ingestion.ts";

interface ObjectAvailableFileRow {
    id: string;
    object_id: string;
    tenant_id: string;
    archive_file_key: string;
    artifact_kind: ArtifactKind;
    variant: string | null;
    display_name: string;
    content_type: string | null;
    size_bytes: DbInt | null;
    checksum_sha256: string | null;
    metadata: JsonObject;
    is_available: boolean;
    synced_at: Date;
}

export interface ObjectAvailableFileRecord {
    id: string;
    objectId: string;
    tenantId: string;
    archiveFileKey: string;
    artifactKind: ArtifactKind;
    variant: string | null;
    displayName: string;
    contentType: string | null;
    sizeBytes: number | null;
    checksumSha256: string | null;
    metadata: JsonObject;
    isAvailable: boolean;
    syncedAt: Date;
}

export interface ReplaceObjectAvailableFileInput {
    archiveFileKey: string;
    artifactKind: ArtifactKind;
    variant: string | null;
    displayName: string;
    contentType: string | null;
    sizeBytes: number | null;
    checksumSha256: string | null;
    metadata: JsonObject;
    isAvailable: boolean;
}

function mapObjectAvailableFile(
    row: ObjectAvailableFileRow,
): ObjectAvailableFileRecord {
    return {
        id: row.id,
        objectId: row.object_id,
        tenantId: row.tenant_id,
        archiveFileKey: row.archive_file_key,
        artifactKind: row.artifact_kind,
        variant: row.variant,
        displayName: row.display_name,
        contentType: row.content_type,
        sizeBytes: toNullableSafeNumberFromDbInt(
            row.size_bytes,
            "object_available_files.size_bytes",
        ),
        checksumSha256: row.checksum_sha256,
        metadata: row.metadata,
        isAvailable: row.is_available,
        syncedAt: row.synced_at,
    };
}

export async function listAvailableFilesByObjectId(params: {
    tenantId: string;
    objectId: string;
}): Promise<ObjectAvailableFileRecord[]> {
    const rows = await withSchemaClient(async (sql) => {
        return await sql<ObjectAvailableFileRow[]>`
      SELECT file.id, file.object_id, file.tenant_id, file.archive_file_key,
             file.artifact_kind, file.variant, file.display_name, file.content_type,
             file.size_bytes, file.checksum_sha256, file.metadata, file.is_available,
             file.synced_at
      FROM object_available_files file
      INNER JOIN objects obj ON obj.object_id = file.object_id
      WHERE obj.tenant_id = ${params.tenantId}
        AND file.tenant_id = ${params.tenantId}
        AND file.object_id = ${params.objectId}
        AND file.is_available = true
      ORDER BY file.display_name ASC, file.archive_file_key ASC
    `;
    });

    return rows.map(mapObjectAvailableFile);
}

export async function findAvailableFileById(params: {
    tenantId: string;
    objectId: string;
    availableFileId: string;
}): Promise<ObjectAvailableFileRecord | undefined> {
    const rows = await withSchemaClient(async (sql) => {
        return await sql<ObjectAvailableFileRow[]>`
      SELECT file.id, file.object_id, file.tenant_id, file.archive_file_key,
             file.artifact_kind, file.variant, file.display_name, file.content_type,
             file.size_bytes, file.checksum_sha256, file.metadata, file.is_available,
             file.synced_at
      FROM object_available_files file
      INNER JOIN objects obj ON obj.object_id = file.object_id
      WHERE obj.tenant_id = ${params.tenantId}
        AND file.tenant_id = ${params.tenantId}
        AND file.object_id = ${params.objectId}
        AND file.id = ${params.availableFileId}
        AND file.is_available = true
      LIMIT 1
    `;
    });

    const row = rows[0];
    return row ? mapObjectAvailableFile(row) : undefined;
}

export async function replaceObjectAvailableFiles(params: {
    tenantId: string;
    objectId: string;
    files: ReplaceObjectAvailableFileInput[];
}): Promise<number> {
    return await withSchemaClient(async (sql) => {
        return await sql.begin(async (transaction) => {
            for (const file of params.files) {
                await transaction`
                    INSERT INTO object_available_files (
                        id,
                        object_id,
                        tenant_id,
                        archive_file_key,
                        artifact_kind,
                        variant,
                        display_name,
                        content_type,
                        size_bytes,
                        checksum_sha256,
                        metadata,
                        is_available,
                        synced_at
                    )
                    VALUES (
                        ${crypto.randomUUID()},
                        ${params.objectId},
                        ${params.tenantId},
                        ${file.archiveFileKey},
                        ${file.artifactKind}::artifact_kind,
                        ${file.variant},
                        ${file.displayName},
                        ${file.contentType},
                        ${file.sizeBytes},
                        ${file.checksumSha256},
                        ${file.metadata},
                        ${file.isAvailable},
                        now()
                    )
                    ON CONFLICT (tenant_id, object_id, archive_file_key)
                    DO UPDATE SET
                        artifact_kind = EXCLUDED.artifact_kind,
                        variant = EXCLUDED.variant,
                        display_name = EXCLUDED.display_name,
                        content_type = EXCLUDED.content_type,
                        size_bytes = EXCLUDED.size_bytes,
                        checksum_sha256 = EXCLUDED.checksum_sha256,
                        metadata = EXCLUDED.metadata,
                        is_available = EXCLUDED.is_available,
                        synced_at = now()
                    `;
            }

            if (params.files.length === 0) {
                await transaction`
                    UPDATE object_available_files
                    SET is_available = false,
                        synced_at = now()
                    WHERE tenant_id = ${params.tenantId}
                        AND object_id = ${params.objectId}
                    `;

                return 0;
            }

            const archiveKeys = params.files.map((file) => file.archiveFileKey);
            await transaction`
                UPDATE object_available_files
                SET is_available = false,
                    synced_at = now()
                WHERE tenant_id = ${params.tenantId}
                    AND object_id = ${params.objectId}
                    AND archive_file_key NOT IN ${sql(archiveKeys)}
                `;
            return params.files.length;
        });
    });
}
