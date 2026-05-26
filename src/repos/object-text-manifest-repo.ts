import { withSchemaClient } from "../db/client.ts";
import type { JsonObject } from "../validation/ingestion.ts";

export type ObjectTextManifestMediaType =
    | "document"
    | "audio"
    | "video"
    | "photo"
    | "other";

export interface ObjectTextManifestRecord {
    objectId: string;
    tenantId: string;
    mediaType: ObjectTextManifestMediaType;
    projectionVersion: string;
    generatedAt: Date;
    payload: JsonObject;
    syncedAt: Date;
}

interface ObjectTextManifestRow {
    object_id: string;
    tenant_id: string;
    media_type: ObjectTextManifestMediaType;
    projection_version: string;
    generated_at: Date;
    payload: JsonObject;
    synced_at: Date;
}

function mapObjectTextManifest(
    row: ObjectTextManifestRow,
): ObjectTextManifestRecord {
    return {
        objectId: row.object_id,
        tenantId: row.tenant_id,
        mediaType: row.media_type,
        projectionVersion: row.projection_version,
        generatedAt: row.generated_at,
        payload: row.payload,
        syncedAt: row.synced_at,
    };
}

export async function upsertObjectTextManifest(params: {
    objectId: string;
    tenantId: string;
    mediaType: ObjectTextManifestMediaType;
    projectionVersion: string;
    generatedAt: Date;
    payload: JsonObject;
}): Promise<ObjectTextManifestRecord> {
    const rows = await withSchemaClient(async (sql) => {
        return await sql<ObjectTextManifestRow[]>`
      INSERT INTO object_text_manifests (object_id, tenant_id, media_type, projection_version, generated_at, payload, synced_at)
      VALUES (
        ${params.objectId},
        ${params.tenantId},
        ${params.mediaType}::object_text_manifest_media_type,
        ${params.projectionVersion},
        ${params.generatedAt},
        ${params.payload},
        now()
      )
      ON CONFLICT (object_id) DO UPDATE SET
        tenant_id = EXCLUDED.tenant_id,
        media_type = EXCLUDED.media_type,
        projection_version = EXCLUDED.projection_version,
        generated_at = EXCLUDED.generated_at,
        payload = EXCLUDED.payload,
        synced_at = EXCLUDED.synced_at
      RETURNING object_id, tenant_id, media_type, projection_version, generated_at, payload, synced_at
    `;
    });

    return mapObjectTextManifest(rows[0]!);
}
