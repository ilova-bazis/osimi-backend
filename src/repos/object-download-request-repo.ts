import { withSchemaClient } from "../db/client.ts";
import type { JsonObject } from "../validation/ingestion.ts";
import type { ArtifactKind } from "./object-repo.ts";

interface ObjectDownloadRequestRow {
    id: string;
    object_id: string;
    tenant_id: string;
    available_file_id: string | null;
    requested_by: string;
    artifact_kind: ArtifactKind;
    variant: string | null;
    status: "PENDING" | "PROCESSING" | "COMPLETED" | "FAILED" | "CANCELED";
    failure_reason: string | null;
    failure_details: JsonObject | null;
    lease_id: string | null;
    lease_token_id: string | null;
    lease_expires_at: Date | null;
    leased_by: string | null;
    released_at: Date | null;
    created_at: Date;
    updated_at: Date;
    completed_at: Date | null;
}

export interface ObjectDownloadRequestRecord {
    id: string;
    objectId: string;
    tenantId: string;
    availableFileId: string | null;
    requestedBy: string;
    artifactKind: ArtifactKind;
    variant: string | null;
    status: "PENDING" | "PROCESSING" | "COMPLETED" | "FAILED" | "CANCELED";
    failureReason: string | null;
    failureDetails: JsonObject | null;
    leaseId: string | null;
    leaseTokenId: string | null;
    leaseExpiresAt: Date | null;
    leasedBy: string | null;
    releasedAt: Date | null;
    createdAt: Date;
    updatedAt: Date;
    completedAt: Date | null;
}

function mapObjectDownloadRequest(
    row: ObjectDownloadRequestRow,
): ObjectDownloadRequestRecord {
    return {
        id: row.id,
        objectId: row.object_id,
        tenantId: row.tenant_id,
        availableFileId: row.available_file_id,
        requestedBy: row.requested_by,
        artifactKind: row.artifact_kind,
        variant: row.variant,
        status: row.status,
        failureReason: row.failure_reason,
        failureDetails: row.failure_details,
        leaseId: row.lease_id,
        leaseTokenId: row.lease_token_id,
        leaseExpiresAt: row.lease_expires_at,
        leasedBy: row.leased_by,
        releasedAt: row.released_at,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
        completedAt: row.completed_at,
    };
}

export interface LeasedObjectDownloadRequestRecord {
    request: ObjectDownloadRequestRecord;
    leaseId: string;
    leaseTokenId: string;
    leaseExpiresAt: Date;
}

export async function findActiveObjectDownloadRequest(params: {
    tenantId: string;
    objectId: string;
    artifactKind: ArtifactKind;
    variant: string | null;
}): Promise<ObjectDownloadRequestRecord | undefined> {
    const rows = await withSchemaClient(async (sql) => {
        return await sql<ObjectDownloadRequestRow[]>`
      SELECT req.id, req.object_id, req.tenant_id, req.available_file_id, req.requested_by,
             req.artifact_kind, req.variant, req.status, req.failure_reason,
             req.failure_details, req.lease_id, req.lease_token_id, req.lease_expires_at,
             req.leased_by, req.released_at, req.created_at, req.updated_at, req.completed_at
      FROM object_download_requests req
      INNER JOIN objects obj ON obj.object_id = req.object_id
      WHERE req.tenant_id = ${params.tenantId}
        AND obj.tenant_id = ${params.tenantId}
        AND req.object_id = ${params.objectId}
        AND req.artifact_kind = ${params.artifactKind}::artifact_kind
        AND req.variant IS NOT DISTINCT FROM ${params.variant}
        AND req.status IN ('PENDING', 'PROCESSING')
      ORDER BY req.created_at DESC, req.id DESC
      LIMIT 1
    `;
    });

    const row = rows[0];
    return row ? mapObjectDownloadRequest(row) : undefined;
}

export async function createObjectDownloadRequest(params: {
    tenantId: string;
    objectId: string;
    availableFileId: string | null;
    requestedBy: string;
    artifactKind: ArtifactKind;
    variant: string | null;
}): Promise<ObjectDownloadRequestRecord> {
    const rows = await withSchemaClient(async (sql) => {
        return await sql<ObjectDownloadRequestRow[]>`
      INSERT INTO object_download_requests (
        id,
        object_id,
        tenant_id,
        available_file_id,
        requested_by,
        artifact_kind,
        variant,
        status
      )
      VALUES (
        ${crypto.randomUUID()},
        ${params.objectId},
        ${params.tenantId},
        ${params.availableFileId},
        ${params.requestedBy},
        ${params.artifactKind}::artifact_kind,
        ${params.variant},
        'PENDING'
      )
      RETURNING id, object_id, tenant_id, available_file_id, requested_by,
                artifact_kind, variant, status, failure_reason, failure_details,
                lease_id, lease_token_id, lease_expires_at, leased_by, released_at,
                created_at, updated_at, completed_at
    `;
    });

    return mapObjectDownloadRequest(rows[0]!);
}

export async function listObjectDownloadRequestsByObjectId(params: {
    tenantId: string;
    objectId: string;
}): Promise<ObjectDownloadRequestRecord[]> {
    const rows = await withSchemaClient(async (sql) => {
        return await sql<ObjectDownloadRequestRow[]>`
      SELECT req.id, req.object_id, req.tenant_id, req.available_file_id, req.requested_by,
             req.artifact_kind, req.variant, req.status, req.failure_reason,
             req.failure_details, req.lease_id, req.lease_token_id, req.lease_expires_at,
             req.leased_by, req.released_at, req.created_at, req.updated_at, req.completed_at
      FROM object_download_requests req
      INNER JOIN objects obj ON obj.object_id = req.object_id
      WHERE req.tenant_id = ${params.tenantId}
        AND obj.tenant_id = ${params.tenantId}
        AND req.object_id = ${params.objectId}
      ORDER BY req.created_at DESC, req.id DESC
    `;
    });

    return rows.map(mapObjectDownloadRequest);
}

export async function findObjectDownloadRequestById(params: {
    requestId: string;
}): Promise<ObjectDownloadRequestRecord | undefined> {
    const rows = await withSchemaClient(async (sql) => {
        return await sql<ObjectDownloadRequestRow[]>`
      SELECT req.id, req.object_id, req.tenant_id, req.available_file_id, req.requested_by,
             req.artifact_kind, req.variant, req.status, req.failure_reason,
             req.failure_details, req.lease_id, req.lease_token_id, req.lease_expires_at,
             req.leased_by, req.released_at, req.created_at, req.updated_at, req.completed_at
      FROM object_download_requests req
      WHERE req.id = ${params.requestId}
      LIMIT 1
    `;
    });

    const row = rows[0];
    return row ? mapObjectDownloadRequest(row) : undefined;
}

export async function leaseNextPendingObjectDownloadRequest(params: {
    workerId?: string;
    leaseDurationSeconds: number;
}): Promise<LeasedObjectDownloadRequestRecord | undefined> {
    return await withSchemaClient(async (sql) => {
        return await sql.begin(async (transaction) => {
            const candidates = await transaction<ObjectDownloadRequestRow[]>`
        SELECT req.id, req.object_id, req.tenant_id, req.available_file_id, req.requested_by,
               req.artifact_kind, req.variant, req.status, req.failure_reason,
               req.failure_details, req.lease_id, req.lease_token_id, req.lease_expires_at,
               req.leased_by, req.released_at, req.created_at, req.updated_at, req.completed_at
        FROM object_download_requests req
        WHERE req.status = 'PENDING'
        ORDER BY req.created_at ASC, req.id ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1
      `;

            const candidate = candidates[0];
            if (!candidate) {
                return undefined;
            }

            const leaseId = crypto.randomUUID();
            const leaseTokenId = crypto.randomUUID();

            const updatedRows = await transaction<ObjectDownloadRequestRow[]>`
        UPDATE object_download_requests req
        SET status = 'PROCESSING',
            lease_id = ${leaseId},
            lease_token_id = ${leaseTokenId},
            lease_expires_at = now() + (${params.leaseDurationSeconds}::int * interval '1 second'),
            leased_by = ${params.workerId ?? null},
            released_at = NULL,
            updated_at = now()
        WHERE req.id = ${candidate.id}
          AND req.status = 'PENDING'
        RETURNING req.id, req.object_id, req.tenant_id, req.available_file_id, req.requested_by,
                  req.artifact_kind, req.variant, req.status, req.failure_reason,
                  req.failure_details, req.lease_id, req.lease_token_id, req.lease_expires_at,
                  req.leased_by, req.released_at, req.created_at, req.updated_at, req.completed_at
      `;

            const updated = updatedRows[0];
            if (
                !updated ||
                !updated.lease_id ||
                !updated.lease_token_id ||
                !updated.lease_expires_at
            ) {
                return undefined;
            }

            return {
                request: mapObjectDownloadRequest(updated),
                leaseId: updated.lease_id,
                leaseTokenId: updated.lease_token_id,
                leaseExpiresAt: updated.lease_expires_at,
            };
        });
    });
}

export async function extendObjectDownloadRequestLease(params: {
    requestId: string;
    leaseId: string;
    leaseTokenId: string;
    leaseDurationSeconds: number;
}): Promise<LeasedObjectDownloadRequestRecord | undefined> {
    const rows = await withSchemaClient(async (sql) => {
        return await sql<ObjectDownloadRequestRow[]>`
      UPDATE object_download_requests req
      SET lease_expires_at = now() + (${params.leaseDurationSeconds}::int * interval '1 second'),
          updated_at = now()
      WHERE req.id = ${params.requestId}
        AND req.status = 'PROCESSING'
        AND req.lease_id = ${params.leaseId}
        AND req.lease_token_id = ${params.leaseTokenId}
        AND req.released_at IS NULL
        AND req.lease_expires_at > now()
      RETURNING req.id, req.object_id, req.tenant_id, req.available_file_id, req.requested_by,
                req.artifact_kind, req.variant, req.status, req.failure_reason,
                req.failure_details, req.lease_id, req.lease_token_id, req.lease_expires_at,
                req.leased_by, req.released_at, req.created_at, req.updated_at, req.completed_at
    `;
    });

    const row = rows[0];
    if (!row || !row.lease_id || !row.lease_token_id || !row.lease_expires_at) {
        return undefined;
    }

    return {
        request: mapObjectDownloadRequest(row),
        leaseId: row.lease_id,
        leaseTokenId: row.lease_token_id,
        leaseExpiresAt: row.lease_expires_at,
    };
}

export async function releaseObjectDownloadRequestLease(params: {
    requestId: string;
    leaseId: string;
    leaseTokenId: string;
}): Promise<ObjectDownloadRequestRecord | undefined> {
    const rows = await withSchemaClient(async (sql) => {
        return await sql<ObjectDownloadRequestRow[]>`
      UPDATE object_download_requests req
      SET status = 'PENDING',
          released_at = now(),
          updated_at = now(),
          lease_expires_at = NULL
      WHERE req.id = ${params.requestId}
        AND req.status = 'PROCESSING'
        AND req.lease_id = ${params.leaseId}
        AND req.lease_token_id = ${params.leaseTokenId}
        AND req.released_at IS NULL
      RETURNING req.id, req.object_id, req.tenant_id, req.available_file_id, req.requested_by,
                req.artifact_kind, req.variant, req.status, req.failure_reason,
                req.failure_details, req.lease_id, req.lease_token_id, req.lease_expires_at,
                req.leased_by, req.released_at, req.created_at, req.updated_at, req.completed_at
    `;
    });

    const row = rows[0];
    return row ? mapObjectDownloadRequest(row) : undefined;
}

export async function completeObjectDownloadRequest(params: {
    requestId: string;
    leaseId: string;
    leaseTokenId: string;
}): Promise<ObjectDownloadRequestRecord | undefined> {
    const rows = await withSchemaClient(async (sql) => {
        return await sql<ObjectDownloadRequestRow[]>`
      UPDATE object_download_requests req
      SET status = 'COMPLETED',
          completed_at = COALESCE(req.completed_at, now()),
          released_at = now(),
          lease_expires_at = NULL,
          failure_reason = NULL,
          failure_details = NULL,
          updated_at = now()
      WHERE req.id = ${params.requestId}
        AND req.status = 'PROCESSING'
        AND req.lease_id = ${params.leaseId}
        AND req.lease_token_id = ${params.leaseTokenId}
      RETURNING req.id, req.object_id, req.tenant_id, req.available_file_id, req.requested_by,
                req.artifact_kind, req.variant, req.status, req.failure_reason,
                req.failure_details, req.lease_id, req.lease_token_id, req.lease_expires_at,
                req.leased_by, req.released_at, req.created_at, req.updated_at, req.completed_at
    `;
    });

    const row = rows[0];
    return row ? mapObjectDownloadRequest(row) : undefined;
}

export async function failObjectDownloadRequest(params: {
    requestId: string;
    leaseId: string;
    leaseTokenId: string;
    failureReason: string;
    failureDetails: JsonObject;
}): Promise<ObjectDownloadRequestRecord | undefined> {
    const rows = await withSchemaClient(async (sql) => {
        return await sql<ObjectDownloadRequestRow[]>`
      UPDATE object_download_requests req
      SET status = 'FAILED',
          failure_reason = ${params.failureReason},
          failure_details = ${params.failureDetails},
          released_at = now(),
          lease_expires_at = NULL,
          updated_at = now()
      WHERE req.id = ${params.requestId}
        AND req.status = 'PROCESSING'
        AND req.lease_id = ${params.leaseId}
        AND req.lease_token_id = ${params.leaseTokenId}
      RETURNING req.id, req.object_id, req.tenant_id, req.available_file_id, req.requested_by,
                req.artifact_kind, req.variant, req.status, req.failure_reason,
                req.failure_details, req.lease_id, req.lease_token_id, req.lease_expires_at,
                req.leased_by, req.released_at, req.created_at, req.updated_at, req.completed_at
    `;
    });

    const row = rows[0];
    return row ? mapObjectDownloadRequest(row) : undefined;
}

export async function findActiveObjectDownloadRequestLeaseByToken(params: {
    requestId: string;
    leaseId: string;
    leaseTokenId: string;
}): Promise<ObjectDownloadRequestRecord | undefined> {
    const rows = await withSchemaClient(async (sql) => {
        return await sql<ObjectDownloadRequestRow[]>`
      SELECT req.id, req.object_id, req.tenant_id, req.available_file_id, req.requested_by,
             req.artifact_kind, req.variant, req.status, req.failure_reason,
             req.failure_details, req.lease_id, req.lease_token_id, req.lease_expires_at,
             req.leased_by, req.released_at, req.created_at, req.updated_at, req.completed_at
      FROM object_download_requests req
      WHERE req.id = ${params.requestId}
        AND req.status = 'PROCESSING'
        AND req.lease_id = ${params.leaseId}
        AND req.lease_token_id = ${params.leaseTokenId}
        AND req.released_at IS NULL
        AND req.lease_expires_at > now()
      LIMIT 1
    `;
    });

    const row = rows[0];
    return row ? mapObjectDownloadRequest(row) : undefined;
}

export async function sweepExpiredObjectDownloadRequestLeases(): Promise<void> {
    await withSchemaClient(async (sql) => {
        await sql`
      UPDATE object_download_requests req
      SET status = 'PENDING',
          released_at = now(),
          lease_expires_at = NULL,
          updated_at = now()
      WHERE req.status = 'PROCESSING'
        AND req.released_at IS NULL
        AND req.lease_expires_at <= now()
    `;
    });
}
