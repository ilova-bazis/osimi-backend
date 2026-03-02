import { withSchemaClient } from "../db/client.ts";
import type { RequestedArtifactKind } from "../validation/object.ts";

interface ObjectDownloadRequestRow {
  id: string;
  object_id: string;
  tenant_id: string;
  requested_by: string;
  artifact_kind: RequestedArtifactKind;
  status: "PENDING" | "PROCESSING" | "COMPLETED" | "FAILED" | "CANCELED";
  failure_reason: string | null;
  created_at: Date;
  updated_at: Date;
  completed_at: Date | null;
}

export interface ObjectDownloadRequestRecord {
  id: string;
  objectId: string;
  tenantId: string;
  requestedBy: string;
  artifactKind: RequestedArtifactKind;
  status: "PENDING" | "PROCESSING" | "COMPLETED" | "FAILED" | "CANCELED";
  failureReason: string | null;
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
    requestedBy: row.requested_by,
    artifactKind: row.artifact_kind,
    status: row.status,
    failureReason: row.failure_reason,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    completedAt: row.completed_at,
  };
}

export async function findActiveObjectDownloadRequest(params: {
  tenantId: string;
  objectId: string;
  artifactKind: RequestedArtifactKind;
}): Promise<ObjectDownloadRequestRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<ObjectDownloadRequestRow[]>`
      SELECT req.id, req.object_id, req.tenant_id, req.requested_by, req.artifact_kind,
             req.status, req.failure_reason, req.created_at, req.updated_at, req.completed_at
      FROM object_download_requests req
      INNER JOIN objects obj ON obj.object_id = req.object_id
      WHERE req.tenant_id = ${params.tenantId}
        AND obj.tenant_id = ${params.tenantId}
        AND req.object_id = ${params.objectId}
        AND req.artifact_kind = ${params.artifactKind}::requested_artifact_kind
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
  requestedBy: string;
  artifactKind: RequestedArtifactKind;
}): Promise<ObjectDownloadRequestRecord> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<ObjectDownloadRequestRow[]>`
      INSERT INTO object_download_requests (
        id,
        object_id,
        tenant_id,
        requested_by,
        artifact_kind,
        status
      )
      VALUES (
        ${crypto.randomUUID()},
        ${params.objectId},
        ${params.tenantId},
        ${params.requestedBy},
        ${params.artifactKind}::requested_artifact_kind,
        'PENDING'
      )
      RETURNING id, object_id, tenant_id, requested_by, artifact_kind,
                status, failure_reason, created_at, updated_at, completed_at
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
      SELECT req.id, req.object_id, req.tenant_id, req.requested_by, req.artifact_kind,
             req.status, req.failure_reason, req.created_at, req.updated_at, req.completed_at
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
