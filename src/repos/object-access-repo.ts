import { withSchemaClient } from "../db/client.ts";

export interface ObjectAccessAssignmentRecord {
  objectId: string;
  tenantId: string;
  userId: string;
  grantedLevel: "family" | "private";
  createdAt: Date;
  createdBy: string;
}

export interface ObjectAccessRequestRecord {
  id: string;
  objectId: string;
  tenantId: string;
  requesterUserId: string;
  requestedLevel: "family" | "private";
  reason?: string;
  status: "PENDING" | "APPROVED" | "REJECTED" | "CANCELED";
  reviewedBy?: string;
  reviewedAt?: Date;
  decisionNote?: string;
  createdAt: Date;
  updatedAt: Date;
}

interface AssignmentRow {
  object_id: string;
  tenant_id: string;
  user_id: string;
  granted_level: "family" | "private";
  created_at: Date;
  created_by: string;
}

interface RequestRow {
  id: string;
  object_id: string;
  tenant_id: string;
  requester_user_id: string;
  requested_level: "family" | "private";
  reason: string | null;
  status: "PENDING" | "APPROVED" | "REJECTED" | "CANCELED";
  reviewed_by: string | null;
  reviewed_at: Date | null;
  decision_note: string | null;
  created_at: Date;
  updated_at: Date;
}

function mapAssignment(row: AssignmentRow): ObjectAccessAssignmentRecord {
  return {
    objectId: row.object_id,
    tenantId: row.tenant_id,
    userId: row.user_id,
    grantedLevel: row.granted_level,
    createdAt: new Date(row.created_at),
    createdBy: row.created_by,
  };
}

function mapRequest(row: RequestRow): ObjectAccessRequestRecord {
  return {
    id: row.id,
    objectId: row.object_id,
    tenantId: row.tenant_id,
    requesterUserId: row.requester_user_id,
    requestedLevel: row.requested_level,
    reason: row.reason ?? undefined,
    status: row.status,
    reviewedBy: row.reviewed_by ?? undefined,
    reviewedAt: row.reviewed_at ? new Date(row.reviewed_at) : undefined,
    decisionNote: row.decision_note ?? undefined,
    createdAt: new Date(row.created_at),
    updatedAt: new Date(row.updated_at),
  };
}

export async function upsertObjectAccessAssignment(params: {
  objectId: string;
  tenantId: string;
  userId: string;
  grantedLevel: "family" | "private";
  createdBy: string;
}): Promise<ObjectAccessAssignmentRecord> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<AssignmentRow[]>`
      INSERT INTO object_access_assignments (
        object_id,
        tenant_id,
        user_id,
        granted_level,
        created_by
      )
      VALUES (
        ${params.objectId},
        ${params.tenantId},
        ${params.userId},
        ${params.grantedLevel}::object_access_granted_level,
        ${params.createdBy}
      )
      ON CONFLICT (object_id, user_id)
      DO UPDATE
      SET granted_level = EXCLUDED.granted_level,
          created_by = EXCLUDED.created_by
      RETURNING object_id, tenant_id, user_id, granted_level, created_at, created_by
    `;
  });

  return mapAssignment(rows[0]!);
}

export async function deleteObjectAccessAssignment(params: {
  objectId: string;
  userId: string;
}): Promise<boolean> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<{ object_id: string }[]>`
      DELETE FROM object_access_assignments
      WHERE object_id = ${params.objectId}
        AND user_id = ${params.userId}
      RETURNING object_id
    `;
  });

  return rows.length > 0;
}

export async function listObjectAccessAssignmentsByObjectId(params: {
  objectId: string;
  tenantId: string;
}): Promise<ObjectAccessAssignmentRecord[]> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<AssignmentRow[]>`
      SELECT asg.object_id, asg.tenant_id, asg.user_id, asg.granted_level, asg.created_at, asg.created_by
      FROM object_access_assignments asg
      INNER JOIN objects obj ON obj.object_id = asg.object_id
      WHERE asg.object_id = ${params.objectId}
        AND obj.tenant_id = ${params.tenantId}
      ORDER BY asg.created_at ASC
    `;
  });

  return rows.map(mapAssignment);
}

export async function findObjectAccessAssignmentForUser(params: {
  objectId: string;
  tenantId: string;
  userId: string;
}): Promise<ObjectAccessAssignmentRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<AssignmentRow[]>`
      SELECT asg.object_id, asg.tenant_id, asg.user_id, asg.granted_level, asg.created_at, asg.created_by
      FROM object_access_assignments asg
      INNER JOIN objects obj ON obj.object_id = asg.object_id
      WHERE asg.object_id = ${params.objectId}
        AND asg.user_id = ${params.userId}
        AND obj.tenant_id = ${params.tenantId}
      LIMIT 1
    `;
  });

  const row = rows[0];
  return row ? mapAssignment(row) : undefined;
}

export async function listObjectAccessAssignmentsForUserByObjectIds(params: {
  tenantId: string;
  userId: string;
  objectIds: string[];
}): Promise<Map<string, "family" | "private">> {
  if (params.objectIds.length === 0) {
    return new Map();
  }

  const rows = await withSchemaClient(async (sql) => {
    return await sql<AssignmentRow[]>`
      SELECT asg.object_id, asg.tenant_id, asg.user_id, asg.granted_level, asg.created_at, asg.created_by
      FROM object_access_assignments asg
      INNER JOIN objects obj ON obj.object_id = asg.object_id
      WHERE obj.tenant_id = ${params.tenantId}
        AND asg.user_id = ${params.userId}
    `;
  });

  const targetObjectIds = new Set(params.objectIds);

  const filteredRows = rows.filter((row) => targetObjectIds.has(row.object_id));

  return new Map(filteredRows.map((row) => [row.object_id, row.granted_level]));
}

export async function createObjectAccessRequest(params: {
  objectId: string;
  tenantId: string;
  requesterUserId: string;
  requestedLevel: "family" | "private";
  reason?: string;
}): Promise<ObjectAccessRequestRecord> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<RequestRow[]>`
      INSERT INTO object_access_requests (
        id,
        object_id,
        tenant_id,
        requester_user_id,
        requested_level,
        reason,
        status
      )
      VALUES (
        ${crypto.randomUUID()},
        ${params.objectId},
        ${params.tenantId},
        ${params.requesterUserId},
        ${params.requestedLevel}::object_access_granted_level,
        ${params.reason ?? null},
        'PENDING'
      )
      RETURNING id, object_id, tenant_id, requester_user_id, requested_level, reason, status, reviewed_by, reviewed_at, decision_note, created_at, updated_at
    `;
  });

  return mapRequest(rows[0]!);
}

export async function findPendingObjectAccessRequestForUser(params: {
  objectId: string;
  tenantId: string;
  requesterUserId: string;
}): Promise<ObjectAccessRequestRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<RequestRow[]>`
      SELECT req.id, req.object_id, req.tenant_id, req.requester_user_id, req.requested_level, req.reason, req.status, req.reviewed_by, req.reviewed_at, req.decision_note, req.created_at, req.updated_at
      FROM object_access_requests req
      INNER JOIN objects obj ON obj.object_id = req.object_id
      WHERE req.object_id = ${params.objectId}
        AND req.requester_user_id = ${params.requesterUserId}
        AND req.status = 'PENDING'
        AND obj.tenant_id = ${params.tenantId}
      LIMIT 1
    `;
  });

  const row = rows[0];
  return row ? mapRequest(row) : undefined;
}

export async function listObjectAccessRequests(params: {
  objectId: string;
  tenantId: string;
}): Promise<ObjectAccessRequestRecord[]> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<RequestRow[]>`
      SELECT req.id, req.object_id, req.tenant_id, req.requester_user_id, req.requested_level, req.reason, req.status, req.reviewed_by, req.reviewed_at, req.decision_note, req.created_at, req.updated_at
      FROM object_access_requests req
      INNER JOIN objects obj ON obj.object_id = req.object_id
      WHERE req.object_id = ${params.objectId}
        AND obj.tenant_id = ${params.tenantId}
      ORDER BY req.created_at DESC
    `;
  });

  return rows.map(mapRequest);
}

export async function findObjectAccessRequestById(params: {
  requestId: string;
  objectId: string;
  tenantId: string;
}): Promise<ObjectAccessRequestRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<RequestRow[]>`
      SELECT req.id, req.object_id, req.tenant_id, req.requester_user_id, req.requested_level, req.reason, req.status, req.reviewed_by, req.reviewed_at, req.decision_note, req.created_at, req.updated_at
      FROM object_access_requests req
      INNER JOIN objects obj ON obj.object_id = req.object_id
      WHERE req.id = ${params.requestId}
        AND req.object_id = ${params.objectId}
        AND obj.tenant_id = ${params.tenantId}
      LIMIT 1
    `;
  });

  const row = rows[0];
  return row ? mapRequest(row) : undefined;
}

export async function updateObjectAccessRequestStatus(params: {
  requestId: string;
  objectId: string;
  tenantId: string;
  status: "APPROVED" | "REJECTED" | "CANCELED";
  reviewedBy: string;
  decisionNote?: string;
}): Promise<ObjectAccessRequestRecord | undefined> {
  const rows = await withSchemaClient(async (sql) => {
    return await sql<RequestRow[]>`
      UPDATE object_access_requests req
      SET status = ${params.status}::object_access_request_status,
          reviewed_by = ${params.reviewedBy},
          reviewed_at = now(),
          decision_note = ${params.decisionNote ?? null},
          updated_at = now()
      FROM objects obj
      WHERE req.id = ${params.requestId}
        AND req.object_id = ${params.objectId}
        AND req.status = 'PENDING'
        AND obj.object_id = req.object_id
        AND obj.tenant_id = ${params.tenantId}
      RETURNING req.id, req.object_id, req.tenant_id, req.requester_user_id, req.requested_level, req.reason, req.status, req.reviewed_by, req.reviewed_at, req.decision_note, req.created_at, req.updated_at
    `;
  });

  const row = rows[0];
  return row ? mapRequest(row) : undefined;
}
