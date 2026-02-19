import { requireRole } from "../auth/guards.ts";
import { ValidationError } from "../http/errors.ts";
import { jsonResponse } from "../http/response.ts";
import { parseJsonBody } from "../validation/common.ts";
import {
  createObjectAccessRequestForTenant,
  deleteObjectAccessAssignmentForTenant,
  downloadObjectArtifactForTenant,
  getObjectDetail,
  listObjectAccessAssignmentsForTenant,
  listObjectAccessRequestsForTenant,
  listObjectArtifactsForTenant,
  listObjectsForTenant,
  patchObjectTitleForTenant,
  resolveObjectAccessRequestForTenant,
  updateObjectAccessPolicyForTenant,
  upsertObjectAccessAssignmentForTenant,
} from "../services/object-service.ts";
import { extractPathParam } from "./params.ts";
import type { RouteDefinition } from "./types.ts";

async function parseOptionalJsonBody(request: Request): Promise<unknown> {
  const rawBody = await request.text();
  if (rawBody.trim().length === 0) {
    return {};
  }

  try {
    return JSON.parse(rawBody) as unknown;
  } catch {
    throw new ValidationError("Request body must be valid JSON.");
  }
}

const listObjectsRoute: RouteDefinition = {
  method: "GET",
  path: "/api/objects",
  handler: async (request, context) => {
    const authenticated = requireRole(context, ["viewer", "archiver", "admin"]);
    const tenantId = authenticated.tenantId;
    const url = new URL(request.url);
    return jsonResponse(
      await listObjectsForTenant({
        tenantId,
        userId: authenticated.userId,
        role: authenticated.role,
        url,
      }),
    );
  },
};

const getObjectRoute: RouteDefinition = {
  method: "GET",
  path: "/api/objects/:object_id",
  handler: async (request, context) => {
    const authenticated = requireRole(context, ["viewer", "archiver", "admin"]);
    const tenantId = authenticated.tenantId;
    const pathname = new URL(request.url).pathname;
    const objectId = extractPathParam(pathname, /^\/api\/objects\/([^/]+)$/, "object_id");
    return jsonResponse(
      await getObjectDetail({
        tenantId,
        userId: authenticated.userId,
        role: authenticated.role,
        objectId,
      }),
    );
  },
};

const patchObjectRoute: RouteDefinition = {
  method: "PATCH",
  path: "/api/objects/:object_id",
  handler: async (request, context) => {
    const authenticated = requireRole(context, ["archiver", "admin"]);
    const tenantId = authenticated.tenantId;
    const pathname = new URL(request.url).pathname;
    const objectId = extractPathParam(pathname, /^\/api\/objects\/([^/]+)$/, "object_id");
    const body = await parseJsonBody(request);
    return jsonResponse(await patchObjectTitleForTenant({ tenantId, objectId, body }));
  },
};

const listArtifactsRoute: RouteDefinition = {
  method: "GET",
  path: "/api/objects/:object_id/artifacts",
  handler: async (request, context) => {
    const authenticated = requireRole(context, ["viewer", "archiver", "admin"]);
    const tenantId = authenticated.tenantId;
    const pathname = new URL(request.url).pathname;
    const objectId = extractPathParam(pathname, /^\/api\/objects\/([^/]+)\/artifacts$/, "object_id");
    return jsonResponse(await listObjectArtifactsForTenant({ tenantId, objectId }));
  },
};

const downloadArtifactRoute: RouteDefinition = {
  method: "GET",
  path: "/api/objects/:object_id/artifacts/:artifact_id/download",
  handler: async (request, context) => {
    const authenticated = requireRole(context, ["viewer", "archiver", "admin"]);
    const tenantId = authenticated.tenantId;
    const pathname = new URL(request.url).pathname;
    const objectId = extractPathParam(
      pathname,
      /^\/api\/objects\/([^/]+)\/artifacts\/[^/]+\/download$/,
      "object_id",
    );
    const artifactId = extractPathParam(
      pathname,
      /^\/api\/objects\/[^/]+\/artifacts\/([^/]+)\/download$/,
      "artifact_id",
    );
    return downloadObjectArtifactForTenant({
      tenantId,
      userId: authenticated.userId,
      role: authenticated.role,
      objectId,
      artifactId,
    });
  },
};

const patchObjectAccessPolicyRoute: RouteDefinition = {
  method: "PATCH",
  path: "/api/objects/:object_id/access-policy",
  handler: async (request, context) => {
    const authenticated = requireRole(context, ["admin"]);
    const tenantId = authenticated.tenantId;
    const pathname = new URL(request.url).pathname;
    const objectId = extractPathParam(
      pathname,
      /^\/api\/objects\/([^/]+)\/access-policy$/,
      "object_id",
    );
    const body = await parseJsonBody(request);
    return jsonResponse(
      await updateObjectAccessPolicyForTenant({
        tenantId,
        objectId,
        body,
      }),
    );
  },
};

const createObjectAccessRequestRoute: RouteDefinition = {
  method: "POST",
  path: "/api/objects/:object_id/access-requests",
  handler: async (request, context) => {
    const authenticated = requireRole(context, ["viewer", "archiver", "admin"]);
    const tenantId = authenticated.tenantId;
    const pathname = new URL(request.url).pathname;
    const objectId = extractPathParam(
      pathname,
      /^\/api\/objects\/([^/]+)\/access-requests$/,
      "object_id",
    );
    const body = await parseJsonBody(request);
    return jsonResponse(
      await createObjectAccessRequestForTenant({
        tenantId,
        objectId,
        userId: authenticated.userId,
        body,
      }),
      {
        status: 201,
      },
    );
  },
};

const listObjectAccessRequestsRoute: RouteDefinition = {
  method: "GET",
  path: "/api/objects/:object_id/access-requests",
  handler: async (request, context) => {
    const authenticated = requireRole(context, ["admin"]);
    const tenantId = authenticated.tenantId;
    const pathname = new URL(request.url).pathname;
    const objectId = extractPathParam(
      pathname,
      /^\/api\/objects\/([^/]+)\/access-requests$/,
      "object_id",
    );
    return jsonResponse(await listObjectAccessRequestsForTenant({ tenantId, objectId }));
  },
};

const approveObjectAccessRequestRoute: RouteDefinition = {
  method: "POST",
  path: "/api/objects/:object_id/access-requests/:request_id/approve",
  handler: async (request, context) => {
    const authenticated = requireRole(context, ["admin"]);
    const tenantId = authenticated.tenantId;
    const pathname = new URL(request.url).pathname;
    const objectId = extractPathParam(
      pathname,
      /^\/api\/objects\/([^/]+)\/access-requests\/[^/]+\/approve$/,
      "object_id",
    );
    const requestId = extractPathParam(
      pathname,
      /^\/api\/objects\/[^/]+\/access-requests\/([^/]+)\/approve$/,
      "request_id",
    );
    const body = await parseOptionalJsonBody(request);
    return jsonResponse(
      await resolveObjectAccessRequestForTenant({
        tenantId,
        objectId,
        requestId,
        reviewerUserId: authenticated.userId,
        action: "approve",
        body,
      }),
    );
  },
};

const rejectObjectAccessRequestRoute: RouteDefinition = {
  method: "POST",
  path: "/api/objects/:object_id/access-requests/:request_id/reject",
  handler: async (request, context) => {
    const authenticated = requireRole(context, ["admin"]);
    const tenantId = authenticated.tenantId;
    const pathname = new URL(request.url).pathname;
    const objectId = extractPathParam(
      pathname,
      /^\/api\/objects\/([^/]+)\/access-requests\/[^/]+\/reject$/,
      "object_id",
    );
    const requestId = extractPathParam(
      pathname,
      /^\/api\/objects\/[^/]+\/access-requests\/([^/]+)\/reject$/,
      "request_id",
    );
    const body = await parseOptionalJsonBody(request);
    return jsonResponse(
      await resolveObjectAccessRequestForTenant({
        tenantId,
        objectId,
        requestId,
        reviewerUserId: authenticated.userId,
        action: "reject",
        body,
      }),
    );
  },
};

const listObjectAccessAssignmentsRoute: RouteDefinition = {
  method: "GET",
  path: "/api/objects/:object_id/access-assignments",
  handler: async (request, context) => {
    const authenticated = requireRole(context, ["admin"]);
    const tenantId = authenticated.tenantId;
    const pathname = new URL(request.url).pathname;
    const objectId = extractPathParam(
      pathname,
      /^\/api\/objects\/([^/]+)\/access-assignments$/,
      "object_id",
    );
    return jsonResponse(await listObjectAccessAssignmentsForTenant({ tenantId, objectId }));
  },
};

const upsertObjectAccessAssignmentRoute: RouteDefinition = {
  method: "PUT",
  path: "/api/objects/:object_id/access-assignments",
  handler: async (request, context) => {
    const authenticated = requireRole(context, ["admin"]);
    const tenantId = authenticated.tenantId;
    const pathname = new URL(request.url).pathname;
    const objectId = extractPathParam(
      pathname,
      /^\/api\/objects\/([^/]+)\/access-assignments$/,
      "object_id",
    );
    const body = await parseJsonBody(request);
    return jsonResponse(
      await upsertObjectAccessAssignmentForTenant({
        tenantId,
        objectId,
        actorUserId: authenticated.userId,
        body,
      }),
    );
  },
};

const deleteObjectAccessAssignmentRoute: RouteDefinition = {
  method: "DELETE",
  path: "/api/objects/:object_id/access-assignments/:user_id",
  handler: async (request, context) => {
    const authenticated = requireRole(context, ["admin"]);
    const tenantId = authenticated.tenantId;
    const pathname = new URL(request.url).pathname;
    const objectId = extractPathParam(
      pathname,
      /^\/api\/objects\/([^/]+)\/access-assignments\/[^/]+$/,
      "object_id",
    );
    const userId = extractPathParam(
      pathname,
      /^\/api\/objects\/[^/]+\/access-assignments\/([^/]+)$/,
      "user_id",
    );
    return jsonResponse(await deleteObjectAccessAssignmentForTenant({ tenantId, objectId, userId }));
  },
};

export const objectRoutes: RouteDefinition[] = [
  listObjectsRoute,
  getObjectRoute,
  patchObjectRoute,
  listArtifactsRoute,
  downloadArtifactRoute,
  patchObjectAccessPolicyRoute,
  createObjectAccessRequestRoute,
  listObjectAccessRequestsRoute,
  approveObjectAccessRequestRoute,
  rejectObjectAccessRequestRoute,
  listObjectAccessAssignmentsRoute,
  upsertObjectAccessAssignmentRoute,
  deleteObjectAccessAssignmentRoute,
];
