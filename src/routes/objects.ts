import { requireRole, requireTenantScope } from "../auth/guards.ts";
import { ValidationError } from "../http/errors.ts";
import { jsonResponse } from "../http/response.ts";
import {
  downloadObjectArtifactForTenant,
  getObjectDetail,
  listObjectArtifactsForTenant,
  listObjectsForTenant,
  patchObjectTitleForTenant,
} from "../services/object-service.ts";
import type { RouteDefinition } from "./types.ts";

function extractPathParam(pathname: string, pattern: RegExp, parameterName: string): string {
  const match = pathname.match(pattern);
  const value = match?.[1];

  if (!value) {
    throw new ValidationError(`Path parameter '${parameterName}' is invalid.`);
  }

  return value;
}

async function parseJsonBody(request: Request): Promise<unknown> {
  try {
    return await request.json();
  } catch {
    throw new ValidationError("Request body must be valid JSON.");
  }
}

const listObjectsRoute: RouteDefinition = {
  method: "GET",
  path: "/api/objects",
  handler: async (request, context) => {
    requireRole(context, ["viewer", "operator", "admin"]);
    const tenantId = requireTenantScope(context);
    const url = new URL(request.url);
    return jsonResponse(await listObjectsForTenant({ tenantId, url }));
  },
};

const getObjectRoute: RouteDefinition = {
  method: "GET",
  path: "/api/objects/:object_id",
  handler: async (request, context) => {
    requireRole(context, ["viewer", "operator", "admin"]);
    const tenantId = requireTenantScope(context);
    const pathname = new URL(request.url).pathname;
    const objectId = extractPathParam(pathname, /^\/api\/objects\/([^/]+)$/, "object_id");
    return jsonResponse(await getObjectDetail({ tenantId, objectId }));
  },
};

const patchObjectRoute: RouteDefinition = {
  method: "PATCH",
  path: "/api/objects/:object_id",
  handler: async (request, context) => {
    requireRole(context, ["operator", "admin"]);
    const tenantId = requireTenantScope(context);
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
    requireRole(context, ["viewer", "operator", "admin"]);
    const tenantId = requireTenantScope(context);
    const pathname = new URL(request.url).pathname;
    const objectId = extractPathParam(pathname, /^\/api\/objects\/([^/]+)\/artifacts$/, "object_id");
    return jsonResponse(await listObjectArtifactsForTenant({ tenantId, objectId }));
  },
};

const downloadArtifactRoute: RouteDefinition = {
  method: "GET",
  path: "/api/objects/:object_id/artifacts/:artifact_id/download",
  handler: async (request, context) => {
    requireRole(context, ["viewer", "operator", "admin"]);
    const tenantId = requireTenantScope(context);
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
    return downloadObjectArtifactForTenant({ tenantId, objectId, artifactId });
  },
};

export const objectRoutes: RouteDefinition[] = [
  listObjectsRoute,
  getObjectRoute,
  patchObjectRoute,
  listArtifactsRoute,
  downloadArtifactRoute,
];
