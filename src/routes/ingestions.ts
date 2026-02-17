import { requireRole, requireTenantScope } from "../auth/guards.ts";
import { jsonResponse } from "../http/response.ts";
import {
  parseJsonBody,
  requireObject,
  requireStringField,
} from "../validation/common.ts";
import {
  cancelIngestion,
  commitUploadedFile,
  createIngestionDraft,
  createPresignedUpload,
  getIngestion,
  getIngestionList,
  retryIngestion,
  submitIngestion,
  uploadFileBySignedToken,
} from "../services/ingestion-service.ts";
import { extractPathParam } from "./params.ts";
import type { RouteDefinition } from "./types.ts";

const createIngestionRoute: RouteDefinition = {
  method: "POST",
  path: "/api/ingestions",
  handler: async (request, context) => {
    const auth = requireRole(context, ["operator", "admin"]);
    const body = requireObject(await parseJsonBody(request));
    const batchLabel = requireStringField(body, "batch_label");

    return jsonResponse(
      await createIngestionDraft({
        tenantId: auth.tenantId,
        userId: auth.userId,
        batchLabel,
      }),
      {
        status: 201,
      },
    );
  },
};

const listIngestionsRoute: RouteDefinition = {
  method: "GET",
  path: "/api/ingestions",
  handler: async (request, context) => {
    requireRole(context, ["viewer", "operator", "admin"]);
    const tenantId = requireTenantScope(context);
    const url = new URL(request.url);
    const result = await getIngestionList({ tenantId, url });

    return jsonResponse({
      ingestions: result.items,
      next_cursor: result.nextCursor ?? null,
    });
  },
};

const getIngestionRoute: RouteDefinition = {
  method: "GET",
  path: "/api/ingestions/:id",
  handler: async (request, context) => {
    requireRole(context, ["viewer", "operator", "admin"]);
    const tenantId = requireTenantScope(context);
    const pathname = new URL(request.url).pathname;
    const ingestionId = extractPathParam(pathname, /^\/api\/ingestions\/([^/]+)$/, "id");
    return jsonResponse(await getIngestion({ tenantId, ingestionId }));
  },
};

const presignFileRoute: RouteDefinition = {
  method: "POST",
  path: "/api/ingestions/:id/files/presign",
  handler: async (request, context) => {
    requireRole(context, ["operator", "admin"]);
    const tenantId = requireTenantScope(context);
    const pathname = new URL(request.url).pathname;
    const ingestionId = extractPathParam(pathname, /^\/api\/ingestions\/([^/]+)\/files\/presign$/, "id");
    const body = await parseJsonBody(request);

    return jsonResponse(
      await createPresignedUpload({
        tenantId,
        ingestionId,
        body,
      }),
      {
        status: 201,
      },
    );
  },
};

const commitFileRoute: RouteDefinition = {
  method: "POST",
  path: "/api/ingestions/:id/files/commit",
  handler: async (request, context) => {
    requireRole(context, ["operator", "admin"]);
    const tenantId = requireTenantScope(context);
    const pathname = new URL(request.url).pathname;
    const ingestionId = extractPathParam(pathname, /^\/api\/ingestions\/([^/]+)\/files\/commit$/, "id");
    const body = await parseJsonBody(request);

    return jsonResponse(
      await commitUploadedFile({
        tenantId,
        ingestionId,
        body,
      }),
    );
  },
};

const submitIngestionRoute: RouteDefinition = {
  method: "POST",
  path: "/api/ingestions/:id/submit",
  handler: async (request, context) => {
    requireRole(context, ["operator", "admin"]);
    const tenantId = requireTenantScope(context);
    const pathname = new URL(request.url).pathname;
    const ingestionId = extractPathParam(pathname, /^\/api\/ingestions\/([^/]+)\/submit$/, "id");

    return jsonResponse(await submitIngestion({ tenantId, ingestionId }));
  },
};

const cancelIngestionRoute: RouteDefinition = {
  method: "POST",
  path: "/api/ingestions/:id/cancel",
  handler: async (request, context) => {
    requireRole(context, ["operator", "admin"]);
    const tenantId = requireTenantScope(context);
    const pathname = new URL(request.url).pathname;
    const ingestionId = extractPathParam(pathname, /^\/api\/ingestions\/([^/]+)\/cancel$/, "id");

    return jsonResponse(await cancelIngestion({ tenantId, ingestionId }));
  },
};

const retryIngestionRoute: RouteDefinition = {
  method: "POST",
  path: "/api/ingestions/:id/retry",
  handler: async (request, context) => {
    requireRole(context, ["operator", "admin"]);
    const tenantId = requireTenantScope(context);
    const pathname = new URL(request.url).pathname;
    const ingestionId = extractPathParam(pathname, /^\/api\/ingestions\/([^/]+)\/retry$/, "id");

    return jsonResponse(await retryIngestion({ tenantId, ingestionId }));
  },
};

const uploadBySignedUrlRoute: RouteDefinition = {
  method: "PUT",
  path: "/api/uploads/:token",
  handler: async (request, _context) => {
    const pathname = new URL(request.url).pathname;
    const uploadToken = extractPathParam(pathname, /^\/api\/uploads\/([^/]+)$/, "token");
    return jsonResponse(
      await uploadFileBySignedToken({
        uploadToken,
        request,
      }),
    );
  },
};

export const ingestionRoutes: RouteDefinition[] = [
  createIngestionRoute,
  listIngestionsRoute,
  getIngestionRoute,
  presignFileRoute,
  commitFileRoute,
  submitIngestionRoute,
  cancelIngestionRoute,
  retryIngestionRoute,
  uploadBySignedUrlRoute,
];
