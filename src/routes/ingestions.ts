import { requireRole } from "../auth/guards.ts";
import { jsonResponse } from "../http/response.ts";
import { parseJsonBody } from "../validation/common.ts";
import {
  parseCommitUploadedFileBody,
  parseCreateIngestionBody,
  parseCreatePresignedUploadBody,
  parseIngestionFileIdParam,
  parseIngestionIdParam,
  parseIngestionListQuery,
  parseUpdateIngestionFileOverridesBody,
  parseUpdateIngestionBody,
  parseUploadTokenParam,
} from "../validation/ingestion.ts";
import {
  cancelIngestion,
  commitUploadedFile,
  createIngestionDraft,
  createPresignedUpload,
  deleteIngestionRecord,
  getIngestionCapabilities,
  getIngestion,
  getIngestionList,
  removeIngestionFile,
  restoreIngestion,
  retryIngestion,
  submitIngestion,
  updateIngestion,
  updateIngestionFileOverrides,
  uploadFileBySignedToken,
} from "../services/ingestion-service.ts";
import { extractPathParam } from "./params.ts";
import type { RouteDefinition } from "./types.ts";

const createIngestionRoute: RouteDefinition = {
  method: "POST",
  path: "/api/ingestions",
  handler: async (request, context) => {
    const auth = requireRole(context, ["archiver", "admin"]);
    const body = parseCreateIngestionBody(await parseJsonBody(request));
    return jsonResponse(
      await createIngestionDraft({
        auth,
        body,
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
    const auth = requireRole(context, ["viewer", "archiver", "admin"]);
    const url = new URL(request.url);
    const query = parseIngestionListQuery(url);
    const result = await getIngestionList({ auth, query });

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
    const auth = requireRole(context, ["viewer", "archiver", "admin"]);
    const pathname = new URL(request.url).pathname;
    const ingestionId = parseIngestionIdParam(
      extractPathParam(pathname, /^\/api\/ingestions\/([^/]+)$/, "id"),
    );
    return jsonResponse(await getIngestion({ auth, ingestionId }));
  },
};

const updateIngestionRoute: RouteDefinition = {
  method: "PATCH",
  path: "/api/ingestions/:id",
  handler: async (request, context) => {
    const auth = requireRole(context, ["archiver", "admin"]);
    const pathname = new URL(request.url).pathname;
    const ingestionId = parseIngestionIdParam(
      extractPathParam(pathname, /^\/api\/ingestions\/([^/]+)$/, "id"),
    );
    const body = parseUpdateIngestionBody(await parseJsonBody(request));

    return jsonResponse(
      await updateIngestion({
        auth,
        ingestionId,
        body,
      }),
    );
  },
};

const deleteIngestionRoute: RouteDefinition = {
  method: "DELETE",
  path: "/api/ingestions/:id",
  handler: async (request, context) => {
    const auth = requireRole(context, ["archiver", "admin"]);
    const pathname = new URL(request.url).pathname;
    const ingestionId = parseIngestionIdParam(
      extractPathParam(pathname, /^\/api\/ingestions\/([^/]+)$/, "id"),
    );

    return jsonResponse(
      await deleteIngestionRecord({
        auth,
        ingestionId,
      }),
    );
  },
};

const ingestionCapabilitiesRoute: RouteDefinition = {
  method: "GET",
  path: "/api/ingestions/capabilities",
  handler: async (_request, context) => {
    requireRole(context, ["viewer", "archiver", "admin"]);
    return jsonResponse(getIngestionCapabilities());
  },
};

const presignFileRoute: RouteDefinition = {
  method: "POST",
  path: "/api/ingestions/:id/files/presign",
  handler: async (request, context) => {
    const auth = requireRole(context, ["archiver", "admin"]);
    const pathname = new URL(request.url).pathname;
    const ingestionId = parseIngestionIdParam(
      extractPathParam(
        pathname,
        /^\/api\/ingestions\/([^/]+)\/files\/presign$/,
        "id",
      ),
    );
    const body = parseCreatePresignedUploadBody(await parseJsonBody(request));

    return jsonResponse(
      await createPresignedUpload({
        auth,
        ingestionId,
        body,
      }),
      {
        status: 201,
      },
    );
  },
};

const removeFileRoute: RouteDefinition = {
  method: "DELETE",
  path: "/api/ingestions/:id/files/:fileId",
  handler: async (request, context) => {
    const auth = requireRole(context, ["archiver", "admin"]);
    const pathname = new URL(request.url).pathname;
    const ingestionId = parseIngestionIdParam(
      extractPathParam(
        pathname,
        /^\/api\/ingestions\/([^/]+)\/files\/[^/]+$/,
        "id",
      ),
    );
    const fileId = parseIngestionFileIdParam(
      extractPathParam(
        pathname,
        /^\/api\/ingestions\/[^/]+\/files\/([^/]+)$/,
        "fileId",
      ),
    );

    return jsonResponse(
      await removeIngestionFile({
        auth,
        ingestionId,
        fileId,
      }),
    );
  },
};

const updateFileOverridesRoute: RouteDefinition = {
  method: "POST",
  path: "/api/ingestions/:id/files/:fileId/overrides",
  handler: async (request, context) => {
    const auth = requireRole(context, ["archiver", "admin"]);
    const pathname = new URL(request.url).pathname;
    const ingestionId = parseIngestionIdParam(
      extractPathParam(
        pathname,
        /^\/api\/ingestions\/([^/]+)\/files\/[^/]+\/overrides$/,
        "id",
      ),
    );
    const fileId = parseIngestionFileIdParam(
      extractPathParam(
        pathname,
        /^\/api\/ingestions\/[^/]+\/files\/([^/]+)\/overrides$/,
        "fileId",
      ),
    );
    const body = parseUpdateIngestionFileOverridesBody(
      await parseJsonBody(request),
    );

    return jsonResponse(
      await updateIngestionFileOverrides({
        auth,
        ingestionId,
        fileId,
        body,
      }),
    );
  },
};

const commitFileRoute: RouteDefinition = {
  method: "POST",
  path: "/api/ingestions/:id/files/commit",
  handler: async (request, context) => {
    const auth = requireRole(context, ["archiver", "admin"]);
    const pathname = new URL(request.url).pathname;
    const ingestionId = parseIngestionIdParam(
      extractPathParam(
        pathname,
        /^\/api\/ingestions\/([^/]+)\/files\/commit$/,
        "id",
      ),
    );
    const body = parseCommitUploadedFileBody(await parseJsonBody(request));

    return jsonResponse(
      await commitUploadedFile({
        auth,
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
    const auth = requireRole(context, ["archiver", "admin"]);
    const pathname = new URL(request.url).pathname;
    const ingestionId = parseIngestionIdParam(
      extractPathParam(pathname, /^\/api\/ingestions\/([^/]+)\/submit$/, "id"),
    );

    return jsonResponse(
      await submitIngestion({
        auth,
        ingestionId,
      }),
    );
  },
};

const restoreIngestionRoute: RouteDefinition = {
  method: "POST",
  path: "/api/ingestions/:id/restore",
  handler: async (request, context) => {
    const auth = requireRole(context, ["archiver", "admin"]);
    const pathname = new URL(request.url).pathname;
    const ingestionId = parseIngestionIdParam(
      extractPathParam(pathname, /^\/api\/ingestions\/([^/]+)\/restore$/, "id"),
    );

    return jsonResponse(
      await restoreIngestion({
        auth,
        ingestionId,
      }),
    );
  },
};

const cancelIngestionRoute: RouteDefinition = {
  method: "POST",
  path: "/api/ingestions/:id/cancel",
  handler: async (request, context) => {
    const auth = requireRole(context, ["archiver", "admin"]);
    const pathname = new URL(request.url).pathname;
    const ingestionId = parseIngestionIdParam(
      extractPathParam(pathname, /^\/api\/ingestions\/([^/]+)\/cancel$/, "id"),
    );

    return jsonResponse(
      await cancelIngestion({
        auth,
        ingestionId,
      }),
    );
  },
};

const retryIngestionRoute: RouteDefinition = {
  method: "POST",
  path: "/api/ingestions/:id/retry",
  handler: async (request, context) => {
    const auth = requireRole(context, ["archiver", "admin"]);
    const pathname = new URL(request.url).pathname;
    const ingestionId = parseIngestionIdParam(
      extractPathParam(pathname, /^\/api\/ingestions\/([^/]+)\/retry$/, "id"),
    );

    return jsonResponse(
      await retryIngestion({
        auth,
        ingestionId,
      }),
    );
  },
};

const uploadBySignedUrlRoute: RouteDefinition = {
  method: "PUT",
  path: "/api/uploads/:token",
  handler: async (request, _context) => {
    const pathname = new URL(request.url).pathname;
    const uploadToken = parseUploadTokenParam(
      extractPathParam(pathname, /^\/api\/uploads\/([^/]+)$/, "token"),
    );
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
  ingestionCapabilitiesRoute,
  getIngestionRoute,
  updateIngestionRoute,
  deleteIngestionRoute,
  presignFileRoute,
  removeFileRoute,
  updateFileOverridesRoute,
  commitFileRoute,
  submitIngestionRoute,
  cancelIngestionRoute,
  restoreIngestionRoute,
  retryIngestionRoute,
  uploadBySignedUrlRoute,
];
