import { requireRole } from "../auth/guards.ts";
import { jsonResponse } from "../http/response.ts";
import { parseJsonBody } from "../validation/common.ts";
import {
  parseAddIngestionItemFileBody,
  parseCommitUploadedFileBody,
  parseCreateIngestionItemBody,
  parseCreateIngestionBody,
  parseCreatePresignedUploadBody,
  parseIngestionFileIdParam,
  parseIngestionIdParam,
  parseIngestionItemIdParam,
  parseIngestionListQuery,
  parseReorderIngestionItemsBody,
  parseReorderIngestionItemFilesBody,
  parseUpdateIngestionItemBody,
  parseUpdateIngestionFileOverridesBody,
  parseUpdateIngestionBody,
  parseUploadTokenParam,
} from "../validation/ingestion.ts";
import {
  addIngestionFileToItem,
  createIngestionItemForIngestion,
  listIngestionItemFilesForIngestionItem,
  listIngestionItemsForIngestion,
  reorderIngestionItemsForIngestion,
  reorderFilesInIngestionItem,
  updateIngestionItemMetadata,
} from "../services/ingestion-item-service.ts";
import {
  cancelIngestion as cancelIngestionRecord,
  commitUploadedFile as commitUploadedFileRecord,
  createIngestionDraft as createIngestionDraftRecord,
  createPresignedUpload as createPresignedUploadRecord,
  deleteIngestionRecord as deleteIngestionRecordService,
  getIngestionCapabilities as getIngestionCapabilitiesRecord,
  getIngestion as getIngestionRecord,
  getIngestionList as getIngestionListRecord,
  removeIngestionFile as removeIngestionFileRecord,
  restoreIngestion as restoreIngestionRecord,
  retryIngestion as retryIngestionRecord,
  streamIngestionFilePreview as streamIngestionFilePreviewRecord,
  submitIngestion as submitIngestionRecord,
  updateIngestion as updateIngestionRecord,
  updateIngestionFileOverrides as updateIngestionFileOverridesRecord,
  uploadFileBySignedToken as uploadFileBySignedTokenRecord,
} from "../services/ingestion-service.ts";
import { runIdempotentIngestionMutation } from "../services/ingestion-idempotency-service.ts";
import { extractPathParam } from "./params.ts";
import type { RouteDefinition } from "./types.ts";

const createIngestionRoute: RouteDefinition = {
  method: "POST",
  path: "/api/ingestions",
  handler: async (request, context) => {
    const auth = requireRole(context, ["archiver", "admin"]);
    const body = parseCreateIngestionBody(await parseJsonBody(request));
    const result = await runIdempotentIngestionMutation({
      auth,
      idempotencyKey: context.idempotencyKey,
      endpoint: "ingestions.create.v1",
      request: { body },
      statusCode: 201,
      handler: (executor) => createIngestionDraftRecord({ auth, body, executor }),
    });
    return jsonResponse(result.body, { status: result.statusCode });
  },
};

const listIngestionsRoute: RouteDefinition = {
  method: "GET",
  path: "/api/ingestions",
  handler: async (request, context) => {
    const auth = requireRole(context, ["viewer", "archiver", "admin"]);
    const url = new URL(request.url);
    const query = parseIngestionListQuery(url);
    const result = await getIngestionListRecord({ auth, query });

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
    return jsonResponse(await getIngestionRecord({ auth, ingestionId }));
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
      await updateIngestionRecord({
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
      await deleteIngestionRecordService({
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
    return jsonResponse(getIngestionCapabilitiesRecord());
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

    const result = await runIdempotentIngestionMutation({
      auth,
      idempotencyKey: context.idempotencyKey,
      endpoint: "ingestions.files.presign.v1",
      request: { ingestion_id: ingestionId, body },
      statusCode: 201,
      handler: (executor) => createPresignedUploadRecord({
        auth,
        ingestionId,
        body,
        executor,
      }),
    });
    return jsonResponse(result.body, { status: result.statusCode });
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
      await removeIngestionFileRecord({
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
      await updateIngestionFileOverridesRecord({
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

    const result = await runIdempotentIngestionMutation({
      auth,
      idempotencyKey: context.idempotencyKey,
      endpoint: "ingestions.files.commit.v1",
      request: { ingestion_id: ingestionId, body },
      statusCode: 200,
      handler: (executor) => commitUploadedFileRecord({
        auth,
        ingestionId,
        body,
        executor,
      }),
    });
    return jsonResponse(result.body, { status: result.statusCode });
  },
};

const getIngestionFilePreviewRoute: RouteDefinition = {
  method: "GET",
  path: "/api/ingestions/:id/files/:fileId/preview",
  handler: async (request, context) => {
    const auth = requireRole(context, ["viewer", "archiver", "admin"]);
    const pathname = new URL(request.url).pathname;
    const ingestionId = parseIngestionIdParam(
      extractPathParam(
        pathname,
        /^\/api\/ingestions\/([^/]+)\/files\/[^/]+\/preview$/,
        "id",
      ),
    );
    const fileId = parseIngestionFileIdParam(
      extractPathParam(
        pathname,
        /^\/api\/ingestions\/[^/]+\/files\/([^/]+)\/preview$/,
        "fileId",
      ),
    );

    return await streamIngestionFilePreviewRecord({
      auth,
      ingestionId,
      fileId,
    });
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

    const result = await runIdempotentIngestionMutation({
      auth,
      idempotencyKey: context.idempotencyKey,
      endpoint: "ingestions.submit.v1",
      request: { ingestion_id: ingestionId },
      statusCode: 200,
      handler: (executor) => submitIngestionRecord({ auth, ingestionId, executor }),
    });
    return jsonResponse(result.body, { status: result.statusCode });
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
      await restoreIngestionRecord({
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
      await cancelIngestionRecord({
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

    const result = await runIdempotentIngestionMutation({
      auth,
      idempotencyKey: context.idempotencyKey,
      endpoint: "ingestions.retry.v1",
      request: { ingestion_id: ingestionId },
      statusCode: 200,
      handler: (executor) => retryIngestionRecord({ auth, ingestionId, executor }),
    });
    return jsonResponse(result.body, { status: result.statusCode });
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
      await uploadFileBySignedTokenRecord({
        uploadToken,
        request,
      }),
    );
  },
};

const createIngestionItemRoute: RouteDefinition = {
  method: "POST",
  path: "/api/ingestions/:id/items",
  handler: async (request, context) => {
    const auth = requireRole(context, ["archiver", "admin"]);
    const pathname = new URL(request.url).pathname;
    const ingestionId = parseIngestionIdParam(
      extractPathParam(pathname, /^\/api\/ingestions\/([^/]+)\/items$/, "id"),
    );
    const body = parseCreateIngestionItemBody(await parseJsonBody(request));

    return jsonResponse(
      await createIngestionItemForIngestion({
        auth,
        ingestionId,
        body,
      }),
      { status: 201 },
    );
  },
};

const listIngestionItemsRoute: RouteDefinition = {
  method: "GET",
  path: "/api/ingestions/:id/items",
  handler: async (request, context) => {
    const auth = requireRole(context, ["viewer", "archiver", "admin"]);
    const pathname = new URL(request.url).pathname;
    const ingestionId = parseIngestionIdParam(
      extractPathParam(pathname, /^\/api\/ingestions\/([^/]+)\/items$/, "id"),
    );

    return jsonResponse(
      await listIngestionItemsForIngestion({
        auth,
        ingestionId,
      }),
    );
  },
};

const reorderIngestionItemsRoute: RouteDefinition = {
  method: "PATCH",
  path: "/api/ingestions/:id/items/order",
  handler: async (request, context) => {
    const auth = requireRole(context, ["archiver", "admin"]);
    const pathname = new URL(request.url).pathname;
    const ingestionId = parseIngestionIdParam(
      extractPathParam(pathname, /^\/api\/ingestions\/([^/]+)\/items\/order$/, "id"),
    );
    const body = parseReorderIngestionItemsBody(await parseJsonBody(request));

    return jsonResponse(
      await reorderIngestionItemsForIngestion({
        auth,
        ingestionId,
        body,
      }),
    );
  },
};

const updateIngestionItemRoute: RouteDefinition = {
  method: "PATCH",
  path: "/api/ingestions/:id/items/:itemId",
  handler: async (request, context) => {
    const auth = requireRole(context, ["archiver", "admin"]);
    const pathname = new URL(request.url).pathname;
    const ingestionId = parseIngestionIdParam(
      extractPathParam(pathname, /^\/api\/ingestions\/([^/]+)\/items\/[^/]+$/, "id"),
    );
    const ingestionItemId = parseIngestionItemIdParam(
      extractPathParam(pathname, /^\/api\/ingestions\/[^/]+\/items\/([^/]+)$/, "itemId"),
    );
    const body = parseUpdateIngestionItemBody(await parseJsonBody(request));

    return jsonResponse(
      await updateIngestionItemMetadata({
        auth,
        ingestionId,
        ingestionItemId,
        body,
      }),
    );
  },
};

const addIngestionItemFileRoute: RouteDefinition = {
  method: "POST",
  path: "/api/ingestions/:id/items/:itemId/files",
  handler: async (request, context) => {
    const auth = requireRole(context, ["archiver", "admin"]);
    const pathname = new URL(request.url).pathname;
    const ingestionId = parseIngestionIdParam(
      extractPathParam(
        pathname,
        /^\/api\/ingestions\/([^/]+)\/items\/[^/]+\/files$/,
        "id",
      ),
    );
    const ingestionItemId = parseIngestionItemIdParam(
      extractPathParam(
        pathname,
        /^\/api\/ingestions\/[^/]+\/items\/([^/]+)\/files$/,
        "itemId",
      ),
    );
    const body = parseAddIngestionItemFileBody(await parseJsonBody(request));

    return jsonResponse(
      await addIngestionFileToItem({
        auth,
        ingestionId,
        ingestionItemId,
        body,
      }),
      { status: 201 },
    );
  },
};

const listIngestionItemFilesRoute: RouteDefinition = {
  method: "GET",
  path: "/api/ingestions/:id/items/:itemId/files",
  handler: async (request, context) => {
    const auth = requireRole(context, ["viewer", "archiver", "admin"]);
    const pathname = new URL(request.url).pathname;
    const ingestionId = parseIngestionIdParam(
      extractPathParam(
        pathname,
        /^\/api\/ingestions\/([^/]+)\/items\/[^/]+\/files$/,
        "id",
      ),
    );
    const ingestionItemId = parseIngestionItemIdParam(
      extractPathParam(
        pathname,
        /^\/api\/ingestions\/[^/]+\/items\/([^/]+)\/files$/,
        "itemId",
      ),
    );

    return jsonResponse(
      await listIngestionItemFilesForIngestionItem({
        auth,
        ingestionId,
        ingestionItemId,
      }),
    );
  },
};

const reorderIngestionItemFilesRoute: RouteDefinition = {
  method: "PATCH",
  path: "/api/ingestions/:id/items/:itemId/files/order",
  handler: async (request, context) => {
    const auth = requireRole(context, ["archiver", "admin"]);
    const pathname = new URL(request.url).pathname;
    const ingestionId = parseIngestionIdParam(
      extractPathParam(
        pathname,
        /^\/api\/ingestions\/([^/]+)\/items\/[^/]+\/files\/order$/,
        "id",
      ),
    );
    const ingestionItemId = parseIngestionItemIdParam(
      extractPathParam(
        pathname,
        /^\/api\/ingestions\/[^/]+\/items\/([^/]+)\/files\/order$/,
        "itemId",
      ),
    );
    const body = parseReorderIngestionItemFilesBody(await parseJsonBody(request));

    return jsonResponse(
      await reorderFilesInIngestionItem({
        auth,
        ingestionId,
        ingestionItemId,
        body,
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
  createIngestionItemRoute,
  listIngestionItemsRoute,
  reorderIngestionItemsRoute,
  updateIngestionItemRoute,
  presignFileRoute,
  addIngestionItemFileRoute,
  listIngestionItemFilesRoute,
  reorderIngestionItemFilesRoute,
  removeFileRoute,
  updateFileOverridesRoute,
  commitFileRoute,
  getIngestionFilePreviewRoute,
  submitIngestionRoute,
  cancelIngestionRoute,
  restoreIngestionRoute,
  retryIngestionRoute,
  uploadBySignedUrlRoute,
];
