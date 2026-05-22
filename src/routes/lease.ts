import { jsonResponse } from "../http/response.ts";
import { ingestWorkerEvents } from "../services/event-service.ts";
import {
  claimNextIngestionPreview,
  completeIngestionPreview,
  failIngestionPreview,
  presignIngestionPreviewUpload,
} from "../services/ingestion-service.ts";
import {
  downloadStagedFileByToken,
  heartbeatLease,
  leaseIngestionById,
  leaseNextIngestion,
  releaseActiveLease,
} from "../services/lease-service.ts";
import { parseIngestWorkerEventsBody } from "../validation/event.ts";
import { parseLeaseTokenBody } from "../validation/lease.ts";
import {
  parseIngestionIdParam,
  parseIngestionFileIdParam,
  parseWorkerCompleteIngestionPreviewBody,
  parseWorkerFailIngestionPreviewBody,
  parseWorkerPresignIngestionPreviewUploadBody,
} from "../validation/ingestion.ts";
import { withWorkerAuth, withWorkerAuthorizedLease } from "./middleware.ts";
import { extractPathParam } from "./params.ts";
import { parseUploadTokenParam } from "../validation/ingestion.ts";
import { parseJsonBody } from "../validation/common.ts";
import type { RouteDefinition } from "./types.ts";

const leaseRoute: RouteDefinition = {
  method: "POST",
  path: "/api/ingestions/lease",
  handler: withWorkerAuth(async (_request, _context, worker) => {
    return jsonResponse(
      await leaseNextIngestion({
        workerId: worker.workerId,
      }),
    );
  }),
};

const heartbeatRoute: RouteDefinition = {
  method: "POST",
  path: "/api/ingestions/:id/lease/heartbeat",
  handler: withWorkerAuthorizedLease({
    pathPattern: /^\/api\/ingestions\/([^/]+)\/lease\/heartbeat$/,
    pathParamName: "id",
    parseParam: parseIngestionIdParam,
    parseBody: parseLeaseTokenBody,
    handler: async (_request, _context, data) => {
      return jsonResponse(
        await heartbeatLease({
          authorizedLease: data.authorizedLease,
        }),
      );
    },
  }),
};

const leaseByIdRoute: RouteDefinition = {
  method: "POST",
  path: "/api/ingestions/:id/lease",
  handler: withWorkerAuth(async (request, _context, worker) => {
    const pathname = new URL(request.url).pathname;
    const ingestionId = parseIngestionIdParam(
      extractPathParam(pathname, /^\/api\/ingestions\/([^/]+)\/lease$/, "id"),
    );

    return jsonResponse(
      await leaseIngestionById({
        ingestionId,
        workerId: worker.workerId,
      }),
    );
  }),
};

const releaseRoute: RouteDefinition = {
  method: "POST",
  path: "/api/ingestions/:id/lease/release",
  handler: withWorkerAuthorizedLease({
    pathPattern: /^\/api\/ingestions\/([^/]+)\/lease\/release$/,
    pathParamName: "id",
    parseParam: parseIngestionIdParam,
    parseBody: parseLeaseTokenBody,
    handler: async (_request, _context, data) => {
      return jsonResponse(
        await releaseActiveLease({
          authorizedLease: data.authorizedLease,
        }),
      );
    },
  }),
};

const workerDownloadRoute: RouteDefinition = {
  method: "GET",
  path: "/api/worker/downloads/:token",
  handler: async (request, _context) => {
    const pathname = new URL(request.url).pathname;
    const token = parseUploadTokenParam(
      extractPathParam(
        pathname,
        /^\/api\/worker\/downloads\/([^/]+)$/,
        "token",
      ),
    );
    return downloadStagedFileByToken({ token });
  },
};

const workerEventsRoute: RouteDefinition = {
  method: "POST",
  path: "/api/ingestions/:id/events",
  handler: withWorkerAuthorizedLease({
    pathPattern: /^\/api\/ingestions\/([^/]+)\/events$/,
    pathParamName: "id",
    parseParam: parseIngestionIdParam,
    parseBody: parseIngestWorkerEventsBody,
    handler: async (_request, _context, data) => {
      return jsonResponse(
        await ingestWorkerEvents({
          authorizedLease: data.authorizedLease,
          events: data.body.events,
        }),
      );
    },
  }),
};

const claimIngestionPreviewRoute: RouteDefinition = {
  method: "POST",
  path: "/api/worker/ingestion-previews/claim",
  handler: withWorkerAuth(async (_request, _context, worker) => {
    return jsonResponse(
      await claimNextIngestionPreview({
        workerId: worker.workerId,
      }),
    );
  }),
};

const presignIngestionPreviewRoute: RouteDefinition = {
  method: "POST",
  path: "/api/worker/ingestion-previews/:id/files/:fileId/presign",
  handler: withWorkerAuth(async (request, _context, worker) => {
    const pathname = new URL(request.url).pathname;
    const ingestionId = parseIngestionIdParam(
      extractPathParam(
        pathname,
        /^\/api\/worker\/ingestion-previews\/([^/]+)\/files\/[^/]+\/presign$/,
        "id",
      ),
    );
    const fileId = parseIngestionFileIdParam(
      extractPathParam(
        pathname,
        /^\/api\/worker\/ingestion-previews\/[^/]+\/files\/([^/]+)\/presign$/,
        "fileId",
      ),
    );
    const body = parseWorkerPresignIngestionPreviewUploadBody(
      await parseJsonBody(request),
    );

    return jsonResponse(
      await presignIngestionPreviewUpload({
        workerId: worker.workerId,
        ingestionId,
        fileId,
        body,
      }),
    );
  }),
};

const completeIngestionPreviewRoute: RouteDefinition = {
  method: "POST",
  path: "/api/worker/ingestion-previews/:id/files/:fileId/complete",
  handler: withWorkerAuth(async (request, _context, worker) => {
    const pathname = new URL(request.url).pathname;
    const ingestionId = parseIngestionIdParam(
      extractPathParam(
        pathname,
        /^\/api\/worker\/ingestion-previews\/([^/]+)\/files\/[^/]+\/complete$/,
        "id",
      ),
    );
    const fileId = parseIngestionFileIdParam(
      extractPathParam(
        pathname,
        /^\/api\/worker\/ingestion-previews\/[^/]+\/files\/([^/]+)\/complete$/,
        "fileId",
      ),
    );
    const body = parseWorkerCompleteIngestionPreviewBody(
      await parseJsonBody(request),
    );

    return jsonResponse(
      await completeIngestionPreview({
        workerId: worker.workerId,
        ingestionId,
        fileId,
        body,
      }),
    );
  }),
};

const failIngestionPreviewRoute: RouteDefinition = {
  method: "POST",
  path: "/api/worker/ingestion-previews/:id/files/:fileId/fail",
  handler: withWorkerAuth(async (request, _context, worker) => {
    const pathname = new URL(request.url).pathname;
    const ingestionId = parseIngestionIdParam(
      extractPathParam(
        pathname,
        /^\/api\/worker\/ingestion-previews\/([^/]+)\/files\/[^/]+\/fail$/,
        "id",
      ),
    );
    const fileId = parseIngestionFileIdParam(
      extractPathParam(
        pathname,
        /^\/api\/worker\/ingestion-previews\/[^/]+\/files\/([^/]+)\/fail$/,
        "fileId",
      ),
    );
    const body = parseWorkerFailIngestionPreviewBody(await parseJsonBody(request));

    return jsonResponse(
      await failIngestionPreview({
        workerId: worker.workerId,
        ingestionId,
        fileId,
        body,
      }),
    );
  }),
};

export const leaseRoutes: RouteDefinition[] = [
  leaseRoute,
  leaseByIdRoute,
  heartbeatRoute,
  releaseRoute,
  workerDownloadRoute,
  workerEventsRoute,
  claimIngestionPreviewRoute,
  presignIngestionPreviewRoute,
  completeIngestionPreviewRoute,
  failIngestionPreviewRoute,
];
