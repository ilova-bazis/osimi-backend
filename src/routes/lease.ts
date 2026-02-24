import { jsonResponse } from "../http/response.ts";
import { ingestWorkerEvents } from "../services/event-service.ts";
import {
  downloadStagedFileByToken,
  heartbeatLease,
  leaseNextIngestion,
  releaseActiveLease,
} from "../services/lease-service.ts";
import { parseIngestWorkerEventsBody } from "../validation/event.ts";
import { parseLeaseTokenBody } from "../validation/lease.ts";
import { parseIngestionIdParam } from "../validation/ingestion.ts";
import { withWorkerAuth, withWorkerAuthorizedLease } from "./middleware.ts";
import { extractPathParam } from "./params.ts";
import { parseUploadTokenParam } from "../validation/ingestion.ts";
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

export const leaseRoutes: RouteDefinition[] = [
  leaseRoute,
  heartbeatRoute,
  releaseRoute,
  workerDownloadRoute,
  workerEventsRoute,
];
