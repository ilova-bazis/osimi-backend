import { requireWorkerAuthentication } from "../auth/worker.ts";
import { ValidationError } from "../http/errors.ts";
import { jsonResponse } from "../http/response.ts";
import { ingestWorkerEvents } from "../services/event-service.ts";
import {
  downloadStagedFileByToken,
  heartbeatLease,
  leaseNextIngestion,
  releaseActiveLease,
} from "../services/lease-service.ts";
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

const leaseRoute: RouteDefinition = {
  method: "POST",
  path: "/api/ingestions/lease",
  handler: async (request, _context) => {
    const worker = requireWorkerAuthentication(request);

    return jsonResponse(
      await leaseNextIngestion({
        workerId: worker.workerId,
      }),
    );
  },
};

const heartbeatRoute: RouteDefinition = {
  method: "POST",
  path: "/api/ingestions/:id/lease/heartbeat",
  handler: async (request, _context) => {
    requireWorkerAuthentication(request);
    const pathname = new URL(request.url).pathname;
    const ingestionId = extractPathParam(pathname, /^\/api\/ingestions\/([^/]+)\/lease\/heartbeat$/, "id");
    const body = await parseJsonBody(request);

    return jsonResponse(
      await heartbeatLease({
        ingestionId,
        body,
      }),
    );
  },
};

const releaseRoute: RouteDefinition = {
  method: "POST",
  path: "/api/ingestions/:id/lease/release",
  handler: async (request, _context) => {
    requireWorkerAuthentication(request);
    const pathname = new URL(request.url).pathname;
    const ingestionId = extractPathParam(pathname, /^\/api\/ingestions\/([^/]+)\/lease\/release$/, "id");
    const body = await parseJsonBody(request);

    return jsonResponse(
      await releaseActiveLease({
        ingestionId,
        body,
      }),
    );
  },
};

const workerDownloadRoute: RouteDefinition = {
  method: "GET",
  path: "/api/worker/downloads/:token",
  handler: async (request, _context) => {
    const pathname = new URL(request.url).pathname;
    const token = extractPathParam(pathname, /^\/api\/worker\/downloads\/([^/]+)$/, "token");
    return downloadStagedFileByToken({ token });
  },
};

const workerEventsRoute: RouteDefinition = {
  method: "POST",
  path: "/api/ingestions/:id/events",
  handler: async (request, _context) => {
    requireWorkerAuthentication(request);
    const pathname = new URL(request.url).pathname;
    const ingestionId = extractPathParam(pathname, /^\/api\/ingestions\/([^/]+)\/events$/, "id");
    const body = await parseJsonBody(request);

    return jsonResponse(
      await ingestWorkerEvents({
        ingestionId,
        body,
      }),
    );
  },
};

export const leaseRoutes: RouteDefinition[] = [
  leaseRoute,
  heartbeatRoute,
  releaseRoute,
  workerDownloadRoute,
  workerEventsRoute,
];
