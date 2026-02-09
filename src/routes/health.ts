import { jsonResponse } from "../http/response.ts";
import type { RouteDefinition } from "./types.ts";

export const healthRoute: RouteDefinition = {
  method: "GET",
  path: "/healthz",
  handler: (_request, context) => {
    return jsonResponse({
      status: "ok",
      service: "osimi-backend",
      request_id: context.requestId,
      timestamp: new Date().toISOString(),
    });
  },
};
