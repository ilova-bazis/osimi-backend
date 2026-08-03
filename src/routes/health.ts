import { jsonResponse } from "../http/response.ts";
import { createReadinessService } from "../services/readiness-service.ts";
import type { ReadinessService } from "../services/readiness-service.ts";
import { LifecycleController } from "../runtime/lifecycle.ts";
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

export function createReadinessRoute(readiness: ReadinessService): RouteDefinition {
  return {
    method: "GET",
    path: "/readyz",
    handler: async (_request, context) => {
      const result = await readiness.check();
      return jsonResponse({
        status: result.ready ? "ready" : "not_ready",
        service: "osimi-backend",
        request_id: context.requestId,
        timestamp: new Date().toISOString(),
        checks: result.checks,
      }, {
        status: result.ready ? 200 : 503,
      });
    },
  };
}

const defaultLifecycle = new LifecycleController();
export const readinessRoute = createReadinessRoute(
  createReadinessService({ lifecycle: defaultLifecycle }),
);
