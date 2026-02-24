import { requireRole } from "../auth/guards.ts";
import { jsonResponse } from "../http/response.ts";
import { getDashboardActivityForTenant, getDashboardSummaryForTenant } from "../services/dashboard-service.ts";
import { parseDashboardActivityQuery } from "../validation/dashboard.ts";
import type { RouteDefinition } from "./types.ts";

const summaryRoute: RouteDefinition = {
  method: "GET",
  path: "/api/dashboard/summary",
  handler: async (_request, context) => {
    const auth = requireRole(context, ["viewer", "archiver", "admin"]);
    return jsonResponse(await getDashboardSummaryForTenant({ auth }));
  },
};

const activityRoute: RouteDefinition = {
  method: "GET",
  path: "/api/dashboard/activity",
  handler: async (request, context) => {
    const auth = requireRole(context, ["viewer", "archiver", "admin"]);
    const url = new URL(request.url);
    const query = parseDashboardActivityQuery(url);
    return jsonResponse(await getDashboardActivityForTenant({ auth, query }));
  },
};

export const dashboardRoutes: RouteDefinition[] = [summaryRoute, activityRoute];
