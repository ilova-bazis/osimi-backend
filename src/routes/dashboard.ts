import { requireRole, requireTenantScope } from "../auth/guards.ts";
import { jsonResponse } from "../http/response.ts";
import { getDashboardActivityForTenant, getDashboardSummaryForTenant } from "../services/dashboard-service.ts";
import type { RouteDefinition } from "./types.ts";

const summaryRoute: RouteDefinition = {
  method: "GET",
  path: "/api/dashboard/summary",
  handler: async (_request, context) => {
    requireRole(context, ["viewer", "operator", "admin"]);
    const tenantId = requireTenantScope(context);
    return jsonResponse(await getDashboardSummaryForTenant(tenantId));
  },
};

const activityRoute: RouteDefinition = {
  method: "GET",
  path: "/api/dashboard/activity",
  handler: async (request, context) => {
    requireRole(context, ["viewer", "operator", "admin"]);
    const tenantId = requireTenantScope(context);
    const url = new URL(request.url);
    return jsonResponse(await getDashboardActivityForTenant({ tenantId, url }));
  },
};

export const dashboardRoutes: RouteDefinition[] = [summaryRoute, activityRoute];
