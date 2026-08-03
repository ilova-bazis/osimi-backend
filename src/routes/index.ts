import { authRoutes } from "./auth.ts";
import { dashboardRoutes } from "./dashboard.ts";
import { healthRoute, readinessRoute, createReadinessRoute } from "./health.ts";
import { ingestionRoutes } from "./ingestions.ts";
import { leaseRoutes } from "./lease.ts";
import { objectRoutes } from "./objects.ts";
import type { RouteDefinition } from "./types.ts";
import type { ReadinessService } from "../services/readiness-service.ts";

export const routes: RouteDefinition[] = [
  healthRoute,
  readinessRoute,
  ...authRoutes,
  ...dashboardRoutes,
  ...ingestionRoutes,
  ...leaseRoutes,
  ...objectRoutes,
];

export function createRoutes(readiness: ReadinessService): RouteDefinition[] {
  return [
    healthRoute,
    createReadinessRoute(readiness),
    ...authRoutes,
    ...dashboardRoutes,
    ...ingestionRoutes,
    ...leaseRoutes,
    ...objectRoutes,
  ];
}
