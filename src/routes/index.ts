import { authRoutes } from "./auth.ts";
import { dashboardRoutes } from "./dashboard.ts";
import { healthRoute } from "./health.ts";
import { ingestionRoutes } from "./ingestions.ts";
import { leaseRoutes } from "./lease.ts";
import { objectRoutes } from "./objects.ts";
import type { RouteDefinition } from "./types.ts";

export const routes: RouteDefinition[] = [
  healthRoute,
  ...authRoutes,
  ...dashboardRoutes,
  ...ingestionRoutes,
  ...leaseRoutes,
  ...objectRoutes,
];
