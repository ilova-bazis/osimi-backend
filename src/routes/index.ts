import { authRoutes } from "./auth.ts";
import { healthRoute } from "./health.ts";
import { ingestionRoutes } from "./ingestions.ts";
import { leaseRoutes } from "./lease.ts";
import type { RouteDefinition } from "./types.ts";

export const routes: RouteDefinition[] = [healthRoute, ...authRoutes, ...ingestionRoutes, ...leaseRoutes];
