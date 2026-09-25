import type { RequestContext } from "../http/context.ts";

export type HttpMethod = "GET" | "POST" | "PUT" | "PATCH" | "DELETE";
export type RouteAuthMode = "session" | "none" | "worker";

export type RouteHandler = (
  request: Request,
  context: RequestContext,
) => Response | Promise<Response>;

export type AuthAwareRouteHandler = RouteHandler & {
  authMode?: RouteAuthMode;
};

export interface RouteDefinition {
  method: HttpMethod;
  path: string;
  auth?: RouteAuthMode;
  handler: AuthAwareRouteHandler;
}
