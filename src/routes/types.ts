import type { RequestContext } from "../http/context.ts";

export type HttpMethod = "GET" | "POST" | "PUT" | "PATCH" | "DELETE";

export interface RouteDefinition {
  method: HttpMethod;
  path: string;
  handler: (request: Request, context: RequestContext) => Response | Promise<Response>;
}
