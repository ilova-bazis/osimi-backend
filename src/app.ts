import { withRequestContext } from "./http/context.ts";
import {
  createErrorResponse,
  MethodNotAllowedError,
  NotFoundError,
  ServiceUnavailableError,
} from "./http/errors.ts";
import { createRoutes } from "./routes/index.ts";
import type { RouteDefinition } from "./routes/types.ts";
import {
  createReadinessService,
  type ReadinessService,
} from "./services/readiness-service.ts";
import {
  resolveCorsAllowedOrigins,
  runWithRuntimeConfig,
  validateRuntimeConfiguration,
  type RuntimeConfig,
} from "./runtime/config.ts";
import { LifecycleController } from "./runtime/lifecycle.ts";

interface App {
  fetch: (request: Request) => Promise<Response>;
}

const ALLOWED_METHODS = "GET,POST,PATCH,PUT,DELETE,OPTIONS";
const ALLOWED_HEADERS =
  "authorization,content-type,x-tenant-id,x-request-id,x-idempotency-key,x-worker-auth-token,x-worker-id";

type CorsHeaders = Record<string, string>;

function resolveCorsHeaders(
  origin: string | null,
  allowedOrigins: ReadonlySet<string>,
): CorsHeaders | undefined {
  if (!origin || !allowedOrigins.has(origin)) {
    return undefined;
  }

  return {
    "access-control-allow-origin": origin,
    "access-control-allow-methods": ALLOWED_METHODS,
    "access-control-allow-headers": ALLOWED_HEADERS,
    "access-control-max-age": "600",
  };
}

function addVaryHeader(headers: Headers, value: string): void {
  const existing = headers.get("vary");
  const values = existing ? existing.split(",").map((item) => item.trim()).filter(Boolean) : [];

  if (!values.some((item) => item.toLowerCase() === value.toLowerCase())) {
    values.push(value);
  }

  headers.set("vary", values.join(", "));
}

function normalizePath(pathname: string): string {
  if (pathname.length > 1 && pathname.endsWith("/")) {
    return pathname.slice(0, -1);
  }

  return pathname;
}

function routeKey(method: string, path: string): string {
  return `${method} ${path}`;
}

function isDynamicPath(path: string): boolean {
  return path.includes(":");
}

function pathMatches(pattern: string, pathname: string): boolean {
  const patternParts = pattern.split("/");
  const pathParts = pathname.split("/");

  if (patternParts.length !== pathParts.length) {
    return false;
  }

  for (let index = 0; index < patternParts.length; index += 1) {
    const patternPart = patternParts[index] ?? "";
    const pathPart = pathParts[index] ?? "";

    if (patternPart === pathPart) {
      continue;
    }

    if (patternPart.startsWith(":")) {
      continue;
    }

    if (patternPart !== pathPart) {
      return false;
    }
  }

  return true;
}

interface DynamicRoute {
  method: string;
  path: string;
  handler: RouteDefinition["handler"];
}

export function createApp(
  routeDefinitions?: RouteDefinition[],
): App {
  return createAppWithOptions({ routeDefinitions });
}

interface CreateAppOptions {
  routeDefinitions?: RouteDefinition[];
  runtimeConfig?: RuntimeConfig;
  lifecycle?: LifecycleController;
  readinessService?: ReadinessService;
}

export function createAppWithOptions(options: CreateAppOptions = {}): App {
  const runtimeConfig = options.runtimeConfig ?? {};
  const lifecycle = options.lifecycle ?? new LifecycleController();
  const readiness = options.readinessService ?? createReadinessService({ lifecycle });
  const routeDefinitions = options.routeDefinitions ?? createRoutes(readiness);
  validateRuntimeConfiguration(runtimeConfig);
  const corsAllowedOrigins = new Set(resolveCorsAllowedOrigins(runtimeConfig));
  const handlers = new Map<string, RouteDefinition["handler"]>();
  const methodsByPath = new Map<string, Set<string>>();
  const dynamicRoutes: DynamicRoute[] = [];
  const registeredRouteKeys = new Set<string>();

  for (const route of routeDefinitions) {
    const normalizedPath = normalizePath(route.path);
    const method = route.method.toUpperCase();
    const key = routeKey(method, normalizedPath);

    if (registeredRouteKeys.has(key)) {
      throw new Error(`Duplicate route registration detected for '${key}'.`);
    }

    registeredRouteKeys.add(key);

    if (isDynamicPath(normalizedPath)) {
      dynamicRoutes.push({
        method,
        path: normalizedPath,
        handler: route.handler,
      });
    } else {
      handlers.set(key, route.handler);
    }

    const methods = methodsByPath.get(normalizedPath) ?? new Set<string>();
    methods.add(method);
    methodsByPath.set(normalizedPath, methods);
  }

  return {
    async fetch(request: Request): Promise<Response> {
      const probePath = normalizePath(new URL(request.url).pathname);
      const isProbe = probePath === "/healthz" || probePath === "/readyz";
      const releaseRequest = isProbe ? undefined : lifecycle.admitRequest();
      if (!isProbe && !releaseRequest) {
        const requestId = crypto.randomUUID();
        const response = createErrorResponse(new ServiceUnavailableError(), requestId);
        response.headers.set("x-request-id", requestId);
        return response;
      }

      try {
        return await runWithRuntimeConfig(runtimeConfig, async () => {
        const origin = request.headers.get("origin");
        const corsHeaders = resolveCorsHeaders(origin, corsAllowedOrigins);

        if (request.method.toUpperCase() === "OPTIONS") {
          const headers = new Headers(corsHeaders ?? {});
          if (origin !== null) {
            addVaryHeader(headers, "Origin");
          }
          return new Response(null, {
            status: 204,
            headers,
          });
        }

        let response: Response;

        try {
          response = await withRequestContext(request, async (context) => {
            const url = new URL(request.url);
            const pathname = normalizePath(url.pathname);
            const method = request.method.toUpperCase();
            const key = routeKey(method, pathname);
            let handler = handlers.get(key);

            if (!handler) {
              for (const route of dynamicRoutes) {
                if (
                  route.method === method &&
                  pathMatches(route.path, pathname)
                ) {
                  handler = route.handler;
                  break;
                }
              }
            }

            if (!handler) {
              let allowedMethods = methodsByPath.get(pathname);

              if (!allowedMethods) {
                const matchedMethods = new Set<string>();

                for (const route of dynamicRoutes) {
                  if (pathMatches(route.path, pathname)) {
                    matchedMethods.add(route.method);
                  }
                }

                if (matchedMethods.size > 0) {
                  allowedMethods = matchedMethods;
                }
              }

              if (allowedMethods) {
                throw new MethodNotAllowedError(pathname, [...allowedMethods]);
              }

              throw new NotFoundError(
                `Route '${method} ${pathname}' was not found.`,
              );
            }

            return await handler(request, context);
          }, {
            skipAuth: isProbe,
          });
        } catch (error) {
          const fallbackRequestId = crypto.randomUUID();
          response = createErrorResponse(error, fallbackRequestId);
          response.headers.set("x-request-id", fallbackRequestId);
        }

        if (corsHeaders) {
          for (const [key, value] of Object.entries(corsHeaders)) {
            response.headers.set(key, value);
          }
        }
        if (origin !== null) {
          addVaryHeader(response.headers, "Origin");
        }

        return response;
        });
      } finally {
        releaseRequest?.();
      }
    },
  };
}
