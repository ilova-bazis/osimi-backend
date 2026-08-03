import { createAppWithOptions } from "./app.ts";
import { ConfigurationError, createErrorResponse } from "./http/errors.ts";
import { startBackgroundJobs } from "./jobs/index.ts";
import { runWithRuntimeConfig, resolveShutdownGracePeriodMs } from "./runtime/config.ts";
import type { RuntimeConfig } from "./runtime/config.ts";
import { LifecycleController } from "./runtime/lifecycle.ts";
import { createShutdownCoordinator } from "./runtime/shutdown.ts";
import type { ShutdownResult } from "./runtime/shutdown.ts";
import type { RouteDefinition } from "./routes/types.ts";

const DEFAULT_PORT = 3000;
const DEFAULT_HOSTNAME = "0.0.0.0";

export interface ServerOptions {
  port?: number;
  hostname?: string;
  runtimeConfig?: RuntimeConfig;
  routeDefinitions?: RouteDefinition[];
}

export interface RunningServer {
  server: Bun.Server<unknown>;
  lifecycle: LifecycleController;
  shutdown(reason: string): Promise<ShutdownResult>;
  forceShutdown(reason: string): Promise<ShutdownResult>;
}

function resolvePort(rawValue: string | undefined): number {
  if (!rawValue) {
    return DEFAULT_PORT;
  }

  const parsed = Number.parseInt(rawValue, 10);

  if (!Number.isFinite(parsed) || parsed < 1 || parsed > 65535) {
    throw new ConfigurationError("Environment variable 'PORT' is invalid.", {
      provided_value: rawValue,
    });
  }

  return parsed;
}

export function startServer(options: ServerOptions = {}): RunningServer {
  const lifecycle = new LifecycleController();
  const runtimeConfig = options.runtimeConfig ?? {};
  const app = createAppWithOptions({
    runtimeConfig,
    lifecycle,
    routeDefinitions: options.routeDefinitions,
  });
  const port = options.port ?? resolvePort(process.env.PORT);
  const hostname = options.hostname ?? process.env.HOST ?? DEFAULT_HOSTNAME;

  const server = Bun.serve({
    hostname,
    port,
    fetch: app.fetch,
    error(error: Error): Response {
      const requestId = crypto.randomUUID();
      const response = createErrorResponse(error, requestId);
      response.headers.set("x-request-id", requestId);
      return response;
    },
  });

  let jobs;
  try {
    jobs = runWithRuntimeConfig(runtimeConfig, startBackgroundJobs);
  } catch (error) {
    void server.stop(true);
    throw error;
  }
  const shutdown = createShutdownCoordinator({
    lifecycle,
    server,
    jobs,
    gracePeriodMs: resolveShutdownGracePeriodMs(runtimeConfig),
  });

  console.info(`[server] listening on http://${hostname}:${port}`);
  return {
    server,
    lifecycle,
    shutdown: shutdown.shutdown,
    forceShutdown: shutdown.forceShutdown,
  };
}
