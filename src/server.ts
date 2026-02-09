import { createApp } from "./app.ts";
import { ConfigurationError, createErrorResponse } from "./http/errors.ts";

const DEFAULT_PORT = 3000;
const DEFAULT_HOSTNAME = "0.0.0.0";

export interface ServerOptions {
  port?: number;
  hostname?: string;
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

export function startServer(options: ServerOptions = {}): Bun.Server<unknown> {
  const app = createApp();
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

  console.info(`[server] listening on http://${hostname}:${port}`);
  return server;
}
