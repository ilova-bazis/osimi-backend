import { ConfigurationError, UnauthorizedError } from "../http/errors.ts";

const WORKER_AUTH_HEADER = "x-worker-auth-token";
const WORKER_TOKEN_ENV = "WORKER_AUTH_TOKEN";
const WORKER_ID_HEADER = "x-worker-id";

export interface WorkerPrincipal {
  workerId?: string;
}

export function requireWorkerAuthentication(request: Request): WorkerPrincipal {
  const expectedToken = process.env[WORKER_TOKEN_ENV]?.trim();

  if (!expectedToken) {
    throw new ConfigurationError(`Environment variable '${WORKER_TOKEN_ENV}' is required for worker endpoints.`);
  }

  const providedToken = request.headers.get(WORKER_AUTH_HEADER)?.trim();

  if (!providedToken) {
    throw new UnauthorizedError(`Header '${WORKER_AUTH_HEADER}' is required for worker endpoints.`);
  }

  if (providedToken !== expectedToken) {
    throw new UnauthorizedError("Worker authentication token is invalid.");
  }

  const workerId = request.headers.get(WORKER_ID_HEADER)?.trim() || undefined;

  return {
    workerId,
  };
}
