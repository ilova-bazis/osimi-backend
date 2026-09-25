import { ConfigurationError, RateLimitedError, UnauthorizedError } from "../http/errors.ts";
import { redactTokenPaths } from "../http/redaction.ts";
import { getRuntimeConfig } from "../runtime/config.ts";
import type { RateLimiter } from "./rate-limit.ts";

const WORKER_AUTH_HEADER = "x-worker-auth-token";
const WORKER_TOKEN_ENV = "WORKER_AUTH_TOKEN";
const WORKER_ID_HEADER = "x-worker-id";
const WORKER_AUTH_RATE_LIMIT_KEY = "worker-auth";

export interface WorkerPrincipal {
  workerId?: string;
}

export interface WorkerAuthOptions {
  rateLimiter: RateLimiter;
  requestId?: string;
}

function workerRateLimitError(retryAfterMs: number): RateLimitedError {
  return new RateLimitedError(
    "Too many worker authentication failures. Please retry later.",
    Math.max(1, Math.ceil(retryAfterMs / 1000)),
  );
}

export function requireWorkerAuthentication(
  request: Request,
  options: WorkerAuthOptions,
): WorkerPrincipal {
  const runtimeToken = getRuntimeConfig().workerAuthToken;
  const expectedToken = (runtimeToken ?? process.env[WORKER_TOKEN_ENV])?.trim();

  if (!expectedToken) {
    throw new ConfigurationError(`Environment variable '${WORKER_TOKEN_ENV}' is required for worker endpoints.`);
  }

  const providedToken = request.headers.get(WORKER_AUTH_HEADER)?.trim();

  if (providedToken && providedToken === expectedToken) {
    const workerId = request.headers.get(WORKER_ID_HEADER)?.trim() || undefined;

    return {
      workerId,
    };
  }

  const admission = options.rateLimiter.beginAttempt(WORKER_AUTH_RATE_LIMIT_KEY);
  if (!admission.allowed) {
    throw workerRateLimitError(admission.retryAfterMs);
  }

  const decision = admission.attempt.settleFailure();
  if (!decision.allowed) {
    const pathname = redactTokenPaths(new URL(request.url).pathname);
    console.warn(JSON.stringify({
      event: "worker_auth_rate_limited",
      request_id: options.requestId,
      path: pathname,
      retry_after_ms: decision.retryAfterMs,
    }));
    throw workerRateLimitError(decision.retryAfterMs);
  }

  if (!providedToken) {
    throw new UnauthorizedError(`Header '${WORKER_AUTH_HEADER}' is required for worker endpoints.`);
  }

  throw new UnauthorizedError("Worker authentication token is invalid.");
}
