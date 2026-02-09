import { jsonResponse } from "./response.ts";

export type ErrorCode =
  | "BAD_REQUEST"
  | "UNAUTHORIZED"
  | "FORBIDDEN"
  | "NOT_FOUND"
  | "METHOD_NOT_ALLOWED"
  | "CONFLICT"
  | "CONFIGURATION_ERROR"
  | "INTERNAL_SERVER_ERROR";

export interface AppErrorOptions {
  details?: unknown;
  cause?: unknown;
}

export class AppError extends Error {
  readonly status: number;
  readonly code: ErrorCode;
  readonly details: unknown;

  constructor(status: number, code: ErrorCode, message: string, options: AppErrorOptions = {}) {
    super(message, options.cause ? { cause: options.cause } : undefined);
    this.name = this.constructor.name;
    this.status = status;
    this.code = code;
    this.details = options.details;
  }
}

export class ValidationError extends AppError {
  constructor(message: string, details?: unknown) {
    super(400, "BAD_REQUEST", message, { details });
  }
}

export class UnauthorizedError extends AppError {
  constructor(message = "Authentication is required.", details?: unknown) {
    super(401, "UNAUTHORIZED", message, { details });
  }
}

export class ForbiddenError extends AppError {
  constructor(message = "You do not have permission to perform this action.", details?: unknown) {
    super(403, "FORBIDDEN", message, { details });
  }
}

export class NotFoundError extends AppError {
  constructor(message = "The requested resource was not found.", details?: unknown) {
    super(404, "NOT_FOUND", message, { details });
  }
}

export class MethodNotAllowedError extends AppError {
  constructor(pathname: string, allowedMethods: readonly string[]) {
    super(405, "METHOD_NOT_ALLOWED", `Method is not allowed for path '${pathname}'.`, {
      details: {
        pathname,
        allowed_methods: allowedMethods,
      },
    });
  }
}

export class ConflictError extends AppError {
  constructor(message: string, details?: unknown) {
    super(409, "CONFLICT", message, { details });
  }
}

export class ConfigurationError extends AppError {
  constructor(message: string, details?: unknown) {
    super(500, "CONFIGURATION_ERROR", message, { details });
  }
}

export class InternalServerError extends AppError {
  constructor(message = "An unexpected error occurred.", details?: unknown, cause?: unknown) {
    super(500, "INTERNAL_SERVER_ERROR", message, {
      details,
      cause,
    });
  }
}

export function isAppError(error: unknown): error is AppError {
  return error instanceof AppError;
}

function createErrorBody(error: AppError, requestId: string): Record<string, unknown> {
  const body: Record<string, unknown> = {
    request_id: requestId,
    error: {
      code: error.code,
      message: error.message,
    },
  };

  if (error.details !== undefined) {
    (body.error as Record<string, unknown>).details = error.details;
  }

  return body;
}

export function createErrorResponse(error: unknown, requestId: string): Response {
  if (isAppError(error)) {
    return jsonResponse(createErrorBody(error, requestId), {
      status: error.status,
    });
  }

  console.error(`[${requestId}] Unhandled error`, error);

  const fallback = new InternalServerError();

  return jsonResponse(createErrorBody(fallback, requestId), {
    status: fallback.status,
  });
}
