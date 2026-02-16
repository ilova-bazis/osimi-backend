import { ValidationError } from "./errors.ts";

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export function parseJsonBody(request: Request): Promise<unknown> {
  return request.json().catch(() => {
    throw new ValidationError("Request body must be valid JSON.");
  });
}

export function requireObject(
  value: unknown,
  subject = "Request body",
): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new ValidationError(`${subject} must be an object.`);
  }

  return value as Record<string, unknown>;
}

export function requireStringField(
  payload: Record<string, unknown>,
  key: string,
): string {
  const value = payload[key];

  if (typeof value !== "string") {
    throw new ValidationError(`Field '${key}' must be a string.`);
  }

  return value;
}

export function requireOptionalStringField(
  payload: Record<string, unknown>,
  key: string,
): string | undefined {
  const value = payload[key];

  if (value === undefined) {
    return undefined;
  }

  if (typeof value !== "string") {
    throw new ValidationError(`Field '${key}' must be a string when provided.`);
  }

  return value;
}

export function requireNonEmptyStringField(
  payload: Record<string, unknown>,
  key: string,
): string {
  const value = requireStringField(payload, key).trim();

  if (value.length === 0) {
    throw new ValidationError(`Field '${key}' cannot be empty.`);
  }

  return value;
}

export function requirePositiveIntField(
  payload: Record<string, unknown>,
  key: string,
): number {
  const value = payload[key];

  if (typeof value !== "number" || !Number.isInteger(value) || value < 1) {
    throw new ValidationError(`Field '${key}' must be a positive integer.`);
  }

  return value;
}

export function requireUuid(value: string, fieldName: string): string {
  if (!UUID_PATTERN.test(value)) {
    throw new ValidationError(`Field '${fieldName}' must be a UUID.`);
  }

  return value;
}
