import { ValidationError } from "./errors.ts";

export const IDEMPOTENCY_KEY_HEADER = "x-idempotency-key";

const IDEMPOTENCY_KEY_PATTERN = /^[A-Za-z0-9:_-]{8,128}$/;

export function parseIdempotencyKey(rawValue: string | null): string | undefined {
  if (rawValue === null) {
    return undefined;
  }

  const normalized = rawValue.trim();

  if (normalized.length === 0) {
    throw new ValidationError(`Header '${IDEMPOTENCY_KEY_HEADER}' cannot be empty.`);
  }

  if (!IDEMPOTENCY_KEY_PATTERN.test(normalized)) {
    throw new ValidationError(`Header '${IDEMPOTENCY_KEY_HEADER}' is invalid.`, {
      expected_pattern: IDEMPOTENCY_KEY_PATTERN.source,
    });
  }

  return normalized;
}
