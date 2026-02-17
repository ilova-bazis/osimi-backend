import { ValidationError } from "../http/errors.ts";
import { z } from "zod";

const objectSchema = z.record(z.string(), z.unknown());
const stringSchema = z.string();
const optionalStringSchema = z.string().optional();
const nonEmptyStringSchema = z.string().trim().min(1);
const positiveIntSchema = z.number().int().min(1);
const uuidSchema = z.uuid();

export async function parseJsonBody(request: Request): Promise<unknown> {
  return request.json().catch(() => {
    throw new ValidationError("Request body must be valid JSON.");
  });
}

export function requireObject(
  value: unknown,
  subject = "Request body",
): Record<string, unknown> {
  const parsed = objectSchema.safeParse(value);

  if (!parsed.success) {
    throw new ValidationError(`${subject} must be an object.`);
  }

  return parsed.data;
}

export function requireStringField(
  payload: Record<string, unknown>,
  key: string,
): string {
  const parsed = stringSchema.safeParse(payload[key]);

  if (!parsed.success) {
    throw new ValidationError(`Field '${key}' must be a string.`);
  }

  return parsed.data;
}

export function requireOptionalStringField(
  payload: Record<string, unknown>,
  key: string,
): string | undefined {
  const parsed = optionalStringSchema.safeParse(payload[key]);

  if (!parsed.success) {
    throw new ValidationError(`Field '${key}' must be a string when provided.`);
  }

  return parsed.data;
}

export function requireNonEmptyStringField(
  payload: Record<string, unknown>,
  key: string,
): string {
  const parsed = nonEmptyStringSchema.safeParse(payload[key]);

  if (!parsed.success) {
    throw new ValidationError(`Field '${key}' cannot be empty.`);
  }

  return parsed.data;
}

export function requirePositiveIntField(
  payload: Record<string, unknown>,
  key: string,
): number {
  const parsed = positiveIntSchema.safeParse(payload[key]);

  if (!parsed.success) {
    throw new ValidationError(`Field '${key}' must be a positive integer.`);
  }

  return parsed.data;
}

export function requireUuid(value: string, fieldName: string): string {
  const parsed = uuidSchema.safeParse(value);

  if (!parsed.success) {
    throw new ValidationError(`Field '${fieldName}' must be a UUID.`);
  }

  return parsed.data;
}
