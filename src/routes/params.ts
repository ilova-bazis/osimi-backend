import { ValidationError } from "../http/errors.ts";

export function extractPathParam(
  pathname: string,
  pattern: RegExp,
  parameterName: string,
): string {
  const match = pathname.match(pattern);
  const value = match?.[1];

  if (!value) {
    throw new ValidationError(`Path parameter '${parameterName}' is invalid.`);
  }

  return value;
}
