import {
  UnprocessableEntityError,
  ValidationError,
} from "../http/errors.ts";
import { z } from "zod";

function buildIssueDetails(error: z.ZodError): Array<{ path: string; code: string }> {
  return error.issues.map((issue) => ({
    path: issue.path.reduce<string>((path, segment) => {
      if (typeof segment === "number") {
        return `${path}[${segment}]`;
      }

      return path.length === 0 ? String(segment) : `${path}.${String(segment)}`;
    }, ""),
    code: issue.code === "custom" ? "INVALID" : issue.code.toUpperCase(),
  }));
}

export function mapZodErrorToValidation(error: z.ZodError): ValidationError {
  const firstIssue = error.issues[0];

  if (!firstIssue) {
    return new ValidationError("Invalid request payload.");
  }

  const path = firstIssue.path.join(".");
  const message = path
    ? `Invalid request at '${path}': ${firstIssue.message}`
    : firstIssue.message;

  return new ValidationError(message, {
    issues: error.issues,
  });
}

export function mapZodErrorToUnprocessable(
  error: z.ZodError,
): UnprocessableEntityError {
  return new UnprocessableEntityError("Validation failed.", buildIssueDetails(error));
}
