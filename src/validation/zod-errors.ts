import { ValidationError } from "../http/errors.ts";
import { z } from "zod";

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
