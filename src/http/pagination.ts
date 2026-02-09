import { ValidationError } from "./errors.ts";

export const DEFAULT_PAGE_LIMIT = 50;
export const MAX_PAGE_LIMIT = 200;

export interface PaginationParams {
  limit: number;
  cursor?: string;
}

function parseLimit(rawLimit: string | null): number {
  if (rawLimit === null) {
    return DEFAULT_PAGE_LIMIT;
  }

  const parsed = Number.parseInt(rawLimit, 10);

  if (!Number.isFinite(parsed) || Number.isNaN(parsed)) {
    throw new ValidationError("Query parameter 'limit' must be a valid integer.");
  }

  if (parsed < 1 || parsed > MAX_PAGE_LIMIT) {
    throw new ValidationError("Query parameter 'limit' is out of range.", {
      min: 1,
      max: MAX_PAGE_LIMIT,
    });
  }

  return parsed;
}

export function parsePaginationParams(url: URL): PaginationParams {
  const limit = parseLimit(url.searchParams.get("limit"));
  const cursor = url.searchParams.get("cursor") ?? undefined;

  if (cursor !== undefined && cursor.trim().length === 0) {
    throw new ValidationError("Query parameter 'cursor' cannot be empty when provided.");
  }

  return {
    limit,
    cursor,
  };
}

export function encodeCursor(payload: Record<string, unknown>): string {
  const serialized = JSON.stringify(payload);
  return Buffer.from(serialized, "utf8").toString("base64url");
}

export function decodeCursor<T extends Record<string, unknown>>(cursor: string): T {
  if (cursor.length > 1024) {
    throw new ValidationError("Query parameter 'cursor' is too long.");
  }

  try {
    const decoded = Buffer.from(cursor, "base64url").toString("utf8");
    const parsed = JSON.parse(decoded) as unknown;

    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new ValidationError("Query parameter 'cursor' must decode to an object.");
    }

    return parsed as T;
  } catch (error) {
    if (error instanceof ValidationError) {
      throw error;
    }

    throw new ValidationError("Query parameter 'cursor' is invalid.", {
      cause: "decode_failed",
    });
  }
}
