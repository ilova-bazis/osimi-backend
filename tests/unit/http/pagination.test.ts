import { describe, expect, test } from "bun:test";

import { ValidationError } from "../../../src/http/errors.ts";
import {
  DEFAULT_PAGE_LIMIT,
  MAX_PAGE_LIMIT,
  decodeCursor,
  encodeCursor,
  parsePaginationParams,
} from "../../../src/http/pagination.ts";

describe("pagination helpers", () => {
  test("uses default pagination params", () => {
    const url = new URL("http://localhost/items");
    const pagination = parsePaginationParams(url);

    expect(pagination).toEqual({
      limit: DEFAULT_PAGE_LIMIT,
      cursor: undefined,
    });
  });

  test("parses explicit limit and cursor", () => {
    const encoded = encodeCursor({
      created_at: "2026-01-01T00:00:00.000Z",
      id: "abc",
    });

    const url = new URL(`http://localhost/items?limit=${MAX_PAGE_LIMIT}&cursor=${encoded}`);
    const pagination = parsePaginationParams(url);
    const decoded = decodeCursor<{ created_at: string; id: string }>(pagination.cursor!);

    expect(pagination.limit).toBe(MAX_PAGE_LIMIT);
    expect(decoded.created_at).toBe("2026-01-01T00:00:00.000Z");
    expect(decoded.id).toBe("abc");
  });

  test("rejects out of range limit", () => {
    const url = new URL("http://localhost/items?limit=999");

    expect(() => parsePaginationParams(url)).toThrow(ValidationError);
  });

  test("rejects invalid cursor data", () => {
    expect(() => decodeCursor("bad-cursor")).toThrow(ValidationError);
    expect(() => decodeCursor(Buffer.from("[]", "utf8").toString("base64url"))).toThrow(
      ValidationError,
    );
  });
});
