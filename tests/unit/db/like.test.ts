import { describe, expect, test } from "bun:test";

import { escapeLikePattern } from "../../../src/db/like.ts";

describe("escapeLikePattern", () => {
  test("escapes backslash, percent, and underscore for literal LIKE matching", () => {
    expect(escapeLikePattern(String.raw`a\b%c_d`)).toBe(
      String.raw`a\\b\%c\_d`,
    );
  });

  test("leaves ordinary search text unchanged", () => {
    expect(escapeLikePattern("Archive 2026")).toBe("Archive 2026");
  });
});
