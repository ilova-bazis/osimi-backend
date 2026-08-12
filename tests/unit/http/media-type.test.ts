import { describe, expect, test } from "bun:test";

import { parseMediaType } from "../../../src/http/media-type.ts";

describe("HTTP media types", () => {
  test("parses base types case-insensitively while preserving the declared value", () => {
    expect(parseMediaType(' Text/Plain ; charset="utf-8" ')).toEqual({
      value: 'Text/Plain ; charset="utf-8"',
      essence: "text/plain",
    });
    expect(parseMediaType("application/vnd.example+json; version=2")).toEqual({
      value: "application/vnd.example+json; version=2",
      essence: "application/vnd.example+json",
    });
  });

  test("supports quoted semicolons and escapes", () => {
    expect(parseMediaType('text/plain; note="semi;colon\\\"quoted"')?.essence).toBe(
      "text/plain",
    );
    expect(parseMediaType('text/plain; title="caf\u00e9"')?.essence).toBe("text/plain");
  });

  test("rejects malformed and ambiguous values", () => {
    for (const value of [
      "text",
      "text/",
      "text/plain; charset",
      "text/plain; charset=",
      'text/plain; charset="unterminated',
      'text/plain; note="a""b"',
      "text/plain; charset=utf-8; CHARSET=ascii",
      "text/plain\u0000",
    ]) {
      expect(parseMediaType(value)).toBeUndefined();
    }
  });
});
