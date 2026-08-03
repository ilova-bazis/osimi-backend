import { describe, expect, test } from "bun:test";

import {
  ifRangeMatches,
  parseSingleByteRange,
  type ByteRangeResult,
} from "../../../src/http/range.ts";

describe("parseSingleByteRange", () => {
  const cases: Array<[string, ByteRangeResult]> = [
    ["bytes=0-2", { kind: "range", range: { start: 0, end: 2 } }],
    ["bytes=3-", { kind: "range", range: { start: 3, end: 11 } }],
    ["bytes=-3", { kind: "range", range: { start: 9, end: 11 } }],
    ["bytes=0-999", { kind: "range", range: { start: 0, end: 11 } }],
    ["bytes=-999", { kind: "range", range: { start: 0, end: 11 } }],
    ["bytes=12-", { kind: "unsatisfiable" }],
    ["bytes=5-4", { kind: "unsatisfiable" }],
    ["bytes=-0", { kind: "unsatisfiable" }],
    ["bytes=0-1,4-5", { kind: "ignore" }],
    ["items=0-1", { kind: "ignore" }],
    ["bytes=a-b", { kind: "ignore" }],
  ];

  test.each(cases)("handles %s", (header, expected) => {
    expect(parseSingleByteRange(header, 12)).toEqual(expected);
  });

  test("does not overflow attacker-provided range bounds", () => {
    expect(parseSingleByteRange("bytes=999999999999999999999999999999-", 12)).toEqual({
      kind: "unsatisfiable",
    });
  });
});

describe("ifRangeMatches", () => {
  const etag = '"artifact-00000000-0000-0000-0000-000000000001"';
  const lastModified = new Date("2026-01-01T12:34:56.999Z");

  test("accepts a matching strong ETag and an equal HTTP date", () => {
    expect(ifRangeMatches(etag, etag, lastModified)).toBe(true);
    expect(ifRangeMatches(lastModified.toUTCString(), etag, lastModified)).toBe(true);
  });

  test("rejects stale, weak, and malformed validators", () => {
    expect(ifRangeMatches('"stale"', etag, lastModified)).toBe(false);
    expect(ifRangeMatches(`W/${etag}`, etag, lastModified)).toBe(false);
    expect(ifRangeMatches("not-a-date", etag, lastModified)).toBe(false);
  });
});
