import { describe, expect, test } from "bun:test";

import { InternalServerError } from "../../../src/http/errors.ts";
import {
  toNullableSafeNumberFromDbInt,
  toSafeNumberFromDbInt,
} from "../../../src/db/number.ts";

describe("db number helpers", () => {
  test("converts bigint, number, and string integers to number", () => {
    expect(toSafeNumberFromDbInt(42n, "size_bytes")).toBe(42);
    expect(toSafeNumberFromDbInt(42, "size_bytes")).toBe(42);
    expect(toSafeNumberFromDbInt("42", "size_bytes")).toBe(42);
  });

  test("converts null values in nullable helper", () => {
    expect(toNullableSafeNumberFromDbInt(null, "size_bytes")).toBeNull();
    expect(toNullableSafeNumberFromDbInt(7n, "size_bytes")).toBe(7);
  });

  test("rejects invalid integer strings", () => {
    expect(() => toSafeNumberFromDbInt("12.5", "size_bytes")).toThrow(
      InternalServerError,
    );
  });

  test("rejects unsafe integer ranges", () => {
    const overflow = BigInt(Number.MAX_SAFE_INTEGER) + 1n;
    expect(() => toSafeNumberFromDbInt(overflow, "size_bytes")).toThrow(
      InternalServerError,
    );
    expect(() =>
      toSafeNumberFromDbInt(Number.MAX_SAFE_INTEGER + 1, "size_bytes"),
    ).toThrow(InternalServerError);
  });
});
