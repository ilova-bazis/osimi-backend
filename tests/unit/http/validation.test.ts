import { describe, expect, test } from "bun:test";

import { ValidationError } from "../../../src/http/errors.ts";
import {
  parseJsonBody,
  requireNonEmptyStringField,
  requireObject,
  requireOptionalStringField,
  requirePositiveIntField,
  requireStringField,
  requireUuid,
} from "../../../src/validation/common.ts";

describe("http validation helpers", () => {
  test("parses valid JSON body", async () => {
    const request = new Request("http://localhost", {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ ok: true }),
    });

    await expect(parseJsonBody(request)).resolves.toEqual({ ok: true });
  });

  test("rejects invalid JSON body", async () => {
    const request = new Request("http://localhost", {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: "{invalid",
    });

    await expect(parseJsonBody(request)).rejects.toThrow(ValidationError);
  });

  test("requires object values", () => {
    expect(requireObject({ a: 1 })).toEqual({ a: 1 });
    expect(() => requireObject([], "Each event")).toThrow(ValidationError);
  });

  test("validates required and optional string fields", () => {
    const payload: Record<string, unknown> = {
      name: "alpha",
      optional: "beta",
    };

    expect(requireStringField(payload, "name")).toBe("alpha");
    expect(requireOptionalStringField(payload, "optional")).toBe("beta");
    expect(requireOptionalStringField(payload, "missing")).toBeUndefined();
    expect(() => requireStringField(payload, "missing")).toThrow(ValidationError);
  });

  test("validates non-empty string and positive integer fields", () => {
    const payload: Record<string, unknown> = {
      label: "  hello  ",
      size: 3,
    };

    expect(requireNonEmptyStringField(payload, "label")).toBe("hello");
    expect(requirePositiveIntField(payload, "size")).toBe(3);
    expect(() => requirePositiveIntField({ size: 0 }, "size")).toThrow(ValidationError);
  });

  test("validates UUID format", () => {
    const value = crypto.randomUUID();
    expect(requireUuid(value, "event_id")).toBe(value);
    expect(() => requireUuid("not-a-uuid", "event_id")).toThrow(ValidationError);
  });
});
