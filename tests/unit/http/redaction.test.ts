import { describe, expect, test } from "bun:test";

import { redactTokenPaths } from "../../../src/http/redaction.ts";

describe("token path redaction", () => {
  test("redacts signed upload and download token segments", () => {
    expect(redactTokenPaths("/api/uploads/some-secret-token"))
      .toBe("/api/uploads/<redacted-token>");
    expect(redactTokenPaths("/api/archive-requests/uploads/some-secret-token"))
      .toBe("/api/archive-requests/uploads/<redacted-token>");
    expect(redactTokenPaths("/api/object-download-requests/uploads/some-secret-token"))
      .toBe("/api/object-download-requests/uploads/<redacted-token>");
    expect(redactTokenPaths("/api/worker/downloads/some-secret-token"))
      .toBe("/api/worker/downloads/<redacted-token>");
  });

  test("redacts the entire suffix when extra path segments follow the token", () => {
    expect(redactTokenPaths("/api/uploads/SECRET/extra"))
      .toBe("/api/uploads/<redacted-token>");
    expect(redactTokenPaths("/api/archive-requests/uploads/SECRET/extra/parts"))
      .toBe("/api/archive-requests/uploads/<redacted-token>");
    expect(redactTokenPaths("/api/worker/downloads/SECRET/extra"))
      .toBe("/api/worker/downloads/<redacted-token>");
  });

  test("never leaves the token segment in the redacted path", () => {
    for (const pathname of [
      "/api/uploads/SECRET",
      "/api/uploads/SECRET/extra",
      "/api/uploads/SECRET//",
      "/api/worker/downloads/SECRET/",
    ]) {
      expect(redactTokenPaths(pathname)).not.toContain("SECRET");
    }
  });

  test("leaves unrelated paths untouched", () => {
    expect(redactTokenPaths("/api/ingestions/abc/files/presign"))
      .toBe("/api/ingestions/abc/files/presign");
    expect(redactTokenPaths("/login")).toBe("/login");
    expect(redactTokenPaths("/api/uploads")).toBe("/api/uploads");
  });
});
