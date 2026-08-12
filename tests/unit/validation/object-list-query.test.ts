import { describe, expect, test } from "bun:test";

import { parseObjectListQuery } from "../../../src/validation/object.ts";

describe("object list query validation", () => {
  test("rejects PostgreSQL-incompatible NUL characters", () => {
    expect(() =>
      parseObjectListQuery(new URL("http://localhost/api/objects?q=before%00after")),
    ).toThrow("Invalid request at 'q': Search query must not contain NUL characters.");
  });

  test("preserves ordinary Unicode and literal wildcard characters", () => {
    const parsed = parseObjectListQuery(
      new URL("http://localhost/api/objects?q=caf%C3%A9%25_%5C"),
    );

    expect(parsed.query).toBe("caf\u00e9%_\\");
  });
});
