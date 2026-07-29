import { describe, expect, test } from "bun:test";

import { validateTestDatabaseUrl } from "../../integration/test-database-config.ts";

describe("integration test database configuration", () => {
  test("requires TEST_DATABASE_URL", () => {
    expect(() => validateTestDatabaseUrl(undefined)).toThrow("TEST_DATABASE_URL is required");
  });

  test("rejects database names that are not explicitly test databases", () => {
    expect(() => validateTestDatabaseUrl("postgres://user:password@localhost:5432/osimi")).toThrow(
      "must contain 'test'",
    );
  });

  test("accepts a PostgreSQL test database URL", () => {
    const url = "postgres://user:password@localhost:5432/osimi_test";
    expect(validateTestDatabaseUrl(url)).toBe(url);
  });

  test("permits a known disposable non-test database only with an explicit override", () => {
    const url = "postgres://user:password@localhost:5432/disposable";
    expect(validateTestDatabaseUrl(url, true)).toBe(url);
  });
});
