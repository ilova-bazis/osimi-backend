import { describe, expect, test } from "bun:test";

import { resolveMigrationSchema } from "../../../src/db/migrate.ts";

describe("migration schema resolution", () => {
  test("uses public when no schema is configured", () => {
    expect(resolveMigrationSchema({})).toEqual({
      schema: "public",
      warnings: [],
    });
  });

  test("uses DB_SCHEMA when no explicit schema is provided", () => {
    expect(resolveMigrationSchema({ dbSchema: " App_Test " })).toEqual({
      schema: "app_test",
      warnings: [],
    });
  });

  test("lets an explicit schema override DB_SCHEMA with a warning", () => {
    expect(resolveMigrationSchema({
      schema: " Deployment ",
      dbSchema: "app_test",
    })).toEqual({
      schema: "deployment",
      warnings: [
        "Explicit migration schema 'deployment' overrides DB_SCHEMA 'app_test'.",
      ],
    });
  });

  test("does not warn when explicit and environment schemas normalize equally", () => {
    expect(resolveMigrationSchema({
      schema: " App_Test ",
      dbSchema: "app_test",
    })).toEqual({
      schema: "app_test",
      warnings: [],
    });
  });

  test("does not validate lower-precedence DB_SCHEMA when an explicit schema is valid", () => {
    expect(resolveMigrationSchema({
      schema: "app_test",
      dbSchema: "invalid-schema-name",
    })).toEqual({
      schema: "app_test",
      warnings: [
        "Explicit migration schema 'app_test' overrides DB_SCHEMA 'invalid-schema-name'.",
      ],
    });
  });

  test("rejects an invalid selected schema", () => {
    expect(() => resolveMigrationSchema({ dbSchema: "invalid-schema-name" })).toThrow(
      "Invalid DB_SCHEMA",
    );
  });
});
