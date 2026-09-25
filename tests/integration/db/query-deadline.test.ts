import { afterAll, describe, expect, test } from "bun:test";

import {
  DatabaseQueryTimeoutError,
  createSqlClient,
  executeWithDeadline,
} from "../../../src/db/client.ts";
import { TEST_DATABASE_URL } from "../test-database.ts";

describe("database query deadline", () => {
  const sql = createSqlClient(TEST_DATABASE_URL);

  afterAll(async () => {
    await sql.close();
  });

  test("cancels a slow PostgreSQL query and leaves the pool reusable", async () => {
    const client = await sql.reserve();

    try {
      await expect(executeWithDeadline(
        client`SELECT pg_sleep(5)`,
        50,
      )).rejects.toBeInstanceOf(DatabaseQueryTimeoutError);

      const reusedRows = await client<Array<{ value: number }>>`SELECT 1::int AS value`;
      expect(reusedRows[0]?.value).toBe(1);
    } finally {
      client.release();
    }

    const rows = await sql<Array<{ value: number }>>`SELECT 1::int AS value`;
    expect(rows[0]?.value).toBe(1);
  });
});
