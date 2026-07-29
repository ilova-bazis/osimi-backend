import { createSqlClient } from "../../src/db/client.ts";
import { getTestDatabaseUrl } from "./test-database-config.ts";

export const TEST_DATABASE_URL = getTestDatabaseUrl();

export async function verifyTestDatabaseConnection(): Promise<void> {
  const sql = createSqlClient(TEST_DATABASE_URL);

  try {
    await sql`SELECT 1`;
  } finally {
    await sql.close();
  }
}
