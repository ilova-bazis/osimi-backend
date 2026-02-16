import { sql, SQL } from "bun";
import { db, resolveDbSchema } from "./runtime";
import { getRuntimeConfig } from "../runtime/config.ts";

const DATABASE_URL_ENV = "DATABASE_URL";
export type DbClient = Awaited<ReturnType<ReturnType<typeof db>["reserve"]>>;

export function resolveDatabaseUrl(override?: string): string {
  const runtimeOverride = getRuntimeConfig().databaseUrl;
  const candidate = (override ?? runtimeOverride ?? process.env[DATABASE_URL_ENV])?.trim();

  if (!candidate) {
    throw new Error(
      `Database connection string is required. Set '${DATABASE_URL_ENV}' or pass an explicit database URL.`,
    );
  }

  return candidate;
}

export function createSqlClient(databaseUrl?: string): SQL {
  return new SQL(resolveDatabaseUrl(databaseUrl));
}

export async function withSchemaClient<T>(
  handler: (sql: DbClient) => Promise<T>,
): Promise<T> {
  const pool = db();
  const client = await pool.reserve();

  try {
    const schema = resolveDbSchema();
    await client`SET search_path TO ${sql(schema)}, public`;
    return await handler(client);
  } finally {
    client.release();
  }
}
