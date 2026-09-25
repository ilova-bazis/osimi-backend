import { sql, SQL } from "bun";
import { db, resolveDbSchema } from "./runtime";
import { getRuntimeConfig } from "../runtime/config.ts";

const DATABASE_URL_ENV = "DATABASE_URL";
const DATABASE_WAIT_TIMEOUT_SECONDS = 15;

export type DbClient = Awaited<ReturnType<ReturnType<typeof db>["reserve"]>>;

export class DatabaseQueryTimeoutError extends Error {
  constructor(message = "Database query timed out.") {
    super(message);
    this.name = "DatabaseQueryTimeoutError";
  }
}

export async function executeWithDeadline<T>(
  query: Promise<T> & { cancel(): void },
  timeoutMs: number | undefined,
): Promise<T> {
  if (timeoutMs === undefined) {
    return await query;
  }

  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    try {
      query.cancel();
    } catch {
      // Keep awaiting the query so callers do not release guarded work early.
    }
  }, timeoutMs);

  try {
    const result = await query;
    if (timedOut) {
      throw new DatabaseQueryTimeoutError();
    }
    return result;
  } catch (error) {
    if (timedOut) {
      throw new DatabaseQueryTimeoutError();
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

export interface SqlExecutor {
  <T = unknown>(
    strings: TemplateStringsArray,
    ...values: unknown[]
  ): Promise<T>;
}

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
  return new SQL(resolveDatabaseUrl(databaseUrl), {
    bigint: true,
    idleTimeout: DATABASE_WAIT_TIMEOUT_SECONDS,
    connectionTimeout: DATABASE_WAIT_TIMEOUT_SECONDS,
  });
}

async function reserveWithDeadline(
  pool: ReturnType<typeof db>,
  timeoutMs: number | undefined,
): Promise<DbClient> {
  if (timeoutMs === undefined) {
    return await pool.reserve();
  }

  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
  }, timeoutMs);

  try {
    const client = await pool.reserve();
    if (timedOut) {
      client.release();
      throw new DatabaseQueryTimeoutError("Timed out waiting for a database connection.");
    }
    return client;
  } catch (error) {
    if (timedOut) {
      throw new DatabaseQueryTimeoutError("Timed out waiting for a database connection.");
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

export async function withSchemaClient<T>(
  handler: (sql: DbClient) => Promise<T>,
  options: { timeoutMs?: number } = {},
): Promise<T> {
  const pool = db();
  const client = await reserveWithDeadline(pool, options.timeoutMs);

  try {
    const schema = resolveDbSchema();
    await executeWithDeadline(
      client`SET search_path TO ${sql(schema)}, public`,
      options.timeoutMs,
    );
    return await handler(client);
  } finally {
    client.release();
  }
}

export async function withExecutor<T>(
  executor: SqlExecutor | undefined,
  handler: (sql: SqlExecutor) => Promise<T>,
): Promise<T> {
  if (executor) {
    return handler(executor);
  }

  return withSchemaClient(handler);
}
