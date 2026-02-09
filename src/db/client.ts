import { SQL } from "bun";

const DATABASE_URL_ENV = "DATABASE_URL";

export function resolveDatabaseUrl(override?: string): string {
  const candidate = (override ?? process.env[DATABASE_URL_ENV])?.trim();

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
