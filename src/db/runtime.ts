import { createSqlClient, resolveDatabaseUrl } from "./client.ts";
import { getRuntimeConfig } from "../runtime/config.ts";

const SCHEMA_PATTERN = /^[a-z_][a-z0-9_]*$/;
const IDENTIFIER_PATTERN = /^[a-z_][a-z0-9_]*$/;

const cachedClientsByKey = new Map<string, ReturnType<typeof createSqlClient>>();

function validateIdentifier(value: string, kind: string): string {
  if (!IDENTIFIER_PATTERN.test(value)) {
    throw new Error(`Invalid ${kind} '${value}'. Must match ${IDENTIFIER_PATTERN.source}.`);
  }

  return value;
}

export function resolveDbSchema(): string {
  const runtimeSchema = getRuntimeConfig().dbSchema;
  const schema = (runtimeSchema ?? process.env.DB_SCHEMA ?? "public")
    .trim()
    .toLowerCase();

  if (!SCHEMA_PATTERN.test(schema)) {
    throw new Error(`Invalid DB_SCHEMA '${schema}'. Must match ${SCHEMA_PATTERN.source}.`);
  }

  return schema;
}

export function qualifiedTableName(tableName: string): string {
  const schema = validateIdentifier(resolveDbSchema(), "schema");
  const table = validateIdentifier(tableName, "table name");
  return `"${schema}"."${table}"`;
}

export function db(): ReturnType<typeof createSqlClient> {
  const url = resolveDatabaseUrl();
  const schema = resolveDbSchema();
  const cacheKey = `${url}::${schema}`;
  const cachedClient = cachedClientsByKey.get(cacheKey);

  if (cachedClient) {
    return cachedClient;
  }

  const createdClient = createSqlClient(url);
  cachedClientsByKey.set(cacheKey, createdClient);
  return createdClient;
}
