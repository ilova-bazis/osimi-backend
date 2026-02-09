import { createSqlClient, resolveDatabaseUrl } from "./client.ts";

const SCHEMA_PATTERN = /^[a-z_][a-z0-9_]*$/;
const IDENTIFIER_PATTERN = /^[a-z_][a-z0-9_]*$/;

let cachedUrl: string | undefined;
let cachedClient: ReturnType<typeof createSqlClient> | undefined;

function validateIdentifier(value: string, kind: string): string {
  if (!IDENTIFIER_PATTERN.test(value)) {
    throw new Error(`Invalid ${kind} '${value}'. Must match ${IDENTIFIER_PATTERN.source}.`);
  }

  return value;
}

export function resolveDbSchema(): string {
  const schema = (process.env.DB_SCHEMA ?? "public").trim().toLowerCase();

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

  if (!cachedClient || cachedUrl !== url) {
    cachedClient = createSqlClient(url);
    cachedUrl = url;
  }

  return cachedClient;
}
