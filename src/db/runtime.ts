import { createSqlClient, resolveDatabaseUrl } from "./client.ts";
import { getRuntimeConfig } from "../runtime/config.ts";

export const DB_SCHEMA_PATTERN = /^[a-z_][a-z0-9_]*$/;
const IDENTIFIER_PATTERN = /^[a-z_][a-z0-9_]*$/;

const cachedClientsByKey = new Map<
    string,
    ReturnType<typeof createSqlClient>
>();

function validateIdentifier(value: string, kind: string): string {
    if (!IDENTIFIER_PATTERN.test(value)) {
        throw new Error(
            `Invalid ${kind} '${value}'. Must match ${IDENTIFIER_PATTERN.source}.`,
        );
    }

    return value;
}

export function normalizeDbSchema(value: string, source: string): string {
    const schema = value
        .trim()
        .toLowerCase();

    if (!DB_SCHEMA_PATTERN.test(schema)) {
        throw new Error(
            `Invalid ${source} '${schema}'. Must match ${DB_SCHEMA_PATTERN.source}.`,
        );
    }

    return schema;
}

export function resolveDbSchema(): string {
    const runtimeSchema = getRuntimeConfig().dbSchema;
    const source = runtimeSchema !== undefined
        ? "runtime database schema"
        : process.env.DB_SCHEMA !== undefined
        ? "DB_SCHEMA"
        : "default database schema";
    return normalizeDbSchema(runtimeSchema ?? process.env.DB_SCHEMA ?? "public", source);
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

export async function closeDatabaseClients(params: {
    timeoutMs: number;
    force?: boolean;
}): Promise<void> {
    const clients = [...new Set(cachedClientsByKey.values())];
    cachedClientsByKey.clear();
    const timeout = params.force ? 0 : Math.max(0, params.timeoutMs) / 1000;

    await Promise.allSettled(clients.map((client) => client.close({ timeout })));
}
