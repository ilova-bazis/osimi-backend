import { AsyncLocalStorage } from "node:async_hooks";

import { ConfigurationError } from "../http/errors.ts";

const MINIMUM_SIGNING_SECRET_LENGTH = 32;
export const DEFAULT_MAX_UPLOAD_SIZE_BYTES = 2 * 1024 * 1024 * 1024;
export const DEFAULT_MAX_ARTIFACT_SEARCH_TEXT_BYTES = 10 * 1024 * 1024;
export const DEFAULT_READINESS_TIMEOUT_MS = 1_000;
export const DEFAULT_SHUTDOWN_GRACE_PERIOD_MS = 60_000;
export const DEFAULT_CORS_ALLOWED_ORIGINS = [
  "http://localhost:4444",
  "http://localhost:5173",
] as const;

export interface RuntimeConfig {
  databaseUrl?: string;
  dbSchema?: string;
  stagingRoot?: string;
  workerAuthToken?: string;
  uploadSigningSecret?: string;
  leaseSigningSecret?: string;
  maxUploadSizeBytes?: number;
  maxArtifactSearchTextBytes?: number;
  readinessTimeoutMs?: number;
  shutdownGracePeriodMs?: number;
  corsAllowedOrigins?: readonly string[];
}

const runtimeConfigStore = new AsyncLocalStorage<RuntimeConfig>();

export function getRuntimeConfig(): RuntimeConfig {
  return runtimeConfigStore.getStore() ?? {};
}

function resolveSigningSecret(params: {
  variableName: "UPLOAD_SIGNING_SECRET" | "LEASE_SIGNING_SECRET";
  runtimeValue: string | undefined;
  environmentValue: string | undefined;
}): string {
  const value = params.runtimeValue ?? params.environmentValue;

  if (!value || value.trim().length < MINIMUM_SIGNING_SECRET_LENGTH) {
    throw new ConfigurationError(
      `Environment variable '${params.variableName}' must be at least ${MINIMUM_SIGNING_SECRET_LENGTH} non-whitespace characters.`,
    );
  }

  return value.trim();
}

export function resolveUploadSigningSecret(config: RuntimeConfig = getRuntimeConfig()): string {
  return resolveSigningSecret({
    variableName: "UPLOAD_SIGNING_SECRET",
    runtimeValue: config.uploadSigningSecret,
    environmentValue: process.env.UPLOAD_SIGNING_SECRET,
  });
}

export function resolveLeaseSigningSecret(config: RuntimeConfig = getRuntimeConfig()): string {
  return resolveSigningSecret({
    variableName: "LEASE_SIGNING_SECRET",
    runtimeValue: config.leaseSigningSecret,
    environmentValue: process.env.LEASE_SIGNING_SECRET,
  });
}

export function validateSigningConfiguration(config: RuntimeConfig = getRuntimeConfig()): void {
  resolveUploadSigningSecret(config);
  resolveLeaseSigningSecret(config);
}

function validateMaxUploadSizeBytes(value: number, source: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new ConfigurationError(`${source} must be a positive safe integer.`);
  }

  return value;
}

export function resolveMaxUploadSizeBytes(config: RuntimeConfig = getRuntimeConfig()): number {
  if (config.maxUploadSizeBytes !== undefined) {
    return validateMaxUploadSizeBytes(config.maxUploadSizeBytes, "Runtime upload size limit");
  }

  const rawValue = process.env.MAX_UPLOAD_SIZE_BYTES;

  if (rawValue === undefined) {
    return DEFAULT_MAX_UPLOAD_SIZE_BYTES;
  }

  if (!/^[1-9][0-9]*$/.test(rawValue)) {
    throw new ConfigurationError("Environment variable 'MAX_UPLOAD_SIZE_BYTES' must be a positive decimal integer.");
  }

  return validateMaxUploadSizeBytes(
    Number(rawValue),
    "Environment variable 'MAX_UPLOAD_SIZE_BYTES'",
  );
}

export function resolveMaxArtifactSearchTextBytes(
  config: RuntimeConfig = getRuntimeConfig(),
): number {
  return resolvePositiveInteger({
    runtimeValue: config.maxArtifactSearchTextBytes,
    environmentValue: process.env.MAX_ARTIFACT_SEARCH_TEXT_BYTES,
    defaultValue: DEFAULT_MAX_ARTIFACT_SEARCH_TEXT_BYTES,
    source: "MAX_ARTIFACT_SEARCH_TEXT_BYTES",
  });
}

function normalizeCorsOrigin(value: string, source: string): string {
  const trimmedValue = value.trim();

  if (!trimmedValue) {
    throw new ConfigurationError(`${source} must not include empty origins.`);
  }

  let parsed: URL;
  try {
    parsed = new URL(trimmedValue);
  } catch {
    throw new ConfigurationError(`${source} must contain valid HTTP(S) origins.`);
  }

  if (
    (parsed.protocol !== "http:" && parsed.protocol !== "https:") ||
    parsed.username ||
    parsed.password ||
    parsed.pathname !== "/" ||
    parsed.search ||
    parsed.hash
  ) {
    throw new ConfigurationError(
      `${source} must contain HTTP(S) origins without credentials, paths, queries, or fragments.`,
    );
  }

  return parsed.origin;
}

function normalizeCorsOrigins(values: readonly string[], source: string): readonly string[] {
  return [...new Set(values.map((value) => normalizeCorsOrigin(value, source)))];
}

export function parseCorsAllowedOrigins(value: string): readonly string[] {
  if (value.trim() === "") {
    return [];
  }

  return normalizeCorsOrigins(
    value.split(","),
    "Environment variable 'CORS_ALLOWED_ORIGINS'",
  );
}

export function resolveCorsAllowedOrigins(
  config: RuntimeConfig = getRuntimeConfig(),
): readonly string[] {
  if (config.corsAllowedOrigins !== undefined) {
    return normalizeCorsOrigins(config.corsAllowedOrigins, "Runtime CORS allowed origins");
  }

  const rawValue = process.env.CORS_ALLOWED_ORIGINS;
  if (rawValue === undefined) {
    return [...DEFAULT_CORS_ALLOWED_ORIGINS];
  }

  return parseCorsAllowedOrigins(rawValue);
}

export function validateRuntimeConfiguration(config: RuntimeConfig = getRuntimeConfig()): void {
  validateSigningConfiguration(config);
  resolveMaxUploadSizeBytes(config);
  resolveMaxArtifactSearchTextBytes(config);
  resolveCorsAllowedOrigins(config);
}

function resolvePositiveInteger(params: {
  runtimeValue: number | undefined;
  environmentValue: string | undefined;
  defaultValue: number;
  source: string;
}): number {
  if (params.runtimeValue !== undefined) {
    return validateMaxUploadSizeBytes(params.runtimeValue, params.source);
  }
  if (params.environmentValue === undefined) {
    return params.defaultValue;
  }
  if (!/^[1-9][0-9]*$/.test(params.environmentValue)) {
    throw new ConfigurationError(`${params.source} must be a positive decimal integer.`);
  }
  return validateMaxUploadSizeBytes(Number(params.environmentValue), params.source);
}

export function resolveReadinessTimeoutMs(config: RuntimeConfig = getRuntimeConfig()): number {
  return resolvePositiveInteger({
    runtimeValue: config.readinessTimeoutMs,
    environmentValue: process.env.READINESS_TIMEOUT_MS,
    defaultValue: DEFAULT_READINESS_TIMEOUT_MS,
    source: "READINESS_TIMEOUT_MS",
  });
}

export function validateWorkerConfiguration(config: RuntimeConfig = getRuntimeConfig()): void {
  const value = config.workerAuthToken ?? process.env.WORKER_AUTH_TOKEN;
  if (!value || value.trim().length === 0) {
    throw new ConfigurationError("Environment variable 'WORKER_AUTH_TOKEN' is required.");
  }
}

export function resolveShutdownGracePeriodMs(
  config: RuntimeConfig = getRuntimeConfig(),
): number {
  return resolvePositiveInteger({
    runtimeValue: config.shutdownGracePeriodMs,
    environmentValue: process.env.SHUTDOWN_GRACE_PERIOD_MS,
    defaultValue: DEFAULT_SHUTDOWN_GRACE_PERIOD_MS,
    source: "SHUTDOWN_GRACE_PERIOD_MS",
  });
}

export function runWithRuntimeConfig<T>(
  config: RuntimeConfig,
  handler: () => T,
): T {
  return runtimeConfigStore.run(config, handler);
}
