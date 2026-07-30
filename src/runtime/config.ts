import { AsyncLocalStorage } from "node:async_hooks";

import { ConfigurationError } from "../http/errors.ts";

const MINIMUM_SIGNING_SECRET_LENGTH = 32;
export const DEFAULT_MAX_UPLOAD_SIZE_BYTES = 2 * 1024 * 1024 * 1024;

export interface RuntimeConfig {
  databaseUrl?: string;
  dbSchema?: string;
  stagingRoot?: string;
  workerAuthToken?: string;
  uploadSigningSecret?: string;
  leaseSigningSecret?: string;
  maxUploadSizeBytes?: number;
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

export function validateRuntimeConfiguration(config: RuntimeConfig = getRuntimeConfig()): void {
  validateSigningConfiguration(config);
  resolveMaxUploadSizeBytes(config);
}

export function runWithRuntimeConfig<T>(
  config: RuntimeConfig,
  handler: () => T,
): T {
  return runtimeConfigStore.run(config, handler);
}
