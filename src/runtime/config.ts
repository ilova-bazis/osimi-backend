import { AsyncLocalStorage } from "node:async_hooks";

export interface RuntimeConfig {
  databaseUrl?: string;
  dbSchema?: string;
  stagingRoot?: string;
  workerAuthToken?: string;
  uploadSigningSecret?: string;
  leaseSigningSecret?: string;
}

const runtimeConfigStore = new AsyncLocalStorage<RuntimeConfig>();

export function getRuntimeConfig(): RuntimeConfig {
  return runtimeConfigStore.getStore() ?? {};
}

export function runWithRuntimeConfig<T>(
  config: RuntimeConfig,
  handler: () => T,
): T {
  return runtimeConfigStore.run(config, handler);
}
