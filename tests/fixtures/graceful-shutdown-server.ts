import { startServer } from "../../src/server.ts";
import type { RouteDefinition } from "../../src/routes/types.ts";

const routes: RouteDefinition[] = [
  {
    method: "GET",
    path: "/block",
    handler: async () => {
      console.info(JSON.stringify({ event: "fixture.request_entered" }));
      await new Promise((resolve) => setTimeout(resolve, 100));
      return new Response("completed");
    },
  },
  {
    method: "GET",
    path: "/block-forever",
    handler: async () => {
      console.info(JSON.stringify({ event: "fixture.request_entered" }));
      return await new Promise<Response>(() => {});
    },
  },
];

const runtime = startServer({
  hostname: "127.0.0.1",
  port: 0,
  routeDefinitions: routes,
  runtimeConfig: {
    uploadSigningSecret: "fixture-upload-signing-secret-000001",
    leaseSigningSecret: "fixture-lease-signing-secret-000001",
    workerAuthToken: "fixture-worker-token",
  },
});

console.info(JSON.stringify({ event: "fixture.listening", port: runtime.server.port }));

let receivedSignal = false;
const shutdown = (signal: "SIGINT" | "SIGTERM"): void => {
  if (receivedSignal) {
    void runtime.forceShutdown(signal).then(() => process.exit(1));
    return;
  }
  receivedSignal = true;
  void runtime.shutdown(signal).then((result) => process.exit(result.forced ? 1 : 0));
};

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
