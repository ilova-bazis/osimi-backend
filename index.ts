import { startServer } from "./src/server.ts";

try {
  const runtime = startServer();
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
} catch (error) {
  console.error("[bootstrap] failed to start server", error);
  process.exit(1);
}
