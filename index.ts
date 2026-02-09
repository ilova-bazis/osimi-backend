import { startServer } from "./src/server.ts";

try {
  startServer();
} catch (error) {
  console.error("[bootstrap] failed to start server", error);
  process.exit(1);
}
