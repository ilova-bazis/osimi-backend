import { closeDatabaseClients } from "../db/runtime.ts";
import type { JobRuntime } from "../jobs/index.ts";
import { LifecycleController } from "./lifecycle.ts";

interface StoppableServer {
  stop(closeActiveConnections?: boolean): Promise<void>;
  pendingRequests: number;
}

export interface ShutdownResult {
  forced: boolean;
  reason: string;
}

export interface ShutdownCoordinator {
  shutdown(reason: string): Promise<ShutdownResult>;
  forceShutdown(reason: string): Promise<ShutdownResult>;
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

export function createShutdownCoordinator(params: {
  lifecycle: LifecycleController;
  server: StoppableServer;
  jobs: JobRuntime;
  gracePeriodMs: number;
}): ShutdownCoordinator {
  let shutdownPromise: Promise<ShutdownResult> | undefined;
  let forceRequested = false;
  let requestForce: (() => void) | undefined;

  const run = async (reason: string): Promise<ShutdownResult> => {
    const startedAt = performance.now();
    const deadline = startedAt + params.gracePeriodMs;
    params.lifecycle.beginDrain();
    console.info(JSON.stringify({ event: "server.shutdown.started", reason }));

    const serverStopped = params.server.stop(false);
    const jobsStopped = params.jobs.stop();
    const forceSignal = new Promise<void>((resolve) => {
      requestForce = resolve;
    });
    const graceful = Promise.all([serverStopped, jobsStopped, params.lifecycle.waitForIdle()]);
    const outcome = await Promise.race([
      graceful.then(() => "graceful" as const),
      delay(Math.max(0, deadline - performance.now())).then(() => "deadline" as const),
      forceSignal.then(() => "forced" as const),
    ]);

    if (outcome !== "graceful") {
      forceRequested = true;
      console.warn(JSON.stringify({
        event: outcome === "deadline" ? "server.shutdown.deadline_exceeded" : "server.shutdown.forced",
        reason,
      }));
      void params.server.stop(true);
      await closeDatabaseClients({ timeoutMs: 0, force: true });
      params.lifecycle.markStopped();
      return { forced: true, reason };
    }

    const remainingMs = Math.max(0, deadline - performance.now());
    const databaseClosed = await Promise.race([
      closeDatabaseClients({ timeoutMs: remainingMs }),
      delay(remainingMs).then(() => false),
    ]);
    if (databaseClosed === false) {
      forceRequested = true;
      void params.server.stop(true);
      await closeDatabaseClients({ timeoutMs: 0, force: true });
      params.lifecycle.markStopped();
      return { forced: true, reason };
    }

    params.lifecycle.markStopped();
    console.info(JSON.stringify({
      event: "server.shutdown.completed",
      reason,
      duration_ms: Math.round(performance.now() - startedAt),
    }));
    return { forced: false, reason };
  };

  const start = (reason: string): Promise<ShutdownResult> => {
    if (!shutdownPromise) {
      shutdownPromise = run(reason);
    }
    return shutdownPromise;
  };

  return {
    shutdown: start,
    forceShutdown(reason: string): Promise<ShutdownResult> {
      if (forceRequested) {
        return start(reason);
      }
      forceRequested = true;
      const promise = start(reason);
      requestForce?.();
      return promise;
    },
  };
}
