import { afterEach, describe, expect, test } from "bun:test";

interface Fixture {
  process: Bun.Subprocess;
  port: number;
  waitForEvent(event: string): Promise<void>;
}

const processes = new Set<Bun.Subprocess>();

async function startFixture(gracePeriodMs: number): Promise<Fixture> {
  const child = Bun.spawn(["bun", "tests/fixtures/graceful-shutdown-server.ts"], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      BACKGROUND_JOBS_ENABLED: "false",
      HTTP_ACCESS_LOGS: "false",
      SHUTDOWN_GRACE_PERIOD_MS: String(gracePeriodMs),
    },
    stdout: "pipe",
    stderr: "pipe",
  });
  processes.add(child);

  const events = new Set<string>();
  const waiters = new Map<string, Array<() => void>>();
  let port: number | undefined;
  void (async () => {
    const reader = child.stdout.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        return;
      }
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n");
      buffer = lines.pop() ?? "";
      for (const line of lines) {
        try {
          const message = JSON.parse(line) as { event?: string; port?: number };
          if (!message.event) {
            continue;
          }
          events.add(message.event);
          if (message.event === "fixture.listening") {
            port = message.port;
          }
          for (const resolve of waiters.get(message.event) ?? []) {
            resolve();
          }
          waiters.delete(message.event);
        } catch {
          // Startup logs are not fixture control events.
        }
      }
    }
  })();

  const waitForEvent = (event: string): Promise<void> => {
    if (events.has(event)) {
      return Promise.resolve();
    }
    return new Promise((resolve) => {
      const existing = waiters.get(event) ?? [];
      existing.push(resolve);
      waiters.set(event, existing);
    });
  };

  await waitForEvent("fixture.listening");
  if (!port) {
    throw new Error("Fixture did not provide a listening port.");
  }
  return { process: child, port, waitForEvent };
}

afterEach(async () => {
  await Promise.all([...processes].map(async (process) => {
    if (process.exitCode === null) {
      process.kill("SIGKILL");
      await process.exited;
    }
    processes.delete(process);
  }));
});

describe("graceful server shutdown", () => {
  test("drains active work and rejects fresh connections", async () => {
    const fixture = await startFixture(500);
    const activeResponse = fetch(`http://127.0.0.1:${fixture.port}/block`);
    await fixture.waitForEvent("fixture.request_entered");

    fixture.process.kill("SIGTERM");
    await fixture.waitForEvent("server.shutdown.started");

    await expect(fetch(`http://127.0.0.1:${fixture.port}/block`)).rejects.toThrow();
    expect((await activeResponse).status).toBe(200);
    expect(await fixture.process.exited).toBe(0);
  }, 5_000);

  test("forces bounded exit after the grace period", async () => {
    const fixture = await startFixture(300);
    void fetch(`http://127.0.0.1:${fixture.port}/block-forever`).catch(() => undefined);
    await fixture.waitForEvent("fixture.request_entered");

    fixture.process.kill("SIGTERM");
    expect(await fixture.process.exited).toBe(1);
  }, 5_000);
});
