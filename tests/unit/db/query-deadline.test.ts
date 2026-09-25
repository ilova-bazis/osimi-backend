import { describe, expect, test } from "bun:test";

import { DatabaseQueryTimeoutError, executeWithDeadline } from "../../../src/db/client.ts";

function cancellable<T>(promise: Promise<T>, onCancel: () => void): Promise<T> & { cancel(): void } {
  return Object.assign(promise, { cancel: onCancel });
}

describe("executeWithDeadline", () => {
  test("passes through a result that arrives before the deadline", async () => {
    const result = await executeWithDeadline(
      cancellable(Promise.resolve("ok"), () => {}),
      1_000,
    );
    expect(result).toBe("ok");
  });

  test("passes through the query error when it arrives before the deadline", async () => {
    await expect(executeWithDeadline(
      cancellable(Promise.reject(new Error("boom")), () => {}),
      1_000,
    )).rejects.toThrow("boom");
  });

  test("cancels at the deadline but waits for the query to stop before rejecting", async () => {
    let cancelled = false;
    let rejectQuery: ((error: Error) => void) | undefined;
    const pending = new Promise<never>((_resolve, reject) => {
      rejectQuery = reject;
    });
    const query = cancellable(pending, () => {
      cancelled = true;
    });
    let settled = false;
    const result = executeWithDeadline(query, 10).finally(() => {
      settled = true;
    });

    await Bun.sleep(20);

    expect(cancelled).toBe(true);
    expect(settled).toBe(false);

    rejectQuery?.(new Error("query cancelled"));

    await expect(result).rejects.toBeInstanceOf(
      DatabaseQueryTimeoutError,
    );
    expect(settled).toBe(true);
  });

  test("reports a timeout when a cancelled query resolves during the race", async () => {
    let resolveQuery: ((value: string) => void) | undefined;
    const pending = new Promise<string>((resolve) => {
      resolveQuery = resolve;
    });
    const query = cancellable(pending, () => {
      resolveQuery?.("cancelled");
    });

    await expect(executeWithDeadline(query, 10)).rejects.toBeInstanceOf(
      DatabaseQueryTimeoutError,
    );
  });

  test("awaits normally when no deadline is provided", async () => {
    const result = await executeWithDeadline(
      cancellable(Promise.resolve("ok"), () => {}),
      undefined,
    );
    expect(result).toBe("ok");
  });
});
