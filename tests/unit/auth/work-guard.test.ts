import { describe, expect, test } from "bun:test";

import { WorkGuard } from "../../../src/auth/work-guard.ts";

describe("work guard", () => {
  test("allows work up to the configured limit", () => {
    const guard = new WorkGuard(3);

    expect(guard.acquire()).toBe(true);
    expect(guard.acquire()).toBe(true);
    expect(guard.acquire()).toBe(true);
    expect(guard.activeCount).toBe(3);
  });

  test("rejects work beyond the configured limit", () => {
    const guard = new WorkGuard(2);

    guard.acquire();
    guard.acquire();
    expect(guard.acquire()).toBe(false);
    expect(guard.activeCount).toBe(2);
  });

  test("release frees a slot for new work", () => {
    const guard = new WorkGuard(1);

    expect(guard.acquire()).toBe(true);
    expect(guard.acquire()).toBe(false);
    guard.release();
    expect(guard.activeCount).toBe(0);
    expect(guard.acquire()).toBe(true);
  });

  test("release never drops the count below zero", () => {
    const guard = new WorkGuard(1);

    guard.release();
    guard.release();
    expect(guard.activeCount).toBe(0);
  });
});
