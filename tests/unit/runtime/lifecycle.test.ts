import { describe, expect, test } from "bun:test";

import { LifecycleController } from "../../../src/runtime/lifecycle.ts";

describe("LifecycleController", () => {
  test("rejects new admissions after draining while waiting for admitted work", async () => {
    const lifecycle = new LifecycleController();
    const release = lifecycle.admitRequest();

    expect(release).toBeDefined();
    expect(lifecycle.beginDrain()).toBe(true);
    expect(lifecycle.beginDrain()).toBe(false);
    expect(lifecycle.isReady()).toBe(false);
    expect(lifecycle.admitRequest()).toBeUndefined();

    let idle = false;
    const waitForIdle = lifecycle.waitForIdle().then(() => {
      idle = true;
    });
    expect(idle).toBe(false);
    release?.();
    await waitForIdle;
    expect(lifecycle.activeRequestCount()).toBe(0);
  });
});
