import { describe, expect, test } from "bun:test";

import { createRateLimiter, type RateLimiter } from "../../../src/auth/rate-limit.ts";

function fakeClock(start = 0): { now: () => number; advance: (ms: number) => void } {
  let now = start;
  return {
    now: () => now,
    advance: (ms: number) => {
      now += ms;
    },
  };
}

const BASE_POLICY = {
  maxFailures: 3,
  windowMs: 1_000,
  cooldownMs: 5_000,
  maxEntries: 10,
};

function fail(limiter: RateLimiter, key: string) {
  const admission = limiter.beginAttempt(key);
  if (!admission.allowed) {
    throw new Error(`Expected admission for '${key}'.`);
  }
  return admission.attempt.settleFailure();
}

describe("rate limiter", () => {
  test("allows failures until the threshold and then blocks with a retry window", () => {
    const clock = fakeClock();
    const limiter = createRateLimiter({ ...BASE_POLICY, now: clock.now });

    expect(fail(limiter, "a")).toEqual({ allowed: true, retryAfterMs: 0 });
    expect(fail(limiter, "a")).toEqual({ allowed: true, retryAfterMs: 0 });
    expect(fail(limiter, "a")).toEqual({ allowed: false, retryAfterMs: 5_000 });

    const blocked = limiter.beginAttempt("a");
    expect(blocked.allowed).toBe(false);
    if (!blocked.allowed) {
      expect(blocked.retryAfterMs).toBeGreaterThan(0);
    }
  });

  test("rejects attempts beyond the admitted in-flight budget", () => {
    const clock = fakeClock();
    const limiter = createRateLimiter({ ...BASE_POLICY, now: clock.now });

    const first = limiter.beginAttempt("a");
    const second = limiter.beginAttempt("a");
    const third = limiter.beginAttempt("a");

    expect(first.allowed).toBe(true);
    expect(second.allowed).toBe(true);
    expect(third.allowed).toBe(true);

    const rejected = limiter.beginAttempt("a");
    expect(rejected.allowed).toBe(false);
    if (!rejected.allowed) {
      expect(rejected.retryAfterMs).toBe(1_000);
    }
  });

  test("in-flight rejection uses a short retry hint and releases capacity on settle", () => {
    const limiter = createRateLimiter({ ...BASE_POLICY, now: fakeClock().now });

    const first = limiter.beginAttempt("a");
    const second = limiter.beginAttempt("a");
    const third = limiter.beginAttempt("a");

    const rejected = limiter.beginAttempt("a");
    expect(rejected.allowed).toBe(false);
    if (!rejected.allowed) {
      expect(rejected.retryAfterMs).toBe(1_000);
    }

    if (first.allowed && second.allowed) {
      first.attempt.cancel();
      second.attempt.settleSuccess();
    }

    const admission = limiter.beginAttempt("a");
    expect(admission.allowed).toBe(true);
    if (admission.allowed) {
      admission.attempt.cancel();
    }
    if (third.allowed) {
      third.attempt.cancel();
    }
    expect(limiter.size).toBe(0);
  });

  test("an active cooldown reports the remaining cooldown, not the short hint", () => {
    const clock = fakeClock();
    const limiter = createRateLimiter({ ...BASE_POLICY, now: clock.now });

    fail(limiter, "a");
    fail(limiter, "a");
    fail(limiter, "a");

    clock.advance(2_000);

    const rejected = limiter.beginAttempt("a");
    expect(rejected.allowed).toBe(false);
    if (!rejected.allowed) {
      expect(rejected.retryAfterMs).toBe(3_000);
    }
  });

  test("settled failures release in-flight budget and block at the threshold", () => {
    const clock = fakeClock();
    const limiter = createRateLimiter({ ...BASE_POLICY, now: clock.now });

    const first = limiter.beginAttempt("a");
    const second = limiter.beginAttempt("a");
    const third = limiter.beginAttempt("a");
    const rejectedWhilePending = limiter.beginAttempt("a");
    expect(rejectedWhilePending.allowed).toBe(false);
    if (!rejectedWhilePending.allowed) {
      expect(rejectedWhilePending.retryAfterMs).toBe(1_000);
    }

    if (first.allowed && second.allowed && third.allowed) {
      expect(first.attempt.settleFailure().allowed).toBe(true);
      expect(second.attempt.settleFailure().allowed).toBe(true);
      expect(third.attempt.settleFailure().allowed).toBe(false);
    }
  });

  test("cooldown expiry allows retries and the stale window resets failures", () => {
    const clock = fakeClock();
    const limiter = createRateLimiter({ ...BASE_POLICY, now: clock.now });

    fail(limiter, "a");
    fail(limiter, "a");
    fail(limiter, "a");

    clock.advance(6_000);
    const admission = limiter.beginAttempt("a");
    expect(admission.allowed).toBe(true);
    if (admission.allowed) {
      expect(admission.attempt.settleFailure().allowed).toBe(true);
    }
  });

  test("admits success after cooldown expiry within the failure window", () => {
    const clock = fakeClock();
    const limiter = createRateLimiter({
      maxFailures: 2,
      windowMs: 60_000,
      cooldownMs: 5_000,
      maxEntries: 10,
      now: clock.now,
    });

    fail(limiter, "a");
    fail(limiter, "a");

    clock.advance(5_100);

    const admission = limiter.beginAttempt("a");
    expect(admission.allowed).toBe(true);
    if (admission.allowed) {
      admission.attempt.settleSuccess();
    }
    expect(limiter.size).toBe(0);
  });

  test("a failure after cooldown expiry re-blocks within the window", () => {
    const clock = fakeClock();
    const limiter = createRateLimiter({
      maxFailures: 2,
      windowMs: 60_000,
      cooldownMs: 5_000,
      maxEntries: 10,
      now: clock.now,
    });

    fail(limiter, "a");
    fail(limiter, "a");

    clock.advance(5_100);

    const admission = limiter.beginAttempt("a");
    expect(admission.allowed).toBe(true);
    if (admission.allowed) {
      expect(admission.attempt.settleFailure().allowed).toBe(false);
    }
  });

  test("a successful attempt clears recorded failures", () => {
    const clock = fakeClock();
    const limiter = createRateLimiter({ ...BASE_POLICY, now: clock.now });

    fail(limiter, "a");
    fail(limiter, "a");

    const admission = limiter.beginAttempt("a");
    expect(admission.allowed).toBe(true);
    if (admission.allowed) {
      admission.attempt.settleSuccess();
    }

    expect(limiter.size).toBe(0);
    expect(fail(limiter, "a").allowed).toBe(true);
  });

  test("success keeps the entry until in-flight attempts settle", () => {
    const limiter = createRateLimiter({ ...BASE_POLICY, now: fakeClock().now });

    const first = limiter.beginAttempt("a");
    const second = limiter.beginAttempt("a");

    if (first.allowed && second.allowed) {
      first.attempt.settleSuccess();
      expect(limiter.size).toBe(1);
      second.attempt.cancel();
    }

    expect(limiter.size).toBe(0);
  });

  test("cancelling attempts without failures removes the entry", () => {
    const limiter = createRateLimiter({ ...BASE_POLICY, now: fakeClock().now });

    const admission = limiter.beginAttempt("a");
    if (admission.allowed) {
      admission.attempt.cancel();
    }

    expect(limiter.size).toBe(0);
  });

  test("tracks keys independently", () => {
    const limiter = createRateLimiter({ ...BASE_POLICY, now: fakeClock().now });

    fail(limiter, "a");
    fail(limiter, "a");
    fail(limiter, "a");
    expect(fail(limiter, "b").allowed).toBe(true);
  });

  test("fails closed at capacity instead of admitting new identities", () => {
    const limiter = createRateLimiter({
      maxFailures: 3,
      windowMs: 1_000,
      cooldownMs: 5_000,
      maxEntries: 2,
      now: fakeClock().now,
    });

    fail(limiter, "a");
    fail(limiter, "b");

    const admission = limiter.beginAttempt("c");
    expect(admission.allowed).toBe(false);
    if (!admission.allowed) {
      expect(admission.retryAfterMs).toBe(5_000);
    }
    expect(limiter.size).toBe(2);
  });

  test("evicts stale entries when new identities arrive at capacity", () => {
    const clock = fakeClock();
    const limiter = createRateLimiter({
      maxFailures: 3,
      windowMs: 1_000,
      cooldownMs: 5_000,
      maxEntries: 2,
      now: clock.now,
    });

    fail(limiter, "a");
    fail(limiter, "b");
    clock.advance(2_000);

    const admission = limiter.beginAttempt("c");
    expect(admission.allowed).toBe(true);
    expect(limiter.size).toBeLessThanOrEqual(2);
  });

  test("does not evict entries with in-flight attempts", () => {
    const clock = fakeClock();
    const limiter = createRateLimiter({
      maxFailures: 3,
      windowMs: 1_000,
      cooldownMs: 5_000,
      maxEntries: 1,
      now: clock.now,
    });

    const pending = limiter.beginAttempt("b");
    expect(pending.allowed).toBe(true);

    clock.advance(2_000);

    const admission = limiter.beginAttempt("c");
    expect(admission.allowed).toBe(false);
    expect(limiter.size).toBe(1);
  });

  test("rolls the failure window without reaching the threshold", () => {
    const clock = fakeClock();
    const limiter = createRateLimiter({
      maxFailures: 5,
      windowMs: 1_000,
      cooldownMs: 5_000,
      maxEntries: 10,
      now: clock.now,
    });

    fail(limiter, "a");
    clock.advance(1_500);
    fail(limiter, "a");
    expect(fail(limiter, "a").allowed).toBe(true);
  });

  test("prunes expired pending attempts so they no longer block admission", () => {
    const clock = fakeClock();
    const limiter = createRateLimiter({
      maxFailures: 1,
      windowMs: 10_000,
      cooldownMs: 5_000,
      maxEntries: 10,
      attemptTimeoutMs: 1_000,
      now: clock.now,
    });

    const first = limiter.beginAttempt("a");
    expect(first.allowed).toBe(true);

    const rejected = limiter.beginAttempt("a");
    expect(rejected.allowed).toBe(false);

    clock.advance(1_500);

    const admission = limiter.beginAttempt("a");
    expect(admission.allowed).toBe(true);
    if (admission.allowed) {
      admission.attempt.cancel();
    }
    expect(limiter.size).toBe(0);
  });

  test("an expired attempt cannot count a failure without a newer admission", () => {
    const clock = fakeClock();
    const limiter = createRateLimiter({
      maxFailures: 2,
      windowMs: 60_000,
      cooldownMs: 5_000,
      maxEntries: 10,
      attemptTimeoutMs: 1_000,
      now: clock.now,
    });

    const stalled = limiter.beginAttempt("a");
    expect(stalled.allowed).toBe(true);

    clock.advance(1_500);

    if (stalled.allowed) {
      expect(stalled.attempt.settleFailure().allowed).toBe(true);
    }

    expect(limiter.size).toBe(1);

    const second = limiter.beginAttempt("a");
    if (second.allowed) {
      expect(second.attempt.settleFailure().allowed).toBe(true);
    }

    const third = limiter.beginAttempt("a");
    if (third.allowed) {
      expect(third.attempt.settleFailure().allowed).toBe(false);
    }
  });

  test("an expired attempt cannot clear failures without a newer admission", () => {
    const clock = fakeClock();
    const limiter = createRateLimiter({
      maxFailures: 2,
      windowMs: 60_000,
      cooldownMs: 5_000,
      maxEntries: 10,
      attemptTimeoutMs: 1_000,
      now: clock.now,
    });

    fail(limiter, "a");

    const stalled = limiter.beginAttempt("a");
    expect(stalled.allowed).toBe(true);

    clock.advance(1_500);

    if (stalled.allowed) {
      stalled.attempt.settleSuccess();
    }

    const next = limiter.beginAttempt("a");
    if (next.allowed) {
      expect(next.attempt.settleFailure().allowed).toBe(false);
    }
  });

  test("an expired attempt is removed by cancel without touching failures", () => {
    const clock = fakeClock();
    const limiter = createRateLimiter({
      maxFailures: 2,
      windowMs: 60_000,
      cooldownMs: 5_000,
      maxEntries: 10,
      attemptTimeoutMs: 1_000,
      now: clock.now,
    });

    const stalled = limiter.beginAttempt("a");
    expect(stalled.allowed).toBe(true);

    clock.advance(1_500);

    if (stalled.allowed) {
      stalled.attempt.cancel();
    }

    expect(limiter.size).toBe(0);
  });

  test("a late failure after expiry is not counted", () => {
    const clock = fakeClock();
    const limiter = createRateLimiter({
      maxFailures: 3,
      windowMs: 60_000,
      cooldownMs: 5_000,
      maxEntries: 10,
      attemptTimeoutMs: 1_000,
      now: clock.now,
    });

    const stalled = limiter.beginAttempt("a");
    expect(stalled.allowed).toBe(true);

    clock.advance(1_500);

    const second = limiter.beginAttempt("a");
    expect(second.allowed).toBe(true);
    if (second.allowed) {
      expect(second.attempt.settleFailure().allowed).toBe(true);
    }

    if (stalled.allowed) {
      expect(stalled.attempt.settleFailure().allowed).toBe(true);
    }

    const third = limiter.beginAttempt("a");
    if (third.allowed) {
      expect(third.attempt.settleFailure().allowed).toBe(true);
    }

    const fourth = limiter.beginAttempt("a");
    if (fourth.allowed) {
      expect(fourth.attempt.settleFailure().allowed).toBe(false);
    }
  });

  test("a late success after expiry does not clear newer failures", () => {
    const clock = fakeClock();
    const limiter = createRateLimiter({
      maxFailures: 2,
      windowMs: 60_000,
      cooldownMs: 5_000,
      maxEntries: 10,
      attemptTimeoutMs: 1_000,
      now: clock.now,
    });

    const stalled = limiter.beginAttempt("a");
    expect(stalled.allowed).toBe(true);

    clock.advance(1_500);

    const second = limiter.beginAttempt("a");
    expect(second.allowed).toBe(true);
    if (second.allowed) {
      expect(second.attempt.settleFailure().allowed).toBe(true);
    }

    if (stalled.allowed) {
      stalled.attempt.settleSuccess();
    }

    const third = limiter.beginAttempt("a");
    if (third.allowed) {
      expect(third.attempt.settleFailure().allowed).toBe(false);
    }
  });

  test("settle methods are one-shot", () => {
    const limiter = createRateLimiter({ ...BASE_POLICY, now: fakeClock().now });

    const admission = limiter.beginAttempt("a");
    expect(admission.allowed).toBe(true);
    if (admission.allowed) {
      expect(admission.attempt.settleFailure().allowed).toBe(true);
      admission.attempt.settleSuccess();
    }

    expect(limiter.size).toBe(1);
  });
});
