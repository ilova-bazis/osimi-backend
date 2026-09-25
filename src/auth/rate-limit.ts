export interface RateLimitPolicy {
  maxFailures: number;
  windowMs: number;
  cooldownMs: number;
  maxEntries: number;
  attemptTimeoutMs?: number;
  now?: () => number;
}

export interface RateLimitDecision {
  allowed: boolean;
  retryAfterMs: number;
}

export interface RateLimitAttempt {
  settleFailure(): RateLimitDecision;
  settleSuccess(): void;
  cancel(): void;
}

interface PendingAttempt {
  id: number;
  deadlineMs: number;
}

interface RateLimitEntry {
  failures: number;
  pending: PendingAttempt[];
  windowStartedAtMs: number;
  blockedUntilMs: number;
}

export type RateLimitAdmission =
  | { allowed: true; attempt: RateLimitAttempt }
  | { allowed: false; retryAfterMs: number };

const ALLOWED_DECISION: RateLimitDecision = { allowed: true, retryAfterMs: 0 };
const IN_FLIGHT_RETRY_AFTER_MS = 1_000;
const DEFAULT_ATTEMPT_TIMEOUT_MS = 30_000;

export class RateLimiter {
  private readonly policy: RateLimitPolicy;
  private readonly entries = new Map<string, RateLimitEntry>();
  private nextAttemptId = 1;

  constructor(policy: RateLimitPolicy) {
    this.policy = policy;
  }

  get size(): number {
    return this.entries.size;
  }

  beginAttempt(key: string): RateLimitAdmission {
    const now = this.now();
    let entry = this.entries.get(key);

    if (entry) {
      this.prunePending(entry, now);
    }

    if (entry && entry.blockedUntilMs > now) {
      return { allowed: false, retryAfterMs: entry.blockedUntilMs - now };
    }

    if (entry && now - entry.windowStartedAtMs > this.policy.windowMs) {
      if (entry.pending.length === 0) {
        this.entries.delete(key);
        entry = undefined;
      } else {
        entry.failures = 0;
        entry.windowStartedAtMs = now;
      }
    }

    if (entry && entry.pending.length > 0 && entry.failures + entry.pending.length >= this.policy.maxFailures) {
      return { allowed: false, retryAfterMs: IN_FLIGHT_RETRY_AFTER_MS };
    }

    if (!entry) {
      if (this.entries.size >= this.policy.maxEntries) {
        this.removeStaleEntries(now);
      }

      if (this.entries.size >= this.policy.maxEntries) {
        return { allowed: false, retryAfterMs: this.policy.cooldownMs };
      }

      entry = {
        failures: 0,
        pending: [],
        windowStartedAtMs: now,
        blockedUntilMs: 0,
      };
      this.entries.set(key, entry);
    }

    const id = this.nextAttemptId;
    this.nextAttemptId += 1;
    entry.pending.push({ id, deadlineMs: now + this.attemptTimeoutMs() });

    let settled = false;
    const settleOnce = (settle: () => RateLimitDecision): RateLimitDecision => {
      if (settled) {
        return ALLOWED_DECISION;
      }
      settled = true;
      return settle();
    };

    return {
      allowed: true,
      attempt: {
        settleFailure: () => settleOnce(() => this.settleFailure(key, id)),
        settleSuccess: () => {
          if (settled) {
            return;
          }
          settled = true;
          this.settleSuccess(key, id);
        },
        cancel: () => {
          if (settled) {
            return;
          }
          settled = true;
          this.cancel(key, id);
        },
      },
    };
  }

  private settleFailure(key: string, id: number): RateLimitDecision {
    const now = this.now();
    const entry = this.entries.get(key);

    if (!entry || !this.removePending(entry, id, now)) {
      return ALLOWED_DECISION;
    }

    if (entry.blockedUntilMs > now) {
      return { allowed: false, retryAfterMs: entry.blockedUntilMs - now };
    }

    if (now - entry.windowStartedAtMs > this.policy.windowMs) {
      entry.failures = 0;
      entry.windowStartedAtMs = now;
    }

    entry.failures += 1;

    if (entry.failures >= this.policy.maxFailures) {
      entry.blockedUntilMs = now + this.policy.cooldownMs;
      return { allowed: false, retryAfterMs: this.policy.cooldownMs };
    }

    return ALLOWED_DECISION;
  }

  private settleSuccess(key: string, id: number): void {
    const entry = this.entries.get(key);
    if (!entry || !this.removePending(entry, id, this.now())) {
      return;
    }

    entry.failures = 0;
    entry.windowStartedAtMs = this.now();

    if (entry.pending.length === 0) {
      this.entries.delete(key);
    }
  }

  private cancel(key: string, id: number): void {
    const entry = this.entries.get(key);
    if (!entry) {
      return;
    }

    this.removePending(entry, id, this.now());

    if (entry.failures === 0 && entry.pending.length === 0) {
      this.entries.delete(key);
    }
  }

  private removePending(entry: RateLimitEntry, id: number, now: number): boolean {
    let matched = false;
    let expired = false;

    entry.pending = entry.pending.filter((attempt) => {
      if (attempt.id === id) {
        matched = true;
        expired = attempt.deadlineMs <= now;
        return false;
      }
      return attempt.deadlineMs > now;
    });

    return matched && !expired;
  }

  private prunePending(entry: RateLimitEntry, now: number): void {
    entry.pending = entry.pending.filter((attempt) => attempt.deadlineMs > now);
  }

  private removeStaleEntries(now: number): void {
    for (const [key, entry] of this.entries) {
      this.prunePending(entry, now);
      const stale = entry.pending.length === 0 &&
        entry.blockedUntilMs <= now &&
        now - entry.windowStartedAtMs > this.policy.windowMs;

      if (stale) {
        this.entries.delete(key);
      }
    }
  }

  private attemptTimeoutMs(): number {
    return this.policy.attemptTimeoutMs ?? DEFAULT_ATTEMPT_TIMEOUT_MS;
  }

  private now(): number {
    return this.policy.now ? this.policy.now() : Date.now();
  }
}

export function createRateLimiter(policy: RateLimitPolicy): RateLimiter {
  return new RateLimiter(policy);
}
