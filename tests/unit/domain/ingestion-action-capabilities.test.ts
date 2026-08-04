import { describe, expect, test } from "bun:test";

import {
  resolveIngestionActionCapabilities,
  resolveStagingPurgeState,
} from "../../../src/domain/ingestions/action-capabilities.ts";

const noActions = {
  canResume: false,
  canRetry: false,
  canCancel: false,
  canRestore: false,
  canDelete: false,
};

describe("ingestion action capabilities", () => {
  test("exposes only executable actions for each mutable status", () => {
    expect(resolveIngestionActionCapabilities({
      status: "DRAFT",
      role: "archiver",
      purgeState: "NOT_SCHEDULED",
      hasActiveLease: false,
    })).toEqual({ ...noActions, canResume: true, canCancel: true, canDelete: true });
    expect(resolveIngestionActionCapabilities({
      status: "FAILED",
      role: "archiver",
      purgeState: "NOT_SCHEDULED",
      hasActiveLease: false,
    })).toEqual({ ...noActions, canRetry: true });
    expect(resolveIngestionActionCapabilities({
      status: "CANCELED",
      role: "admin",
      purgeState: "NOT_SCHEDULED",
      hasActiveLease: false,
    })).toEqual({ ...noActions, canRestore: true, canDelete: true });
  });

  test("suppresses all actions for viewers, leases, and purge intent", () => {
    for (const params of [
      { role: "viewer" as const, purgeState: "NOT_SCHEDULED" as const, hasActiveLease: false },
      { role: "archiver" as const, purgeState: "PENDING" as const, hasActiveLease: false },
      { role: "archiver" as const, purgeState: "PURGED" as const, hasActiveLease: false },
      { role: "archiver" as const, purgeState: "NOT_SCHEDULED" as const, hasActiveLease: true },
    ]) {
      expect(resolveIngestionActionCapabilities({ status: "FAILED", ...params })).toEqual(noActions);
    }
  });

  test("gives completed ingestion states no mutation actions", () => {
    for (const status of ["PROCESSING", "COMPLETED", "COMPLETED_WITH_ERRORS"] as const) {
      expect(resolveIngestionActionCapabilities({
        status,
        role: "archiver",
        purgeState: "NOT_SCHEDULED",
        hasActiveLease: false,
      })).toEqual(noActions);
    }
  });

  test("prioritizes completed purge state over contradictory started timestamps", () => {
    expect(resolveStagingPurgeState({})).toBe("NOT_SCHEDULED");
    expect(resolveStagingPurgeState({ startedAt: new Date("2026-01-01T00:00:00.000Z") })).toBe("PENDING");
    expect(resolveStagingPurgeState({
      startedAt: new Date("2026-01-01T00:00:00.000Z"),
      purgedAt: new Date("2026-01-02T00:00:00.000Z"),
    })).toBe("PURGED");
  });
});
