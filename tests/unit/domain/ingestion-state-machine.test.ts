import { describe, expect, test } from "bun:test";

import {
  assertIngestionStatusTransition,
  canTransitionIngestionStatus,
  isTerminalIngestionStatus,
  type IngestionStatus,
} from "../../../src/domain/ingestions/state-machine.ts";

describe("ingestion state machine", () => {
  test.each([
    ["DRAFT", "UPLOADING"],
    ["UPLOADING", "QUEUED"],
    ["UPLOADING", "FAILED"],
    ["QUEUED", "PROCESSING"],
    ["PROCESSING", "QUEUED"],
    ["PROCESSING", "COMPLETED"],
    ["FAILED", "QUEUED"],
    ["CANCELED", "QUEUED"],
    ["QUEUED", "QUEUED"],
  ] as const)("allows %s -> %s", (from, to) => {
    expect(canTransitionIngestionStatus(from, to)).toBe(true);
    expect(() => assertIngestionStatusTransition(from, to)).not.toThrow();
  });

  test.each([
    ["DRAFT", "PROCESSING"],
    ["DRAFT", "COMPLETED"],
    ["COMPLETED", "QUEUED"],
    ["QUEUED", "COMPLETED"],
    ["FAILED", "COMPLETED"],
  ] as const)("rejects %s -> %s", (from, to) => {
    expect(canTransitionIngestionStatus(from, to)).toBe(false);
    expect(() => assertIngestionStatusTransition(from, to)).toThrow(
      `Invalid ingestion transition from '${from}' to '${to}'.`,
    );
  });

  test("returns terminal statuses", () => {
    const terminalStatuses: IngestionStatus[] = ["COMPLETED", "FAILED", "CANCELED"];
    const activeStatuses: IngestionStatus[] = ["DRAFT", "UPLOADING", "QUEUED", "PROCESSING"];

    for (const status of terminalStatuses) {
      expect(isTerminalIngestionStatus(status)).toBe(true);
    }

    for (const status of activeStatuses) {
      expect(isTerminalIngestionStatus(status)).toBe(false);
    }
  });
});
