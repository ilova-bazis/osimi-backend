import type { UserRole } from "../../auth/types.ts";
import type { IngestionStatus } from "./state-machine.ts";

export type StagingPurgeState = "NOT_SCHEDULED" | "PENDING" | "PURGED";

export interface IngestionActionCapabilities {
  canResume: boolean;
  canRetry: boolean;
  canCancel: boolean;
  canRestore: boolean;
  canDelete: boolean;
}

const NO_ACTIONS: IngestionActionCapabilities = {
  canResume: false,
  canRetry: false,
  canCancel: false,
  canRestore: false,
  canDelete: false,
};

export function resolveStagingPurgeState(params: {
  startedAt?: Date;
  purgedAt?: Date;
}): StagingPurgeState {
  if (params.purgedAt) return "PURGED";
  if (params.startedAt) return "PENDING";
  return "NOT_SCHEDULED";
}

export function resolveIngestionActionCapabilities(params: {
  status: IngestionStatus;
  role: UserRole;
  purgeState: StagingPurgeState;
  hasActiveLease: boolean;
}): IngestionActionCapabilities {
  if (
    params.role === "viewer" ||
    params.purgeState !== "NOT_SCHEDULED" ||
    params.hasActiveLease
  ) {
    return { ...NO_ACTIONS };
  }

  switch (params.status) {
    case "DRAFT":
      return { ...NO_ACTIONS, canResume: true, canCancel: true, canDelete: true };
    case "UPLOADING":
      return { ...NO_ACTIONS, canResume: true, canCancel: true, canDelete: true };
    case "QUEUED":
      return { ...NO_ACTIONS, canCancel: true };
    case "FAILED":
      return { ...NO_ACTIONS, canRetry: true };
    case "CANCELED":
      return { ...NO_ACTIONS, canRestore: true, canDelete: true };
    default:
      return { ...NO_ACTIONS };
  }
}
