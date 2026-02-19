import type { UserRole } from "../../auth/types.ts";

export type AccessLevel = "private" | "family" | "public";
export type AssignmentLevel = "family" | "private";
export type AvailabilityState =
  | "AVAILABLE"
  | "ARCHIVED"
  | "RESTORE_PENDING"
  | "RESTORING"
  | "UNAVAILABLE";
export type CurationState =
  | "needs_review"
  | "review_in_progress"
  | "reviewed"
  | "curation_failed";
export type EmbargoKind = "none" | "timed" | "curation_state";

export type AccessReasonCode =
  | "OK"
  | "FORBIDDEN_POLICY"
  | "EMBARGO_ACTIVE"
  | "RESTORE_REQUIRED"
  | "RESTORE_IN_PROGRESS"
  | "TEMP_UNAVAILABLE";

export interface AccessDecision {
  isAuthorized: boolean;
  isDeliverable: boolean;
  canDownload: boolean;
  accessReasonCode: AccessReasonCode;
}

function isAuthorized(params: {
  role: UserRole;
  accessLevel: AccessLevel;
  assignmentLevel?: AssignmentLevel;
}): boolean {
  if (params.role === "admin") {
    return true;
  }

  if (params.accessLevel === "public") {
    return true;
  }

  if (params.accessLevel === "family") {
    return (
      params.assignmentLevel === "family" ||
      params.assignmentLevel === "private"
    );
  }

  return params.assignmentLevel === "private";
}

function isEmbargoActive(params: {
  embargoKind: EmbargoKind;
  embargoUntil?: string;
  embargoCurationState?: CurationState;
  objectCurationState: CurationState;
  now: number;
}): boolean {
  if (params.embargoKind === "none") {
    return false;
  }

  if (params.embargoKind === "timed") {
    if (!params.embargoUntil) {
      return false;
    }

    return new Date(params.embargoUntil).getTime() > params.now;
  }

  if (!params.embargoCurationState) {
    return false;
  }

  return params.objectCurationState !== params.embargoCurationState;
}

function isDeliverable(availabilityState: AvailabilityState): boolean {
  return availabilityState === "AVAILABLE";
}

function reasonForUnavailable(
  availabilityState: AvailabilityState,
): AccessReasonCode {
  if (availabilityState === "ARCHIVED") {
    return "RESTORE_REQUIRED";
  }

  if (
    availabilityState === "RESTORE_PENDING" ||
    availabilityState === "RESTORING"
  ) {
    return "RESTORE_IN_PROGRESS";
  }

  return "TEMP_UNAVAILABLE";
}

export function buildAccessDecision(params: {
  role: UserRole;
  accessLevel: AccessLevel;
  assignmentLevel?: AssignmentLevel;
  embargoKind: EmbargoKind;
  embargoUntil?: string;
  embargoCurationState?: CurationState;
  objectCurationState: CurationState;
  availabilityState: AvailabilityState;
  now?: number;
}): AccessDecision {
  const now = params.now ?? Date.now();
  const authorized = isAuthorized({
    role: params.role,
    accessLevel: params.accessLevel,
    assignmentLevel: params.assignmentLevel,
  });

  if (!authorized) {
    return {
      isAuthorized: false,
      isDeliverable: false,
      canDownload: false,
      accessReasonCode: "FORBIDDEN_POLICY",
    };
  }

  if (
    isEmbargoActive({
      embargoKind: params.embargoKind,
      embargoUntil: params.embargoUntil,
      embargoCurationState: params.embargoCurationState,
      objectCurationState: params.objectCurationState,
      now,
    })
  ) {
    return {
      isAuthorized: true,
      isDeliverable: false,
      canDownload: false,
      accessReasonCode: "EMBARGO_ACTIVE",
    };
  }

  const deliverable = isDeliverable(params.availabilityState);
  if (!deliverable) {
    return {
      isAuthorized: true,
      isDeliverable: false,
      canDownload: false,
      accessReasonCode: reasonForUnavailable(params.availabilityState),
    };
  }

  return {
    isAuthorized: true,
    isDeliverable: true,
    canDownload: true,
    accessReasonCode: "OK",
  };
}
