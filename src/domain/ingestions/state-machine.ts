export const INGESTION_STATUSES = [
  "DRAFT",
  "UPLOADING",
  "QUEUED",
  "PROCESSING",
  "COMPLETED",
  "FAILED",
  "CANCELED",
] as const;

export type IngestionStatus = (typeof INGESTION_STATUSES)[number];

export class InvalidIngestionTransitionError extends Error {
  readonly from: IngestionStatus;
  readonly to: IngestionStatus;

  constructor(from: IngestionStatus, to: IngestionStatus) {
    super(`Invalid ingestion transition from '${from}' to '${to}'.`);
    this.name = "InvalidIngestionTransitionError";
    this.from = from;
    this.to = to;
  }
}

const ALLOWED_TRANSITIONS: Record<IngestionStatus, readonly IngestionStatus[]> = {
  DRAFT: ["UPLOADING", "CANCELED"],
  UPLOADING: ["QUEUED", "FAILED", "CANCELED"],
  QUEUED: ["PROCESSING", "CANCELED", "UPLOADING"],
  PROCESSING: ["COMPLETED", "FAILED", "CANCELED", "QUEUED"],
  COMPLETED: [],
  FAILED: ["QUEUED"],
  CANCELED: ["QUEUED", "UPLOADING", "DRAFT"],
};

const TERMINAL_STATUSES = new Set<IngestionStatus>(["COMPLETED", "FAILED", "CANCELED"]);

export function isIngestionStatus(value: string): value is IngestionStatus {
  return (INGESTION_STATUSES as readonly string[]).includes(value);
}

export function canTransitionIngestionStatus(from: IngestionStatus, to: IngestionStatus): boolean {
  if (from === to) {
    return true;
  }

  return ALLOWED_TRANSITIONS[from].includes(to);
}

export function assertIngestionStatusTransition(from: IngestionStatus, to: IngestionStatus): void {
  if (!canTransitionIngestionStatus(from, to)) {
    throw new InvalidIngestionTransitionError(from, to);
  }
}

export function isTerminalIngestionStatus(status: IngestionStatus): boolean {
  return TERMINAL_STATUSES.has(status);
}
