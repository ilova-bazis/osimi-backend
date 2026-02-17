import type { AuthorizedWorkerLease } from "../auth/worker-lease.ts";
import type { IncomingWorkerEvent } from "../validation/event.ts";

export interface IngestWorkerEventsInput {
  authorizedLease: AuthorizedWorkerLease;
  events: IncomingWorkerEvent[];
}

export interface IngestWorkerEventsResponse {
  status: "ok";
  ingestion_id: string;
  inserted_events: number;
  duplicate_events: number;
  object_id: string | null;
}
