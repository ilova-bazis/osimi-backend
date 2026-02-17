import { z } from "zod";

import { mapZodErrorToValidation } from "./zod-errors.ts";

export const OBJECT_EVENT_TYPES = [
  "INGESTION_SUBMITTED",
  "INGESTION_QUEUED",
  "INGESTION_PROCESSING",
  "INGESTION_COMPLETED",
  "INGESTION_FAILED",
  "INGESTION_CANCELED",
  "LEASE_GRANTED",
  "LEASE_RENEWED",
  "LEASE_EXPIRED",
  "LEASE_RELEASED",
  "FILE_VALIDATED",
  "FILE_FAILED",
  "PIPELINE_STEP_STARTED",
  "PIPELINE_STEP_COMPLETED",
  "PIPELINE_STEP_FAILED",
  "OBJECT_CREATED",
  "ARTIFACT_CREATED",
] as const;

const objectEventTypeSchema = z.enum(OBJECT_EVENT_TYPES);

const OBJECT_ID_REQUIRED_EVENT_TYPES = [
  "INGESTION_COMPLETED",
  "OBJECT_CREATED",
  "ARTIFACT_CREATED",
] as const;

const OBJECT_ID_PATTERN = /^OBJ-[0-9]{8}-[A-Z0-9]+$/;

const jsonObjectSchema = z.record(z.string(), z.unknown());

const baseEventSchema = z.object({
  event_id: z.uuid(),
  timestamp: z.string().datetime({ offset: true }),
  payload: jsonObjectSchema,
});

const requiredObjectIdEventSchema = baseEventSchema.extend({
  event_type: objectEventTypeSchema.extract(OBJECT_ID_REQUIRED_EVENT_TYPES),
  object_id: z.string().regex(OBJECT_ID_PATTERN, {
    message: "object_id must match format OBJ-YYYYMMDD-XXXXXX.",
  }),
});

const optionalObjectIdEventSchema = baseEventSchema.extend({
  event_type: objectEventTypeSchema.exclude(OBJECT_ID_REQUIRED_EVENT_TYPES),
  object_id: z
    .string()
    .regex(OBJECT_ID_PATTERN, {
      message: "object_id must match format OBJ-YYYYMMDD-XXXXXX.",
    })
    .optional(),
});

const incomingEventSchema = z.discriminatedUnion("event_type", [
  requiredObjectIdEventSchema,
  optionalObjectIdEventSchema,
]);

const ingestWorkerEventsSchema = z.object({
  lease_token: z.string().trim().min(1),
  events: z.array(incomingEventSchema),
});

export type IncomingWorkerEvent = z.infer<typeof incomingEventSchema>;

export interface IngestWorkerEventsBody {
  lease_token: string;
  events: IncomingWorkerEvent[];
}

export function parseIngestWorkerEventsBody(
  body: unknown,
): IngestWorkerEventsBody {
  const parsed = ingestWorkerEventsSchema.safeParse(body);

  if (!parsed.success) {
    throw mapZodErrorToValidation(parsed.error);
  }

  return parsed.data;
}
