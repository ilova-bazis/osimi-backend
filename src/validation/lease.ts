import { z } from "zod";

import { mapZodErrorToValidation } from "./zod-errors.ts";

const leaseTokenBodySchema = z.object({
  lease_token: z.string().trim().min(1),
});

export type LeaseTokenBody = z.infer<typeof leaseTokenBodySchema>;

export function parseLeaseTokenBody(body: unknown): LeaseTokenBody {
  const parsed = leaseTokenBodySchema.safeParse(body);

  if (!parsed.success) {
    throw mapZodErrorToValidation(parsed.error);
  }

  return parsed.data;
}
