import { describe, expect, test } from "bun:test";

import {
  createErrorResponse,
  PublicationAlreadyActiveError,
} from "../../../src/http/errors.ts";

describe("publication errors", () => {
  test("serializes the active request identity and status", async () => {
    const response = createErrorResponse(
      new PublicationAlreadyActiveError(
        "A curation publication is already active for this object.",
        {
          existing_request_id: "70000000-0000-4000-8000-000000000001",
          existing_request_status: "PROCESSING",
        },
      ),
      "80000000-0000-4000-8000-000000000001",
    );

    expect(response.status).toBe(409);
    expect(await response.json()).toEqual({
      request_id: "80000000-0000-4000-8000-000000000001",
      error: {
        code: "PUBLICATION_ALREADY_ACTIVE",
        message: "A curation publication is already active for this object.",
        details: {
          existing_request_id: "70000000-0000-4000-8000-000000000001",
          existing_request_status: "PROCESSING",
        },
      },
    });
  });
});
