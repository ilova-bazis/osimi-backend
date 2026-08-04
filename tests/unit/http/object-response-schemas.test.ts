import { describe, expect, test } from "bun:test";

import {
  createObjectDownloadRequestResponseSchema,
  listObjectDownloadRequestsResponseSchema,
  workerLeaseObjectDownloadRequestResponseSchema,
} from "../../../src/validation/object.ts";

const availableFileId = "70000000-0000-4000-8000-000000000001";

const downloadRequest = {
  id: "60000000-0000-4000-8000-000000000001",
  object_id: "OBJ-20260101-SCHEMA1",
  available_file_id: availableFileId,
  requested_by: "10000000-0000-4000-8000-000000000001",
  artifact_kind: "pdf",
  variant: null,
  status: "PENDING",
  failure_reason: null,
  failure_details: null,
  created_at: "2026-01-01T00:00:00.000Z",
  updated_at: "2026-01-01T00:00:00.000Z",
  completed_at: null,
};

describe("object download request response schemas", () => {
  test("accepts queued and list responses with a selected available file", () => {
    expect(createObjectDownloadRequestResponseSchema.safeParse({
      status: "queued",
      object_id: downloadRequest.object_id,
      request: downloadRequest,
    }).success).toBe(true);
    expect(listObjectDownloadRequestsResponseSchema.safeParse({
      object_id: downloadRequest.object_id,
      requests: [downloadRequest],
    }).success).toBe(true);
  });

  test("rejects null or omitted available file IDs in download request responses", () => {
    for (const request of [
      { ...downloadRequest, available_file_id: null },
      Object.fromEntries(
        Object.entries(downloadRequest).filter(([key]) => key !== "available_file_id"),
      ),
    ]) {
      expect(createObjectDownloadRequestResponseSchema.safeParse({
        status: "queued",
        object_id: downloadRequest.object_id,
        request,
      }).success).toBe(false);
      expect(listObjectDownloadRequestsResponseSchema.safeParse({
        object_id: downloadRequest.object_id,
        requests: [request],
      }).success).toBe(false);
    }
  });

  test("permits an empty worker lease but requires an ID for a populated lease", () => {
    expect(workerLeaseObjectDownloadRequestResponseSchema.safeParse({
      request: null,
    }).success).toBe(true);

    const request = {
      request_id: "60000000-0000-4000-8000-000000000001",
      lease_id: "50000000-0000-4000-8000-000000000001",
      lease_token: "lease-token",
      lease_expires_at: "2026-01-01T00:05:00.000Z",
      object_id: downloadRequest.object_id,
      tenant_id: "20000000-0000-4000-8000-000000000001",
      available_file_id: availableFileId,
      artifact_kind: "pdf",
      variant: null,
      available_file: null,
    };

    expect(workerLeaseObjectDownloadRequestResponseSchema.safeParse({ request }).success).toBe(true);
    expect(workerLeaseObjectDownloadRequestResponseSchema.safeParse({
      request: { ...request, available_file_id: null },
    }).success).toBe(false);
  });
});
