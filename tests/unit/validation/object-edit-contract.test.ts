import { describe, expect, test } from "bun:test";

import {
  objectEditResponseSchema,
  parsePatchObjectMetadataBody,
  parsePutDocumentCurationBody,
  parseSubmitObjectCurationBody,
  patchObjectMetadataResponseSchema,
  putDocumentCurationResponseSchema,
  releaseObjectEditLockResponseSchema,
  submitObjectCurationResponseSchema,
} from "../../../src/validation/object.ts";

const fixture = JSON.parse(
  await Bun.file(
    new URL("../../../docs/object-edit-contract-fixtures.json", import.meta.url),
  ).text(),
) as {
  contract_version: number;
  get_edit: unknown;
  save_metadata: { request: unknown; response: unknown };
  save_document_curation: { request: unknown; response: unknown };
  submit_curation: { request: unknown; response: unknown };
  release_lock: { response: unknown };
  errors: Record<string, { status: number; body: { error: { code: string } } }>;
};

describe("object edit contract fixtures", () => {
  test("validate the backend-owned request and response contract", () => {
    expect(fixture.contract_version).toBe(1);
    expect(objectEditResponseSchema.safeParse(fixture.get_edit).success).toBe(true);
    expect(() => parsePatchObjectMetadataBody(fixture.save_metadata.request)).not.toThrow();
    expect(patchObjectMetadataResponseSchema.safeParse(fixture.save_metadata.response).success).toBe(true);
    expect(() => parsePutDocumentCurationBody(fixture.save_document_curation.request)).not.toThrow();
    expect(putDocumentCurationResponseSchema.safeParse(fixture.save_document_curation.response).success).toBe(true);
    expect(() => parseSubmitObjectCurationBody(fixture.submit_curation.request)).not.toThrow();
    expect(submitObjectCurationResponseSchema.safeParse(fixture.submit_curation.response).success).toBe(true);
    expect(releaseObjectEditLockResponseSchema.safeParse(fixture.release_lock.response).success).toBe(true);
  });

  test("documents the stable editor error codes and statuses", () => {
    expect(fixture.errors.revision_conflict).toMatchObject({ status: 409, body: { error: { code: "REVISION_CONFLICT" } } });
    expect(fixture.errors.validation_failed).toMatchObject({ status: 422, body: { error: { code: "VALIDATION_FAILED" } } });
    expect(fixture.errors.locked).toMatchObject({ status: 423, body: { error: { code: "LOCKED" } } });
  });
});
