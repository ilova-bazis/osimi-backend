import { afterAll, beforeAll, beforeEach, describe, expect, test } from "bun:test";
import { sql as sqlIdentifier } from "bun";

import { createSqlClient } from "../../../src/db/client.ts";
import { runMigrations } from "../../../src/db/migrate.ts";
import { closeDatabaseClients } from "../../../src/db/runtime.ts";
import {
  findArtifactSearchDocument,
  listArtifactSearchBackfillCandidates,
  upsertArtifactSearchProvenance,
  upsertArtifactSearchText,
} from "../../../src/repos/object-artifact-search-document-repo.ts";
import { runWithRuntimeConfig } from "../../../src/runtime/config.ts";
import { TEST_DATABASE_URL } from "../test-database.ts";

const tenantOneId = "00000000-0000-0000-0000-000000000001";
const tenantTwoId = "00000000-0000-0000-0000-000000000002";
const objectOneId = "OBJ-20260805-SEARCH1";
const objectTwoId = "OBJ-20260805-SEARCH2";
const artifactOneId = "10000000-0000-0000-0000-000000000001";
const artifactTwoId = "10000000-0000-0000-0000-000000000002";
const availableFileOneId = "20000000-0000-0000-0000-000000000001";
const availableFileTwoId = "20000000-0000-0000-0000-000000000002";

describe("object artifact search document repository", () => {
  let schema = "";

  function inTestSchema<T>(callback: () => T): T {
    return runWithRuntimeConfig(
      { databaseUrl: TEST_DATABASE_URL, dbSchema: schema },
      callback,
    );
  }

  beforeAll(async () => {
    schema = `artifact_search_${Date.now()}_${Math.floor(Math.random() * 100000)}`;
    await runMigrations({ databaseUrl: TEST_DATABASE_URL, schema });

    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`
        INSERT INTO tenants (id, slug, name)
        VALUES
          (${tenantOneId}, ${"search-one"}, ${"Search One"}),
          (${tenantTwoId}, ${"search-two"}, ${"Search Two"})
      `;
      await sql`
        INSERT INTO objects (object_id, tenant_id, title)
        VALUES
          (${objectOneId}, ${tenantOneId}, ${"First object"}),
          (${objectTwoId}, ${tenantTwoId}, ${"Second object"})
      `;
      await sql`
        INSERT INTO object_artifacts (
          id, object_id, kind, storage_key, content_type, size_bytes
        )
        VALUES
          (${artifactOneId}, ${objectOneId}, ${"ocr_text"}::artifact_kind, ${"search/one.txt"}, ${"text/plain"}, 10),
          (${artifactTwoId}, ${objectTwoId}, ${"ocr_text"}::artifact_kind, ${"search/two.txt"}, ${"text/plain"}, 10)
      `;
      await sql`
        INSERT INTO object_available_files (
          id, object_id, tenant_id, archive_file_key, artifact_kind, display_name
        )
        VALUES
          (${availableFileOneId}, ${objectOneId}, ${tenantOneId}, ${"available/one.txt"}, ${"ocr_text"}::artifact_kind, ${"one.txt"}),
          (${availableFileTwoId}, ${objectTwoId}, ${tenantTwoId}, ${"available/two.txt"}, ${"ocr_text"}::artifact_kind, ${"two.txt"})
      `;
    } finally {
      await sql.close();
    }
  });

  beforeEach(async () => {
    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`DELETE FROM ${sqlIdentifier(schema)}.object_artifact_search_documents`;
    } finally {
      await sql.close();
    }
  });

  afterAll(async () => {
    if (!schema) return;

    await closeDatabaseClients({ timeoutMs: 1_000 });
    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`DROP SCHEMA IF EXISTS ${sqlIdentifier(schema)} CASCADE`;
    } finally {
      await sql.close();
    }
  });

  test("upserts text idempotently and provenance-only updates preserve text", async () => {
    const firstIndexedAt = new Date("2026-08-05T10:00:00.000Z");
    const secondIndexedAt = new Date("2026-08-05T11:00:00.000Z");

    const first = await inTestSchema(() =>
      upsertArtifactSearchText({
        tenantId: tenantOneId,
        objectId: objectOneId,
        artifactId: artifactOneId,
        textContent: "First extracted text",
        indexedAt: firstIndexedAt,
      }),
    );
    expect(first?.availableFileId).toBeNull();
    expect(first?.textContent).toBe("First extracted text");
    expect(first?.indexedAt).toEqual(firstIndexedAt);

    const withProvenance = await inTestSchema(() =>
      upsertArtifactSearchProvenance({
        tenantId: tenantOneId,
        objectId: objectOneId,
        artifactId: artifactOneId,
        availableFileId: availableFileOneId,
      }),
    );
    expect(withProvenance?.availableFileId).toBe(availableFileOneId);
    expect(withProvenance?.textContent).toBe("First extracted text");
    expect(withProvenance?.indexedAt).toEqual(firstIndexedAt);

    const updated = await inTestSchema(() =>
      upsertArtifactSearchText({
        tenantId: tenantOneId,
        objectId: objectOneId,
        artifactId: artifactOneId,
        textContent: "Replacement extracted text",
        indexedAt: secondIndexedAt,
      }),
    );
    expect(updated?.availableFileId).toBe(availableFileOneId);
    expect(updated?.textContent).toBe("Replacement extracted text");
    expect(updated?.indexedAt).toEqual(secondIndexedAt);
  });

  test("lists missing OCR text across tenants in stable keyset batches", async () => {
    const firstBatch = await inTestSchema(() =>
      listArtifactSearchBackfillCandidates({ limit: 1 }),
    );
    expect(firstBatch).toHaveLength(1);
    expect(firstBatch[0]?.artifact.id).toBe(artifactOneId);
    expect(firstBatch[0]?.tenantId).toBe(tenantOneId);

    const secondBatch = await inTestSchema(() =>
      listArtifactSearchBackfillCandidates({
        afterArtifactId: firstBatch[0]!.artifact.id,
        limit: 1,
      }),
    );
    expect(secondBatch).toHaveLength(1);
    expect(secondBatch[0]?.artifact.id).toBe(artifactTwoId);
    expect(secondBatch[0]?.tenantId).toBe(tenantTwoId);

    await inTestSchema(() =>
      upsertArtifactSearchText({
        tenantId: tenantOneId,
        objectId: objectOneId,
        artifactId: artifactOneId,
        textContent: "Already indexed",
        indexedAt: new Date(),
      }),
    );
    const rerun = await inTestSchema(() =>
      listArtifactSearchBackfillCandidates({ limit: 10 }),
    );
    expect(rerun.map((item) => item.artifact.id)).toEqual([artifactTwoId]);
  });

  test("scopes writes and lookup through the artifact object and tenant", async () => {
    const wrongTenantWrite = await inTestSchema(() =>
      upsertArtifactSearchText({
        tenantId: tenantTwoId,
        objectId: objectOneId,
        artifactId: artifactOneId,
        textContent: "Must not be stored",
        indexedAt: new Date(),
      }),
    );
    expect(wrongTenantWrite).toBeUndefined();

    const wrongObjectProvenance = await inTestSchema(() =>
      upsertArtifactSearchProvenance({
        tenantId: tenantOneId,
        objectId: objectOneId,
        artifactId: artifactOneId,
        availableFileId: availableFileTwoId,
      }),
    );
    expect(wrongObjectProvenance).toBeUndefined();

    await inTestSchema(() =>
      upsertArtifactSearchText({
        tenantId: tenantOneId,
        objectId: objectOneId,
        artifactId: artifactOneId,
        textContent: "Scoped text",
        indexedAt: new Date(),
      }),
    );
    expect(
      await inTestSchema(() =>
        findArtifactSearchDocument({
          tenantId: tenantTwoId,
          objectId: objectOneId,
          artifactId: artifactOneId,
        }),
      ),
    ).toBeUndefined();
  });

  test("enforces content constraints and foreign-key deletion behavior", async () => {
    const constraintSql = createSqlClient(TEST_DATABASE_URL);
    try {
      await constraintSql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      let emptyTextRejected = false;
      try {
        await constraintSql`
          INSERT INTO object_artifact_search_documents (artifact_id, text_content, indexed_at)
          VALUES (${artifactOneId}, ${"   "}, now())
        `;
      } catch {
        emptyTextRejected = true;
      }
      expect(emptyTextRejected).toBe(true);

      let missingTimestampRejected = false;
      try {
        await constraintSql`
          INSERT INTO object_artifact_search_documents (artifact_id, text_content)
          VALUES (${artifactOneId}, ${"Missing timestamp"})
        `;
      } catch {
        missingTimestampRejected = true;
      }
      expect(missingTimestampRejected).toBe(true);
    } finally {
      await constraintSql.close();
    }

    await inTestSchema(() =>
      upsertArtifactSearchText({
        tenantId: tenantOneId,
        objectId: objectOneId,
        artifactId: artifactOneId,
        availableFileId: availableFileOneId,
        textContent: "Text survives provenance deletion",
        indexedAt: new Date(),
      }),
    );

    const sql = createSqlClient(TEST_DATABASE_URL);
    try {
      await sql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await sql`DELETE FROM object_available_files WHERE id = ${availableFileOneId}`;
    } finally {
      await sql.close();
    }

    const afterAvailableFileDelete = await inTestSchema(() =>
      findArtifactSearchDocument({
        tenantId: tenantOneId,
        objectId: objectOneId,
        artifactId: artifactOneId,
      }),
    );
    expect(afterAvailableFileDelete?.availableFileId).toBeNull();
    expect(afterAvailableFileDelete?.textContent).toBe(
      "Text survives provenance deletion",
    );

    const verifySql = createSqlClient(TEST_DATABASE_URL);
    try {
      await verifySql`SET search_path TO ${sqlIdentifier(schema)}, public`;
      await verifySql`DELETE FROM object_artifacts WHERE id = ${artifactOneId}`;
      const rows = await verifySql<Array<{ count: number }>>`
        SELECT COUNT(*)::int AS count
        FROM object_artifact_search_documents
        WHERE artifact_id = ${artifactOneId}
      `;
      expect(rows[0]?.count).toBe(0);
    } finally {
      await verifySql.close();
    }
  });
});
