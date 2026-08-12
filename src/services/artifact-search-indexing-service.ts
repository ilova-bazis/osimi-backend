import { resolveMaxArtifactSearchTextBytes } from "../runtime/config.ts";
import {
  upsertArtifactSearchProvenance,
  upsertArtifactSearchText,
} from "../repos/object-artifact-search-document-repo.ts";
import type { ObjectArtifactRecord } from "../repos/object-repo.ts";
import type { SqlExecutor } from "../db/client.ts";
import { resolveStagingPath } from "../storage/staging.ts";
import { parseMediaType } from "../http/media-type.ts";

export type ArtifactSearchTextSkipReason =
  | "unsupported"
  | "over_limit"
  | "unavailable"
  | "malformed_utf8"
  | "empty";

export type ArtifactSearchTextResult =
  | { outcome: "indexed"; text: string }
  | { outcome: "skipped"; reason: ArtifactSearchTextSkipReason };

export function classifyArtifactSearchText(params: {
  kind: ObjectArtifactRecord["kind"];
  contentType: string;
  sizeBytes: number;
  maxBytes: number;
}): { outcome: "eligible" } | { outcome: "skipped"; reason: "unsupported" | "over_limit" } {
  const mediaType = parseMediaType(params.contentType);
  if (
    (params.kind !== "ocr_text" && params.kind !== "transcript") ||
    !mediaType?.essence.startsWith("text/")
  ) {
    return { outcome: "skipped", reason: "unsupported" };
  }

  if (params.sizeBytes > params.maxBytes) {
    return { outcome: "skipped", reason: "over_limit" };
  }

  return { outcome: "eligible" };
}

export function decodeArtifactSearchText(bytes: Uint8Array): ArtifactSearchTextResult {
  let text: string;
  try {
    text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes);
  } catch (error) {
    if (error instanceof TypeError) {
      return { outcome: "skipped", reason: "malformed_utf8" };
    }
    throw error;
  }

  if (text.trim().length === 0) {
    return { outcome: "skipped", reason: "empty" };
  }

  return { outcome: "indexed", text };
}

export async function prepareArtifactSearchText(
  artifact: Pick<ObjectArtifactRecord, "kind" | "contentType" | "sizeBytes" | "storageKey">,
): Promise<ArtifactSearchTextResult> {
  const eligibility = classifyArtifactSearchText({
    kind: artifact.kind,
    contentType: artifact.contentType,
    sizeBytes: artifact.sizeBytes,
    maxBytes: resolveMaxArtifactSearchTextBytes(),
  });
  if (eligibility.outcome === "skipped") return eligibility;

  const file = Bun.file(resolveStagingPath(artifact.storageKey));
  if (!(await file.exists())) return { outcome: "skipped", reason: "unavailable" };
  return decodeArtifactSearchText(new Uint8Array(await file.arrayBuffer()));
}

export async function persistArtifactSearchProjection(params: {
  tenantId: string;
  objectId: string;
  availableFileId?: string;
  artifact: ObjectArtifactRecord;
  prepared: ArtifactSearchTextResult;
  executor?: SqlExecutor;
}): Promise<void> {
  if (params.availableFileId !== undefined) {
    const provenance = await upsertArtifactSearchProvenance({
      tenantId: params.tenantId,
      objectId: params.objectId,
      artifactId: params.artifact.id,
      availableFileId: params.availableFileId,
      executor: params.executor,
    });
    if (!provenance) {
      throw new Error(`Could not persist search provenance for artifact '${params.artifact.id}'.`);
    }
  }

  if (params.prepared.outcome === "skipped") return;
  const indexed = await upsertArtifactSearchText({
    tenantId: params.tenantId,
    objectId: params.objectId,
    artifactId: params.artifact.id,
    availableFileId: params.availableFileId,
    textContent: params.prepared.text,
    indexedAt: new Date(),
    executor: params.executor,
  });
  if (!indexed) {
    throw new Error(`Could not persist search text for artifact '${params.artifact.id}'.`);
  }
}

export async function indexMaterializedArtifact(params: {
  tenantId: string;
  objectId: string;
  availableFileId?: string;
  artifact: ObjectArtifactRecord;
}): Promise<ArtifactSearchTextResult> {
  const prepared = await prepareArtifactSearchText(params.artifact);
  await persistArtifactSearchProjection({ ...params, prepared });
  return prepared;
}
