import { NotFoundError, ValidationError } from "../http/errors.ts";
import {
  findObjectByIdUnscoped,
} from "../repos/object-repo.ts";
import {
  upsertObjectTextManifest,
  type ObjectTextManifestMediaType,
} from "../repos/object-text-manifest-repo.ts";
import type { ReplaceObjectTextManifestBody } from "../validation/object.ts";

export async function replaceObjectTextManifest(params: {
  objectId: string;
  body: ReplaceObjectTextManifestBody;
}): Promise<{ object_id: string; status: "ok" }> {
  const object = await findObjectByIdUnscoped({ objectId: params.objectId });

  if (!object) {
    throw new NotFoundError(`Object '${params.objectId}' was not found.`);
  }

  if (params.body.object_text_manifest.object_id !== params.objectId) {
    throw new ValidationError(
      "object_text_manifest.object_id must match path object_id.",
    );
  }

  const manifest = params.body.object_text_manifest;

  await upsertObjectTextManifest({
    objectId: params.objectId,
    tenantId: object.tenantId,
    mediaType: manifest.media_type as ObjectTextManifestMediaType,
    projectionVersion: manifest.projection_version,
    generatedAt: new Date(manifest.generated_at),
    payload: {
      object_id: manifest.object_id,
      media_type: manifest.media_type,
      projection_version: manifest.projection_version,
      generated_at: manifest.generated_at,
      text_artifacts: manifest.text_artifacts.map((artifact) => ({
        kind: artifact.kind,
        version: artifact.version,
        is_active: artifact.is_active,
        ...(artifact.metadata ? { metadata: artifact.metadata } : {}),
      })),
    },
  });

  return {
    object_id: params.objectId,
    status: "ok",
  };
}
