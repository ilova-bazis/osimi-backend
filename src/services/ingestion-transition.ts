import { NotFoundError } from "../http/errors.ts";
import {
  findIngestionById,
  updateIngestionStatus,
} from "../repos/ingestion-repo.ts";
import type { IngestionStatus } from "../domain/ingestions/state-machine.ts";

export async function applyStatusTransition(params: {
  tenantId: string;
  ingestionId: string;
  fromStatus: IngestionStatus;
  toStatus: IngestionStatus;
}): Promise<IngestionStatus> {
  if (params.fromStatus === params.toStatus) {
    return params.fromStatus;
  }

  const updated = await updateIngestionStatus({
    ingestionId: params.ingestionId,
    tenantId: params.tenantId,
    fromStatus: params.fromStatus,
    toStatus: params.toStatus,
  });

  if (updated) {
    return updated.status;
  }

  const latest = await findIngestionById(params.tenantId, params.ingestionId);

  if (!latest) {
    throw new NotFoundError(`Ingestion '${params.ingestionId}' was not found.`);
  }

  return latest.status;
}
