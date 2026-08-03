import { withSchemaClient } from "../db/client.ts";
import type { SqlExecutor } from "../db/client.ts";
import { ConflictError, NotFoundError } from "../http/errors.ts";
import {
  findIngestionByIdForUpdate,
  type IngestionRecord,
} from "../repos/ingestion-repo.ts";

export async function withMutableIngestion<T>(params: {
  tenantId: string;
  ingestionId: string;
  handler: (ingestion: IngestionRecord, executor: SqlExecutor) => Promise<T>;
  executor?: SqlExecutor;
}): Promise<T> {
  type Result = { value: T } | { error: ConflictError | NotFoundError };
  const run = async (executor: SqlExecutor): Promise<Result> => {
    const ingestion = await findIngestionByIdForUpdate(
      params.tenantId,
      params.ingestionId,
      executor,
    );
    if (!ingestion) {
      return { error: new NotFoundError(`Ingestion '${params.ingestionId}' was not found.`) };
    }

    if (ingestion.stagingPurgeStartedAt) {
      return {
        error: new ConflictError("Ingestion staging has been scheduled for purge.", {
          ingestion_id: ingestion.id,
        }),
      };
    }

    if (
      ingestion.status !== "DRAFT" &&
      ingestion.status !== "UPLOADING" &&
      ingestion.status !== "CANCELED"
    ) {
      return {
        error: new ConflictError("Ingestion manifest is frozen.", {
          ingestion_id: ingestion.id,
          status: ingestion.status,
        }),
      };
    }

    const activeLeaseRows = await executor<Array<{ exists: boolean }>>`
      SELECT EXISTS(
        SELECT 1
        FROM ingestion_leases
        WHERE ingestion_id = ${ingestion.id}
          AND released_at IS NULL
          AND lease_expires_at > now()
      ) AS exists
    `;
    if (activeLeaseRows[0]?.exists) {
      return {
        error: new ConflictError("Ingestion cannot be modified after lease assignment.", {
          ingestion_id: ingestion.id,
        }),
      };
    }

    return { value: await params.handler(ingestion, executor) };
  };

  let result: Result;
  if (params.executor) {
    result = await run(params.executor);
  } else {
    result = await withSchemaClient((sql) => sql.begin(run));
  }

  if ("error" in result) {
    throw result.error;
  }

  return result.value;
}
