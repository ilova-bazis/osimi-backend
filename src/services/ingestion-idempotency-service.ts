import { withSchemaClient } from "../db/client.ts";
import type { SqlExecutor } from "../db/client.ts";
import { AppError, ConflictError } from "../http/errors.ts";
import {
  completeIngestionIdempotencyKey,
  lockIngestionIdempotencyKey,
  releaseIngestionIdempotencyKey,
  reserveIngestionIdempotencyKey,
  type IdempotencyRecord,
} from "../repos/ingestion-idempotency-repo.ts";
import type { AuthenticatedContext } from "../auth/guards.ts";

const LOCK_SECONDS = 30;
const RETENTION_DAYS = 7;
const PROCESSING_RETRY_COUNT = 20;
const PROCESSING_RETRY_DELAY_MS = 25;

export interface IdempotentMutationResult<T> {
  body: T;
  statusCode: number;
  replayed: boolean;
}

function canonicalize(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(canonicalize);
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, child]) => [key, canonicalize(child)]),
    );
  }
  return value;
}

function fingerprint(value: unknown): string {
  return new Bun.CryptoHasher("sha256")
    .update(JSON.stringify(canonicalize(value)))
    .digest("hex");
}

function requireReplay<T>(record: IdempotencyRecord): IdempotentMutationResult<T> {
  if (record.statusCode === null || record.responseBody === null) {
    throw new ConflictError("Idempotency request is still processing.");
  }

  return {
    body: record.responseBody as T,
    statusCode: record.statusCode,
    replayed: true,
  };
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

export async function runIdempotentIngestionMutation<T>(params: {
  auth: AuthenticatedContext;
  idempotencyKey?: string;
  endpoint: string;
  request: unknown;
  statusCode: number;
  handler: (executor: SqlExecutor) => Promise<T>;
}): Promise<IdempotentMutationResult<T>> {
  if (!params.idempotencyKey) {
    const body = await withSchemaClient(params.handler);
    return { body, statusCode: params.statusCode, replayed: false };
  }

  const requestFingerprint = fingerprint({
    version: 1,
    endpoint: params.endpoint,
    request: params.request,
  });
  const ownerToken = crypto.randomUUID();
  let reservation;

  for (let attempt = 0; attempt <= PROCESSING_RETRY_COUNT; attempt += 1) {
    reservation = await reserveIngestionIdempotencyKey({
      tenantId: params.auth.tenantId,
      actorUserId: params.auth.userId,
      endpoint: params.endpoint,
      idempotencyKey: params.idempotencyKey,
      requestFingerprint,
      ownerToken,
      lockSeconds: LOCK_SECONDS,
      retentionDays: RETENTION_DAYS,
    });
    if (reservation.kind === "replay") {
      return requireReplay<T>(reservation.record);
    }
    if (reservation.kind === "mismatch") {
      throw new ConflictError("Idempotency key was already used for a different request.");
    }
    if (reservation.kind === "acquired") {
      break;
    }
    if (attempt === PROCESSING_RETRY_COUNT) {
      throw new ConflictError("Idempotency request is still processing.", {
        retry_after_ms: PROCESSING_RETRY_DELAY_MS,
      });
    }
    await sleep(PROCESSING_RETRY_DELAY_MS);
  }

  if (!reservation || reservation.kind !== "acquired") {
    throw new ConflictError("Idempotency request could not be reserved.");
  }

  try {
    const result = await withSchemaClient(async (sql) => {
      return sql.begin(async (executor) => {
        const record = await lockIngestionIdempotencyKey({
          tenantId: params.auth.tenantId,
          actorUserId: params.auth.userId,
          endpoint: params.endpoint,
          idempotencyKey: params.idempotencyKey!,
          executor,
        });
        if (!record || record.ownerToken !== ownerToken || record.state !== "PROCESSING") {
          throw new ConflictError("Idempotency request ownership was lost.");
        }

        let body: T;
        try {
          body = await params.handler(executor);
        } catch (error) {
          if (error instanceof AppError) {
            return { error };
          }
          throw error;
        }
        const completed = await completeIngestionIdempotencyKey({
          recordId: record.id,
          ownerToken,
          statusCode: params.statusCode,
          responseBody: body,
          retentionDays: RETENTION_DAYS,
          executor,
        });
        if (!completed) {
          throw new ConflictError("Idempotency request ownership was lost.");
        }

        return { body };
      });
    });

    if ("error" in result) {
      throw result.error;
    }

    return { body: result.body, statusCode: params.statusCode, replayed: false };
  } catch (error) {
    await releaseIngestionIdempotencyKey({
      recordId: reservation.record.id,
      ownerToken,
    });
    throw error;
  }
}
