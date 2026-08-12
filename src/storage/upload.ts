import { createHash, randomUUID } from "node:crypto";
import { constants } from "node:fs";
import { open, link, mkdir, rm, stat } from "node:fs/promises";
import { dirname } from "node:path";
import { Readable } from "node:stream";

import { ConflictError, ValidationError } from "../http/errors.ts";

export interface UploadInspection {
  sizeBytes: number;
  checksumSha256: string;
}

export interface ImmutableUploadInspection extends UploadInspection {
  bytes?: Uint8Array;
}

export type ImmutableUploadInspectionErrorReason =
  | "verified_storage_missing"
  | "verified_storage_not_regular"
  | "verified_storage_changed_during_read"
  | "verified_storage_io_error";

export class ImmutableUploadInspectionError extends Error {
  readonly reason: ImmutableUploadInspectionErrorReason;

  constructor(reason: ImmutableUploadInspectionErrorReason, path: string, cause?: unknown) {
    super(`${reason}: '${path}'`, cause ? { cause } : undefined);
    this.name = "ImmutableUploadInspectionError";
    this.reason = reason;
  }
}

export interface UploadFileOperations {
  write(bytes: Uint8Array): Promise<number>;
  sync(): Promise<void>;
  close(): Promise<void>;
}

export interface UploadDurabilityOperations {
  createDirectory(path: string): Promise<void>;
  openExclusive(path: string): Promise<UploadFileOperations>;
  link(sourcePath: string, destinationPath: string): Promise<void>;
  inspect(path: string): Promise<UploadInspection>;
  syncDirectory(path: string): Promise<void>;
  remove(path: string): Promise<void>;
  temporaryPath(destinationPath: string): string;
}

export interface StagedImmutableUpload {
  readonly inspection: UploadInspection;
  readonly temporaryPath: string;
  publish(authoritativeExpected?: UploadInspection): Promise<void>;
  cleanup(): Promise<void>;
}

export function parseContentLength(value: string | null): number {
  if (!value || !/^(0|[1-9][0-9]*)$/.test(value)) {
    throw new ValidationError("Header 'content-length' must be a non-negative decimal integer.");
  }

  const parsed = Number(value);

  if (!Number.isSafeInteger(parsed)) {
    throw new ValidationError("Header 'content-length' must be a safe integer.");
  }

  return parsed;
}

async function inspectPath(path: string): Promise<UploadInspection> {
  const inspected = await inspectImmutableUpload({ path });
  return {
    sizeBytes: inspected.sizeBytes,
    checksumSha256: inspected.checksumSha256,
  };
}

export async function inspectImmutableUpload(params: {
  path: string;
  captureBytes?: boolean;
  maxCaptureBytes?: number;
}): Promise<ImmutableUploadInspection> {
  let input;
  try {
    input = await open(
      params.path,
      constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK,
    );
  } catch (error) {
    if (hasErrorCode(error, "ENOENT")) {
      throw new ImmutableUploadInspectionError("verified_storage_missing", params.path, error);
    }
    if (hasErrorCode(error, "ELOOP")) {
      throw new ImmutableUploadInspectionError("verified_storage_not_regular", params.path, error);
    }
    throw new ImmutableUploadInspectionError("verified_storage_io_error", params.path, error);
  }
  const checksum = createHash("sha256");
  const buffer = Buffer.allocUnsafe(64 * 1024);
  const captured: Buffer[] = [];
  let sizeBytes = 0;

  try {
    const before = await input.stat({ bigint: true });
    if (!before.isFile()) {
      throw new ImmutableUploadInspectionError("verified_storage_not_regular", params.path);
    }
    while (true) {
      const { bytesRead } = await input.read(buffer, 0, buffer.byteLength, null);
      if (bytesRead === 0) break;
      sizeBytes += bytesRead;
      checksum.update(buffer.subarray(0, bytesRead));
      if (
        params.captureBytes &&
        sizeBytes <= (params.maxCaptureBytes ?? Number.MAX_SAFE_INTEGER)
      ) {
        captured.push(Buffer.from(buffer.subarray(0, bytesRead)));
      }
    }
    const after = await input.stat({ bigint: true });
    if (
      before.dev !== after.dev || before.ino !== after.ino || before.size !== after.size ||
      before.mtimeNs !== after.mtimeNs || before.ctimeNs !== after.ctimeNs
    ) {
      throw new ImmutableUploadInspectionError("verified_storage_changed_during_read", params.path);
    }
  } catch (error) {
    if (error instanceof ImmutableUploadInspectionError) throw error;
    throw new ImmutableUploadInspectionError("verified_storage_io_error", params.path, error);
  } finally {
    await input.close();
  }

  return {
    sizeBytes,
    checksumSha256: checksum.digest("hex"),
    bytes: params.captureBytes && sizeBytes <= (params.maxCaptureBytes ?? Number.MAX_SAFE_INTEGER)
      ? Buffer.concat(captured)
      : undefined,
  };
}

async function syncDirectoryPath(path: string): Promise<void> {
  const directory = await open(path, "r");
  try {
    await directory.sync();
  } finally {
    await directory.close();
  }
}

async function createDurableDirectory(path: string): Promise<void> {
  const missing: string[] = [];
  let current = path;
  while (true) {
    try {
      const entry = await stat(current);
      if (!entry.isDirectory()) throw new Error(`Upload directory '${current}' is not a directory.`);
      break;
    } catch (error) {
      if (!hasErrorCode(error, "ENOENT")) throw error;
      missing.push(current);
      const parent = dirname(current);
      if (parent === current) throw error;
      current = parent;
    }
  }

  for (const directory of missing.reverse()) {
    try {
      await mkdir(directory);
    } catch (error) {
      if (!hasErrorCode(error, "EEXIST")) throw error;
    }
    await syncDirectoryPath(dirname(directory));
  }
}

const defaultOperations: UploadDurabilityOperations = {
  createDirectory: createDurableDirectory,
  async openExclusive(path) {
    const file = await open(path, "wx");
    return {
      async write(bytes) {
        const { bytesWritten } = await file.write(bytes);
        return bytesWritten;
      },
      async sync() {
        await file.sync();
      },
      async close() {
        await file.close();
      },
    };
  },
  async link(sourcePath, destinationPath) {
    await link(sourcePath, destinationPath);
  },
  inspect: inspectPath,
  syncDirectory: syncDirectoryPath,
  async remove(path) {
    await rm(path, { force: true });
  },
  temporaryPath(destinationPath) {
    return `${destinationPath}.upload-${randomUUID()}.tmp`;
  },
};

function hasErrorCode(error: unknown, code: string): boolean {
  return error instanceof Error && "code" in error && error.code === code;
}

function inspectionsMatch(left: UploadInspection, right: UploadInspection): boolean {
  return left.sizeBytes === right.sizeBytes &&
    left.checksumSha256 === right.checksumSha256;
}

async function removeTemporaryUpload(
  operations: UploadDurabilityOperations,
  temporaryPath: string,
  destinationDirectory: string,
): Promise<void> {
  await operations.remove(temporaryPath);
  await operations.syncDirectory(destinationDirectory);
}

export async function stageImmutableUpload(params: {
  body: ReadableStream<Uint8Array> | null;
  destinationPath: string;
  expectedSizeBytes: number;
  maxSizeBytes: number;
  operations?: UploadDurabilityOperations;
}): Promise<StagedImmutableUpload> {
  if (!params.body) {
    throw new ValidationError("Upload body is required.");
  }

  const operations = params.operations ?? defaultOperations;
  const destinationDirectory = dirname(params.destinationPath);
  const temporaryPath = operations.temporaryPath(params.destinationPath);
  const checksum = createHash("sha256");
  let sizeBytes = 0;
  let temporaryCreated = false;
  let file: UploadFileOperations | undefined;

  await operations.createDirectory(destinationDirectory);

  try {
    file = await operations.openExclusive(temporaryPath);
    temporaryCreated = true;

    for await (const chunk of Readable.fromWeb(params.body)) {
      const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      sizeBytes += bytes.byteLength;

      if (sizeBytes > params.expectedSizeBytes || sizeBytes > params.maxSizeBytes) {
        throw new ValidationError("Upload body size exceeds signed URL constraints.");
      }

      checksum.update(bytes);
      let offset = 0;
      while (offset < bytes.byteLength) {
        const bytesWritten = await file.write(bytes.subarray(offset));
        if (bytesWritten <= 0) throw new Error("Temporary upload write made no progress.");
        offset += bytesWritten;
      }
    }

    if (sizeBytes !== params.expectedSizeBytes) {
      throw new ValidationError("Upload body size does not match signed URL constraints.");
    }

    await file.sync();
    await file.close();
    file = undefined;
  } catch (error) {
    try {
      await file?.close();
    } catch {
      // Preserve the upload failure; residue is recoverable operationally.
    }
    if (temporaryCreated) {
      try {
        await removeTemporaryUpload(operations, temporaryPath, destinationDirectory);
      } catch {
        // Preserve the upload failure; residue is recoverable operationally.
      }
    }
    throw error;
  }

  const inspection = {
    sizeBytes,
    checksumSha256: checksum.digest("hex"),
  };
  let cleaned = false;

  return {
    inspection,
    temporaryPath,
    async publish(authoritativeExpected) {
      if (cleaned) throw new Error("Staged upload has already been cleaned up.");
      if (authoritativeExpected && !inspectionsMatch(authoritativeExpected, inspection)) {
        throw new ConflictError("Upload body does not match the accepted artifact checkpoint.");
      }

      try {
        await operations.link(temporaryPath, params.destinationPath);
      } catch (error) {
        if (!hasErrorCode(error, "EEXIST")) throw error;

        let existing: UploadInspection;
        try {
          existing = await operations.inspect(params.destinationPath);
        } catch (inspectError) {
          if (
            hasErrorCode(inspectError, "ENOENT") ||
            inspectError instanceof ImmutableUploadInspectionError &&
              inspectError.reason === "verified_storage_missing"
          ) {
            await operations.link(temporaryPath, params.destinationPath);
          } else {
            throw inspectError;
          }
          await operations.syncDirectory(destinationDirectory);
          return;
        }

        if (!inspectionsMatch(existing, inspection)) {
          throw new ConflictError("Upload token has already been used with different content.");
        }
      }

      await operations.syncDirectory(destinationDirectory);
    },
    async cleanup() {
      if (cleaned) return;
      await removeTemporaryUpload(operations, temporaryPath, destinationDirectory);
      cleaned = true;
    },
  };
}

export async function streamUploadToImmutablePath(params: {
  body: ReadableStream<Uint8Array> | null;
  destinationPath: string;
  expectedSizeBytes: number;
  maxSizeBytes: number;
}): Promise<UploadInspection> {
  const staged = await stageImmutableUpload(params);
  try {
    await staged.publish();
    return staged.inspection;
  } finally {
    try {
      await staged.cleanup();
    } catch (error) {
      console.error(
        `immutable_upload_cleanup_failed temporary_path=${staged.temporaryPath} error=${error instanceof Error ? error.message : "unexpected"}`,
      );
    }
  }
}
