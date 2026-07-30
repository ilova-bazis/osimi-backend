import { createHash, randomUUID } from "node:crypto";
import { once } from "node:events";
import { createReadStream, createWriteStream } from "node:fs";
import { link, mkdir, rm } from "node:fs/promises";
import { dirname } from "node:path";
import { Readable } from "node:stream";

import { ConflictError, ValidationError } from "../http/errors.ts";

export interface UploadInspection {
  sizeBytes: number;
  checksumSha256: string;
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
  const checksum = createHash("sha256");
  let sizeBytes = 0;

  for await (const chunk of createReadStream(path)) {
    const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    sizeBytes += bytes.byteLength;
    checksum.update(bytes);
  }

  return {
    sizeBytes,
    checksumSha256: checksum.digest("hex"),
  };
}

export async function streamUploadToImmutablePath(params: {
  body: ReadableStream<Uint8Array> | null;
  destinationPath: string;
  expectedSizeBytes: number;
  maxSizeBytes: number;
}): Promise<UploadInspection> {
  if (!params.body) {
    throw new ValidationError("Upload body is required.");
  }

  const temporaryPath = `${params.destinationPath}.upload-${randomUUID()}.tmp`;
  const checksum = createHash("sha256");
  let sizeBytes = 0;
  let completed = false;

  await mkdir(dirname(params.destinationPath), { recursive: true });
  const output = createWriteStream(temporaryPath, { flags: "wx" });

  try {
    for await (const chunk of Readable.fromWeb(params.body)) {
      const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      sizeBytes += bytes.byteLength;

      if (sizeBytes > params.expectedSizeBytes || sizeBytes > params.maxSizeBytes) {
        throw new ValidationError("Upload body size exceeds signed URL constraints.");
      }

      checksum.update(bytes);

      if (!output.write(bytes)) {
        await once(output, "drain");
      }
    }

    await new Promise<void>((resolve, reject) => {
      output.end((error?: Error | null) => error ? reject(error) : resolve());
    });

    if (sizeBytes !== params.expectedSizeBytes) {
      throw new ValidationError("Upload body size does not match signed URL constraints.");
    }

    const inspection = {
      sizeBytes,
      checksumSha256: checksum.digest("hex"),
    };

    try {
      await link(temporaryPath, params.destinationPath);
    } catch (error) {
      if (!(error instanceof Error) || !("code" in error) || error.code !== "EEXIST") {
        throw error;
      }

      const existing = await inspectPath(params.destinationPath);

      if (
        existing.sizeBytes !== inspection.sizeBytes ||
        existing.checksumSha256 !== inspection.checksumSha256
      ) {
        throw new ConflictError("Upload token has already been used with different content.");
      }
    }

    completed = true;
    return inspection;
  } finally {
    if (!completed) {
      output.destroy();
    }
    await rm(temporaryPath, { force: true });
  }
}
