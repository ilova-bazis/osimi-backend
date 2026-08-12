import { createHash } from "node:crypto";
import { mkdtemp, rm, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, test } from "bun:test";

import {
  inspectImmutableUpload,
  stageImmutableUpload,
  type UploadDurabilityOperations,
  type UploadInspection,
} from "../../../src/storage/upload.ts";

function body(value: string): ReadableStream<Uint8Array> {
  return new ReadableStream({
    start(controller) {
      controller.enqueue(Buffer.from(value));
      controller.close();
    },
  });
}

function inspection(value: string): UploadInspection {
  return {
    sizeBytes: Buffer.byteLength(value),
    checksumSha256: createHash("sha256").update(value).digest("hex"),
  };
}

function errorWithCode(code: string): Error {
  return Object.assign(new Error(code), { code });
}

class RecordingOperations implements UploadDurabilityOperations {
  readonly events: string[] = [];
  destination: UploadInspection | undefined;
  failFileSync = false;
  failDirectorySync = false;
  temporaryExists = false;
  private temporaryBytes: Buffer[] = [];

  async createDirectory(): Promise<void> {
    this.events.push("mkdir");
  }

  async openExclusive(): Promise<{
    write(bytes: Uint8Array): Promise<number>;
    sync(): Promise<void>;
    close(): Promise<void>;
  }> {
    this.events.push("open-wx");
    this.temporaryExists = true;
    return {
      write: async (bytes) => {
        this.events.push("write");
        this.temporaryBytes.push(Buffer.from(bytes));
        return bytes.byteLength;
      },
      sync: async () => {
        this.events.push("file-sync");
        if (this.failFileSync) throw new Error("file sync failed");
      },
      close: async () => {
        this.events.push("close");
      },
    };
  }

  async link(): Promise<void> {
    this.events.push("link");
    if (this.destination) throw errorWithCode("EEXIST");
    this.destination = inspection(Buffer.concat(this.temporaryBytes).toString());
  }

  async inspect(): Promise<UploadInspection> {
    this.events.push("inspect");
    if (!this.destination) throw errorWithCode("ENOENT");
    return this.destination;
  }

  async syncDirectory(): Promise<void> {
    this.events.push("directory-sync");
    if (this.failDirectorySync) throw new Error("directory sync failed");
  }

  async remove(): Promise<void> {
    this.events.push("remove");
    this.temporaryExists = false;
  }

  temporaryPath(): string {
    return "/store/item.upload-test.tmp";
  }

  getDestination(): UploadInspection | undefined {
    return this.destination;
  }
}

async function stage(operations: RecordingOperations, value = "payload") {
  return stageImmutableUpload({
    body: body(value),
    destinationPath: "/store/item",
    expectedSizeBytes: Buffer.byteLength(value),
    maxSizeBytes: 1024,
    operations,
  });
}

describe("staged immutable uploads", () => {
  test("syncs file bytes before linking and the directory before resolving", async () => {
    const operations = new RecordingOperations();
    const staged = await stage(operations);

    await staged.publish();

    expect(operations.events).toEqual([
      "mkdir",
      "open-wx",
      "write",
      "file-sync",
      "close",
      "link",
      "directory-sync",
    ]);
  });

  test("fails closed and cleans up when file sync fails", async () => {
    const operations = new RecordingOperations();
    operations.failFileSync = true;

    await expect(stage(operations)).rejects.toThrow("file sync failed");
    expect(operations.events).toEqual([
      "mkdir",
      "open-wx",
      "write",
      "file-sync",
      "close",
      "remove",
      "directory-sync",
    ]);
    expect(operations.destination).toBeUndefined();
    expect(operations.temporaryExists).toBe(false);
  });

  test("does not resolve publication when directory sync fails", async () => {
    const operations = new RecordingOperations();
    const staged = await stage(operations);
    operations.failDirectorySync = true;

    await expect(staged.publish()).rejects.toThrow("directory sync failed");
    expect(operations.destination).toEqual(inspection("payload"));
    expect(operations.temporaryExists).toBe(true);
    operations.failDirectorySync = false;
    await staged.cleanup();
  });

  test("accepts an exact existing destination", async () => {
    const operations = new RecordingOperations();
    operations.destination = inspection("payload");
    const staged = await stage(operations);

    await staged.publish();

    expect(operations.events.slice(-3)).toEqual(["link", "inspect", "directory-sync"]);
  });

  test("rejects a different existing destination", async () => {
    const operations = new RecordingOperations();
    operations.destination = inspection("different");
    const staged = await stage(operations);

    await expect(staged.publish()).rejects.toThrow(
      "Upload token has already been used with different content.",
    );
    expect(operations.events).not.toContain("directory-sync");
  });

  test("rejects an authoritative checksum before linking", async () => {
    const operations = new RecordingOperations();
    operations.destination = inspection("existing");
    const staged = await stage(operations);

    await expect(staged.publish({
      sizeBytes: staged.inspection.sizeBytes,
      checksumSha256: "0".repeat(64),
    })).rejects.toThrow("Upload body does not match the accepted artifact checkpoint.");
    await staged.cleanup();
    expect(operations.events).not.toContain("link");
    expect(operations.destination).toEqual(inspection("existing"));
    expect(operations.temporaryExists).toBe(false);
    expect(operations.events.slice(-2)).toEqual(["remove", "directory-sync"]);
  });

  test("restores the exact staged inode when the destination is missing", async () => {
    const operations = new RecordingOperations();
    const staged = await stage(operations);
    await staged.publish();
    operations.destination = undefined;

    await staged.publish();

    expect(operations.getDestination()).toEqual(staged.inspection);
    expect(operations.events.filter((event) => event === "link")).toHaveLength(2);
  });

  test("cleanup removes only the temporary path and is idempotent", async () => {
    const operations = new RecordingOperations();
    const staged = await stage(operations);
    await staged.publish();

    await staged.cleanup();
    await staged.cleanup();

    expect(operations.events.filter((event) => event === "remove")).toHaveLength(1);
    expect(operations.destination).toEqual(staged.inspection);
    expect(operations.temporaryExists).toBe(false);
  });
});

describe("immutable upload inspection", () => {
  test("hashes a regular file and captures its exact bytes", async () => {
    const root = await mkdtemp(join(tmpdir(), "osimi-upload-inspection-"));
    const path = join(root, "artifact.txt");
    try {
      await Bun.write(path, "verified bytes");
      const inspected = await inspectImmutableUpload({
        path,
        captureBytes: true,
        maxCaptureBytes: 1024,
      });
      expect(inspected).toEqual({
        ...inspection("verified bytes"),
        bytes: Buffer.from("verified bytes"),
      });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  test("rejects missing and non-regular storage with stable reasons", async () => {
    const root = await mkdtemp(join(tmpdir(), "osimi-upload-inspection-"));
    try {
      await expect(inspectImmutableUpload({ path: join(root, "missing") }))
        .rejects.toThrow("verified_storage_missing");
      await expect(inspectImmutableUpload({ path: root }))
        .rejects.toThrow("verified_storage_not_regular");
      const linkPath = join(root, "link");
      await symlink(root, linkPath);
      await expect(inspectImmutableUpload({ path: linkPath }))
        .rejects.toThrow("verified_storage_not_regular");
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});
