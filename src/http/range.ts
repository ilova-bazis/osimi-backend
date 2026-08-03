export interface ByteRange {
  start: number;
  end: number;
}

export type ByteRangeResult =
  | { kind: "ignore" }
  | { kind: "unsatisfiable" }
  | { kind: "range"; range: ByteRange };

export function parseSingleByteRange(
  header: string | null,
  size: number,
): ByteRangeResult {
  if (!header || header.includes(",")) {
    return { kind: "ignore" };
  }

  const match = /^bytes=(\d*)-(\d*)$/.exec(header.trim());
  if (!match || (!match[1] && !match[2])) {
    return { kind: "ignore" };
  }

  const startValue = match[1] ? BigInt(match[1]) : undefined;
  const endValue = match[2] ? BigInt(match[2]) : undefined;
  const total = BigInt(size);

  if (size === 0) {
    return { kind: "unsatisfiable" };
  }

  if (startValue === undefined) {
    if (endValue === 0n) {
      return { kind: "unsatisfiable" };
    }

    const length = endValue! > total ? total : endValue!;
    return {
      kind: "range",
      range: {
        start: Number(total - length),
        end: size - 1,
      },
    };
  }

  if (startValue >= total || (endValue !== undefined && startValue > endValue)) {
    return { kind: "unsatisfiable" };
  }

  return {
    kind: "range",
    range: {
      start: Number(startValue),
      end: Number(endValue === undefined || endValue >= total ? total - 1n : endValue),
    },
  };
}

export function ifRangeMatches(
  header: string | null,
  etag: string,
  lastModified: Date,
): boolean {
  if (!header) {
    return true;
  }

  if (header === etag) {
    return true;
  }

  if (header.startsWith("W/")) {
    return false;
  }

  const timestamp = Date.parse(header);
  if (Number.isNaN(timestamp)) {
    return false;
  }

  return Math.floor(lastModified.getTime() / 1_000) <= Math.floor(timestamp / 1_000);
}
