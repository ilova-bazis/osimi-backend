import { InternalServerError } from "../http/errors.ts";

const MAX_SAFE_INTEGER_BIGINT = BigInt(Number.MAX_SAFE_INTEGER);

export type DbInt = number | string | bigint;

function toSafeBigInt(value: DbInt, fieldName: string): bigint {
  if (typeof value === "bigint") {
    return value;
  }

  if (typeof value === "number") {
    if (!Number.isInteger(value) || !Number.isSafeInteger(value)) {
      throw new InternalServerError(
        `Field '${fieldName}' is out of safe integer range.`,
      );
    }

    return BigInt(value);
  }

  if (typeof value === "string") {
    if (!/^-?\d+$/.test(value)) {
      throw new InternalServerError(
        `Field '${fieldName}' must be an integer value.`,
      );
    }

    try {
      return BigInt(value);
    } catch {
      throw new InternalServerError(
        `Field '${fieldName}' must be a valid integer value.`,
      );
    }
  }

  throw new InternalServerError(`Field '${fieldName}' has unsupported type.`);
}

export function toSafeNumberFromDbInt(value: DbInt, fieldName: string): number {
  const asBigInt = toSafeBigInt(value, fieldName);

  if (
    asBigInt > MAX_SAFE_INTEGER_BIGINT ||
    asBigInt < -MAX_SAFE_INTEGER_BIGINT
  ) {
    throw new InternalServerError(
      `Field '${fieldName}' is out of safe integer range.`,
    );
  }

  return Number(asBigInt);
}

export function toNullableSafeNumberFromDbInt(
  value: DbInt | null,
  fieldName: string,
): number | null {
  if (value === null) {
    return null;
  }

  return toSafeNumberFromDbInt(value, fieldName);
}
