const TEST_DATABASE_URL_ENV = "TEST_DATABASE_URL";
const UNSAFE_DATABASE_OVERRIDE_ENV = "ALLOW_UNSAFE_TEST_DATABASE_NAME";

export function validateTestDatabaseUrl(
  rawValue: string | undefined,
  allowUnsafeDatabaseName = false,
): string {
  const value = rawValue?.trim();

  if (!value) {
    throw new Error(
      `${TEST_DATABASE_URL_ENV} is required for integration tests. DATABASE_URL is never used to prevent accidental production access.`,
    );
  }

  let url: URL;

  try {
    url = new URL(value);
  } catch {
    throw new Error(`${TEST_DATABASE_URL_ENV} must be a valid PostgreSQL URL.`);
  }

  if (url.protocol !== "postgres:" && url.protocol !== "postgresql:") {
    throw new Error(`${TEST_DATABASE_URL_ENV} must use the postgres or postgresql protocol.`);
  }

  const pathSegments = url.pathname.split("/").filter(Boolean);
  const databaseName = pathSegments[0] ? decodeURIComponent(pathSegments[0]) : "";

  if (!databaseName || pathSegments.length !== 1) {
    throw new Error(`${TEST_DATABASE_URL_ENV} must identify exactly one database.`);
  }

  if (!allowUnsafeDatabaseName && !databaseName.toLowerCase().includes("test")) {
    throw new Error(
      `${TEST_DATABASE_URL_ENV} database name '${databaseName}' must contain 'test'. Set ${UNSAFE_DATABASE_OVERRIDE_ENV}=true only for a known disposable database.`,
    );
  }

  return value;
}

export function getTestDatabaseUrl(): string {
  return validateTestDatabaseUrl(
    process.env[TEST_DATABASE_URL_ENV],
    process.env[UNSAFE_DATABASE_OVERRIDE_ENV] === "true",
  );
}
