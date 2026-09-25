const TOKEN_PATH_PREFIXES = [
  "/api/uploads/",
  "/api/archive-requests/uploads/",
  "/api/object-download-requests/uploads/",
  "/api/worker/downloads/",
] as const;

export function redactTokenPaths(pathname: string): string {
  for (const prefix of TOKEN_PATH_PREFIXES) {
    if (pathname.startsWith(prefix)) {
      return `${prefix}<redacted-token>`;
    }
  }

  return pathname;
}
