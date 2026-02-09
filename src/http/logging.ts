type LogFormat = "pretty" | "json";
type LogLevel = "INFO" | "WARN" | "ERROR";

const ANSI_RESET = "\u001b[0m";
const ANSI_BOLD = "\u001b[1m";
const ANSI_CYAN = "\u001b[36m";
const ANSI_BLUE = "\u001b[34m";
const ANSI_YELLOW = "\u001b[33m";
const ANSI_MAGENTA = "\u001b[35m";
const ANSI_RED = "\u001b[31m";
const ANSI_GREEN = "\u001b[32m";
const ANSI_BRIGHT_WHITE = "\u001b[97m";

export interface HttpRequestLogEntry {
  timestamp: string;
  level: LogLevel;
  event: "http_request";
  request_id: string;
  method: string;
  path: string;
  status: number;
  duration_ms: number;
  tenant_id?: string;
  user_id?: string;
  role?: string;
  idempotency_key?: string;
  error_code?: string;
  error_message?: string;
}

function parseBooleanEnv(rawValue: string | undefined, defaultValue: boolean): boolean {
  if (!rawValue) {
    return defaultValue;
  }

  const normalized = rawValue.trim().toLowerCase();

  if (["1", "true", "yes", "on"].includes(normalized)) {
    return true;
  }

  if (["0", "false", "no", "off"].includes(normalized)) {
    return false;
  }

  return defaultValue;
}

function resolveLogFormat(rawValue: string | undefined): LogFormat {
  const normalized = rawValue?.trim().toLowerCase();
  if (normalized === "json") {
    return "json";
  }

  return "pretty";
}

function useColorOutput(): boolean {
  if (process.env.NO_COLOR !== undefined) {
    return false;
  }

  const override = process.env.LOG_COLOR?.trim().toLowerCase();
  if (override && ["1", "true", "yes", "on"].includes(override)) {
    return true;
  }

  if (override && ["0", "false", "no", "off"].includes(override)) {
    return false;
  }

  return process.stdout.isTTY;
}

function colorize(value: string, ansiColor: string, enabled: boolean): string {
  if (!enabled) {
    return value;
  }

  return `${ansiColor}${value}${ANSI_RESET}`;
}

function colorizeMethod(method: string, enabled: boolean): string {
  const upper = method.toUpperCase();

  switch (upper) {
    case "GET":
      return colorize(upper, ANSI_CYAN, enabled);
    case "POST":
      return colorize(upper, ANSI_BLUE, enabled);
    case "PUT":
      return colorize(upper, ANSI_YELLOW, enabled);
    case "PATCH":
      return colorize(upper, ANSI_MAGENTA, enabled);
    case "DELETE":
      return colorize(upper, ANSI_RED, enabled);
    default:
      return colorize(upper, ANSI_BRIGHT_WHITE, enabled);
  }
}

function colorizePath(path: string, enabled: boolean): string {
  return colorize(path, `${ANSI_BOLD}${ANSI_BRIGHT_WHITE}`, enabled);
}

function resultLabel(status: number): "SUCCESS" | "FAIL" {
  return status >= 400 ? "FAIL" : "SUCCESS";
}

function colorizeResult(status: number, enabled: boolean): string {
  const result = resultLabel(status);
  return colorize(result, result === "SUCCESS" ? ANSI_GREEN : ANSI_RED, enabled);
}

function formatPrettyValue(value: string | number): string {
  if (typeof value === "number") {
    return String(value);
  }

  if (value.length === 0) {
    return '""';
  }

  if (/\s/.test(value) || value.includes('"')) {
    return JSON.stringify(value);
  }

  return value;
}

function formatPretty(entry: HttpRequestLogEntry, colorsEnabled: boolean): string {
  const parts: string[] = [
    entry.timestamp,
    entry.level,
    entry.event,
    `request_id=${formatPrettyValue(entry.request_id)}`,
    `method=${formatPrettyValue(colorizeMethod(entry.method, colorsEnabled))}`,
    `path=${formatPrettyValue(colorizePath(entry.path, colorsEnabled))}`,
    `result=${formatPrettyValue(colorizeResult(entry.status, colorsEnabled))}`,
    `status=${formatPrettyValue(entry.status)}`,
    `duration_ms=${formatPrettyValue(entry.duration_ms)}`,
  ];

  if (entry.tenant_id) {
    parts.push(`tenant_id=${formatPrettyValue(entry.tenant_id)}`);
  }

  if (entry.user_id) {
    parts.push(`user_id=${formatPrettyValue(entry.user_id)}`);
  }

  if (entry.role) {
    parts.push(`role=${formatPrettyValue(entry.role)}`);
  }

  if (entry.idempotency_key) {
    parts.push(`idempotency_key=${formatPrettyValue(entry.idempotency_key)}`);
  }

  if (entry.error_code) {
    parts.push(`error_code=${formatPrettyValue(entry.error_code)}`);
  }

  if (entry.error_message) {
    parts.push(`error_message=${formatPrettyValue(entry.error_message)}`);
  }

  return parts.join(" ");
}

function levelToConsoleMethod(level: LogLevel): "info" | "warn" | "error" {
  if (level === "ERROR") {
    return "error";
  }

  if (level === "WARN") {
    return "warn";
  }

  return "info";
}

export function logHttpRequest(entry: HttpRequestLogEntry): void {
  const enabled = parseBooleanEnv(process.env.HTTP_ACCESS_LOGS, true);
  if (!enabled) {
    return;
  }

  const format = resolveLogFormat(process.env.LOG_FORMAT);
  const method = levelToConsoleMethod(entry.level);

  if (format === "json") {
    const jsonPayload = {
      ...entry,
      level: entry.level.toLowerCase(),
    };
    console[method](JSON.stringify(jsonPayload));
    return;
  }

  const colorsEnabled = useColorOutput();
  console[method](formatPretty(entry, colorsEnabled));
}

export function logLevelFromStatus(status: number): LogLevel {
  if (status >= 500) {
    return "ERROR";
  }

  if (status >= 400) {
    return "WARN";
  }

  return "INFO";
}
