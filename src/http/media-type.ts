const TOKEN_PATTERN = /^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/;

export interface ParsedMediaType {
  value: string;
  essence: string;
}

function splitParameters(value: string): string[] | undefined {
  const parts: string[] = [];
  let start = 0;
  let quoted = false;
  let escaped = false;

  for (let index = 0; index < value.length; index += 1) {
    const character = value[index]!;
    if (escaped) {
      escaped = false;
      continue;
    }
    if (quoted && character === "\\") {
      escaped = true;
      continue;
    }
    if (character === '"') {
      quoted = !quoted;
      continue;
    }
    if (!quoted && character === ";") {
      parts.push(value.slice(start, index));
      start = index + 1;
    }
  }

  if (quoted || escaped) return undefined;
  parts.push(value.slice(start));
  return parts;
}

function isQuotedValue(value: string): boolean {
  if (value.length < 2 || value[0] !== '"' || value.at(-1) !== '"') return false;
  for (let index = 1; index < value.length - 1; index += 1) {
    const code = value.charCodeAt(index);
    if (code === 0x5c) {
      index += 1;
      if (index >= value.length - 1) return false;
      const escapedCode = value.charCodeAt(index);
      if (escapedCode !== 0x09 && (escapedCode < 0x20 || escapedCode === 0x7f || escapedCode > 0xff)) {
        return false;
      }
      continue;
    }
    if (
      code === 0x22 ||
      (code !== 0x09 && (code < 0x20 || code === 0x7f || code > 0xff))
    ) return false;
  }
  return true;
}

export function parseMediaType(input: string): ParsedMediaType | undefined {
  const value = input.trim();
  const parts = splitParameters(value);
  if (!parts) return undefined;

  const typeParts = parts[0]?.trim().split("/");
  if (
    typeParts?.length !== 2 ||
    !TOKEN_PATTERN.test(typeParts[0]!) ||
    !TOKEN_PATTERN.test(typeParts[1]!)
  ) return undefined;

  const parameterNames = new Set<string>();
  for (const rawParameter of parts.slice(1)) {
    const parameter = rawParameter.trim();
    const equalsAt = parameter.indexOf("=");
    if (equalsAt <= 0) return undefined;
    const name = parameter.slice(0, equalsAt).trim();
    const parameterValue = parameter.slice(equalsAt + 1).trim();
    const normalizedName = name.toLowerCase();
    if (
      !TOKEN_PATTERN.test(name) ||
      parameterNames.has(normalizedName) ||
      (!TOKEN_PATTERN.test(parameterValue) && !isQuotedValue(parameterValue))
    ) return undefined;
    parameterNames.add(normalizedName);
  }

  return {
    value,
    essence: `${typeParts[0]!.toLowerCase()}/${typeParts[1]!.toLowerCase()}`,
  };
}
