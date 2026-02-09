---
name: bun-testing
description: Write concise unit tests using Bun's built-in test runner
compatibility: opencode
---

## What I do

- Provide minimal patterns for Bun unit tests
- Show core assertions and async testing
- Demonstrate mocks/spies and time control

## When to use me

Use this for quick unit test guidance in Bun-only projects.
Ask clarifying questions if integration or end-to-end testing is requested.

## How to run

- `bun test`
- `bun test --watch`

## Test file conventions

- `*.test.ts` or `*.spec.ts`

## Minimal examples

```ts
import { describe, expect, test } from "bun:test";

describe("sum", () => {
  test("adds two numbers", () => {
    expect(1 + 2).toBe(3);
  });
});
```

```ts
import { test, expect } from "bun:test";

test("resolves async value", async () => {
  const value = await Promise.resolve("ok");
  expect(value).toBe("ok");
});
```

## More examples

Short description: Async error handling with `rejects`.

```ts
import { test, expect } from "bun:test";

test("async error", async () => {
  await expect(async () => {
    throw new Error("boom");
  }).rejects.toThrow("boom");
});
```

Short description: Per-test timeout.

```ts
import { test, expect } from "bun:test";

test("per-test timeout", async () => {
  await Promise.resolve();
  expect(true).toBe(true);
}, 500);
```

Short description: Retry flaky tests.

```ts
import { test, expect } from "bun:test";

test(
  "retry flaky",
  () => {
    expect(Math.random()).toBeLessThan(1);
  },
  { retry: 3 },
);
```

Short description: Parametrized tests with `test.each`.

```ts
import { test, expect } from "bun:test";

test.each([
  [1, 2, 3],
  [3, 4, 7],
])("%i + %i = %i", (a, b, expected) => {
  expect(a + b).toBe(expected);
});
```

Short description: Conditional skip with `test.skipIf`.

```ts
import { test } from "bun:test";

test.skipIf(process.platform === "win32")("posix only", () => {
  // skipped on Windows
});
```

Short description: Assertion counting.

```ts
import { test, expect } from "bun:test";

test("assertion count", () => {
  expect.assertions(2);
  expect(1 + 1).toBe(2);
  expect("hello").toContain("ell");
});
```

## Tips

- Keep unit tests deterministic and fast
- Avoid I/O in unit tests; mock boundaries
- Use descriptive test names
