# AGENTS.md

Operational guide for agentic coding assistants working in this repository.

## Project Snapshot

- Runtime: Bun
- Language: TypeScript (ESM)
- DB: PostgreSQL via Bun SQL client
- App shape: route -> service -> repo -> db
- Tests: `bun test` (unit + integration)

## Important Paths

- `index.ts` - bootstrap entry
- `src/routes/` - HTTP route definitions
- `src/services/` - business logic and orchestration
- `src/repos/` - SQL/data access
- `src/domain/` - domain rules/state machines
- `src/db/migrations/` - SQL schema migrations
- `tests/` - integration and behavior tests
- `docs/` - requirements and architecture contracts

## Build / Run / Test Commands

### Install

```bash
bun install
```

### Development server

```bash
bun run dev
```

Equivalent:

```bash
bun run --watch index.ts
```

### Production-style run

```bash
bun run start
```

Equivalent:

```bash
bun run index.ts
```

### Type-check (no dedicated npm script yet)

```bash
bunx tsc --noEmit
```

### Test suite

```bash
bun test
```

### Run a single test file

```bash
bun test tests/integration/http/event-routes.test.ts
```

### Run tests by test name pattern

```bash
bun test --test-name-pattern "lease exclusivity"
```

Shorthand also works:

```bash
bun test -t "ingestion completed"
```

### Useful focused test modes

```bash
bun test --only
bun test --only-failures
bun test --timeout 10000
```

### Migrations

```bash
bun run migrate
```

Direct invocation with flags:

```bash
bun run src/db/migrate.ts --schema=public
bun run src/db/migrate.ts --dry-run
```

### CLI utility

```bash
bun run create-user
```

## Environment Notes

- Integration migration tests require `TEST_DATABASE_URL` or `DATABASE_URL`.
- Lease signing and background jobs use env vars documented in `README.md`.
- For schema-aware local/testing runs, ensure migration schema and runtime schema are aligned.

## Code Style and Conventions

### Imports

- Use ESM imports with explicit `.ts` extensions for local modules.
- Keep import groups ordered: node built-ins -> internal modules.
- Use `import type` for type-only imports.

### Formatting

- Follow existing file style (2 spaces, trailing commas where present, semicolons used consistently).
- Keep functions small and keep route handlers thin.
- Avoid adding comments unless the logic is non-obvious.

### Types

- `tsconfig.json` is strict; write code that satisfies strict typing.
- Prefer explicit interfaces/types for row and DTO shapes.
- Use row mappers (`mapX`) to translate DB snake_case to app camelCase.
- Avoid assertion casts (`as T`) after SQL queries when possible.
- If a cast is unavoidable, isolate and justify it briefly.

### Naming

- DB columns and wire payload keys: snake_case.
- Internal TS fields/variables: camelCase.
- Repo row interfaces often use `*Row` suffix.
- Service return payloads are generally API-shaped (snake_case keys).

### SQL and Repository Rules

- Prefer Bun tagged SQL (`sql<T>\`...\``) over `sql.unsafe(...)` for app CRUD/query paths.
- Keep tenant scoping explicit in query predicates.
- For multi-step invariants, enforce correctness at DB boundary (CAS updates, constraints, transactions).
- Use schema-scoped clients when repository pattern requires it.

### Error Handling

- Throw typed HTTP/domain errors from `src/http/errors.ts`:
  - `ValidationError`
  - `NotFoundError`
  - `ConflictError`
  - `UnauthorizedError`
- Keep error messages clear and field-specific.
- Validate request body shape and required fields early in routes/services.

### Service Layer Patterns

- Services orchestrate repos and domain checks.
- Domain transition validation should happen before writes; DB CAS should guard race conditions.
- Keep idempotency and out-of-order event tolerance explicit where required by docs.

### Testing Expectations

- Prefer integration tests for route/service/repo flows.
- Add focused tests for concurrency-sensitive behavior.
- When changing API contracts, update tests and docs in the same change set.

## Documentation Contract Rules

- `docs/project-requirements.md` is the primary product/API contract.
- `docs/testing-spec.md` defines release-gate test scenarios.
- `docs/implementation-phases.md` describes delivery sequencing and invariants.
- If behavior changes, update docs immediately (do not defer).

## Cursor / Copilot Rules

Checked locations:

- `.cursor/rules/`
- `.cursorrules`
- `.github/copilot-instructions.md`

Current status: no Cursor/Copilot instruction files were found in this repository.

## Agent Workflow Checklist

1. Read relevant docs and target module before editing.
2. Keep changes scoped to requested step.
3. Prefer safe SQL patterns and strict typing.
4. Run focused tests first, then full `bun test` when practical.
5. Update docs/contracts when API or semantics change.
