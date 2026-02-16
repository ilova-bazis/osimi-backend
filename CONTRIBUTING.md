# Contributing

## Coding Standards

### SQL and Type Safety

- Do not use convenience casts like `as T` after `sql<T>` queries
- Prefer `sql<T>` generics and explicit row mapping functions
- Keep query result typing strict and transparent
- If a cast is unavoidable due to Bun typing limitations, isolate it and document why with a short comment
- In auth and other security-sensitive paths, avoid assertion casts by default
