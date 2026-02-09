# Backend Tech Stack

This document captures the implementation stack for the Osimi backend (VPS control plane + staging).

## Runtime

- Bun (server runtime)
- Bun.serve with built-in routes (requires Bun v1.2.3+)

## Language

- TypeScript

## Database

- PostgreSQL
- Bun SQL client (`import { sql, SQL } from "bun"`)

## Storage

- Local disk storage on the VPS (staging)
- Signed URL flow for uploads and worker downloads
- Storage abstraction designed for future migration to object storage (S3/MinIO)

## Auth

- Session or JWT-based auth (tenant-aware)

## Background Tasks

- Periodic cleanup job for staging retention rules
- Stuck ingestion detection (no heartbeat/progress)

## Testing

- `bun test` for unit and integration tests

## Deployment

- Bun process on VPS (systemd/PM2 recommended)
