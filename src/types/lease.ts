import type { AuthorizedWorkerLease } from "../auth/worker-lease.ts";

export interface WorkerDownloadUrl {
  file_id: string;
  storage_key: string;
  content_type: string;
  size_bytes: number;
  download_url: string;
}

export interface LeaseDto {
  lease_id: string;
  lease_token: string;
  lease_expires_at: string;
  ingestion_id: string;
  batch_label: string;
  tenant_id: string;
  download_urls: WorkerDownloadUrl[];
}

export interface HeartbeatLeaseInput {
  authorizedLease: AuthorizedWorkerLease;
}

export interface HeartbeatLeaseResponse {
  lease: LeaseDto;
}

export interface ReleaseLeaseInput {
  authorizedLease: AuthorizedWorkerLease;
}

export interface ReleaseLeaseResponse {
  status: "ok";
  ingestion_id: string;
  lease_id: string;
}
