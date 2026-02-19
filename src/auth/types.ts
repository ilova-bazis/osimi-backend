export type UserRole = "viewer" | "archiver" | "admin";

export interface AuthenticatedPrincipal {
  sessionToken: string;
  userId: string;
  username: string;
  tenantId: string;
  role: UserRole;
  createdAt: Date;
  lastSeenAt: Date;
}
