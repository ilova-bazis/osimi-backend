export type UserRole = "viewer" | "operator" | "admin";

export interface AuthenticatedPrincipal {
  sessionToken: string;
  userId: string;
  username: string;
  tenantId: string;
  role: UserRole;
  createdAt: Date;
  lastSeenAt: Date;
}
