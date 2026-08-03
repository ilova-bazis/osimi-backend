import type { AuthenticatedContext } from "../auth/guards.ts";
import { isObjectAccessAuthorized } from "../domain/objects/access-policy.ts";
import { ForbiddenError, NotFoundError } from "../http/errors.ts";
import { findObjectAccessAssignmentForUser } from "../repos/object-access-repo.ts";
import { findObjectById } from "../repos/object-repo.ts";

export async function requireObjectEditAccess(params: {
  auth: AuthenticatedContext;
  objectId: string;
}): Promise<void> {
  const object = await findObjectById({
    tenantId: params.auth.tenantId,
    objectId: params.objectId,
  });

  if (!object) {
    throw new NotFoundError(`Object '${params.objectId}' was not found.`);
  }

  const assignment = await findObjectAccessAssignmentForUser({
    tenantId: params.auth.tenantId,
    objectId: params.objectId,
    userId: params.auth.userId,
  });

  if (!isObjectAccessAuthorized({
    role: params.auth.role,
    accessLevel: object.accessLevel,
    assignmentLevel: assignment?.grantedLevel,
  })) {
    throw new ForbiddenError("You are not authorized to edit this object.");
  }
}
