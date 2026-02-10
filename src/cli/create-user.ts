import { createInterface } from "node:readline/promises";
import { stdin as input, stdout as output } from "node:process";
import {
  findAllActiveTenants,
  findUserByUsername,
  createUser,
  updateUserPassword,
  updateUserRole,
  findUsersByTenant,
  getUserRole,
  deleteUser,
  type TenantSummary,
  type UserWithRole,
} from "../repos/auth-repo.ts";
import type { UserRole } from "../auth/types.ts";

interface Choice<T> {
  value: T;
  label: string;
}

type Action = "create" | "edit" | "cancel";

async function promptText(question: string): Promise<string> {
  const rl = createInterface({ input, output });
  try {
    const answer = await rl.question(question);
    return answer.trim();
  } finally {
    rl.close();
  }
}

async function promptSelect<T>(question: string, choices: Choice<T>[]): Promise<T> {
  console.log(question);
  choices.forEach((choice, index) => {
    console.log(`  ${index + 1}) ${choice.label}`);
  });

  while (true) {
    const answer = await promptText("> ");
    const num = parseInt(answer, 10);

    if (isNaN(num) || num < 1 || num > choices.length) {
      console.error("Invalid selection. Please enter a number from the list.");
      continue;
    }

    const choice = choices[num - 1];
    if (!choice) {
      console.error("Invalid selection.");
      continue;
    }
    return choice.value;
  }
}

async function promptConfirm(question: string, defaultValue = true): Promise<boolean> {
  const suffix = defaultValue ? " (Y/n): " : " (y/N): ";
  const answer = await promptText(question + suffix);

  if (answer === "") {
    return defaultValue;
  }

  const normalized = answer.toLowerCase();
  if (normalized === "y" || normalized === "yes") {
    return true;
  }
  if (normalized === "n" || normalized === "no") {
    return false;
  }

  console.error("Please enter 'y' or 'n'");
  return promptConfirm(question, defaultValue);
}

async function selectAction(): Promise<Action> {
  const choices: Choice<Action>[] = [
    { value: "create", label: "Create new user" },
    { value: "edit", label: "Edit existing user" },
    { value: "cancel", label: "Cancel / Exit" },
  ];

  return await promptSelect("What would you like to do?", choices);
}

async function selectTenant(): Promise<TenantSummary> {
  const tenants = await findAllActiveTenants();

  if (tenants.length === 0) {
    throw new Error("No active tenants found in the database.");
  }

  const choices: Choice<TenantSummary>[] = tenants.map((tenant) => ({
    value: tenant,
    label: `${tenant.name} (${tenant.slug})`,
  }));

  const selected = await promptSelect("Select tenant:", choices);
  console.log(`Selected: ${selected.name}`);
  return selected;
}

async function promptNewUsername(): Promise<string> {
  while (true) {
    const username = await promptText("Username: ");

    if (!username) {
      console.error("Username is required.");
      continue;
    }

    if (username.length < 3) {
      console.error("Username must be at least 3 characters.");
      continue;
    }

    const usernameNormalized = username.toLowerCase().trim();
    const existingUser = await findUserByUsername(usernameNormalized);

    if (existingUser) {
      console.error(`Username '${username}' already exists.`);
      continue;
    }

    return username;
  }
}

async function promptPasswordWithConfirmation(): Promise<string> {
  while (true) {
    const password = await promptText("Password: ");

    if (!password) {
      console.error("Password is required.");
      continue;
    }

    if (password.length < 8) {
      console.error("Password must be at least 8 characters.");
      continue;
    }

    const confirmPassword = await promptText("Confirm password: ");

    if (password !== confirmPassword) {
      console.error("Passwords do not match.");
      continue;
    }

    return password;
  }
}

async function promptPasswordWithoutConfirmation(): Promise<string> {
  while (true) {
    const password = await promptText("New password: ");

    if (!password) {
      console.error("Password is required.");
      continue;
    }

    if (password.length < 8) {
      console.error("Password must be at least 8 characters.");
      continue;
    }

    return password;
  }
}

async function selectRole(): Promise<UserRole> {
  const choices: Choice<UserRole>[] = [
    { value: "viewer", label: "viewer - Read-only access" },
    { value: "operator", label: "operator - Can perform operations" },
    { value: "admin", label: "admin - Full administrative access" },
  ];

  return await promptSelect("Select role:", choices);
}

function normalizeUsername(username: string): string {
  return username.toLowerCase().trim();
}

async function selectExistingUser(tenantId: string): Promise<UserWithRole> {
  const users = await findUsersByTenant(tenantId);

  if (users.length === 0) {
    throw new Error("No users found in this tenant.");
  }

  const choices: Choice<UserWithRole>[] = users.map((user) => ({
    value: user,
    label: `${user.username} (${user.role})`,
  }));

  return await promptSelect("Select user to edit:", choices);
}

async function selectEditAction(): Promise<"password" | "role" | "delete" | "cancel"> {
  const choices: Choice<"password" | "role" | "delete" | "cancel">[] = [
    { value: "password", label: "Reset password" },
    { value: "role", label: "Change role" },
    { value: "delete", label: "Delete user" },
    { value: "cancel", label: "Go back" },
  ];

  return await promptSelect("What would you like to do?", choices);
}

async function runCreateUser(): Promise<void> {
  console.log("=== Create New User ===\n");

  const tenant = await selectTenant();
  console.log();

  const username = await promptNewUsername();
  console.log();

  const password = await promptPasswordWithConfirmation();
  console.log();

  const role = await selectRole();
  console.log();

  console.log("\n--- Summary ---");
  console.log(`Tenant: ${tenant.name} (${tenant.slug})`);
  console.log(`Username: ${username}`);
  console.log(`Role: ${role}`);
  console.log();

  const shouldCreate = await promptConfirm("Create user with these details?");

  if (!shouldCreate) {
    console.log("\nCancelled. No user was created.");
    return;
  }

  console.log("\nCreating user...");

  const passwordHash = await Bun.password.hash(password);

  await createUser({
    userId: crypto.randomUUID(),
    username: username,
    usernameNormalized: normalizeUsername(username),
    passwordHash,
    tenantId: tenant.id,
    role,
    membershipId: crypto.randomUUID(),
  });

  console.log("\n✓ User created successfully");
  console.log(`  Username: ${username}`);
  console.log(`  Role: ${role}`);
  console.log(`  Tenant: ${tenant.name}`);
}

async function runEditUser(): Promise<void> {
  console.log("=== Edit User ===\n");

  const tenant = await selectTenant();
  console.log();

  const user = await selectExistingUser(tenant.id);
  console.log();

  const editAction = await selectEditAction();
  console.log();

  if (editAction === "password") {
    const password = await promptPasswordWithoutConfirmation();
    console.log();

    const shouldUpdate = await promptConfirm(`Reset password for user '${user.username}'?`);

    if (!shouldUpdate) {
      console.log("\nCancelled. No changes were made.");
      return;
    }

    console.log("\nUpdating password...");

    const passwordHash = await Bun.password.hash(password);
    await updateUserPassword(user.id, passwordHash);

    console.log(`\n✓ Password updated successfully for user '${user.username}'`);
  } else if (editAction === "role") {
    const currentRole = await getUserRole(user.id, tenant.id);
    console.log(`Current role: ${currentRole || "unknown"}`);
    console.log();

    const newRole = await selectRole();
    console.log();

    if (currentRole === newRole) {
      console.log(`User already has role '${newRole}'. No changes needed.`);
      return;
    }

    const shouldUpdate = await promptConfirm(`Change role from '${currentRole}' to '${newRole}' for user '${user.username}'?`);

    if (!shouldUpdate) {
      console.log("\nCancelled. No changes were made.");
      return;
    }

    console.log("\nUpdating role...");

    await updateUserRole(user.id, tenant.id, newRole);

    console.log(`\n✓ Role updated successfully for user '${user.username}'`);
    console.log(`  Old role: ${currentRole}`);
    console.log(`  New role: ${newRole}`);
  } else if (editAction === "delete") {
    await runDeleteUser(user, tenant);
  } else if (editAction === "cancel") {
    console.log("Going back...");
    return;
  }
}

async function runDeleteUser(user: UserWithRole, tenant: TenantSummary): Promise<void> {
  console.log("\n⚠️  WARNING: You are about to delete a user");
  console.log(`User: ${user.username}`);
  console.log(`Role: ${user.role}`);
  console.log(`Tenant: ${tenant.name}`);
  console.log("\nThis action will soft-delete the user (set as inactive).");
  console.log("The user will no longer be able to log in.\n");

  const confirmFirst = await promptConfirm("Do you want to proceed with deletion?", false);

  if (!confirmFirst) {
    console.log("\nCancelled. No changes were made.");
    return;
  }

  console.log("\nFor security, please type the username to confirm deletion:");
  const confirmationText = await promptText(`Type '${user.username}' to confirm: `);

  if (confirmationText !== user.username) {
    console.log("\n✗ Username mismatch. Deletion cancelled.");
    return;
  }

  console.log("\nDeleting user...");

  await deleteUser(user.id);

  console.log(`\n✓ User '${user.username}' has been deleted successfully`);
}

export async function runUserManagement(): Promise<void> {
  console.log("=== User Management ===\n");

  const action = await selectAction();
  console.log();

  if (action === "cancel") {
    console.log("Goodbye!");
    return;
  }

  if (action === "create") {
    await runCreateUser();
  } else {
    await runEditUser();
  }
}

if (import.meta.main) {
  runUserManagement().catch((error) => {
    console.error("\n✗ Failed:", error instanceof Error ? error.message : error);
    process.exit(1);
  });
}
