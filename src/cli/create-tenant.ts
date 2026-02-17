import { createInterface } from "node:readline/promises";
import { stdin as input, stdout as output } from "node:process";
import {
  findAllActiveTenants,
  findTenantBySlug,
  createTenant,
  type TenantSummary,
} from "../repos/auth-repo.ts";

interface Choice<T> {
  value: T;
  label: string;
}

type Action = "create" | "list" | "cancel";

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
    { value: "list", label: "List all tenants" },
    { value: "create", label: "Create new tenant" },
    { value: "cancel", label: "Cancel / Exit" },
  ];

  return await promptSelect("What would you like to do?", choices);
}

function validateSlug(slug: string): { valid: boolean; error?: string } {
  if (!slug) {
    return { valid: false, error: "Slug is required." };
  }

  if (slug.length < 3) {
    return { valid: false, error: "Slug must be at least 3 characters." };
  }

  if (slug.length > 50) {
    return { valid: false, error: "Slug must be at most 50 characters." };
  }

  const slugPattern = /^[a-z0-9-]+$/;
  if (!slugPattern.test(slug)) {
    return { valid: false, error: "Slug can only contain lowercase letters, numbers, and hyphens." };
  }

  if (slug.startsWith("-") || slug.endsWith("-")) {
    return { valid: false, error: "Slug cannot start or end with a hyphen." };
  }

  if (slug.includes("--")) {
    return { valid: false, error: "Slug cannot contain consecutive hyphens." };
  }

  return { valid: true };
}

function normalizeSlug(slug: string): string {
  return slug
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9-]/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

async function promptSlug(): Promise<string> {
  while (true) {
    const slug = await promptText("Tenant slug (e.g., 'acme-corp'): ");

    if (!slug) {
      console.error("Slug is required.");
      continue;
    }

    const normalizedSlug = normalizeSlug(slug);
    const validation = validateSlug(normalizedSlug);

    if (!validation.valid) {
      console.error(validation.error);
      continue;
    }

    const existingTenant = await findTenantBySlug(normalizedSlug);

    if (existingTenant) {
      console.error(`Slug '${normalizedSlug}' is already taken.`);
      continue;
    }

    return normalizedSlug;
  }
}

async function promptName(): Promise<string> {
  while (true) {
    const name = await promptText("Tenant name (e.g., 'Acme Corporation'): ");

    if (!name) {
      console.error("Name is required.");
      continue;
    }

    if (name.length < 2) {
      console.error("Name must be at least 2 characters.");
      continue;
    }

    if (name.length > 100) {
      console.error("Name must be at most 100 characters.");
      continue;
    }

    return name;
  }
}

async function runListTenants(): Promise<void> {
  console.log("=== All Tenants ===\n");

  const tenants = await findAllActiveTenants();

  if (tenants.length === 0) {
    console.log("No tenants found.");
    return;
  }

  console.log(`Found ${tenants.length} tenant(s):\n`);

  tenants.forEach((tenant, index) => {
    console.log(`  ${index + 1}. ${tenant.name}`);
    console.log(`     Slug: ${tenant.slug}`);
    console.log(`     ID: ${tenant.id}`);
    console.log();
  });
}

async function runCreateTenant(): Promise<void> {
  console.log("=== Create New Tenant ===\n");

  const slug = await promptSlug();
  console.log();

  const name = await promptName();
  console.log();

  console.log("\n--- Summary ---");
  console.log(`Slug: ${slug}`);
  console.log(`Name: ${name}`);
  console.log();

  const shouldCreate = await promptConfirm("Create tenant with these details?");

  if (!shouldCreate) {
    console.log("\nCancelled. No tenant was created.");
    return;
  }

  console.log("\nCreating tenant...");

  await createTenant({
    tenantId: crypto.randomUUID(),
    slug,
    name,
  });

  console.log("\n✓ Tenant created successfully");
  console.log(`  Slug: ${slug}`);
  console.log(`  Name: ${name}`);
}

export async function runTenantManagement(): Promise<void> {
  console.log("=== Tenant Management ===\n");

  const action = await selectAction();
  console.log();

  if (action === "cancel") {
    console.log("Goodbye!");
    return;
  }

  if (action === "list") {
    await runListTenants();
  } else {
    await runCreateTenant();
  }
}

if (import.meta.main) {
  runTenantManagement().catch((error) => {
    console.error("\n✗ Failed:", error instanceof Error ? error.message : error);
    process.exit(1);
  });
}
