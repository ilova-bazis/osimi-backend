import { verifyTestDatabaseConnection } from "../integration/test-database.ts";

await verifyTestDatabaseConnection();

const testFiles = [
  ...new Bun.Glob("tests/integration/**/*.test.ts").scanSync({
    cwd: process.cwd(),
    onlyFiles: true,
  }),
].sort();
let combinedOutput = "";

for (const testFile of testFiles) {
  const childProcess = Bun.spawn(["bun", "test", testFile], {
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(childProcess.stdout).text(),
    new Response(childProcess.stderr).text(),
    childProcess.exited,
  ]);

  process.stdout.write(stdout);
  process.stderr.write(stderr);
  combinedOutput += stdout + stderr;

  if (exitCode !== 0) {
    process.exit(exitCode);
  }
}

if (/\b[1-9]\d* skip\b/.test(combinedOutput)) {
  console.error("[integration-tests] skipped tests are not allowed in the release gate.");
  process.exit(1);
}
