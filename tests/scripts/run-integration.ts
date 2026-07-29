import { verifyTestDatabaseConnection } from "../integration/test-database.ts";

await verifyTestDatabaseConnection();

const childProcess = Bun.spawn(["bun", "test", "tests/integration"], {
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

if (exitCode !== 0) {
  process.exit(exitCode);
}

if (/\b[1-9]\d* skip\b/.test(stdout) || /\b[1-9]\d* skip\b/.test(stderr)) {
  console.error("[integration-tests] skipped tests are not allowed in the release gate.");
  process.exit(1);
}
