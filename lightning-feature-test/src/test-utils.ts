import { TestCase, TestResult, SuiteResult } from "./types.js";

const GREEN = "\x1b[32m";
const RED = "\x1b[31m";
const YELLOW = "\x1b[33m";
const CYAN = "\x1b[36m";
const BOLD = "\x1b[1m";
const RESET = "\x1b[0m";

export function test(name: string, run: () => Promise<void>): TestCase {
  return { name, run: async () => {
    const start = performance.now();
    try {
      await run();
      return { name, passed: true, durationMs: performance.now() - start };
    } catch (e) {
      const err = e instanceof Error ? e : new Error(String(e));
      return {
        name,
        passed: false,
        error: err.message,
        durationMs: performance.now() - start,
      };
    }
  }};
}

export function assertEq(actual: unknown, expected: unknown, msg?: string): void {
  if (actual !== expected) {
    throw new Error(msg
      ? `${msg}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`
      : `Expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
}

export function assertNeq(actual: unknown, notExpected: unknown, msg?: string): void {
  if (actual === notExpected) {
    throw new Error(msg
      ? `${msg}: value unexpectedly equals ${JSON.stringify(notExpected)}`
      : `Value unexpectedly equals ${JSON.stringify(notExpected)}`);
  }
}

export function assertGt(actual: number, threshold: number, msg?: string): void {
  if (!(actual > threshold)) {
    throw new Error(msg
      ? `${msg}: expected > ${threshold}, got ${actual}`
      : `Expected > ${threshold}, got ${actual}`);
  }
}

export function assertLt(actual: number, threshold: number, msg?: string): void {
  if (!(actual < threshold)) {
    throw new Error(msg
      ? `${msg}: expected < ${threshold}, got ${actual}`
      : `Expected < ${threshold}, got ${actual}`);
  }
}

export function assertContains(haystack: string, needle: string, msg?: string): void {
  if (!haystack.includes(needle)) {
    throw new Error(msg
      ? `${msg}: expected "${haystack}" to contain "${needle}"`
      : `Expected "${haystack}" to contain "${needle}"`);
  }
}

export function assertMatch(str: string, regex: RegExp, msg?: string): void {
  if (!regex.test(str)) {
    throw new Error(msg
      ? `${msg}: "${str}" does not match ${regex}`
      : `"${str}" does not match ${regex}`);
  }
}

export function assertThrows(
  fn: () => Promise<unknown>,
  msg?: string
): Promise<void> {
  return (async () => {
    try {
      await fn();
      throw new Error(msg || "Expected function to throw");
    } catch (e) {
      if (e instanceof Error && e.message.includes("Expected function to throw")) {
        throw e;
      }
    }
  })();
}

export async function runSuite(
  name: string,
  tests: TestCase[]
): Promise<SuiteResult> {
  console.log(`\n${BOLD}${CYAN}=== ${name} ===${RESET}`);

  const start = performance.now();
  const results: TestResult[] = [];

  for (const t of tests) {
    const result = await t.run();
    results.push(result);
    const icon = result.passed ? `${GREEN}✓${RESET}` : `${RED}✗${RESET}`;
    const detail = result.passed
      ? ""
      : `  ${RED}${result.error}${RESET}`;
    console.log(`  ${icon} ${result.name}${detail ? "\n" + detail : ""}`);
  }

  const duration = performance.now() - start;
  const passed = results.filter((r) => r.passed).length;
  const failed = results.filter((r) => !r.passed).length;

  console.log(
    `  ${BOLD}${passed}/${results.length} passed` +
    (failed > 0 ? `, ${RED}${failed} failed${RESET}` : "") +
    ` (${duration.toFixed(0)}ms)${RESET}`
  );

  return { name, results, passed, failed, durationMs: duration };
}

export function printSummary(suites: SuiteResult[]): boolean {
  const totalTests = suites.reduce((s, r) => s + r.results.length, 0);
  const totalPassed = suites.reduce((s, r) => s + r.passed, 0);
  const totalFailed = suites.reduce((s, r) => s + r.failed, 0);
  const totalDuration = suites.reduce((s, r) => s + r.durationMs, 0);

  const line = "═".repeat(60);
  console.log(`\n${BOLD}${line}${RESET}`);
  console.log(`${BOLD}  FEATURE TEST SUMMARY${RESET}`);
  console.log(`${BOLD}${line}${RESET}`);
  console.log(`  Suites:   ${suites.length}`);
  console.log(`  Tests:    ${totalTests}`);
  console.log(`  Passed:   ${GREEN}${totalPassed}${RESET}`);
  if (totalFailed > 0) {
    console.log(`  Failed:   ${RED}${totalFailed}${RESET}`);
  }
  console.log(`  Duration: ${(totalDuration / 1000).toFixed(1)}s`);
  console.log(`${BOLD}${line}${RESET}\n`);

  return totalFailed === 0;
}
