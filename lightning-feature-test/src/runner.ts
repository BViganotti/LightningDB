import { LightningClient } from "./client.js";
import { runSuite, printSummary } from "./test-utils.js";
import { createBasicCrudSuite } from "./suites/01-basic-crud.js";
import { createSortingSuite } from "./suites/02-sorting.js";
import { createSkipLimitSuite } from "./suites/03-skip-limit.js";
import { createAggregationSuite } from "./suites/04-aggregation.js";
import { createGraphSuite } from "./suites/05-graph.js";
import { createDmlSuite } from "./suites/06-dml.js";
import { createExpressionsSuite } from "./suites/07-expressions.js";
import { createTransactionsSuite } from "./suites/08-transactions.js";
import { createConcurrencySuite } from "./suites/09-concurrency.js";
import { createEdgeCasesSuite } from "./suites/10-edge-cases.js";

const BASE_URL = process.env.LIGHTNING_URL || "http://127.0.0.1:8080";
const client = new LightningClient(BASE_URL);

async function waitForServer(maxRetries = 10): Promise<void> {
  for (let i = 0; i < maxRetries; i++) {
    const status = await client.health();
    if (status === 200 || status === 401) return;
    console.log(`  Waiting for server at ${BASE_URL} (attempt ${i + 1}/${maxRetries})...`);
    await new Promise((r) => setTimeout(r, 1000));
  }
  throw new Error(`Server at ${BASE_URL} not available after ${maxRetries} retries`);
}

async function main() {
  console.log("╔══════════════════════════════════════════════════╗");
  console.log("║     LightningDB Feature Test Suite               ║");
  console.log("╚══════════════════════════════════════════════════╝");
  console.log(`  Server: ${BASE_URL}`);
  console.log(`  Node:   ${process.version}\n`);

  console.log("  Connecting...");
  await waitForServer();
  console.log("  Connected.\n");

  const suites = [
    { name: "Basic CRUD", factory: createBasicCrudSuite },
    { name: "Sorting (ORDER BY)", factory: createSortingSuite },
    { name: "Skip & Limit", factory: createSkipLimitSuite },
    { name: "Aggregation", factory: createAggregationSuite },
    { name: "Graph Traversal", factory: createGraphSuite },
    { name: "Data Manipulation (DML)", factory: createDmlSuite },
    { name: "Expressions", factory: createExpressionsSuite },
    { name: "Transactions", factory: createTransactionsSuite },
    { name: "Concurrency", factory: createConcurrencySuite },
    { name: "Edge Cases & Error Handling", factory: createEdgeCasesSuite },
  ];

  const results = [];

  for (const { name, factory } of suites) {
    const { setup, teardown, tests } = factory(client);
    try {
      if (setup) await setup();
    } catch (e) {
      console.log(`  ${name}: setup failed, skipping suite`);
      console.log(`    ${e}`);
      continue;
    }

    const result = await runSuite(name, tests);
    results.push(result);

    try {
      if (teardown) await teardown();
    } catch {
      // ignore teardown errors
    }
  }

  const allPassed = printSummary(results);
  process.exit(allPassed ? 0 : 1);
}

main().catch((e) => {
  console.error(`FATAL: ${e.message}`);
  process.exit(1);
});
