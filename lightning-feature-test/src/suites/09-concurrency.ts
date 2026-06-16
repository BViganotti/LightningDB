import { LightningClient } from "../client.js";
import { test, assertEq } from "../test-utils.js";

export function createConcurrencySuite(baseClient: LightningClient) {
  const TABLE = "ConcTest";

  const setup = async () => {
    await baseClient.query(
      `CREATE NODE TABLE IF NOT EXISTS ${TABLE} (id STRING, counter INT64, PRIMARY KEY(id))`
    );
    await baseClient.query(`CREATE (n:${TABLE} {id: "concurrent-key", counter: 0})`);
  };

  const teardown = async () => {
    await baseClient.queryRaw(`MATCH (n:${TABLE}) DELETE n`);
  };

  return { setup, teardown, tests: [
    test("Sequential read after write", async () => {
      await baseClient.query(
        `MATCH (n:${TABLE} {id: "concurrent-key"}) SET n.counter = n.counter + 1 RETURN n.counter`
      );
      const r = await baseClient.query(
        `MATCH (n:${TABLE} {id: "concurrent-key"}) RETURN n.counter`
      );
      assertEq(r.data.rows[0]["counter"] as number, 1);
    }),

    test("Multiple sequential mutations", async () => {
      for (let i = 0; i < 5; i++) {
        await baseClient.query(
          `MATCH (n:${TABLE} {id: "concurrent-key"}) SET n.counter = n.counter + 1`
        );
      }
      const r = await baseClient.query(
        `MATCH (n:${TABLE} {id: "concurrent-key"}) RETURN n.counter`
      );
      assertEq(r.data.rows[0]["counter"] as number, 6, "counter incremented 5 more times");
    }),
  ]};
}
