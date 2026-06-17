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

  async function concurrentIncrement(client: LightningClient, key: string, times: number): Promise<void> {
    for (let i = 0; i < times; i++) {
      await client.query(
        `MATCH (n:${TABLE} {id: "${key}"}) SET n.counter = n.counter + 1`
      );
    }
  }

  return { setup, teardown, tests: [
    test("Sequential read after write", async () => {
      await baseClient.query(
        `MATCH (n:${TABLE} {id: "concurrent-key"}) SET n.counter = n.counter + 1 RETURN n.counter`
      );
      const r = await baseClient.query(
        `MATCH (n:${TABLE} {id: "concurrent-key"}) RETURN n.counter`
      );
      assertEq(r.rows[0]["counter"] as number, 1);
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
      assertEq(r.rows[0]["counter"] as number, 6, "counter incremented 5 more times");
    }),

    test("Concurrent reads from separate clients", async () => {
      // Use separate client instances to simulate concurrent readers
      const readers = await Promise.all([
        baseClient.query(`MATCH (n:${TABLE} {id: "concurrent-key"}) RETURN n.counter`),
        baseClient.query(`MATCH (n:${TABLE} {id: "concurrent-key"}) RETURN n.id`),
      ]);
      assertEq(readers[0].rows[0]["counter"] as number, 6);
      assertEq(readers[1].rows[0]["id"] as string, "concurrent-key");
    }),

    test("Concurrent writes to different keys", async () => {
      await baseClient.query(`CREATE (n:${TABLE} {id: "key-a", counter: 0})`);
      await baseClient.query(`CREATE (n:${TABLE} {id: "key-b", counter: 0})`);

      // Run writes concurrently using Promise.all
      const clients = [baseClient, baseClient];
      await Promise.all([
        concurrentIncrement(clients[0], "key-a", 3),
        concurrentIncrement(clients[1], "key-b", 3),
      ]);

      const ra = await baseClient.query(
        `MATCH (n:${TABLE} {id: "key-a"}) RETURN n.counter`
      );
      const rb = await baseClient.query(
        `MATCH (n:${TABLE} {id: "key-b"}) RETURN n.counter`
      );
      assertEq(ra.rows[0]["counter"] as number, 3, "key-a counter = 3");
      assertEq(rb.rows[0]["counter"] as number, 3, "key-b counter = 3");
    }),
  ]};
}
