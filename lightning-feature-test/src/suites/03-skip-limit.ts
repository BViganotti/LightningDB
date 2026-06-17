import { LightningClient } from "../client.js";
import { test, assertEq, assertGt } from "../test-utils.js";

export function createSkipLimitSuite(client: LightningClient) {
  const TABLE = "SkipLimitTest";

  const setup = async () => {
    await client.query(
      `CREATE NODE TABLE IF NOT EXISTS ${TABLE} (id STRING, name STRING, score INT64, PRIMARY KEY(id))`
    );
    for (let i = 1; i <= 20; i++) {
      await client.query(
        `CREATE (n:${TABLE} {id: "sl-${i}", name: "User${String(i).padStart(2, "0")}", score: ${Math.floor(Math.random() * 100)}})`
      );
    }
  };

  const teardown = async () => {
    await client.queryRaw(`MATCH (n:${TABLE}) DELETE n`);
  };

  return {
    setup,
    teardown,
    tests: [
      test("LIMIT without ORDER BY", async () => {
        const r = await client.query(
          `MATCH (n:${TABLE}) RETURN n.name LIMIT 5`
        );
        assertEq(r.numRows, 5);
        assertEq(r.rows.length, 5);
      }),

      test("SKIP without ORDER BY", async () => {
        const r = await client.query(
          `MATCH (n:${TABLE}) RETURN n.name SKIP 5`
        );
        assertGt(r.numRows, 0, "rows remain after skip");
        assertEq(r.numRows, 15, "15 rows after skipping 5");
      }),

      test("SKIP + LIMIT without ORDER BY", async () => {
        const r = await client.query(
          `MATCH (n:${TABLE}) RETURN n.name SKIP 3 LIMIT 4`
        );
        assertEq(r.numRows, 4);
      }),

      test("ORDER BY + LIMIT", async () => {
        const r = await client.query(
          `MATCH (n:${TABLE}) RETURN n.name ORDER BY n.name LIMIT 3`
        );
        assertEq(r.numRows, 3);
        const names = r.rows.map((r) => r["name"]);
        assertEq(names[0], "User01");
        assertEq(names[1], "User02");
        assertEq(names[2], "User03");
      }),

      test("ORDER BY + SKIP", async () => {
        const r = await client.query(
          `MATCH (n:${TABLE}) RETURN n.name ORDER BY n.name SKIP 17`
        );
        assertEq(r.numRows, 3, "3 rows after skip 17 of 20");
      }),

      test("ORDER BY + SKIP + LIMIT", async () => {
        const r = await client.query(
          `MATCH (n:${TABLE}) RETURN n.name ORDER BY n.name SKIP 5 LIMIT 5`
        );
        assertEq(r.numRows, 5);
        const names = r.rows.map((r) => r["name"]);
        assertEq(names[0], "User06");
        assertEq(names[4], "User10");
      }),

      test("ORDER BY DESC + LIMIT", async () => {
        const r = await client.query(
          `MATCH (n:${TABLE}) RETURN n.name ORDER BY n.name DESC LIMIT 3`
        );
        assertEq(r.numRows, 3);
        const names = r.rows.map((r) => r["name"]);
        assertEq(names[0], "User20");
        assertEq(names[2], "User18");
      }),

      test("SKIP exceeds total rows", async () => {
        const r = await client.query(
          `MATCH (n:${TABLE}) RETURN n.name SKIP 100`
        );
        assertEq(r.numRows, 0, "skip beyond total returns empty");
      }),

      test("SKIP 0 is no-op", async () => {
        const r = await client.query(
          `MATCH (n:${TABLE}) RETURN n.name SKIP 0`
        );
        assertEq(r.numRows, 20, "skip 0 returns all rows");
      }),

      test("LIMIT 0 returns empty", async () => {
        const r = await client.query(
          `MATCH (n:${TABLE}) RETURN n.name LIMIT 0`
        );
        assertEq(r.numRows, 0, "limit 0 returns empty");
      }),
    ],
  };
}
