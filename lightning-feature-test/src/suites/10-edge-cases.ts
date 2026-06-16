import { LightningClient } from "../client.js";
import { test, assertEq, assertContains } from "../test-utils.js";

export function createEdgeCasesSuite(client: LightningClient) {
  const TABLE = "EdgeTest";

  const setup = async () => {
    await client.query(
      `CREATE NODE TABLE IF NOT EXISTS ${TABLE} (id STRING, name STRING, value INT64, tag STRING, PRIMARY KEY(id))`
    );
    await client.query(`CREATE (n:${TABLE} {id: "edge-1", name: "unique", value: 1, tag: "a"}) RETURN n.id`);
  };

  const teardown = async () => {
    await client.queryRaw(`MATCH (n:${TABLE}) DELETE n`);
  };

  return { setup, teardown, tests: [
    test("Invalid query returns error", async () => {
      const { status } = await client.queryRaw("THIS IS NOT VALID CYPHER");
      assertEq(status, 400, "invalid syntax returns 400");
    }),

    test("Query against non-existent table", async () => {
      const { status, body } = await client.queryRaw(
        "MATCH (n:NonExistentTable12345) RETURN n"
      );
      assertEq(status, 400, "missing table returns 400");
      assertContains(body.toLowerCase(), "not found");
    }),

    test("Large SKIP value", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) RETURN n.name SKIP 999999`
      );
      assertEq(r.data.numRows, 0, "skip larger than total returns empty");
    }),

    test("ORDER BY with LIMIT 1 returns single row", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) RETURN n.name ORDER BY n.name LIMIT 1`
      );
      assertEq(r.data.numRows, 1, "top 1 returns 1 row");
    }),

    test("ORDER BY DESC with LIMIT 1 (reverse top)", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) RETURN n.name ORDER BY n.name DESC LIMIT 1`
      );
      assertEq(r.data.numRows, 1, "bottom 1 returns 1 row");
    }),
  ]};
}
