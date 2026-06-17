import { LightningClient } from "../client.js";
import { test, assertEq, assertGt, assertNeq } from "../test-utils.js";

export function createBasicCrudSuite(client: LightningClient) {
  const TABLE = "CrudTest";

  const setup = async () => {
    await client.query(
      `CREATE NODE TABLE IF NOT EXISTS ${TABLE} (id STRING, name STRING, age INT64, active BOOL, score DOUBLE, PRIMARY KEY(id))`
    );
  };

  const teardown = async () => {
    await client.queryRaw(`MATCH (n:${TABLE}) DELETE n`);
  };

  return { setup, teardown, tests: [
    test("CREATE node with all types", async () => {
      const r = await client.query(
        `CREATE (n:${TABLE} {id: "crud-1", name: "Alice", age: 30, active: true, score: 95.5}) RETURN n.id, n.name, n.age, n.active, n.score`
      );
      assertEq(r.numRows, 1, "should create 1 node");
      assertEq(r.rows[0]["id"], "crud-1");
      assertEq(r.rows[0]["name"], "Alice");
      assertEq(r.rows[0]["age"], 30);
      assertEq(r.rows[0]["active"], true);
      assertEq(r.rows[0]["score"], 95.5);
    }),

    test("MATCH all returns all rows", async () => {
      await client.query(`CREATE (n:${TABLE} {id: "crud-2", name: "Bob", age: 25})`);
      await client.query(`CREATE (n:${TABLE} {id: "crud-3", name: "Charlie", age: 35})`);
      const r = await client.query(`MATCH (n:${TABLE}) RETURN n.id, n.name`);
      assertEq(r.numRows, 3, "should return 3 nodes");
    }),

    test("MATCH with WHERE on STRING", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) WHERE n.name = "Alice" RETURN n.id, n.age`
      );
      assertEq(r.numRows, 1);
      assertEq(r.rows[0]["id"], "crud-1");
    }),

    test("MATCH with WHERE on INT64", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) WHERE n.age > 25 RETURN n.id, n.name ORDER BY n.age`
      );
      assertEq(r.numRows, 2, "2 nodes with age > 25");
      assertEq(r.rows[0]["name"], "Alice");
      assertEq(r.rows[1]["name"], "Charlie");
    }),

    test("MATCH with WHERE on BOOL", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) WHERE n.active = true RETURN n.id`
      );
      assertGt(r.numRows, 0, "active=true nodes are returned");
      // Verify that no null-active nodes appear in results
      for (const row of r.rows) {
        assertNeq(row["id"], "crud-3", "null-active node should not match");
      }
    }),

    test("MATCH with AND/OR conditions", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) WHERE n.age > 20 AND (n.name = "Alice" OR n.name = "Bob") RETURN n.name ORDER BY n.name`
      );
      assertEq(r.numRows, 2);
      assertEq(r.rows[0]["name"], "Alice");
      assertEq(r.rows[1]["name"], "Bob");
    }),

    test("MATCH with NOT", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) WHERE NOT n.name = "Alice" RETURN n.name ORDER BY n.name`
      );
      assertEq(r.numRows, 2);
      assertEq(r.rows[0]["name"], "Bob");
    }),

    test("Empty result set", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) WHERE n.name = "NonExistent" RETURN n.id`
      );
      assertEq(r.numRows, 0, "empty result returns 0 rows");
    }),
  ]};
}
