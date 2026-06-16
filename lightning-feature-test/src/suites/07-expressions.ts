import { LightningClient } from "../client.js";
import { test, assertEq } from "../test-utils.js";

export function createExpressionsSuite(client: LightningClient) {
  const TABLE = "ExprTest";

  const setup = async () => {
    await client.query(
      `CREATE NODE TABLE IF NOT EXISTS ${TABLE} (id STRING, name STRING, age INT64, salary DOUBLE, score DOUBLE, PRIMARY KEY(id))`
    );
    await client.query(`CREATE (n:${TABLE} {id: "e1", name: "Alice", age: 30, salary: 100000, score: 95.5}) RETURN n.id`);
    await client.query(`CREATE (n:${TABLE} {id: "e2", name: "Bob", age: 25, salary: 80000, score: 85.0}) RETURN n.id`);
    await client.query(`CREATE (n:${TABLE} {id: "e3", name: "Charlie", age: 35, salary: 120000, score: 90.0}) RETURN n.id`);
  };

  const teardown = async () => {
    await client.queryRaw(`MATCH (n:${TABLE}) DELETE n`);
  };

  return { setup, teardown, tests: [
    test("Arithmetic in RETURN", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) RETURN n.name, n.salary / 12 AS monthly ORDER BY n.name LIMIT 1`
      );
      assertEq(r.data.rows[0]["name"], "Alice");
      const monthly = r.data.rows[0]["monthly"] as number;
      assertEq(Math.round(monthly), 8333);
    }),

    test("Comparison in WHERE with range pattern", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) WHERE n.age >= 28 AND n.age <= 32 RETURN n.name`
      );
      assertEq(r.data.numRows, 1);
      assertEq(r.data.rows[0]["name"], "Alice");
    }),

    test("ORDER BY with LIMIT 1", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) RETURN n.name ORDER BY n.name LIMIT 1`
      );
      assertEq(r.data.numRows, 1);
      assertEq(r.data.rows[0]["name"], "Alice");
    }),

    test("ORDER BY DESC with LIMIT 1", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) RETURN n.name ORDER BY n.name DESC LIMIT 1`
      );
      assertEq(r.data.rows[0]["name"], "Charlie");
    }),
  ]};
}
