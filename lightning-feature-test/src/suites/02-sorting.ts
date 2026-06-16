import { LightningClient } from "../client.js";
import { test, assertEq } from "../test-utils.js";

export function createSortingSuite(client: LightningClient) {
  const TABLE = "SortTest";

  const setup = async () => {
    await client.query(
      `CREATE NODE TABLE IF NOT EXISTS ${TABLE} (id STRING, name STRING, age INT64, salary DOUBLE, city STRING, PRIMARY KEY(id))`
    );
    for (const [id, name, age, salary, city] of [
      ["s1", "Alice", 30, 100000, "NYC"],
      ["s2", "Bob", 25, 80000, "LA"],
      ["s3", "Charlie", 35, 120000, "NYC"],
      ["s4", "Diana", 28, 95000, "SF"],
      ["s5", "Eve", 32, 110000, "LA"],
    ] as const) {
      await client.query(
        `CREATE (n:${TABLE} {id: "${id}", name: "${name}", age: ${age}, salary: ${salary}, city: "${city}"})`
      );
    }
  };

  const teardown = async () => {
    await client.queryRaw(`MATCH (n:${TABLE}) DELETE n`);
  };

  const vals = (rows: Record<string, unknown>[], col: string): unknown[] =>
    rows.map((r) => r[col]);

  return { setup, teardown, tests: [
    test("ORDER BY STRING ASC", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) RETURN n.name ORDER BY n.name`
      );
      assertEq(r.data.numRows, 5);
      assertEq(JSON.stringify(vals(r.data.rows, "name")),
        '["Alice","Bob","Charlie","Diana","Eve"]');
    }),

    test("ORDER BY STRING DESC", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) RETURN n.name ORDER BY n.name DESC`
      );
      assertEq(JSON.stringify(vals(r.data.rows, "name")),
        '["Eve","Diana","Charlie","Bob","Alice"]');
    }),

    test("ORDER BY INT64 ASC", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) RETURN n.name, n.age ORDER BY n.age`
      );
      const ages = vals(r.data.rows, "age");
      assertEq(JSON.stringify(ages), "[25,28,30,32,35]");
    }),

    test("ORDER BY INT64 DESC", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) RETURN n.name, n.age ORDER BY n.age DESC`
      );
      const ages = vals(r.data.rows, "age");
      assertEq(JSON.stringify(ages), "[35,32,30,28,25]");
    }),

    test("ORDER BY DOUBLE ASC", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) RETURN n.name, n.salary ORDER BY n.salary`
      );
      const salaries = vals(r.data.rows, "salary");
      assertEq(JSON.stringify(salaries), "[80000,95000,100000,110000,120000]");
    }),

    test("ORDER BY DOUBLE DESC", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) RETURN n.name, n.salary ORDER BY n.salary DESC`
      );
      const salaries = vals(r.data.rows, "salary");
      assertEq(JSON.stringify(salaries), "[120000,110000,100000,95000,80000]");
    }),

    test("ORDER BY multiple columns", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) RETURN n.city, n.name ORDER BY n.city, n.name`
      );
      assertEq(r.data.numRows, 5);
      const pairs = r.data.rows.map((r) => `${r["city"]}:${r["name"]}`);
      assertEq(JSON.stringify(pairs),
        '["LA:Bob","LA:Eve","NYC:Alice","NYC:Charlie","SF:Diana"]');
    }),

    test("Second ORDER BY execution (cache safety)", async () => {
      const r1 = await client.query(
        `MATCH (n:${TABLE}) RETURN n.name ORDER BY n.name`
      );
      const r2 = await client.query(
        `MATCH (n:${TABLE}) RETURN n.name ORDER BY n.name`
      );
      assertEq(r1.data.numRows, 5, "first call returns 5");
      assertEq(r2.data.numRows, 5, "second call returns 5 (no cache corruption)");
    }),

    test("ORDER BY on primary key field", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) RETURN n.id ORDER BY n.id`
      );
      assertEq(JSON.stringify(vals(r.data.rows, "id")),
        '["s1","s2","s3","s4","s5"]');
    }),
  ]};
}
