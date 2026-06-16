import { LightningClient } from "../client.js";
import { test, assertEq } from "../test-utils.js";

export function createAggregationSuite(client: LightningClient) {
  const TABLE = "AggTest";

  const setup = async () => {
    await client.query(
      `CREATE NODE TABLE IF NOT EXISTS ${TABLE} (id STRING, dept STRING, salary DOUBLE, age INT64, active BOOL, PRIMARY KEY(id))`
    );
    const data = [
      ["a0", "Engineering", 120000, 30, true],
      ["a1", "Engineering", 95000, 25, true],
      ["a2", "Marketing", 80000, 35, true],
      ["a3", "Marketing", 110000, 28, true],
      ["a4", "Sales", 70000, 32, false],
    ] as const;
    for (const [id, dept, salary, age, active] of data) {
      await client.query(
        `CREATE (n:${TABLE} {id: "${id}", dept: "${dept}", salary: ${salary}, age: ${age}, active: ${active}})`
      );
    }
  };

  const teardown = async () => {
    await client.queryRaw(`MATCH (n:${TABLE}) DELETE n`);
  };

  return { setup, teardown, tests: [
    test("COUNT(*)", async () => {
      const r = await client.query(`MATCH (n:${TABLE}) RETURN count(*) AS cnt`);
      assertEq(r.data.rows[0]["cnt"], 5);
    }),

    test("COUNT(property)", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) RETURN count(n.salary) AS cnt`
      );
      assertEq(r.data.rows[0]["cnt"], 5);
    }),

    test("COUNT with WHERE", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) WHERE n.age > 28 RETURN count(*) AS cnt`
      );
      assertEq(r.data.rows[0]["cnt"], 3);
    }),

    test("SUM", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) RETURN sum(n.salary) AS total`
      );
      assertEq(r.data.rows[0]["total"], 475000);
    }),

    test("AVG", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) RETURN avg(n.salary) AS avg_sal`
      );
      const avg = r.data.rows[0]["avg_sal"] as number;
      assertEq(Math.round(avg), 95000);
    }),

    test("MIN", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) RETURN min(n.salary) AS min_sal`
      );
      assertEq(r.data.rows[0]["min_sal"], 70000);
    }),

    test("MAX", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) RETURN max(n.salary) AS max_sal`
      );
      assertEq(r.data.rows[0]["max_sal"], 120000);
    }),

    test("GROUP BY single column", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) RETURN n.dept, count(*) AS cnt ORDER BY n.dept`
      );
      assertEq(r.data.numRows, 3, "3 departments");
      const deptCounts: Record<string, number> = {};
      for (const row of r.data.rows) {
        deptCounts[row["dept"] as string] = row["cnt"] as number;
      }
      assertEq(deptCounts["Engineering"], 2);
      assertEq(deptCounts["Marketing"], 2);
      assertEq(deptCounts["Sales"], 1);
    }),

    test("GROUP BY with SUM", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) RETURN n.dept, sum(n.salary) AS total ORDER BY total DESC`
      );
      assertEq(r.data.rows[0]["dept"], "Engineering");
      assertEq(r.data.rows[0]["total"], 215000);
    }),

    test("COUNT with alias", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) WHERE n.active = true RETURN count(*) AS active_count`
      );
      assertEq(r.data.rows[0]["active_count"], 4);
    }),

    test("COUNT(DISTINCT expr)", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE}) RETURN count(DISTINCT n.dept) AS dept_count`
      );
      assertEq(r.data.rows[0]["dept_count"], 3);
    }),
  ]};
}
