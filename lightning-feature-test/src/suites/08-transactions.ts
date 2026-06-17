import { LightningClient } from "../client.js";
import { test, assertEq } from "../test-utils.js";

export function createTransactionsSuite(client: LightningClient) {
  const TABLE = "TxnTest";

  const setup = async () => {
    await client.query(
      `CREATE NODE TABLE IF NOT EXISTS ${TABLE} (id STRING, name STRING, value INT64, PRIMARY KEY(id))`
    );
  };

  const teardown = async () => {
    await client.queryRaw(`MATCH (n:${TABLE}) DELETE n`);
  };

  return { setup, teardown, tests: [
    test("CREATE with autocommit persists data", async () => {
      await client.query(`CREATE (n:${TABLE} {id: "t1", name: "persist-test", value: 42})`);
      const r = await client.query(
        `MATCH (n:${TABLE} {id: "t1"}) RETURN n.value`
      );
      assertEq(r.rows[0]["value"], 42);
    }),

    test("MATCH returns after autocommit CREATE", async () => {
      await client.query(`CREATE (n:${TABLE} {id: "t2", name: "second", value: 99})`);
      const r = await client.query(`MATCH (n:${TABLE}) RETURN count(*) AS cnt`);
      assertEq(r.rows[0]["cnt"], 2);
    }),

    test("Multiple CREATEs in sequence", async () => {
      for (let i = 0; i < 5; i++) {
        await client.query(
          `CREATE (n:${TABLE} {id: "txn-batch-${i}", name: "batch", value: ${i}})`
        );
      }
      const r = await client.query(
        `MATCH (n:${TABLE}) WHERE n.name = "batch" RETURN count(*) AS cnt`
      );
      assertEq(r.rows[0]["cnt"], 5);
    }),

    test("MATCH after DELETE", async () => {
      await client.query(`MATCH (n:${TABLE} {id: "t1"}) DELETE n`);
      const r = await client.query(
        `MATCH (n:${TABLE} {id: "t1"}) RETURN n.id`
      );
      assertEq(r.numRows, 0, "deleted node is gone");
    }),

    test("BEGIN/COMMIT explicit transaction", async () => {
      await client.query(`BEGIN`);
      await client.query(`CREATE (n:${TABLE} {id: "t3", name: "explicit-txn", value: 100})`);
      await client.query(`COMMIT`);
      const r = await client.query(`MATCH (n:${TABLE} {id: "t3"}) RETURN n.value`);
      assertEq(r.rows[0]["value"], 100);
    }),

    test("BEGIN/ROLLBACK discards changes", async () => {
      await client.query(`BEGIN`);
      await client.query(`CREATE (n:${TABLE} {id: "t4", name: "rollback-test", value: 200})`);
      await client.query(`ROLLBACK`);
      const r = await client.query(`MATCH (n:${TABLE} {id: "t4"}) RETURN n.id`);
      assertEq(r.numRows, 0, "rolled-back node should not exist");
    }),

    test("ROLLBACK after multiple CREATEs", async () => {
      await client.query(`BEGIN`);
      for (let i = 0; i < 3; i++) {
        await client.query(
          `CREATE (n:${TABLE} {id: "rb-batch-${i}", name: "rollback-batch", value: ${i}})`
        );
      }
      await client.query(`ROLLBACK`);
      const r = await client.query(
        `MATCH (n:${TABLE}) WHERE n.name = "rollback-batch" RETURN count(*) AS cnt`
      );
      assertEq(r.rows[0]["cnt"], 0);
    }),
  ]};
}
      const r = await client.query(
        `MATCH (n:${TABLE}) WHERE n.name = "batch" RETURN count(*) AS cnt`
      );
      assertEq(r.rows[0]["cnt"], 5);
    }),

    test("MATCH after DELETE", async () => {
      await client.query(`MATCH (n:${TABLE} {id: "t1"}) DELETE n`);
      const r = await client.query(
        `MATCH (n:${TABLE} {id: "t1"}) RETURN n.id`
      );
      assertEq(r.numRows, 0, "deleted node is gone");
    }),
  ]};
}
