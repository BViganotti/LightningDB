import { LightningClient } from "../client.js";
import { test, assertEq, assertThrows } from "../test-utils.js";

export function createDmlSuite(client: LightningClient) {
  const TABLE = "DmlTest";

  const setup = async () => {
    await client.query(
      `CREATE NODE TABLE IF NOT EXISTS ${TABLE} (id STRING, name STRING, age INT64, status STRING, score DOUBLE, PRIMARY KEY(id))`
    );
    await client.query(
      `CREATE (n:${TABLE} {id: "d1", name: "Alice", age: 30, status: "active", score: 95.0}) RETURN n.id`
    );
    await client.query(
      `CREATE (n:${TABLE} {id: "d2", name: "Bob", age: 25, status: "active", score: 80.0}) RETURN n.id`
    );
  };

  const teardown = async () => {
    await client.queryRaw(`MATCH (n:${TABLE}) DELETE n`);
  };

  return { setup, teardown, tests: [
    test("SET property update", async () => {
      const r1 = await client.query(
        `MATCH (n:${TABLE} {id: "d1"}) SET n.age = 31 RETURN n.age`
      );
      assertEq(r1.rows[0]["age"], 31, "age updated to 31");
    }),

    test("SET multiple properties", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE} {id: "d2"}) SET n.status = "inactive", n.score = 85.0 RETURN n.status, n.score`
      );
      assertEq(r.rows[0]["status"], "inactive");
      assertEq(r.rows[0]["score"], 85.0);
    }),

    test("DELETE node", async () => {
      await client.query(
        `MERGE (n:${TABLE} {id: "d-del", name: "DeleteMe", age: 99, status: "temp", score: 0.0}) RETURN n.id`
      );
      const rBefore = await client.query(
        `MATCH (n:${TABLE} {id: "d-del"}) RETURN n.id`
      );
      assertEq(rBefore.numRows, 1, "node exists before delete");

      const rDel = await client.query(
        `MATCH (n:${TABLE} {id: "d-del"}) DELETE n RETURN count(*) AS deleted`
      );
      assertEq(rDel.rows[0]["deleted"], 1, "1 node deleted");

      const rAfter = await client.query(
        `MATCH (n:${TABLE} {id: "d-del"}) RETURN n.id`
      );
      assertEq(rAfter.numRows, 0, "node gone after delete");
    }),

    test("MERGE creates new node", async () => {
      const r = await client.query(
        `MERGE (n:${TABLE} {id: "d4"}) ON CREATE SET n.name = "Diana", n.age = 28 RETURN n.id, n.name`
      );
      assertEq(r.numRows, 1, "MERGE returns 1 row");
      assertEq(r.rows[0]["name"], "Diana");

      const rVerify = await client.query(
        `MATCH (n:${TABLE} {id: "d4"}) RETURN n.name`
      );
      assertEq(rVerify.rows[0]["name"], "Diana", "node persisted");
    }),

    test("MERGE matches existing node", async () => {
      const r = await client.query(
        `MERGE (n:${TABLE} {id: "d1"}) ON MATCH SET n.name = "Alicia" RETURN n.name`
      );
      assertEq(r.rows[0]["name"], "Alicia", "MERGE matched and updated");
    }),

    test("DELETE non-existent node is no-op", async () => {
      const r = await client.query(
        `MATCH (n:${TABLE} {id: "non-existent"}) DELETE n RETURN count(*) AS deleted`
      );
      assertEq(r.rows[0]["deleted"], 0);
    }),

    test("Error on duplicate primary key", async () => {
      await assertThrows(async () => {
        await client.query(
          `MERGE (n:${TABLE} {id: "d1", name: "Dup", age: 99, status: "x", score: 0.0}) RETURN n.id`
        );
      }, "duplicate PK should error (or MERGE should update)");
    }),

    test("CREATE and RETURN expressions", async () => {
      const r = await client.query(
        `CREATE (n:${TABLE} {id: "d5", name: "Eve", age: 32, status: "active", score: 88.0}) RETURN n.id, n.name, n.age`
      );
      assertEq(r.numRows, 1);
      assertEq(r.rows[0]["id"], "d5");
      assertEq(r.rows[0]["name"], "Eve");
      assertEq(r.rows[0]["age"], 32);
    }),
  ]};
}
