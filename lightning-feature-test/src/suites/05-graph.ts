import { LightningClient } from "../client.js";
import { test, assertEq, assertGt } from "../test-utils.js";

export function createGraphSuite(client: LightningClient) {
  const PERSON = "GraphPerson";
  const KNOWS = "Knows";

  const setup = async () => {
    await client.query(
      `CREATE NODE TABLE IF NOT EXISTS ${PERSON} (id STRING, name STRING, age INT64, PRIMARY KEY(id))`
    );
    await client.query(
      `CREATE REL TABLE IF NOT EXISTS ${KNOWS} (FROM ${PERSON} TO ${PERSON}, since INT64, weight DOUBLE)`
    );
    const people = [
      ["gp-1", "Alice", 30],
      ["gp-2", "Bob", 25],
      ["gp-3", "Charlie", 35],
      ["gp-4", "Diana", 28],
      ["gp-5", "Eve", 32],
    ] as const;
    for (const [id, name, age] of people) {
      await client.query(`CREATE (n:${PERSON} {id: "${id}", name: "${name}", age: ${age}})`);
    }
    const rels: [string, string, number, number][] = [
      ["gp-1", "gp-2", 2020, 0.9],
      ["gp-1", "gp-3", 2019, 0.8],
      ["gp-2", "gp-3", 2021, 0.7],
      ["gp-2", "gp-4", 2022, 0.6],
      ["gp-3", "gp-4", 2020, 0.5],
      ["gp-3", "gp-5", 2021, 0.85],
      ["gp-4", "gp-5", 2023, 0.75],
    ];
    for (const [src, dst, since, weight] of rels) {
      await client.query(
        `MATCH (a:${PERSON} {id: "${src}"}), (b:${PERSON} {id: "${dst}"}) ` +
        `CREATE (a)-[r:${KNOWS} {since: ${since}, weight: ${weight}}]->(b)`
      );
    }
  };

  const teardown = async () => {
    await client.queryRaw(`MATCH (n:${PERSON})-[r:${KNOWS}]->() DELETE r`);
    await client.queryRaw(`MATCH (n:${PERSON}) DELETE n`);
  };

  return { setup, teardown, tests: [
    test("MATCH with direct relationship", async () => {
      const r = await client.query(
        `MATCH (a:${PERSON} {name: "Alice"})-[r:${KNOWS}]->(b:${PERSON}) RETURN b.name ORDER BY b.name`
      );
      assertEq(r.numRows, 2, "Alice knows 2 people");
      const names = r.rows.map((r) => r["name"]);
      assertEq(JSON.stringify(names), '["Bob","Charlie"]');
    }),

    test("MATCH reverse relationship", async () => {
      const r = await client.query(
        `MATCH (a:${PERSON})<-[r:${KNOWS}]-(b:${PERSON} {name: "Alice"}) RETURN a.name ORDER BY a.name`
      );
      assertEq(r.numRows, 0, "nobody knows Alice");
    }),

    test("CREATE REL between existing nodes", async () => {
      await client.query(
        `MATCH (a:${PERSON} {id: "gp-4"}), (b:${PERSON} {id: "gp-1"}) ` +
        `CREATE (a)-[r:${KNOWS} {since: 2024, weight: 0.95}]->(b)`
      );
      const r = await client.query(
        `MATCH (a:${PERSON} {name: "Diana"})-[r:${KNOWS}]->(b:${PERSON}) RETURN b.name`
      );
      assertEq(r.numRows, 2, "Diana knows 2 people now");
    }),
  ]};
}
