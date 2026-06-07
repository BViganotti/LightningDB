# Cypher Query Reference

Lightning implements a subset of the Cypher graph query language with extensions for vector search and agent memory operations.

## Supported Features

### Reading Data

```cypher
MATCH (n:Label {prop: value})
MATCH (n:Label) WHERE n.prop > 10
MATCH (n) WHERE n.prop IS NULL
MATCH (n) WHERE n.prop IS NOT NULL
MATCH (n) WHERE n.prop IN [1, 2, 3]
MATCH (n) WHERE n.prop NOT IN [1, 2, 3]
MATCH (n)-[:REL_TYPE]->(m)
MATCH (n)-[r]->(m) WHERE r.weight > 0.5
MATCH p = shortestPath((a)-[*]->(b))
MATCH p = allShortestPaths((a)-[*]->(b))
OPTIONAL MATCH (n)-->(m)
MATCH (n) RETURN n.prop AS alias
MATCH (n) RETURN DISTINCT n.prop
MATCH (n) RETURN count(*), sum(n.val), avg(n.val), min(n.val), max(n.val)
MATCH (n) RETURN n.prop ORDER BY n.prop DESC SKIP 10 LIMIT 5
CASE WHEN n.prop > 10 THEN 'big' ELSE 'small' END
```

### Writing Data

```cypher
CREATE (:Label {prop: value})
CREATE (a)-[:REL {weight: 1.0}]->(b)
MERGE (n:Label {id: $id}) ON CREATE SET n.prop = $val
MATCH (n) SET n.prop = $new_val
MATCH (n) REMOVE n.prop
MATCH (n) SET n = {prop1: val1, prop2: val2}
MATCH (n) SET n += {prop1: val1}
DETACH DELETE n
MATCH (n) DELETE n
```

### DDL

```cypher
CREATE NODE TABLE Name(id INT64, prop STRING, PRIMARY KEY (id))
CREATE NODE TABLE IF NOT EXISTS Name(id INT64, prop STRING, PRIMARY KEY (id))
CREATE REL TABLE Name FROM NodeA TO NodeB (prop INT64)
DROP TABLE Name
DROP TABLE Name IF EXISTS
ALTER TABLE Name ADD prop_name INT64
ALTER TABLE Name DROP prop_name
ALTER TABLE Name RENAME TO new_name
ALTER TABLE Name RENAME COLUMN prop_name TO new_prop
CREATE INDEX ON TableName(column_name)
DROP INDEX index_name
CREATE CONSTRAINT ON TableName ASSERT column_name IS UNIQUE
DROP CONSTRAINT constraint_name
```

### Schema and Procedures

```cypher
CALL db.labels()
CALL db.relationshipTypes()
CALL db.schema()
CALL show_tables()
```

### Filtering

```cypher
WHERE n.prop = $param
WHERE n.prop > 10 AND n.prop2 < 20
WHERE n.prop IN [1, 2, 3]
WHERE n.prop NOT IN [1, 2, 3]
WHERE n.prop IS NULL
WHERE n.prop IS NOT NULL
WHERE n.name CONTAINS 'substr'
WHERE n.name STARTS WITH 'prefix'
WHERE n.name ENDS WITH 'suffix'
WHERE EXISTS { MATCH (n)-[:REL]->(m) }
```

### Expressions

```cypher
n.prop + 1, n.prop * 2, n.prop / 3, n.prop % 2
NOT (n.prop = 1)
(n.prop = 1) XOR (n.prop2 = 2)
CASE WHEN n.prop > 10 THEN 'big' ELSE 'small' END
CAST(n.prop AS STRING)
EXTRACT(YEAR FROM n.date_field)
n.prop IN (subquery)
list[0]
list[0..3]
n.prop IS NULL / n.prop IS NOT NULL
```

### Aggregates

```cypher
COUNT(*), COUNT(n.prop), COUNT(DISTINCT n.prop)
SUM(n.val), AVG(n.val), MIN(n.val), MAX(n.val)
COLLECT(n.prop)
GROUP_CONCAT(n.name)
STDDEV(n.val), STDDEV_SAMP(n.val)
VARIANCE(n.val), VAR_POP(n.val), VAR_SAMP(n.val)
MEDIAN(n.val)
```

### Scalar Functions

```cypher
-- String
UPPER(s), LOWER(s), LENGTH(s), REVERSE(s), TRIM(s), LTRIM(s), RTRIM(s)
SUBSTRING(s, start, len), LEFT(s, len), RIGHT(s, len)
REPLACE(s, from, to), SPLIT(s, delim), CONTAINS(s, sub)
STARTS_WITH(s, prefix), ENDS_WITH(s, suffix)
INITCAP(s), LEVENSHTEIN(a, b)

-- Numeric
ABS(n), CEIL(n), FLOOR(n), ROUND(n, decimals), SQRT(n)
SIGN(n), POWER(base, exp), RANGE(start, end, step)
PI(), E(), PHI(), INFINITY()
RADIANS(n), DEGREES(n)
SIN(n), COS(n), TAN(n), ASIN(n), ACOS(n), ATAN(n), ATAN2(y, x)

-- Bitwise
BIT_AND(a, b), BIT_OR(a, b), BIT_XOR(a, b), BIT_NOT(a)

-- Conditional
COALESCE(a, b, ...), IFNULL(a, b), ISNULL(a, b)
NULLIF(a, b), IF(cond, t, f), IIF(cond, t, f)

-- Date/Time
DATE(s), DATE_PART(field, source), NOW(), CURRENT_DATE, CURRENT_TIMESTAMP
YEAR(d), MONTH(d), DAY(d), HOUR(d), MINUTE(d), SECOND(d)
DATE_ADD(d, n), DATE_SUB(d, n)

-- Type conversion
TO_STRING(v), TO_INT(v), TO_FLOAT(v), TO_BOOL(v)
CAST(v AS type)

-- Structural
LIST_EXTRACT(list, idx), LIST_SLICE(list, start, end)
STRUCT_EXTRACT(struct, field), STRUCT_PACK(key, val, ...)
GEN_RANDOM_UUID()

-- Null checks
IS_NULL(expr), IS_NOT_NULL(expr)
```

### Transactions

```cypher
BEGIN TRANSACTION
COMMIT TRANSACTION
ROLLBACK TRANSACTION
```

### Explain / Profile

```cypher
EXPLAIN MATCH (n) RETURN n
EXPLAIN ANALYZE MATCH (n) RETURN n
PROFILE MATCH (n) RETURN n
```

### Data Import/Export

```cypher
COPY TableName FROM '/path/to/file.csv' (DELIM ',', HEADER true)
COPY TableName TO '/path/to/output.csv' (FORMAT CSV, DELIM ',', HEADER true)
COPY TableName TO '/path/to/output.json' (FORMAT JSON)
```

## Not Yet Supported

- Path qualifiers: `WSHORTEST`, `TRAIL`, `ACYCLIC`
- Window functions (`ROW_NUMBER()`, `RANK()`, `LAG()`, `LEAD()`, `OVER`)
- Pattern comprehension (`[... | ... | ...]`, `[x IN list | ...]`)
- `FOREACH`
- `IN` with subquery (static lists work)
- Multi-label matching (`(n:A:B)`)
- `Decimal`, `Time`, `TimestampTZ`, `UUID`, `Interval` types
- `CASE expr WHEN val THEN ...` (expression form; searched `CASE WHEN cond THEN ...` is supported)
