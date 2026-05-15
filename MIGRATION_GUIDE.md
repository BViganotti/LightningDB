# Migration Guide

## From SQLite

```sql
-- SQLite
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
SELECT * FROM users WHERE id = 1;
```

```cypher
-- Lightning
CREATE NODE TABLE User(id INT64, name STRING, email STRING, PRIMARY KEY (id));
CREATE (:User {id: 1, name: 'Alice', email: 'alice@example.com'});
MATCH (u:User {id: 1}) RETURN u.name, u.email;
```

## From PostgreSQL

```sql
-- PostgreSQL
CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);
CREATE TABLE posts (id SERIAL PRIMARY KEY, user_id INT REFERENCES users(id), title TEXT);
SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id;
```

```cypher
-- Lightning
CREATE NODE TABLE User(id INT64, name STRING, PRIMARY KEY (id));
CREATE REL TABLE Posted FROM User TO User (title STRING);
MATCH (u:User)-[:Posted]->(p:User) RETURN u.name, p.title;
```

## From Neo4j

Most Cypher queries are compatible. Key differences:

| Feature | Neo4j | Lightning |
|---------|-------|-----------|
| Schema | Implicit labels | Explicit `CREATE NODE TABLE` / `CREATE REL TABLE` |
| Types | Dynamic | Static: `INT64`, `STRING`, `DOUBLE`, `BOOL`, `FLOAT`, `DATE`, `TIMESTAMP` |
| Indexes | Automatic | Primary key creates hash index. FTS/vector indexes created per-table |
| `DETACH DELETE` | Supported | Supported |
| `MERGE` | ON CREATE SET, ON MATCH SET | Same |
| Procedures | `CALL db.labels()` | Same + `CALL db.schema()`, `CALL db.relationshipTypes()` |
| Import | `LOAD CSV` | `COPY table FROM 'file.csv' (DELIM ',', HEADER true)` |
| Export | `apoc.export.csv` | `COPY table TO 'file.csv' (FORMAT CSV)` |
| Path queries | `MATCH p = (a)-[*]->(b)` | Supported (path not returned as variable) |

## Version Migration

### From v0.1 (pre-alpha) to current

The WAL format now includes:
- Header magic (`LNIW` + version byte)
- CRC32 checksums on every record
- 8-byte aligned records

Old WAL files without headers will be rejected. To migrate:
1. Export all data via `COPY table TO 'backup.csv'`
2. Create a fresh database
3. Import via `COPY table FROM 'backup.csv'`
