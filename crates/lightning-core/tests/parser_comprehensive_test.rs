use lightning_core::parser::parse;

fn test_parse(query: &str) {
    let ast = parse(query).unwrap();
    assert_eq!(ast.union_queries.len(), 1, "Expected 1 union query for: {query}");
}

#[test]
fn test_parse_simple_match() {
    test_parse("MATCH (n:Person) RETURN n.name");
}

#[test]
fn test_parse_match_with_where() {
    test_parse("MATCH (n:Person) WHERE n.age > 30 RETURN n.name");
}

#[test]
fn test_parse_create_node() {
    test_parse("CREATE (:Person {name: 'Alice', age: 30})");
}

#[test]
fn test_parse_merge() {
    test_parse("MERGE (n:Person {name: 'Alice'}) RETURN n.name");
}

#[test]
fn test_parse_unwind() {
    test_parse("UNWIND [1, 2, 3] AS x RETURN x");
}

#[test]
fn test_parse_with() {
    test_parse("MATCH (n:Person) WITH n.name AS name RETURN name");
}

#[test]
fn test_parse_order_by_asc() {
    test_parse("MATCH (n:Person) RETURN n.name ORDER BY n.name ASC");
}

#[test]
fn test_parse_order_by_desc() {
    test_parse("MATCH (n:Person) RETURN n.name ORDER BY n.name DESC");
}

#[test]
fn test_parse_order_by_multiple() {
    test_parse("MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age DESC, n.name ASC");
}

#[test]
fn test_parse_limit() {
    test_parse("MATCH (n:Person) RETURN n.name LIMIT 10");
}

#[test]
fn test_parse_skip() {
    test_parse("MATCH (n:Person) RETURN n.name SKIP 5");
}

#[test]
fn test_parse_skip_limit() {
    test_parse("MATCH (n:Person) RETURN n.name SKIP 5 LIMIT 10");
}

#[test]
fn test_parse_where_and() {
    test_parse("MATCH (n:Person) WHERE n.age > 25 AND n.name = 'Bob' RETURN n.name");
}

#[test]
fn test_parse_where_or() {
    test_parse("MATCH (n:Person) WHERE n.age < 20 OR n.age > 60 RETURN n.name");
}

#[test]
fn test_parse_where_not() {
    test_parse("MATCH (n:Person) WHERE NOT n.name = 'Alice' RETURN n.name");
}

#[test]
fn test_parse_where_parentheses() {
    test_parse("MATCH (n:Person) WHERE (n.age > 25 AND n.name = 'Bob') OR n.age < 20 RETURN n.name");
}

#[test]
fn test_parse_arithmetic_add() {
    test_parse("MATCH (n:Person) RETURN n.age + 5");
}

#[test]
fn test_parse_arithmetic_sub() {
    test_parse("MATCH (n:Person) RETURN n.age - 5");
}

#[test]
fn test_parse_arithmetic_mul() {
    test_parse("MATCH (n:Person) RETURN n.age * 2");
}

#[test]
fn test_parse_arithmetic_div() {
    test_parse("MATCH (n:Person) RETURN n.age / 2");
}

#[test]
fn test_parse_arithmetic_complex() {
    test_parse("MATCH (n:Person) RETURN (n.age + 5) * 2 - 3 / n.age");
}

#[test]
fn test_parse_list_literal() {
    test_parse("MATCH (n:Person) WHERE n.id IN [1, 2, 3] RETURN n.name");
}

#[test]
fn test_parse_starts_with() {
    test_parse("MATCH (n:Person) WHERE n.name STARTS WITH 'A' RETURN n.name");
}

#[test]
fn test_parse_ends_with() {
    test_parse("MATCH (n:Person) WHERE n.name ENDS WITH 'Z' RETURN n.name");
}

#[test]
fn test_parse_contains() {
    test_parse("MATCH (n:Person) WHERE n.name CONTAINS 'li' RETURN n.name");
}

#[test]
fn test_parse_create_node_table() {
    test_parse("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY (name))");
}

#[test]
fn test_parse_create_rel_table() {
    test_parse("CREATE REL TABLE Knows(FROM Person TO Person, since INT64)");
}

#[test]
fn test_parse_drop_table() {
    test_parse("DROP TABLE Person");
}

#[test]
fn test_parse_set_property() {
    test_parse("MATCH (n:Person) WHERE n.name = 'Alice' SET n.age = 31");
}

#[test]
fn test_parse_delete_node() {
    test_parse("MATCH (n:Person) WHERE n.name = 'Alice' DELETE n");
}

#[test]
fn test_parse_detach_delete() {
    test_parse("MATCH (n:Person) WHERE n.name = 'Alice' DETACH DELETE n");
}

#[test]
fn test_parse_relationship_directed() {
    test_parse("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name");
}

#[test]
fn test_parse_relationship_undirected() {
    test_parse("MATCH (a:Person)-[:KNOWS]-(b:Person) RETURN a.name, b.name");
}

#[test]
fn test_parse_variable_length_relationship() {
    test_parse("MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN a.name, b.name");
}

#[test]
fn test_parse_optional_match() {
    test_parse("MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b:Person) RETURN a.name, b.name");
}

#[test]
fn test_parse_multiple_matches() {
    test_parse("MATCH (a:Person), (b:Person), (c:Person) RETURN a.name, b.name, c.name");
}

#[test]
fn test_parse_aggregate_count() {
    test_parse("MATCH (n:Person) RETURN count(n.name)");
}

#[test]
fn test_parse_aggregate_sum() {
    test_parse("MATCH (n:Person) RETURN sum(n.age)");
}

#[test]
fn test_parse_aggregate_avg() {
    test_parse("MATCH (n:Person) RETURN avg(n.age)");
}

#[test]
fn test_parse_aggregate_min_max() {
    test_parse("MATCH (n:Person) RETURN min(n.age), max(n.age)");
}

#[test]
fn test_parse_aggregate_distinct() {
    test_parse("MATCH (n:Person) RETURN count(DISTINCT n.city)");
}

#[test]
fn test_parse_parameterized_query() {
    test_parse("MATCH (n:Person) WHERE n.name = $name RETURN n.age");
}

#[test]
fn test_parse_float_literals() {
    test_parse("MATCH (n:Person) WHERE n.score > 3.14159 RETURN n.name");
}

#[test]
fn test_parse_negative_numbers() {
    test_parse("MATCH (n:Person) WHERE n.temperature > -10 RETURN n.name");
}

#[test]
fn test_parse_match_return_literal() {
    test_parse("MATCH (n:Person) RETURN 42, 'hello', true, false, null");
}

#[test]
fn test_parse_match_return_aliased() {
    test_parse("MATCH (n:Person) RETURN n.name AS person_name, n.age AS person_age");
}

#[test]
fn test_parse_complex_property_path() {
    test_parse("MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.since > 2020 RETURN a.name, b.name, r.since");
}

#[test]
fn test_parse_create_rel_with_properties() {
    test_parse("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS {since: 2020, source: 'work'}]->(b)");
}

#[test]
fn test_parse_boolean_literals() {
    test_parse("MATCH (n:Person) WHERE n.is_active = true AND n.is_admin = false RETURN n.name");
}
