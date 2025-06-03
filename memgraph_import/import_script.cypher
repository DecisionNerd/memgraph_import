// Memgraph Import Script
// Generated from entities_df conversion
// Compatible with Memgraph (no APOC dependencies)

// Clear existing data (CAREFUL!)
// MATCH (n) DETACH DELETE n;

// Create index for faster lookups
CREATE INDEX ON :Author(id);
CREATE INDEX ON :Book(id);
CREATE INDEX ON :Chapter(id);
CREATE INDEX ON :Chunk(id);
CREATE INDEX ON :Actor(id);
CREATE INDEX ON :Object(id);

// Import Nodes
LOAD CSV WITH HEADERS FROM "file:///nodes.csv" AS row
CALL {
  WITH row
  WITH row.id as nodeId, row.labels as nodeLabel, row.properties as propsJson
  WITH nodeId, nodeLabel, 
       CASE 
         WHEN propsJson IS NULL OR propsJson = '' OR propsJson = '{}' 
         THEN {} 
         ELSE json.loads(propsJson) 
       END as props
  // Create node with dynamic label
  CALL { 
    WITH nodeId, nodeLabel, props
    WITH "CREATE (n:" + nodeLabel + " {id: $nodeId}) SET n += $props RETURN n" as query
    CALL query_module.call(query, {nodeId: nodeId, props: props}) YIELD result
    RETURN result
  }
} IN TRANSACTIONS OF 1000 ROWS;

// Import Relationships  
LOAD CSV WITH HEADERS FROM "file:///relationships.csv" AS row
CALL {
  WITH row
  WITH row.start_id as startId, row.end_id as endId, row.type as relType, row.properties as propsJson
  WITH startId, endId, relType,
       CASE 
         WHEN propsJson IS NULL OR propsJson = '' OR propsJson = '{}' 
         THEN {} 
         ELSE json.loads(propsJson) 
       END as props
  MATCH (start {id: startId})
  MATCH (end {id: endId})
  // Create relationship with dynamic type
  CALL {
    WITH start, end, relType, props
    WITH "CREATE (start)-[r:" + relType + "]->(end) SET r += $props RETURN r" as query
    CALL query_module.call(query, {props: props}) YIELD result
    RETURN result
  }
} IN TRANSACTIONS OF 1000 ROWS;

// Verification queries
MATCH (n) RETURN labels(n) as label, count(n) as count ORDER BY count DESC;
MATCH ()-[r]-() RETURN type(r) as relationship_type, count(r) as count ORDER BY count DESC;
