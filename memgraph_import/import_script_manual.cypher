// Simple Memgraph Import Script
// Execute these queries one by one in Memgraph Lab

// Clear existing data (CAREFUL!)
// MATCH (n) DETACH DELETE n;

// Create indexes for performance
CREATE INDEX ON :Author(id);
CREATE INDEX ON :Book(id); 
CREATE INDEX ON :Chapter(id);
CREATE INDEX ON :Chunk(id);
CREATE INDEX ON :Actor(id);
CREATE INDEX ON :Object(id);

// Import Authors
LOAD CSV WITH HEADERS FROM "file:///nodes.csv" AS row
WHERE row.labels = 'Author'
WITH row, json.loads(row.properties) as props
CREATE (n:Author {id: row.id})
SET n += props;

// Import Books
LOAD CSV WITH HEADERS FROM "file:///nodes.csv" AS row
WHERE row.labels = 'Book'
WITH row, json.loads(row.properties) as props
CREATE (n:Book {id: row.id})
SET n += props;

// Import Chapters
LOAD CSV WITH HEADERS FROM "file:///nodes.csv" AS row
WHERE row.labels = 'Chapter'
WITH row, json.loads(row.properties) as props
CREATE (n:Chapter {id: row.id})
SET n += props;

// Import Chunks
LOAD CSV WITH HEADERS FROM "file:///nodes.csv" AS row
WHERE row.labels = 'Chunk'
WITH row, json.loads(row.properties) as props
CREATE (n:Chunk {id: row.id})
SET n += props;

// Import Actors
LOAD CSV WITH HEADERS FROM "file:///nodes.csv" AS row
WHERE row.labels = 'Actor'
WITH row, json.loads(row.properties) as props
CREATE (n:Actor {id: row.id})
SET n += props;

// Import Objects
LOAD CSV WITH HEADERS FROM "file:///nodes.csv" AS row
WHERE row.labels = 'Object'
WITH row, json.loads(row.properties) as props
CREATE (n:Object {id: row.id})
SET n += props;

// Import other node types (add as needed)
LOAD CSV WITH HEADERS FROM "file:///nodes.csv" AS row
WHERE row.labels NOT IN ['Author', 'Book', 'Chapter', 'Chunk', 'Actor', 'Object']
WITH row, json.loads(row.properties) as props, row.labels as label
CALL {
  WITH row, props, label
  WITH "CREATE (n:" + label + " {id: $id}) SET n += $props" as query
  CALL query_module.call(query, {id: row.id, props: props}) YIELD result
  RETURN result
};

// Import WRITTEN_BY relationships
LOAD CSV WITH HEADERS FROM "file:///relationships.csv" AS row
WHERE row.type = 'WRITTEN_BY'
WITH row, json.loads(row.properties) as props
MATCH (start {id: row.start_id})
MATCH (end {id: row.end_id})
CREATE (start)-[r:WRITTEN_BY]->(end)
SET r += props;

// Import PART_OF relationships
LOAD CSV WITH HEADERS FROM "file:///relationships.csv" AS row
WHERE row.type = 'PART_OF'
WITH row, json.loads(row.properties) as props
MATCH (start {id: row.start_id})
MATCH (end {id: row.end_id})
CREATE (start)-[r:PART_OF]->(end)
SET r += props;

// Import MENTIONS relationships
LOAD CSV WITH HEADERS FROM "file:///relationships.csv" AS row
WHERE row.type = 'MENTIONS'
WITH row, json.loads(row.properties) as props
MATCH (start {id: row.start_id})
MATCH (end {id: row.end_id})
CREATE (start)-[r:MENTIONS]->(end)
SET r += props;

// Import REFERENCES relationships
LOAD CSV WITH HEADERS FROM "file:///relationships.csv" AS row
WHERE row.type = 'REFERENCES'
WITH row, json.loads(row.properties) as props
MATCH (start {id: row.start_id})
MATCH (end {id: row.end_id})
CREATE (start)-[r:REFERENCES]->(end)
SET r += props;

// Import other relationship types
LOAD CSV WITH HEADERS FROM "file:///relationships.csv" AS row
WHERE row.type NOT IN ['WRITTEN_BY', 'PART_OF', 'MENTIONS', 'REFERENCES']
WITH row, json.loads(row.properties) as props, row.type as relType
MATCH (start {id: row.start_id})
MATCH (end {id: row.end_id})
CALL {
  WITH start, end, props, relType
  WITH "CREATE (start)-[r:" + relType + "]->(end) SET r += $props" as query
  CALL query_module.call(query, {props: props}) YIELD result
  RETURN result
};

// Verification and sample queries
MATCH (n) RETURN labels(n) as label, count(n) as count ORDER BY count DESC;
MATCH ()-[r]-() RETURN type(r) as relationship_type, count(r) as count ORDER BY count DESC;
MATCH (a:Author) RETURN a.name, a.book LIMIT 5;
MATCH (b:Book)-[r:WRITTEN_BY]->(a:Author) RETURN b.name, a.name LIMIT 5;
MATCH (c:Chapter)-[r:PART_OF]->(b:Book) RETURN c.name, b.name LIMIT 10;
