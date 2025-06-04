# Memgraph Import

A Python package for processing literary texts into knowledge graphs for Memgraph import.

## Features

- Process novel text into structured chunks by chapter
- Generate knowledge graphs from text using Google's Gemini API
- Export knowledge graphs in a format ready for Memgraph import
- Support for various entity types and relationships
- Hierarchical structure (Book → Chapter → Chunk)
- Character analysis and relationship mapping

## Installation

```bash
pip install memgraph-import
```

## Usage

```python
from memgraph_import import process_novel_file, generate_knowledge_graph, export_knowledge_graph

# Process a novel file
novel_data = process_novel_file(
    file_path="path/to/novel.txt",
    author="Mark Twain",
    book="Adventures of Huckleberry Finn"
)

# Generate a knowledge graph
graph = generate_knowledge_graph(
    novel_data=novel_data,
    api_key="your-gemini-api-key"  # Optional if set in GEMINI_API_KEY env var
)

# Export the graph to a JSON file
export_knowledge_graph(
    graph=graph,
    output_path="output/knowledge_graph.json"
)
```

## Knowledge Graph Schema

The package generates knowledge graphs with the following entity types:

- **Actor**: People, organizations, characters, agents
- **Object**: Physical items, tools, documents, artifacts
- **Location**: Places, addresses, geographic areas
- **Event**: Actions, incidents, occurrences, processes
- **Intangible**: Knowledge, concepts, ideas, beliefs
- **Book**: Literary works, novels, publications
- **Author**: Writers, creators of literary works
- **Chapter**: Sections or divisions within books
- **Chunk**: Text segments or passages within chapters

## Memgraph Import

The generated JSON files can be imported into Memgraph using the provided Cypher commands:

```cypher
// Import nodes
CALL json.load_from_file('knowledge_graph.json') YIELD value
UNWIND value.nodes AS node
CREATE (n)
SET n = node.properties
SET n.id = node.id
SET n.name = node.name
SET n.description = node.description
SET n.timestamp = node.timestamp
CALL apoc.create.addLabels(n, [node.label]) YIELD node AS labeled_node
RETURN labeled_node;

// Import relationships
CALL json.load_from_file('knowledge_graph.json') YIELD value
UNWIND value.relationships AS rel
MATCH (start {id: rel.start_id}), (end {id: rel.end_id})
CALL apoc.create.relationship(start, rel.relationship_type,
    {weight: rel.weight, timestamp: rel.timestamp} + coalesce(rel.properties, {}), end)
YIELD rel AS created_rel
RETURN created_rel;
```

## Development

1. Clone the repository
2. Create a virtual environment: `uv venv`
3. Activate the virtual environment: `source .venv/bin/activate`
4. Install development dependencies: `uv sync`
5. Run tests: `pytest`

## License

MIT License - see LICENSE file for details
