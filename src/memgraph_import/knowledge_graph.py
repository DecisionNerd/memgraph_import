"""
Knowledge graph generation and export functionality.
"""

import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path
from .models import KnowledgeGraph, Node, Relationship, NovelData, NovelChunk
from .gemini_client import GeminiClient


def generate_knowledge_graph(
    novel_data: NovelData, api_key: Optional[str] = None
) -> KnowledgeGraph:
    """
    Generate a knowledge graph from novel data.

    Args:
        novel_data: NovelData object containing the novel chunks
        api_key: Optional Gemini API key

    Returns:
        KnowledgeGraph object containing the generated knowledge graph
    """
    client = GeminiClient(api_key=api_key)

    # Process each chunk and combine the results
    all_nodes: List[Node] = []
    all_relationships: List[Relationship] = []
    node_id_counter = 1

    for chunk in novel_data.chunks:
        chunk_graph = client.generate_knowledge_graph(chunk)

        # Update node IDs to be globally unique
        node_id_map = {}
        for node in chunk_graph.nodes:
            new_id = node_id_counter
            node_id_map[node.id] = new_id
            node.id = new_id
            node_id_counter += 1
            all_nodes.append(node)

        # Update relationship IDs to match new node IDs
        for rel in chunk_graph.relationships:
            rel.start_id = node_id_map[rel.start_id]
            rel.end_id = node_id_map[rel.end_id]
            all_relationships.append(rel)

    # Create metadata
    metadata = {
        "generated_at": datetime.utcnow().isoformat(),
        "total_nodes": len(all_nodes),
        "total_relationships": len(all_relationships),
        "entity_types": list(set(node.label.value for node in all_nodes)),
        "book": novel_data.book,
        "author": novel_data.author,
    }

    return KnowledgeGraph(
        metadata=metadata, nodes=all_nodes, relationships=all_relationships
    )


def export_knowledge_graph(graph: KnowledgeGraph, output_path: str) -> None:
    """
    Export a knowledge graph to a JSON file.

    Args:
        graph: KnowledgeGraph object to export
        output_path: Path where the JSON file should be saved
    """
    # Convert the graph to a dictionary
    graph_dict = {
        "metadata": graph.metadata,
        "nodes": [
            {
                "id": node.id,
                "label": node.label.value,
                "name": node.name,
                "description": node.description,
                "properties": node.properties,
                "timestamp": node.timestamp.isoformat(),
            }
            for node in graph.nodes
        ],
        "relationships": [
            {
                "start_id": rel.start_id,
                "end_id": rel.end_id,
                "relationship_type": rel.relationship_type,
                "weight": rel.weight,
                "properties": rel.properties,
                "timestamp": rel.timestamp.isoformat(),
            }
            for rel in graph.relationships
        ],
    }

    # Create output directory if it doesn't exist
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Write to file
    with open(output_path, "w") as f:
        json.dump(graph_dict, f, indent=2)


def generate_memgraph_import_commands(graph_path: str) -> str:
    """
    Generate Memgraph import commands for a knowledge graph JSON file.

    Args:
        graph_path: Path to the knowledge graph JSON file

    Returns:
        String containing the Cypher commands for importing the graph
    """
    return f"""
    // Import nodes from JSON
    CALL json.load_from_file('{graph_path}') YIELD value
    UNWIND value.nodes AS node
    CREATE (n)
    SET n = node.properties
    SET n.id = node.id
    SET n.name = node.name
    SET n.description = node.description
    SET n.timestamp = node.timestamp
    CALL apoc.create.addLabels(n, [node.label]) YIELD node AS labeled_node
    RETURN labeled_node;

    // Import relationships from JSON
    CALL json.load_from_file('{graph_path}') YIELD value
    UNWIND value.relationships AS rel
    MATCH (start {{id: rel.start_id}}), (end {{id: rel.end_id}})
    CALL apoc.create.relationship(start, rel.relationship_type, 
        {{weight: rel.weight, timestamp: rel.timestamp}} + coalesce(rel.properties, {{}}), end)
    YIELD rel AS created_rel
    RETURN created_rel;
    """
