"""Utility functions for processing knowledge graph JSON strings.
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


def generate_deterministic_uuid(
    label: str, name: Optional[str] = None, node_id: Optional[int] = None
) -> str:
    """Generate a deterministic UUID using label and name or node id."""
    namespace = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
    if name:
        combined = f"{label}:{name}"
    elif node_id is not None:
        combined = f"{label}:node_{node_id}"
    else:
        combined = f"{label}:unnamed"
    return str(uuid.uuid5(namespace, combined))


def clean_json_string(json_str: str) -> str:
    """Attempt to clean up common JSON escape issues."""
    try:
        cleaned = re.sub(r"\\\\(?![\"\\\\/bfnrt]|u[0-9a-fA-F]{4})", r"\\\\\\", json_str)
        return cleaned
    except Exception:
        return json_str


def convert_nodes_to_uuid(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Convert node ids in a list of KG chunks to UUIDs."""
    converted: List[Dict[str, Any]] = []
    for chunk in data:
        mapping: Dict[Any, str] = {}
        new_nodes = []
        for node in chunk.get("nodes", []):
            old_id = node.get("id")
            label = node.get("label", "Unknown")
            name = node.get("name")
            new_id = generate_deterministic_uuid(label, name, old_id)
            if old_id is not None:
                mapping[old_id] = new_id
            new_node = dict(node)
            new_node["id"] = new_id
            if not new_node.get("name"):
                new_node["name"] = f"{label}_{old_id}" if old_id else label
            new_nodes.append(new_node)
        new_rels = []
        for rel in chunk.get("relationships", []):
            new_rel = dict(rel)
            if rel.get("start_id") in mapping:
                new_rel["start_id"] = mapping[rel["start_id"]]
            if rel.get("end_id") in mapping:
                new_rel["end_id"] = mapping[rel["end_id"]]
            new_rels.append(new_rel)
        new_chunk = dict(chunk)
        new_chunk["nodes"] = new_nodes
        new_chunk["relationships"] = new_rels
        converted.append(new_chunk)
    return converted


def process_kg_json_row(kg_json_str: str, row_index: int) -> Tuple[str, bool, str]:
    """Process a single KG JSON row and convert ids to UUIDs."""
    try:
        cleaned = clean_json_string(kg_json_str)
        data = json.loads(cleaned)
        chunks = [data] if isinstance(data, dict) else data
        converted = convert_nodes_to_uuid(chunks)
        result = converted[0] if isinstance(data, dict) else converted
        return json.dumps(result), True, ""
    except json.JSONDecodeError as e:
        return kg_json_str, False, f"JSON decode error: {e}"
    except Exception as e:  # pragma: no cover - graceful degradation
        return kg_json_str, False, str(e)


def process_dataframe_kg_json(
    df: pd.DataFrame, batch_size: int = 100
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Process an entire dataframe of ``kg_json`` strings."""
    result_df = df.copy()
    errors = {
        "total_errors": 0,
        "error_rows": [],
        "json_decode_errors": 0,
    }
    converted_json: List[str] = []
    for idx, row in df.iterrows():
        processed, success, msg = process_kg_json_row(row["kg_json"], idx)
        converted_json.append(processed)
        if not success:
            errors["total_errors"] += 1
            errors["error_rows"].append(idx)
            if "JSON decode" in msg:
                errors["json_decode_errors"] += 1
            logger.error("Row %s: %s", idx, msg)
        if (idx + 1) % batch_size == 0:
            logger.info("Processed %s/%s rows", idx + 1, len(df))
    result_df["kg_json"] = converted_json
    return result_df, errors


def _extract_from_chunks(
    processed_df: pd.DataFrame, item: str
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for _, row in processed_df.iterrows():
        try:
            kg_data = json.loads(row["kg_json"])
            chunks = [kg_data] if isinstance(kg_data, dict) else kg_data
            for chunk_index, chunk in enumerate(chunks):
                for element in chunk.get(item, []):
                    rows.append(
                        {
                            "chapter": row["chapter"],
                            "chunk": row["chunk"],
                            "chunk_order_number": row["chunk_order_number"],
                            "author": row["author"],
                            "book": row["book"],
                            "chunk_index": chunk_index,
                            **element,
                        }
                    )
        except Exception as e:  # pragma: no cover - log and skip
            logger.error("Error extracting %s: %s", item, e)
    return rows


def extract_nodes_from_kg_json(processed_df: pd.DataFrame) -> pd.DataFrame:
    """Extract nodes from a processed dataframe into a new dataframe."""
    rows = _extract_from_chunks(processed_df, "nodes")
    return pd.DataFrame(rows)


def extract_relationships_from_kg_json(processed_df: pd.DataFrame) -> pd.DataFrame:
    """Extract relationships from a processed dataframe into a new dataframe."""
    rows = _extract_from_chunks(processed_df, "relationships")
    return pd.DataFrame(rows)


def extract_all_entities(processed_df: pd.DataFrame) -> pd.DataFrame:
    """Extract nodes and relationships together."""
    nodes = extract_nodes_from_kg_json(processed_df)
    relationships = extract_relationships_from_kg_json(processed_df)
    nodes["entity_type"] = "node"
    relationships["entity_type"] = "relationship"
    return pd.concat([nodes, relationships], ignore_index=True)

