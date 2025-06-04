"""
Data models and schemas for the knowledge graph generation.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Union, Any
from enum import Enum


class EntityType(str, Enum):
    """Entity types in the knowledge graph."""

    ACTOR = "Actor"
    OBJECT = "Object"
    LOCATION = "Location"
    EVENT = "Event"
    INTANGIBLE = "Intangible"
    BOOK = "Book"
    AUTHOR = "Author"
    CHAPTER = "Chapter"
    CHUNK = "Chunk"


@dataclass
class Node:
    """Knowledge graph node model."""

    id: int
    label: EntityType
    name: str
    description: str
    properties: Dict[str, Any]
    timestamp: datetime


@dataclass
class Relationship:
    """Knowledge graph relationship model."""

    start_id: int
    end_id: int
    relationship_type: str
    weight: float
    properties: Dict[str, Any]
    timestamp: datetime


@dataclass
class KnowledgeGraph:
    """Complete knowledge graph model."""

    metadata: Dict[str, Any]
    nodes: List[Node]
    relationships: List[Relationship]


@dataclass
class NovelChunk:
    """Model for a chunk of novel text."""

    chapter: str
    chunk: str
    chunk_order_number: int
    author: str
    book: str


@dataclass
class NovelData:
    """Model for the complete novel data."""

    chunks: List[NovelChunk]
    author: str
    book: str
