"""
Memgraph Import - A package for processing literary texts into knowledge graphs.

This package provides functionality to:
1. Process novel text into structured chunks
2. Generate knowledge graphs from text chunks
3. Export knowledge graphs for Memgraph import
"""

from .novel_processor import novel_to_dataframe
from .knowledge_graph import generate_knowledge_graph
from .gemini_client import GeminiClient

__version__ = "0.1.0"
__all__ = ["novel_to_dataframe", "generate_knowledge_graph", "GeminiClient"]
