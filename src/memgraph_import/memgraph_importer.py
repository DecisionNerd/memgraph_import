"""Utilities for importing processed entities directly into Memgraph."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List

from neo4j import GraphDatabase

logger = logging.getLogger(__name__)


class MemgraphImporter:
    """Simple helper for loading entities into Memgraph."""

    def __init__(self, uri: str = "bolt://localhost:7687", user: str = "", password: str = ""):
        self.uri = uri
        self.user = user
        self.password = password
        self.driver = None

    def connect(self) -> bool:
        try:
            self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
            with self.driver.session() as session:
                session.run("RETURN 1")
            logger.info("Connected to Memgraph at %s", self.uri)
            return True
        except Exception as e:  # pragma: no cover - network operations
            logger.error("Failed to connect to Memgraph: %s", e)
            return False

    def close(self) -> None:
        if self.driver:
            self.driver.close()
            self.driver = None

    def create_indexes(self) -> None:
        if not self.driver:
            raise RuntimeError("Not connected")
        indexes = [
            "CREATE INDEX ON :Author(id)",
            "CREATE INDEX ON :Book(id)",
            "CREATE INDEX ON :Chapter(id)",
            "CREATE INDEX ON :Chunk(id)",
            "CREATE INDEX ON :Actor(id)",
            "CREATE INDEX ON :Object(id)",
        ]
        with self.driver.session() as session:
            for query in indexes:
                try:
                    session.run(query)
                except Exception as exc:  # pragma: no cover - depends on db state
                    logger.debug("Index creation skipped: %s", exc)

    def import_nodes_batch(self, nodes: List[Dict[str, Any]], batch_size: int = 1000) -> int:
        if not self.driver:
            raise RuntimeError("Not connected")
        total = 0
        query = "CREATE (n:{label} {props})"
        with self.driver.session() as session:
            for i in range(0, len(nodes), batch_size):
                batch = nodes[i : i + batch_size]
                tx = session.begin_transaction()
                for node in batch:
                    lbl = node.get("label")
                    props = json.dumps(node.get("props", {}))
                    tx.run(query.format(label=lbl, props="{}"), **{"props": json.loads(props)})
                    total += 1
                tx.commit()
        return total

    def import_relationships_batch(
        self, relationships: List[Dict[str, Any]], batch_size: int = 1000
    ) -> int:
        if not self.driver:
            raise RuntimeError("Not connected")
        total = 0
        query = "MATCH (a {id: $s}), (b {id: $e}) CREATE (a)-[r:{type}]->(b) SET r += $p"
        with self.driver.session() as session:
            for i in range(0, len(relationships), batch_size):
                batch = relationships[i : i + batch_size]
                tx = session.begin_transaction()
                for rel in batch:
                    tx.run(
                        query,
                        s=rel["start_id"],
                        e=rel["end_id"],
                        type=rel["type"],
                        p=rel.get("props", {}),
                    )
                    total += 1
                tx.commit()
        return total


