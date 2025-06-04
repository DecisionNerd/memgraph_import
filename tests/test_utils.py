import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import importlib
import json
import types

# --- Fake pandas -----------------------------------------------------------
class FakeDataFrame:
    def __init__(self, data=None, columns=None):
        self.data = [dict(row) for row in (data or [])]
        self.columns = columns or (list(self.data[0].keys()) if self.data else [])

    def copy(self):
        return FakeDataFrame([dict(row) for row in self.data], columns=list(self.columns))

    def iterrows(self):
        for idx, row in enumerate(self.data):
            yield idx, row

    def __getitem__(self, key):
        return [row.get(key) for row in self.data]

    def __setitem__(self, key, value):
        if isinstance(value, list):
            for row, val in zip(self.data, value):
                row[key] = val
        else:
            for row in self.data:
                row[key] = value

    def __len__(self):
        return len(self.data)


def fake_concat(dfs, ignore_index=False):
    combined = []
    for df in dfs:
        combined.extend(df.data)
    return FakeDataFrame(combined)

fake_pd = types.ModuleType("pandas")
fake_pd.DataFrame = FakeDataFrame
fake_pd.concat = fake_concat
sys.modules.setdefault("pandas", fake_pd)

# --- Fake neo4j ------------------------------------------------------------
class FakeTx:
    def run(self, *args, **kwargs):
        pass

    def commit(self):
        pass


class FakeSession:
    def run(self, *args, **kwargs):
        return 1

    def begin_transaction(self):
        return FakeTx()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class FakeDriver:
    def session(self):
        return FakeSession()

    def close(self):
        pass


class FakeGraphDatabase:
    @staticmethod
    def driver(uri, auth=None):
        return FakeDriver()
fake_google = types.ModuleType("google")
fake_google.genai = types.ModuleType("google.genai")
sys.modules.setdefault("google", fake_google)
fake_google.genai.types = types.SimpleNamespace()
sys.modules.setdefault("google.genai", fake_google.genai)

fake_neo4j = types.ModuleType("neo4j")
fake_neo4j.GraphDatabase = FakeGraphDatabase
sys.modules.setdefault("neo4j", fake_neo4j)

# Reload modules with fakes present
utils = importlib.reload(importlib.import_module("memgraph_import.kg_json_utils"))
importer_mod = importlib.reload(importlib.import_module("memgraph_import.memgraph_importer"))


def test_generate_deterministic_uuid_consistency():
    a = utils.generate_deterministic_uuid("Actor", name="Tom")
    b = utils.generate_deterministic_uuid("Actor", name="Tom")
    c = utils.generate_deterministic_uuid("Actor", name="Huck")
    assert a == b
    assert a != c


def test_convert_nodes_to_uuid():
    data = [{
        "nodes": [{"id": 1, "label": "Actor", "name": "Tom"}],
        "relationships": [{"start_id": 1, "end_id": 1, "type": "KNOWS"}],
    }]
    converted = utils.convert_nodes_to_uuid(data)
    node_id = converted[0]["nodes"][0]["id"]
    assert isinstance(node_id, str)
    assert converted[0]["relationships"][0]["start_id"] == node_id


def test_process_kg_json_row_success():
    json_str = json.dumps({"nodes": [{"id": 1, "label": "Actor", "name": "Tom"}], "relationships": []})
    result, success, msg = utils.process_kg_json_row(json_str, 0)
    assert success is True
    parsed = json.loads(result)
    assert parsed["nodes"][0]["id"] != 1


def test_process_dataframe_kg_json_and_extract():
    df = FakeDataFrame([{"kg_json": json.dumps({"nodes": [{"id": 1, "label": "Actor", "name": "Tom"}], "relationships": []}),
                         "chapter": "1", "chunk": "c", "chunk_order_number": 0, "author": "a", "book": "b"}])
    processed, stats = utils.process_dataframe_kg_json(df)
    assert stats["total_errors"] == 0
    entities = utils.extract_all_entities(processed)
    assert len(entities.data) == 1  # one node + relationships (0)


def test_memgraph_importer_connect_and_close():
    imp = importer_mod.MemgraphImporter()
    assert imp.connect() is True
    imp.create_indexes()
    count = imp.import_nodes_batch([{"label": "Actor", "props": {"id": "1"}}])
    assert count == 1
    count_rel = imp.import_relationships_batch([
        {"start_id": "1", "end_id": "1", "type": "KNOWS", "props": {}}
    ])
    assert count_rel == 1
    imp.close()
