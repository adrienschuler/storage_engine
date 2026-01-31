import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
from storage_engine.db import DB

@pytest.fixture
def btree_db(tmp_path):
    return DB(engine_type='btree', directory=str(tmp_path))

@pytest.fixture
def lsmtree_db(tmp_path):
    return DB(engine_type='lsmtree', directory=str(tmp_path))

def test_btree_put_and_get(btree_db):
    btree_db.put("name", "Adrien")
    assert btree_db.get("name") == "Adrien"

def test_lsmtree_put_and_get(lsmtree_db):
    lsmtree_db.put("name", "Adrien")
    assert lsmtree_db.get("name") == "Adrien"

def test_btree_overwrite(btree_db):
    btree_db.put("name", "Adrien")
    btree_db.put("name", "Adrien Schuler")
    assert btree_db.get("name") == "Adrien Schuler"

def test_lsmtree_overwrite(lsmtree_db):
    lsmtree_db.put("name", "Adrien")
    lsmtree_db.put("name", "Adrien Schuler")
    assert lsmtree_db.get("name") == "Adrien Schuler"

def test_btree_delete(btree_db):
    btree_db.put("city", "Paris")
    btree_db.delete("city")
    assert btree_db.get("city") is None

def test_lsmtree_delete(lsmtree_db):
    lsmtree_db.put("city", "Paris")
    lsmtree_db.delete("city")
    assert lsmtree_db.get("city") is None
