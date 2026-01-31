import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
import os
import shutil
from storage_engine.lsmtree import LSMTree
import time

def test_put_and_get(db):
    db.put("name", "Adrien")
    assert db.get("name") == "Adrien"

def test_overwrite(db):
    db.put("name", "Adrien")
    db.put("name", "Adrien Schuler")
    assert db.get("name") == "Adrien Schuler"

def test_delete(db):
    db.put("city", "Paris")
    db.delete("city")
    assert db.get("city") is None

def test_persistence(db):
    data_dir = db.directory
    db.put("country", "France")
    db.close()

    db2 = LSMTree(data_dir)
    assert db2.get("country") == "France"
    db2.close()

def test_flush_memtable(db):
    for i in range(50):
        db.put(f"key{i}", f"value{i}")
    # At this point, the memtable should have been flushed.
    # We can check if a segment file was created.
    assert len(db.segments) > 0
    assert db.get("key25") == "value25"

def test_compaction(db):
    db.put("name", "Adrien")
    db.put("city", "Paris")
    db.put("name", "Adrien Schuler") # Overwrite
    db.delete("city") # Delete

    # Create multiple segments
    for i in range(150):
        db.put(f"key{i}", f"value{i}")

    db.compact()

    assert db.get("name") == "Adrien Schuler"
    assert db.get("city") is None
    assert db.get("key75") == "value75"

    # After compaction, there should be only one segment
    assert len(db.segments) == 1

def test_fuzzy_get(db):
    db.put("apple", "red fruit")
    db.put("apply", "to request something")
    db.put("apples", "plural of apple")
    db.put("banana", "yellow fruit")

    # Test with max_distance=1
    results = db.fuzzy_get("apple", 1)
    assert ("apple", "red fruit") in results
    assert ("apply", "to request something") in results
    assert ("apples", "plural of apple") in results
    assert ("banana", "yellow fruit") not in results

    # Test with max_distance=2
    results = db.fuzzy_get("apple", 2)
    assert ("apple", "red fruit") in results
    assert ("apply", "to request something") in results
    assert ("apples", "plural of apple") in results
    # Depending on the Levenshtein implementation, "banana" might be included here.
    # For this test, we assume it's not within a distance of 2.

    # Test with a key that doesn't exist but is close
    results = db.fuzzy_get("aple", 1)
    assert ("apple", "red fruit") in results

    # Test with no matches
    results = db.fuzzy_get("xyz", 1)
    assert len(results) == 0
