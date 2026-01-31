import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
from storage_engine.btree import BTree, BTreeNode

def test_initialization(btree):
    assert isinstance(btree.root, BTreeNode)
    assert btree.root.leaf
    assert btree.t == 3

def test_insert_and_search(btree):
    btree.put("key1", "value1")
    assert btree.get("key1") == "value1"
    btree.put("key2", "value2")
    assert btree.get("key2") == "value2"
    assert btree.get("key3") is None

def test_insert_split(btree):
    # With t=3, a split occurs when inserting the 6th key into a node.
    # The root is a leaf initially.
    for i in range(1, 6): # Insert 5 keys, root is now full
        btree.put(f"key{i}", f"value{i}")

    assert btree.root.leaf
    assert len(btree.root.keys) == 5

    # This insertion should cause the root to split
    btree.put("key6", "value6")

    # After split, root is no longer a leaf and has 1 key and 2 children
    assert not btree.root.leaf
    assert len(btree.root.keys) == 1
    assert len(btree.root.child) == 2

    # Insert more to test further splits
    for i in range(7, 20):
        btree.put(f"key{i}", f"value{i}")

    for i in range(1, 20):
        assert btree.get(f"key{i}") == f"value{i}"

def test_update_existing_key(btree):
    btree.put("key1", "value1")
    assert btree.get("key1") == "value1"
    btree.put("key1", "new_value1")
    assert btree.get("key1") == "new_value1"

def test_items(btree):
    keys = ["d", "b", "a", "c", "e"]
    values = ["v4", "v2", "v1", "v3", "v5"]
    for k, v in zip(keys, values):
        btree.put(k, v)

    sorted_items = sorted(zip(keys, values))
    assert btree.items() == sorted_items

def test_delete(btree):
    btree.put("key1", "value1")
    btree.put("key2", "value2")
    btree.delete("key1")
    from storage_engine.sstable import TOMBSTONE
    assert btree.get("key1") == TOMBSTONE
    assert btree.get("key2") == "value2"
    btree.delete("key2")
    assert btree.get("key2") == TOMBSTONE

def test_search_non_existent(btree):
    assert btree.get("non_existent") is None
