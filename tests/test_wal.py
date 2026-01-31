import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
from storage_engine.btree import BTree
from storage_engine.wal import WriteAheadLog

@pytest.fixture
def btree_with_wal():
    wal_path = 'test.wal'
    if os.path.exists(wal_path):
        os.remove(wal_path)
    btree = BTree(t=3, wal_path=wal_path)
    yield btree
    btree.wal.close()
    if os.path.exists(wal_path):
        os.remove(wal_path)

def test_wal_recovery(btree_with_wal):
    btree_with_wal.insert("key1", "value1")
    btree_with_wal.insert("key2", "value2")

    # Simulate a crash by creating a new B-Tree with the same WAL
    new_btree = BTree(t=3, wal_path=btree_with_wal.wal.log_path)

    assert new_btree.search("key1") == "value1"
    assert new_btree.search("key2") == "value2"
