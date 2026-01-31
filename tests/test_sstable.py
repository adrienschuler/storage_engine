import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
import os
import json
from storage_engine.sstable import SSTable, TOMBSTONE, SPARSE_INDEX_STRIDE
from storage_engine.bloom_filter import BloomFilter

def test_write_from_memtable_and_load(sstable_setup):
    sstable, memtable = sstable_setup
    assert os.path.exists(sstable.filename)
    assert os.path.exists(sstable.bloom_filter_filename)
    assert os.path.exists(sstable.index_filename)

    # Verify sparse index
    assert len(sstable.sparse_index) == (len(memtable) + SPARSE_INDEX_STRIDE - 1) // SPARSE_INDEX_STRIDE

    # Verify bloom filter
    assert isinstance(sstable.bloom_filter, BloomFilter)
    for key in memtable:
        if memtable[key] != TOMBSTONE:
            assert str(key) in sstable.bloom_filter

    # Load the sstable and check again
    loaded_sstable = SSTable(sstable.filename)
    assert len(loaded_sstable.sparse_index) == len(sstable.sparse_index)
    assert loaded_sstable.bloom_filter is not None

def test_get(sstable_setup):
    sstable, _ = sstable_setup

    # Test existing keys
    assert sstable.get("key1") == "value1"
    assert sstable.get("key5") == "updated_value5"

    # Test tombstone key
    assert sstable.get("key10") is None

    # Test non-existent key
    assert sstable.get("non_existent_key") is None

def test_getitem(sstable_setup):
    sstable, _ = sstable_setup
    assert sstable["key2"] == "value2"

def test_read_iter(sstable_setup):
    sstable, memtable = sstable_setup
    items = list(sstable.read_iter())

    assert len(items) == len(memtable)
    sorted_memtable = sorted(memtable.items())

    for i, (key, value) in enumerate(items):
        assert key == sorted_memtable[i][0]
        assert value == sorted_memtable[i][1]
