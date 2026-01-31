import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
from bloom_filter import BloomFilter

def test_initialization(bloom_filter):
    assert bloom_filter.size == 100
    assert bloom_filter.hash_count == 3
    assert len(bloom_filter.bit_array) == 100
    assert all(bit == 0 for bit in bloom_filter.bit_array)

def test_add_and_contains(bloom_filter):
    bloom_filter.add("hello")
    assert "hello" in bloom_filter
    bloom_filter.add("world")
    assert "world" in bloom_filter

def test_contains_not_added(bloom_filter):
    assert "python" not in bloom_filter
    bloom_filter.add("hello")
    assert "world" not in bloom_filter

def test_hashes(bloom_filter):
    hashes = list(bloom_filter._hashes("test"))
    assert len(hashes) == bloom_filter.hash_count
    for h in hashes:
        assert isinstance(h, int)
        assert h >= 0
        assert h < bloom_filter.size
