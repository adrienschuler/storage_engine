import pytest
import os
import shutil
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from storage_engine.lsmtree import LSMTree
from storage_engine.sstable import SSTable, TOMBSTONE
from storage_engine.btree import BTree
from bloom_filter import BloomFilter
from heap import MinHeap

TEST_DATA_ROOT = os.path.join(os.path.dirname(__file__), 'data')

@pytest.fixture(scope="session")
def db():
    data_dir = os.path.join(TEST_DATA_ROOT, 'test_data_dir')
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    db_instance = LSMTree(data_dir, memtable_threshold=50)
    yield db_instance
    db_instance.close()

@pytest.fixture(scope="session")
def sstable_setup():
    test_dir = os.path.join(TEST_DATA_ROOT, "test_sstable_data")
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    os.makedirs(test_dir, exist_ok=True)
    filename = os.path.join(test_dir, "test.sst")
    memtable = {f"key{i}": f"value{i}" for i in range(20)}
    memtable["key5"] = "updated_value5"
    memtable["key10"] = TOMBSTONE

    sstable = SSTable.write_from_memtable(filename, memtable)

    yield sstable, memtable

@pytest.fixture
def btree():
    wal_path = os.path.join(TEST_DATA_ROOT, 'test.wal')
    if os.path.exists(wal_path):
        os.remove(wal_path)
    b = BTree(t=3, wal_path=wal_path)
    yield b
    b.wal.close()
    if os.path.exists(wal_path):
        os.remove(wal_path)

@pytest.fixture
def bloom_filter():
    return BloomFilter(size=100, hash_count=3)

@pytest.fixture
def heap():
    return MinHeap()
