"""
This module implements the SSTable (Sorted String Table).

SSTables are immutable files on disk that store key-value pairs sorted by key.
This implementation uses a sparse index in memory to speed up lookups without
loading the entire file. It also uses a bloom filter to quickly check for the
non-existence of a key.
"""
import json
import os
import bisect
from storage_engine.bloom_filter import BloomFilter
import logging

logger = logging.getLogger(__name__)

TOMBSTONE = "__TOMBSTONE__"
SPARSE_INDEX_STRIDE = 10  # Store every 10th key in the sparse index

class SSTable:
    """
    Represents an SSTable on disk.

    Attributes:
        filename (str): The path to the SSTable data file.
        bloom_filter_filename (str): The path to the bloom filter file.
        index_filename (str): The path to the sparse index file.
        sparse_index (list): The in-memory sparse index.
        bloom_filter (BloomFilter): The bloom filter for this table.
    """
    def __init__(self, filename):
        """
        Initializes the SSTable, loading its bloom filter and sparse index.

        Args:
            filename (str): The path to the SSTable data file.
        """
        logger.info(f"Initializing SSTable for file: {filename}")
        self.filename = filename
        self.bloom_filter_filename = self.filename.replace('.sst', '.bf')
        self.index_filename = self.filename.replace('.sst', '.index')
        self.sparse_index = []
        self._load_bloom_filter()
        self._load_or_build_index()

    def _load_bloom_filter(self):
        """Load bloom filter from disk."""
        if os.path.exists(self.bloom_filter_filename):
            with open(self.bloom_filter_filename, 'r') as f:
                bf_data = json.load(f)
                self.bloom_filter = BloomFilter(size=bf_data['size'], hash_count=bf_data['hash_count'])
                self.bloom_filter.bit_array = bf_data['bit_array']
            logger.info(f"Bloom filter loaded from {self.bloom_filter_filename}.")
        else:
            self.bloom_filter = None
            logger.warning(f"Bloom filter not found at {self.bloom_filter_filename}.")

    def _load_or_build_index(self):
        """Load sparse index from disk or build it if it doesn't exist."""
        if os.path.exists(self.index_filename):
            with open(self.index_filename, 'r') as f:
                self.sparse_index = json.load(f)
            logger.info(f"Sparse index loaded from {self.index_filename}.")
        elif os.path.exists(self.filename):
            logger.info(f"Sparse index not found. Building from {self.filename}.")
            self._build_sparse_index()

    def _build_sparse_index(self):
        """Build a sparse index of the SSTable and save it to disk."""
        logger.info(f"Building sparse index for {self.filename}...")
        with open(self.filename, 'r') as f:
            offset = 0
            i = 0
            for line in f:
                if i % SPARSE_INDEX_STRIDE == 0:
                    key = list(json.loads(line).keys())[0]
                    self.sparse_index.append((key, offset))
                offset += len(line.encode('utf-8'))
                i += 1
        with open(self.index_filename, 'w') as f:
            json.dump(self.sparse_index, f)
        logger.info(f"Sparse index built and saved to {self.index_filename}.")

    @classmethod
    def write_from_memtable(cls, filename, memtable, append=False):
        """
        Create an SSTable from a memtable (a dictionary).

        This class method writes the memtable's contents to a new SSTable file,
        creates a corresponding bloom filter and sparse index, and saves them
        to disk.

        Args:
            filename (str): The base filename for the new SSTable.
            memtable (dict): The memtable to write.
            append (bool): Whether to append to the file (not typically used
                           for creating new SSTables).

        Returns:
            SSTable: An instance of the newly created SSTable.
        """
        logger.info(f"Writing memtable to SSTable: {filename}")
        mode = 'a' if append else 'w'
        index_filename = filename.replace('.sst', '.index')
        bloom_filter_filename = filename.replace('.sst', '.bf')

        # Create and save a bloom filter
        logger.info(f"Creating bloom filter for {filename}.")
        bloom_filter = BloomFilter(size=max(100, len(memtable) * 10), hash_count=5)
        for key in memtable.keys():
            bloom_filter.add(str(key))
        with open(bloom_filter_filename, 'w') as f:
            json.dump({
                'size': bloom_filter.size,
                'hash_count': bloom_filter.hash_count,
                'bit_array': bloom_filter.bit_array
            }, f)
        logger.info(f"Bloom filter saved to {bloom_filter_filename}.")

        # Write SSTable and build sparse index simultaneously
        logger.info(f"Writing SSTable data and building sparse index for {filename}.")
        sparse_index = []
        with open(filename, mode) as f:
            offset = 0
            i = 0
            sorted_items = sorted(memtable.items())
            for key, value in sorted_items:
                if i % SPARSE_INDEX_STRIDE == 0:
                    sparse_index.append((key, offset))

                line = json.dumps({key: value}) + '\n'
                f.write(line)
                offset += len(line.encode('utf-8'))
                i += 1

        with open(index_filename, 'w') as f:
            json.dump(sparse_index, f)
        logger.info(f"SSTable and sparse index saved for {filename}.")

        return SSTable(filename)

    def get(self, key):
        """
        Retrieve a value by key from the SSTable.

        This method performs a highly optimized lookup:
        1. It checks the bloom filter to see if the key is likely present.
        2. It uses a binary search on the in-memory sparse index to find the
           approximate location of the key on disk.
        3. It seeks to that location and scans a small portion of the file.

        Args:
            key: The key to look up.

        Returns:
            The value associated with the key, or None if not found.
        """
        logger.debug(f"SSTable Get: key='{key}' in file {self.filename}.")
        # 1. Check bloom filter
        if self.bloom_filter and str(key) not in self.bloom_filter:
            logger.debug(f"Key '{key}' not in bloom filter for {self.filename}.")
            return None

        # 2. Binary search the sparse index to find the block to scan
        if not self.sparse_index:
            logger.warning(f"SSTable {self.filename} has no sparse index.")
            return None

        keys = [k for k, o in self.sparse_index]
        idx = bisect.bisect_right(keys, key) - 1
        start_offset = self.sparse_index[idx][1] if idx >= 0 else 0
        logger.debug(f"Scanning for key '{key}' from offset {start_offset} in {self.filename}.")

        # 3. Seek and scan the block on disk
        with open(self.filename, 'r') as f:
            f.seek(start_offset)
            while True:
                line = f.readline()
                if not line:
                    break

                current_pos = f.tell()
                record = json.loads(line)
                record_key = list(record.keys())[0]

                if record_key == key:
                    value = record[key]
                    logger.debug(f"Key '{key}' found in {self.filename}.")
                    return None if value == TOMBSTONE else value

                if record_key > key:
                    logger.debug(f"Key '{key}' not found (overshot) in {self.filename}.")
                    return None

                if idx + 1 < len(self.sparse_index):
                    next_offset = self.sparse_index[idx + 1][1]
                    if current_pos >= next_offset:
                        logger.debug(f"Key '{key}' not found in scanned block of {self.filename}.")
                        return None
        logger.debug(f"Key '{key}' not found at end of {self.filename}.")
        return None

    def __getitem__(self, key):
        """Allows dictionary-style access, e.g., `sstable[key]`."""
        return self.get(key)

    def read_iter(self):
        """Returns an iterator over the key-value pairs in the SSTable."""
        if not os.path.exists(self.filename):
            return
        with open(self.filename, 'r') as f:
            for line in f:
                yield list(json.loads(line).items())[0]
