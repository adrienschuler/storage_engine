"""
This module implements a Log-Structured Merge-Tree (LSM-Tree).

The LSM-Tree is a data structure optimized for write-heavy workloads. It buffers
writes in an in-memory table (memtable) and flushes them to immutable, sorted
files on disk (SSTables) when the memtable is full. Reads are performed by
checking the memtable first, then the SSTables from newest to oldest. This
implementation also supports fuzzy string searching on keys using Levenshtein distance.
"""
import os
import shutil
import time
import json
import sys
import logging
from .btree import BTree
from .sstable import SSTable, TOMBSTONE
from .levensthein import Levenshtein
from .heap import MinHeap
from .bloom_filter import BloomFilter
from .engine import StorageEngine

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

class LSMTree(StorageEngine):
    """
    An LSM-Tree implementation.

    This class orchestrates the memtable, SSTables, and the compaction process.
    """
    def __init__(self, directory, memtable_threshold=100):
        """
        Initializes the LSM-Tree.

        Args:
            directory (str): The directory to store SSTable files.
            memtable_threshold (int): The max number of items in the memtable
                                      before it's flushed to disk.
        """
        self.directory = directory
        wal_path = os.path.join(directory, 'btree.wal')
        self.memtable = BTree(t=5, wal_path=wal_path)
        self.memtable_threshold = memtable_threshold
        self.segments = []
        self.segment_counter = 0
        self._load_segments()
        logger.info(f"LSMTree initialized in directory '{directory}' with memtable threshold {memtable_threshold}.")
    def _load_segments(self):
        """Load existing SSTable segments from disk."""
        logger.info("Loading segments from disk...")
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
            logger.info(f"Created directory '{self.directory}'.")
            return

        files = sorted([f for f in os.listdir(self.directory) if f.endswith('.sst')])
        for filename in files:
            self.segments.append(SSTable(os.path.join(self.directory, filename)))
        logger.info(f"Loaded {len(self.segments)} segments.")

        if self.segments:
            last_segment_filename = os.path.basename(self.segments[-1].filename)
            self.segment_counter = int(last_segment_filename.split('.')[0]) + 1
        else:
            self.segment_counter = 0
        logger.info(f"Segment counter set to {self.segment_counter}.")

    def put(self, key, value):
        """
        Write a key-value pair to the database.

        The pair is inserted into the memtable. If the memtable exceeds its
        threshold, it is flushed to a new SSTable on disk.

        Args:
            key: The key to write.
            value: The value to associate with the key.
        """
        logger.debug(f"Put: key='{key}', value='{value}'.")
        self.memtable.insert(key, value)
        if len(self.memtable.items()) >= self.memtable_threshold:
            logger.info("Memtable threshold reached, flushing to disk.")
            self._flush_memtable()

    def get(self, key):
        """
        Read a value by key.

        It checks the memtable first, then searches the SSTables from the
        newest to the oldest.

        Args:
            key: The key to read.

        Returns:
            The value associated with the key, or None if not found.
        """
        logger.debug(f"Get: key='{key}'.")
        # Check memtable first
        value = self.memtable.search(key)
        if value is not None:
            logger.debug(f"Key '{key}' found in memtable.")
            return None if value == TOMBSTONE else value

        # Check on-disk segments in reverse order (most recent first)
        logger.debug(f"Key '{key}' not in memtable, checking {len(self.segments)} segments.")
        for segment in reversed(self.segments):
            logger.debug(f"Checking segment {segment.filename} for key '{key}'.")
            # Use bloom filter to quickly discard non-existent keys
            if segment.bloom_filter:
                if str(key) not in segment.bloom_filter:
                    logger.debug(f"Key '{key}' not in bloom filter for segment {segment.filename}.")
                    continue  # Key is definitely not in this segment
                value = segment.get(key)
                if value is not None:
                    logger.debug(f"Key '{key}' found in segment {segment.filename}.")
                    return None if value == TOMBSTONE else value
            else:
                # If no bloom filter, search the segment directly
                value = segment.get(key)
                if value is not None:
                    logger.debug(f"Key '{key}' found in segment {segment.filename}.")
                    return None if value == TOMBSTONE else value
        logger.debug(f"Key '{key}' not found in any segment.")
        return None

    def delete(self, key):
        """
        Delete a key by writing a tombstone value for it.

        Args:
            key: The key to delete.
        """
        logger.debug(f"Delete: key='{key}'.")
        self.put(key, TOMBSTONE)

    def _flush_memtable(self):
        """Write the memtable to a new SSTable on disk."""
        if not self.memtable.items():
            logger.debug("Memtable is empty, nothing to flush.")
            return

        logger.info("Flushing memtable to disk...")
        segment_filename = os.path.join(self.directory, f"{self.segment_counter:05d}.sst")
        SSTable.write_from_memtable(segment_filename, dict(self.memtable.items()))
        self.segments.append(SSTable(segment_filename))
        self.segment_counter += 1
        wal_path = os.path.join(self.directory, 'btree.wal')
        self.memtable = BTree(t=5, wal_path=wal_path)
        logger.info(f"Memtable flushed to segment {segment_filename}.")

    def compact(self):
        """
        Merge and compact all existing SSTable segments.

        This process reads all segments, performs a k-way merge to resolve
        duplicates and remove tombstoned entries, and writes the result to a
        new, single SSTable. The old segments are then deleted.
        """
        if len(self.segments) < 2:
            logger.info("Compaction not needed (less than 2 segments).")
            return

        logger.info(f"Starting compaction of {len(self.segments)} segments.")
        temp_filename = os.path.join(self.directory, f"temp_compacted_{int(time.time())}.sst")

        # Get iterators for all segments
        segment_iters = [seg.read_iter() for seg in self.segments]

        # Min-heap for k-way merge
        min_heap = MinHeap()

        # Prime the heap with the first element from each segment iterator
        for i, seg_iter in enumerate(segment_iters):
            try:
                key, value = next(seg_iter)
                min_heap.heappush((key, value, i, seg_iter))
                logger.debug(f"Primed heap with first element from segment {i}.")
            except StopIteration:
                logger.debug(f"Segment {i} is empty.")
                continue

        last_key = None
        merged_data = {}
        all_keys = []

        with open(temp_filename, 'w') as f:
            while min_heap:
                key, value, seg_idx, seg_iter = min_heap.heappop()
                logger.debug(f"Popped key '{key}' from segment {seg_idx} from heap.")

                if key != last_key:
                    if last_key is not None and merged_data.get(last_key) != TOMBSTONE:
                        logger.debug(f"Writing merged key '{last_key}' to temp file.")
                        SSTable.write_from_memtable(f.name, {last_key: merged_data[last_key]}, append=True)
                        all_keys.append(last_key)
                    last_key = key

                merged_data[key] = value

                try:
                    next_key, next_value = next(seg_iter)
                    min_heap.heappush((next_key, next_value, seg_idx, seg_iter))
                    logger.debug(f"Pushed next key '{next_key}' from segment {seg_idx} to heap.")
                except StopIteration:
                    logger.debug(f"Segment {seg_idx} exhausted.")
                    continue

            if last_key is not None and merged_data.get(last_key) != TOMBSTONE:
                logger.debug(f"Writing final merged key '{last_key}' to temp file.")
                SSTable.write_from_memtable(f.name, {last_key: merged_data[last_key]}, append=True)
                all_keys.append(last_key)

        logger.info("Compaction merge complete. Creating new segment.")
        # Create a bloom filter for the new compacted segment
        bloom_filter = BloomFilter(size=max(100, len(all_keys) * 10), hash_count=5)
        for key in all_keys:
            bloom_filter.add(str(key))

        compacted_segment_filename = os.path.join(self.directory, f"{self.segment_counter:05d}.sst")
        bloom_filter_filename = compacted_segment_filename.replace('.sst', '.bf')

        with open(bloom_filter_filename, 'w') as f:
            json.dump({
                'size': bloom_filter.size,
                'hash_count': bloom_filter.hash_count,
                'bit_array': bloom_filter.bit_array
            }, f)
        logger.info(f"New bloom filter created at {bloom_filter_filename}.")

        # Atomically replace old segments with the new compacted one
        logger.info(f"Moving temp file {temp_filename} to {compacted_segment_filename}.")
        shutil.move(temp_filename, compacted_segment_filename)

        # 2. Remove old segment files and their associated index/bloom files
        logger.info("Removing old segments.")
        for segment in self.segments:
            os.remove(segment.filename)
            if os.path.exists(segment.index_filename):
                os.remove(segment.index_filename)
            if os.path.exists(segment.bloom_filter_filename):
                os.remove(segment.bloom_filter_filename)

        # 3. Reset segments and load the new one
        self.segments = [SSTable(compacted_segment_filename)]
        self.segment_counter += 1
        logger.info("Compaction complete.")


    def close(self):
        """Flush the current memtable to disk before closing."""
        self._flush_memtable()

    def fuzzy_get(self, search_key, max_distance):
        """
        Find keys that are similar to the search_key using Levenshtein distance.

        Args:
            search_key: The key to search for.
            max_distance (int): The maximum Levenshtein distance to be considered a match.

        Returns:
            A list of (key, value) tuples that are close to the search key.
        """
        results = []
        checked_keys = set()

        # Search in memtable
        for key, value in self.memtable.items():
            if key not in checked_keys:
                distance = Levenshtein.distance(search_key, key)
                if distance <= max_distance:
                    if value != TOMBSTONE:
                        results.append((key, value))
                    checked_keys.add(key)

        # Search in SSTables from newest to oldest
        for segment in reversed(self.segments):
            for key, value in segment.read_iter():
                if key not in checked_keys:
                    distance = Levenshtein.distance(search_key, key)
                    if distance <= max_distance:
                        if value != TOMBSTONE:
                            results.append((key, value))
                        checked_keys.add(key)

        return results
