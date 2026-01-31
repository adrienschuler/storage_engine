# Storage Engine API Documentation

## `DB`

This is the main entry point for the storage engine. It provides a unified interface for different storage backends.

### `DB(engine_type='btree', directory='data_dir')`

Initializes the database with the specified engine.

-   `engine_type` (str): The storage engine to use. Can be `'btree'` or `'lsmtree'`. Defaults to `'btree'`.
-   `directory` (str): The directory where data will be stored. Defaults to `'data_dir'`.

### `put(key, value)`

Stores a key-value pair in the database.

-   `key`: The key to store.
-   `value`: The value to associate with the key.

### `get(key)`

Retrieves the value associated with a key.

-   `key`: The key to retrieve.

Returns the value, or `None` if the key is not found.

### `delete(key)`

Deletes a key-value pair from the database.

-   `key`: The key to delete.

### `fuzzy_get(search_key, max_distance)`

Finds keys that are similar to `search_key` using Levenshtein distance. This is only available for the LSM-Tree engine.

-   `search_key`: The key to search for.
-   `max_distance` (int): The maximum Levenshtein distance to be considered a match.

Returns a list of `(key, value)` tuples that are close to the search key.

### `close()`

Closes the database and ensures all data is flushed to disk.

# Storage Engine API Documentation

## `lsmtree.py`

This module implements a Log-Structured Merge-Tree (LSM-Tree).

The LSM-Tree is a data structure optimized for write-heavy workloads. It buffers
writes in an in-memory table (memtable) and flushes them to immutable, sorted
files on disk (SSTables) when the memtable is full. Reads are performed by
checking the memtable first, then the SSTables from newest to oldest. This
implementation also supports fuzzy string searching on keys using Levenshtein distance.

### `LSMTree`

An LSM-Tree implementation.

This class orchestrates the memtable, SSTables, and the compaction process.

#### `close(self)`

```python
Flush the current memtable to disk before closing.
```

#### `compact(self)`

```python
Merge and compact all existing SSTable segments.

This process reads all segments, performs a k-way merge to resolve
duplicates and remove tombstoned entries, and writes the result to a
new, single SSTable. The old segments are then deleted.
```

#### `delete(self, key)`

```python
Delete a key by writing a tombstone value for it.

Args:
    key: The key to delete.
```

#### `fuzzy_get(self, search_key, max_distance)`

```python
Find keys that are similar to the search_key using Levenshtein distance.

Args:
    search_key: The key to search for.
    max_distance (int): The maximum Levenshtein distance to be considered a match.

Returns:
    A list of (key, value) tuples that are close to the search key.
```

#### `get(self, key)`

```python
Read a value by key.

It checks the memtable first, then searches the SSTables from the
newest to the oldest.

Args:
    key: The key to read.

Returns:
    The value associated with the key, or None if not found.
```

#### `put(self, key, value)`

```python
Write a key-value pair to the database.

The pair is inserted into the memtable. If the memtable exceeds its
threshold, it is flushed to a new SSTable on disk.

Args:
    key: The key to write.
    value: The value to associate with the key.
```

## `btree.py`

This module implements a B-Tree data structure.

The B-Tree is used as the in-memory memtable for the LSM-Tree storage engine.
It keeps key-value pairs sorted by key and provides efficient insertion,
search, and update operations.

### `BTree`

A B-Tree data structure.

Attributes:
    root (BTreeNode): The root node of the tree.
    t (int): The minimum degree of the B-Tree. This determines the
             minimum and maximum number of keys a node can have.

#### `insert(self, k, v)`

```python
Inserts a key-value pair into the B-Tree.

If the key already exists, its value is updated.

Args:
    k: The key to insert.
    v: The value to associate with the key.
```

#### `insert_non_full(self, x, k, v)`

```python
A helper function to insert a key into a node that is not full.

Args:
    x (BTreeNode): The node to insert into.
    k: The key to insert.
    v: The value to associate with the key.
```

#### `items(self)`

```python
Returns all key-value pairs in the tree, sorted by key.

Returns:
    list: A list of (key, value) tuples.
```

#### `search(self, k, x=None)`

```python
Searches for a key in the B-Tree.

Args:
    k: The key to search for.
    x (BTreeNode, optional): The node to start the search from.
                             If None, starts from the root.

Returns:
    The value associated with the key if found, otherwise None.
```

#### `split_child(self, x, i)`

```python
Splits a full child of a node.

Args:
    x (BTreeNode): The parent node whose child is to be split.
    i (int): The index of the child in the parent's child list.
```

### `BTreeNode`

A node in the B-Tree.

Attributes:
    leaf (bool): True if the node is a leaf, False otherwise.
    keys (list): The list of keys in the node.
    values (list): The list of values corresponding to the keys.
    child (list): The list of child nodes.

## `sstable.py`

This module implements the SSTable (Sorted String Table).

SSTables are immutable files on disk that store key-value pairs sorted by key.
This implementation uses a sparse index in memory to speed up lookups without
loading the entire file. It also uses a bloom filter to quickly check for the
non-existence of a key.

### `SSTable`

Represents an SSTable on disk.

Attributes:
    filename (str): The path to the SSTable data file.
    bloom_filter_filename (str): The path to the bloom filter file.
    index_filename (str): The path to the sparse index file.
    sparse_index (list): The in-memory sparse index.
    bloom_filter (BloomFilter): The bloom filter for this table.

#### `get(self, key)`

```python
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
```

#### `read_iter(self)`

```python
Returns an iterator over the key-value pairs in the SSTable.
```

## `bloom_filter.py`

This module implements a Bloom Filter.

A Bloom filter is a space-efficient probabilistic data structure that is used to
test whether an element is a member of a set. False positive matches are
possible, but false negatives are not.

### `BloomFilter`

A Bloom Filter implementation.

Attributes:
    size (int): The size of the bit array.
    hash_count (int): The number of hash functions to use.
    bit_array (list): The bit array.

#### `add(self, item: str)`

```python
Add an item to the Bloom Filter.

This sets the bits at the hash indices to 1.

Args:
    item (str): The item to add.
```

## `levensthein.py`

This module provides an implementation of the Levenshtein distance algorithm.

### `Levenshtein`

A class for calculating the Levenshtein distance between two strings.

#### `distance(s1, s2)`

```python
Calculates the Levenshtein distance between two strings.

The Levenshtein distance is the number of single-character edits
(insertions, deletions, or substitutions) required to change one
string into the other.

Args:
    s1 (str): The first string.
    s2 (str): The second string.

Returns:
    int: The Levenshtein distance between the two strings.
```

## `heap.py`

This module provides a custom implementation of a Min-Heap data structure.

### `MinHeap`

A Min-Heap implementation.

#### `heappop(self)`

```python
Pops the smallest item from the heap.
```

#### `heappush(self, item)`

```python
Pushes an item onto the heap.
```

