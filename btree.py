"""
This module implements a B-Tree data structure.

The B-Tree can be used as a standalone storage engine or as the in-memory
memtable for the LSM-Tree storage engine. It keeps key-value pairs sorted by
key and provides efficient insertion, search, and update operations. When used
as a standalone engine, it uses a Write-Ahead Log (WAL) for durability.
"""

from storage_engine.wal import WriteAheadLog
import logging
import os
from .engine import StorageEngine
from .sstable import TOMBSTONE

logger = logging.getLogger(__name__)

class BTreeNode:
    """
    A node in the B-Tree.

    Attributes:
        leaf (bool): True if the node is a leaf, False otherwise.
        keys (list): The list of keys in the node.
        values (list): The list of values corresponding to the keys.
        child (list): The list of child nodes.
    """
    def __init__(self, leaf=False):
        self.leaf = leaf
        self.keys = []
        self.values = []
        self.child = []

class BTree(StorageEngine):
    """
    A B-Tree data structure.

    Attributes:
        root (BTreeNode): The root node of the tree.
        t (int): The minimum degree of the B-Tree. This determines the
                 minimum and maximum number of keys a node can have.
        wal (WriteAheadLog): The write-ahead log for crash recovery.
    """
    def __init__(self, t, wal_path='btree.wal'):
        """
        Initializes the B-Tree.

        Args:
            t (int): The minimum degree of the B-Tree.
            wal_path (str): The path to the write-ahead log file.
        """
        logger.info(f"Initializing B-Tree with t={t} and wal_path='{wal_path}'.")
        self.root = BTreeNode(True)
        self.t = t
        dir_path = os.path.dirname(wal_path)
        if dir_path and not os.path.exists(dir_path):
            os.makedirs(dir_path)
        self.wal = WriteAheadLog(wal_path)
        self.wal.recover(self)
        logger.info("B-Tree initialized and recovered from WAL.")

    def put(self, key, value):
        """
        Store a key-value pair.
        """
        self.insert(key, value)

    def get(self, key):
        """
        Retrieve a value by key.
        """
        return self.search(key)

    def delete(self, key):
        """
        Delete a key-value pair by inserting a tombstone.
        """
        self.insert(key, TOMBSTONE)

    def insert(self, k, v, wal=True):
        """
        Inserts a key-value pair into the B-Tree.

        If the key already exists, its value is updated.

        Args:
            k: The key to insert.
            v: The value to associate with the key.
            wal (bool): Whether to write the operation to the WAL.
        """
        logger.debug(f"B-Tree Insert: key='{k}', value='{v}'.")
        if wal:
            self.wal.write('insert', k, v)
        # if key already exists, update it
        existing_value = self.search(k)
        if existing_value is not None:
            logger.debug(f"Key '{k}' already exists. Updating value.")
            self._update(self.root, k, v)
            return

        root = self.root
        if len(root.keys) == (2 * self.t) - 1:
            logger.debug("Root is full. Splitting root.")
            temp = BTreeNode()
            self.root = temp
            temp.child.insert(0, root)
            self.split_child(temp, 0)
            self.insert_non_full(temp, k, v)
        else:
            self.insert_non_full(root, k, v)

    def insert_non_full(self, x, k, v):
        """
        A helper function to insert a key into a node that is not full.

        Args:
            x (BTreeNode): The node to insert into.
            k: The key to insert.
            v: The value to associate with the key.
        """
        logger.debug(f"Inserting key '{k}' into node with {len(x.keys)} keys.")
        i = len(x.keys) - 1
        if x.leaf:
            x.keys.append(None)
            x.values.append(None)
            while i >= 0 and k < x.keys[i]:
                x.keys[i + 1] = x.keys[i]
                x.values[i + 1] = x.values[i]
                i -= 1
            x.keys[i + 1] = k
            x.values[i + 1] = v
            logger.debug(f"Inserted key '{k}' into leaf node at index {i + 1}.")
        else:
            while i >= 0 and k < x.keys[i]:
                i -= 1
            i += 1
            if len(x.child[i].keys) == (2 * self.t) - 1:
                logger.debug(f"Child at index {i} is full. Splitting.")
                self.split_child(x, i)
                if k > x.keys[i]:
                    i += 1
            self.insert_non_full(x.child[i], k, v)

    def split_child(self, x, i):
        """
        Splits a full child of a node.

        Args:
            x (BTreeNode): The parent node whose child is to be split.
            i (int): The index of the child in the parent's child list.
        """
        logger.debug(f"Splitting child at index {i} of node with {len(x.keys)} keys.")
        t = self.t
        y = x.child[i]
        z = BTreeNode(y.leaf)
        x.child.insert(i + 1, z)
        x.keys.insert(i, y.keys[t - 1])
        x.values.insert(i, y.values[t - 1])
        z.keys = y.keys[t:(2 * t) - 1]
        z.values = y.values[t:(2 * t) - 1]
        y.keys = y.keys[0:t - 1]
        y.values = y.values[0:t - 1]
        if not y.leaf:
            z.child = y.child[t:(2 * t)]
            y.child = y.child[0:t]
        logger.debug(f"Split complete. Parent now has {len(x.keys)} keys.")

    def search(self, k, x=None):
        """
        Searches for a key in the B-Tree.

        Args:
            k: The key to search for.
            x (BTreeNode, optional): The node to start the search from.
                                     If None, starts from the root.

        Returns:
            The value associated with the key if found, otherwise None.
        """
        if x is None:
            x = self.root
            logger.debug(f"B-Tree Search: key='{k}' from root.")

        i = 0
        while i < len(x.keys) and k > x.keys[i]:
            i += 1
        if i < len(x.keys) and k == x.keys[i]:
            logger.debug(f"Key '{k}' found in node.")
            return x.values[i]
        elif x.leaf:
            logger.debug(f"Key '{k}' not found, reached leaf.")
            return None
        else:
            logger.debug(f"Key '{k}' not in current node, descending to child {i}.")
            return self.search(k, x.child[i])

    def items(self):
        """
        Returns all key-value pairs in the tree, sorted by key.

        Returns:
            list: A list of (key, value) tuples.
        """
        return self._items(self.root)

    def _items(self, x):
        """
        A recursive helper to traverse the tree and collect all items.

        Args:
            x (BTreeNode): The current node.

        Returns:
            list: A list of (key, value) tuples from the subtree rooted at x.
        """
        if x.leaf:
            return list(zip(x.keys, x.values))
        else:
            result = []
            for i in range(len(x.keys)):
                result.extend(self._items(x.child[i]))
                result.append((x.keys[i], x.values[i]))
            result.extend(self._items(x.child[len(x.keys)]))
            return result

    def _update(self, x, k, v):
        """
        A helper function to find and update a key with a new value.

        Args:
            x (BTreeNode): The node to start the search from.
            k: The key to update.
            v: The new value.

        Returns:
            bool: True if the update was successful, False otherwise.
        """
        if x is not None:
            i = 0
            while i < len(x.keys) and k > x.keys[i]:
                i += 1
            if i < len(x.keys) and k == x.keys[i]:
                x.values[i] = v
                return True
            elif x.leaf:
                return False
            else:
                return self._update(x.child[i], k, v)
        return False
