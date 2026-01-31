from storage_engine.btree import BTree
from storage_engine.lsmtree import LSMTree

class DB:
    """
    A database wrapper that can use different storage engines.
    """
    def __init__(self, engine_type='btree', directory='data_dir'):
        """
        Initializes the database with the specified engine.

        Args:
            engine_type (str): 'btree' or 'lsmtree'.
            directory (str): The directory for data storage.
        """
        if engine_type == 'btree':
            self.engine = BTree(t=5, wal_path=f'{directory}/btree.wal')
        elif engine_type == 'lsmtree':
            self.engine = LSMTree(directory=directory)
        else:
            raise ValueError(f"Unknown engine type: {engine_type}")

    def put(self, key, value):
        """
        Store a key-value pair.
        """
        self.engine.put(key, value)

    def get(self, key):
        """
        Retrieve a value by key.
        """
        value = self.engine.get(key)
        if value == '__TOMBSTONE__':
            return None
        return value

    def delete(self, key):
        """
        Delete a key-value pair.
        """
        self.engine.delete(key)

    def fuzzy_get(self, search_key, max_distance):
        """
        Find keys that are similar to the search_key.
        Only available for the LSM-Tree engine.
        """
        if hasattr(self.engine, 'fuzzy_get'):
            return self.engine.fuzzy_get(search_key, max_distance)
        else:
            raise NotImplementedError("Fuzzy search is not supported by the current storage engine.")

    def close(self):
        """
        Close the database and flush any in-memory data.
        """
        if hasattr(self.engine, 'close'):
            self.engine.close()
