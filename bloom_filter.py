"""
This module implements a Bloom Filter.

A Bloom filter is a space-efficient probabilistic data structure that is used to
test whether an element is a member of a set. False positive matches are
possible, but false negatives are not.
"""

import logging

logger = logging.getLogger(__name__)

class BloomFilter:
    """
    A Bloom Filter implementation.

    Attributes:
        size (int): The size of the bit array.
        hash_count (int): The number of hash functions to use.
        bit_array (list): The bit array.
    """
    def __init__(self, size: int, hash_count: int):
        """
        Initializes the Bloom Filter.

        Args:
            size (int): The size of the bit array.
            hash_count (int): The number of hash functions.
        """
        logger.debug(f"Initializing BloomFilter with size={size} and hash_count={hash_count}.")
        self.size = size
        self.hash_count = hash_count
        self.bit_array = [0] * size

    def _hashes(self, item: str):
        """
        Generate a series of hash values for an item.

        Args:
            item (str): The item to hash.

        Yields:
            int: A hash value.
        """
        import hashlib
        for i in range(self.hash_count):
            hash_result = int(hashlib.md5((item + str(i)).encode()).hexdigest(), 16)
            yield hash_result % self.size

    def add(self, item: str):
        """
        Add an item to the Bloom Filter.

        This sets the bits at the hash indices to 1.

        Args:
            item (str): The item to add.
        """
        for hash_index in self._hashes(item):
            self.bit_array[hash_index] = 1

    def __contains__(self, item: str) -> bool:
        """
        Check if an item is in the Bloom Filter.

        Note that this can return false positives.

        Args:
            item (str): The item to check.

        Returns:
            bool: True if the item is likely in the set, False if it is
                  definitely not.
        """
        return all(self.bit_array[hash_index] for hash_index in self._hashes(item))
