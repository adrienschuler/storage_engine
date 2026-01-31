from abc import ABC, abstractmethod

class StorageEngine(ABC):
    """
    Abstract base class for a storage engine.
    """

    @abstractmethod
    def put(self, key, value):
        """
        Store a key-value pair.
        """
        pass

    @abstractmethod
    def get(self, key):
        """
        Retrieve a value by key.
        """
        pass

    @abstractmethod
    def delete(self, key):
        """
        Delete a key-value pair.
        """
        pass
