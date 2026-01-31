"""
This module provides a custom implementation of a Min-Heap data structure.
"""

class MinHeap:
    """
    A Min-Heap implementation.
    """
    def __init__(self):
        self.heap = []

    def heappush(self, item):
        """
        Pushes an item onto the heap.
        """
        self.heap.append(item)
        self._heapify_up(len(self.heap) - 1)

    def heappop(self):
        """
        Pops the smallest item from the heap.
        """
        if not self.heap:
            raise IndexError("pop from an empty heap")
        if len(self.heap) == 1:
            return self.heap.pop()

        root = self.heap[0]
        self.heap[0] = self.heap.pop()
        self._heapify_down(0)
        return root

    def _heapify_up(self, index):
        parent_index = (index - 1) // 2
        if index > 0 and self.heap[index] < self.heap[parent_index]:
            self.heap[index], self.heap[parent_index] = self.heap[parent_index], self.heap[index]
            self._heapify_up(parent_index)

    def _heapify_down(self, index):
        smallest = index
        left_child_index = 2 * index + 1
        right_child_index = 2 * index + 2

        if left_child_index < len(self.heap) and self.heap[left_child_index] < self.heap[smallest]:
            smallest = left_child_index

        if right_child_index < len(self.heap) and self.heap[right_child_index] < self.heap[smallest]:
            smallest = right_child_index

        if smallest != index:
            self.heap[index], self.heap[smallest] = self.heap[smallest], self.heap[index]
            self._heapify_down(smallest)

    def __len__(self):
        return len(self.heap)
