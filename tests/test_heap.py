import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
from heap import MinHeap

def test_heappush_and_heappop(heap):
    heap.heappush(3)
    heap.heappush(1)
    heap.heappush(4)
    heap.heappush(1)
    heap.heappush(5)
    heap.heappush(9)

    assert len(heap) == 6
    assert heap.heappop() == 1
    assert heap.heappop() == 1
    assert heap.heappop() == 3
    assert heap.heappop() == 4
    assert heap.heappop() == 5
    assert heap.heappop() == 9
    assert len(heap) == 0

def test_heappop_empty(heap):
    with pytest.raises(IndexError):
        heap.heappop()

def test_mixed_types(heap):
    heap.heappush((1, 'a'))
    heap.heappush((3, 'c'))
    heap.heappush((2, 'b'))

    assert heap.heappop() == (1, 'a')
    assert heap.heappop() == (2, 'b')
    assert heap.heappop() == (3, 'c')
