import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
from levensthein import Levenshtein

def test_distance_same_string():
    assert Levenshtein.distance("hello", "hello") == 0

def test_distance_empty_string():
    assert Levenshtein.distance("", "hello") == 5
    assert Levenshtein.distance("hello", "") == 5
    assert Levenshtein.distance("", "") == 0

def test_distance_insertion():
    assert Levenshtein.distance("cat", "cats") == 1

def test_distance_deletion():
    assert Levenshtein.distance("apple", "aple") == 1

def test_distance_substitution():
    assert Levenshtein.distance("book", "back") == 2

def test_distance_complex():
    assert Levenshtein.distance("kitten", "sitting") == 3
    assert Levenshtein.distance("flaw", "lawn") == 2
    assert Levenshtein.distance("saturday", "sunday") == 3

def test_distance_case_sensitive():
    assert Levenshtein.distance("Montrouge", "montchavin") == 7
