"""Pytest configuration: ensure the project root is on sys.path for all tests."""
# Ensure project root is on sys.path so tests can import local packages
import os
import sys

ROOT = os.path.abspath(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
