"""Pytest configuration for pg_dgraph tests."""
import sys
from pathlib import Path

# Add parent directory so pgdgraph can be imported
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
