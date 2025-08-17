#!/usr/bin/env python
"""Smart path finding utilities that use .git landmarks."""

from pathlib import Path


def find_base_module(start_path: Path) -> Path:
    """Find the base module by walking up to .git directories.

    This function intelligently finds the base module by:
    1. Walking up from start_path to find .git directories
    2. Checking if we found the main orchestrator (has base/ subdirectory)
    3. Or if we're already IN base (has backend/mcp/run_server.py)

    NO HARDCODED PARENT COUNTS!
    """
    current = Path(start_path).resolve()
    if current.is_file():
        current = current.parent

    # Walk up to find .git directories
    while current != current.parent:
        if (current / ".git").exists():
            # Check if this is the main orchestrator with base/
            if (current / "base").exists() and (current / "base" / "backend").exists():
                return current / "base"
            # Check if we ARE in base
            if current.name == "base" and (current / "backend" / "mcp" / "run_server.py").exists():
                return current
        current = current.parent

    raise RuntimeError(f"Could not find base module from {start_path}")


def find_module_root(start_path: Path) -> Path:
    """Find the module root (where .venv should be created).

    Module root is the first .git we encounter going up, or
    a directory that has backend/ and requirements.txt.
    """
    current = Path(start_path).resolve()
    if current.is_file():
        current = current.parent

    while current != current.parent:
        # Found a .git - this is a module root
        if (current / ".git").exists():
            return current
        # Or found a module structure without .git (shouldn't happen but defensive)
        if (current / "backend").exists() and (current / "requirements.txt").exists():
            return current
        current = current.parent

    raise RuntimeError(f"Could not find module root from {start_path}")


def setup_base_import(script_path: Path) -> Path:
    """Set up sys.path to import from base. Returns base path.

    Call this at the TOP of any runner script before imports!
    """
    import sys

    base_path = find_base_module(script_path)

    # Add to sys.path if not already there
    base_str = str(base_path)
    if base_str not in sys.path:
        sys.path.insert(0, base_str)

    return base_path
