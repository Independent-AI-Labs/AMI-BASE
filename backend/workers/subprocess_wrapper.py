"""Subprocess wrapper to ensure proper imports in Windows spawn mode."""
import os
import sys
from pathlib import Path


def initialize_subprocess():
    """Initialize subprocess environment for proper imports."""
    # Find the base directory
    current_file = Path(__file__)
    base_dir = current_file.parent.parent.parent

    # Add base to Python path if not already there
    base_str = str(base_dir.resolve())
    if base_str not in sys.path:
        sys.path.insert(0, base_str)

    # Set PYTHONPATH environment variable
    current_pythonpath = os.environ.get("PYTHONPATH", "")
    if current_pythonpath:
        os.environ["PYTHONPATH"] = f"{base_str}{os.pathsep}{current_pythonpath}"
    else:
        os.environ["PYTHONPATH"] = base_str

    return base_str


# Call this at module import time for subprocess
if __name__ != "__main__":
    # This runs when imported by subprocess
    initialize_subprocess()
