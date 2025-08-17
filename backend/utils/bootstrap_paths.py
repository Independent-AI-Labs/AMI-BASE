#!/usr/bin/env python
"""Bootstrap path setup - copy this to any module that needs it.

Usage:
    At the top of any script, just do:

    import bootstrap_paths  # noqa

    That's it! Your paths are set up.
"""

import sys
from pathlib import Path


def bootstrap():
    """Bootstrap the path setup by finding and importing the main path_setup module."""
    current = Path(__file__).resolve().parent

    # Find the main orchestrator root (has base/ and .git)
    while current != current.parent:
        if (current / "base").exists() and (current / ".git").exists():
            # Add base to path
            base_path = current / "base"
            if str(base_path) not in sys.path:
                sys.path.insert(0, str(base_path))

            # Now we can import the real path setup
            from services.utils.path_setup import setup_paths

            return setup_paths()
        current = current.parent

    # Fallback: just add parents to path
    file_path = Path(__file__).resolve()
    for _ in range(5):  # Go up 5 levels max
        file_path = file_path.parent
        if str(file_path) not in sys.path:
            sys.path.append(str(file_path))

    return file_path, None


# Auto-run on import
module_root, main_root = bootstrap()
