"""Common path setup utility for all modules.

This module provides a uniform way to set up Python paths for any script
in the AMI ecosystem, ensuring consistent imports across all modules.
"""

import sys
from pathlib import Path


def find_project_root(start_path: Path) -> Path:
    """Find the project root by looking for .git or .venv directories.

    A project root is identified by:
    1. Having a .git directory (it's a git repository)
    2. Having a .venv directory (it's a Python project)
    3. Having a backend/ directory (it's a module root)

    Args:
        start_path: Path to start searching from

    Returns:
        Path to project root
    """
    current = start_path.resolve()

    # First, traverse up to find a .git or .venv directory
    while current != current.parent:
        # Check if this is a git repo or has venv
        if (current / ".git").exists() or (current / ".venv").exists():
            # If it has backend/, it's likely a module root
            if (current / "backend").exists():
                return current
            # If it has base/, browser/, files/ etc, it's the main orchestrator
            if (current / "base").exists():
                # But we want the module root, not orchestrator root
                # So continue searching for the module
                pass
        current = current.parent

    # Fallback: return the start path's parent a few times
    return start_path.parent.parent.parent


def find_main_orchestrator_root(start_path: Path) -> Path | None:
    """Find the main AMI-ORCHESTRATOR root by looking for base/ directory.

    The main orchestrator is identified by having:
    - A base/ directory (core module)
    - A .git directory (main repo)

    Args:
        start_path: Path to start searching from

    Returns:
        Path to main orchestrator root, or None if not found
    """
    current = start_path.resolve()

    while current != current.parent:
        # The main repo has base/ directory and .git at its root
        if (current / "base").exists() and (current / ".git").exists():
            return current
        current = current.parent

    return None


def setup_paths() -> tuple[Path, Path | None]:
    """Smart path setup that works from ANY location.

    This function:
    1. Finds the module root (has .git or .venv and backend/)
    2. Finds the main orchestrator root (has base/ and .git)
    3. Adds both to sys.path for imports

    Call this at the top of any script:
        from path_setup import setup_paths  # Will work after first import
        module_root, main_root = setup_paths()

    Returns:
        Tuple of (module_root, main_root) where main_root may be None
    """
    import inspect

    # Get the caller's file path
    frame = inspect.currentframe()
    caller_file = Path(frame.f_back.f_code.co_filename).resolve() if frame and frame.f_back else Path.cwd()

    # Find module root
    module_root = find_project_root(caller_file)
    if str(module_root) not in sys.path:
        sys.path.insert(0, str(module_root))

    # Find main orchestrator root
    main_root = find_main_orchestrator_root(module_root)
    if main_root and str(main_root) not in sys.path:
        sys.path.insert(0, str(main_root))

    return module_root, main_root
