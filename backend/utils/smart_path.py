#!/usr/bin/env python
"""Universal smart path finder for the AMI-ORCHESTRATOR project.

This module provides utilities to find project roots, git roots, and other
important directories regardless of where scripts are run from.
"""

import sys
from pathlib import Path


def find_git_root(start_path: Path | None = None) -> Path | None:
    """Find the git repository root by traversing up from start_path.

    Args:
        start_path: Starting directory (defaults to current script's directory)

    Returns:
        Path to git root or None if not found
    """
    if start_path is None:
        # Get the directory of the calling script
        import inspect

        frame = inspect.stack()[1]
        module = inspect.getmodule(frame[0])
        if module and hasattr(module, "__file__"):
            start_path = Path(module.__file__).resolve().parent
        else:
            start_path = Path.cwd()

    current = Path(start_path).resolve()

    # Traverse up until we find .git
    while current != current.parent:
        if (current / ".git").exists():
            return current
        current = current.parent

    return None


def find_venv_root(start_path: Path | None = None) -> Path | None:
    """Find the virtual environment root by traversing up from start_path.

    Args:
        start_path: Starting directory (defaults to current script's directory)

    Returns:
        Path to directory containing .venv or None if not found
    """
    if start_path is None:
        # Get the directory of the calling script
        import inspect

        frame = inspect.stack()[1]
        module = inspect.getmodule(frame[0])
        if module and hasattr(module, "__file__"):
            start_path = Path(module.__file__).resolve().parent
        else:
            start_path = Path.cwd()

    current = Path(start_path).resolve()

    # Traverse up until we find .venv
    while current != current.parent:
        if (current / ".venv").exists():
            return current
        current = current.parent

    return None


def find_project_root(start_path: Path | None = None) -> Path:
    """Find the project root for a given path.

    This looks for indicators like:
    - backend/ directory
    - setup.py or setup.cfg
    - requirements.txt
    - pyproject.toml

    Args:
        start_path: Starting directory (defaults to current script's directory)

    Returns:
        Path to project root
    """
    if start_path is None:
        # Get the directory of the calling script
        import inspect

        frame = inspect.stack()[1]
        module = inspect.getmodule(frame[0])
        if module and hasattr(module, "__file__"):
            start_path = Path(module.__file__).resolve().parent
        else:
            start_path = Path.cwd()

    current = Path(start_path).resolve()

    # Traverse up looking for project indicators
    while current != current.parent:
        # Check for project root indicators
        indicators = ["backend", "setup.py", "setup.cfg", "pyproject.toml", "requirements.txt"]

        if any((current / indicator).exists() for indicator in indicators):
            return current

        current = current.parent

    # If nothing found, return the start path
    return Path(start_path).resolve()


def setup_python_paths():
    """Smart Python path setup that works from anywhere.

    This function:
    1. Finds the git root
    2. Finds the project root (could be a submodule)
    3. Adds necessary paths to sys.path in the correct order

    Returns:
        Dict with information about paths found and added
    """
    import inspect

    frame = inspect.stack()[1]
    module = inspect.getmodule(frame[0])

    if module and hasattr(module, "__file__"):
        script_path = Path(module.__file__).resolve()
    else:
        script_path = Path.cwd() / "script.py"

    script_dir = script_path.parent

    info = {
        "script": str(script_path),
        "script_dir": str(script_dir),
        "git_root": None,
        "venv_root": None,
        "project_root": None,
        "base_path": None,
        "paths_added": [],
    }

    # Find git root
    git_root = find_git_root(script_dir)
    if git_root:
        info["git_root"] = str(git_root)

        # Add git root to path if needed
        if str(git_root) not in sys.path:
            sys.path.insert(0, str(git_root))
            info["paths_added"].append(str(git_root))

    # Find venv root (might be same as git root or different)
    venv_root = find_venv_root(script_dir)
    if venv_root:
        info["venv_root"] = str(venv_root)

    # Find project root (for submodules like files/, browser/, etc.)
    project_root = find_project_root(script_dir)
    info["project_root"] = str(project_root)

    # If project root is different from git root, it's a submodule
    if project_root != git_root and str(project_root) not in sys.path:
        # Add submodule root FIRST to avoid namespace collisions
        sys.path.insert(0, str(project_root))
        info["paths_added"].append(str(project_root))

    # Always add base module if it exists
    if git_root:
        base_path = git_root / "base"
        if base_path.exists() and str(base_path) not in sys.path:
            sys.path.insert(1, str(base_path))  # Insert after project root
            info["paths_added"].append(str(base_path))
            info["base_path"] = str(base_path)

    return info


def get_base_scripts_path() -> Path | None:
    """Get the path to base/scripts directory.

    Returns:
        Path to base/scripts or None if not found
    """
    git_root = find_git_root()
    if git_root:
        base_scripts = git_root / "base" / "scripts"
        if base_scripts.exists():
            return base_scripts
    return None


def get_venv_python() -> Path | None:
    """Get the path to the virtual environment Python executable.

    Returns:
        Path to venv Python or None if not found
    """
    venv_root = find_venv_root()
    if venv_root:
        if sys.platform == "win32":
            python_exe = venv_root / ".venv" / "Scripts" / "python.exe"
        else:
            python_exe = venv_root / ".venv" / "bin" / "python"

        if python_exe.exists():
            return python_exe
    return None


# Convenience function for scripts to call at the top
def auto_setup():
    """Automatically set up paths and return key directories.

    Call this at the top of any script for automatic path setup.

    Returns:
        Namespace with git_root, project_root, base_path, etc.
    """
    from types import SimpleNamespace

    info = setup_python_paths()

    return SimpleNamespace(
        git_root=Path(info["git_root"]) if info["git_root"] else None,
        project_root=Path(info["project_root"]) if info["project_root"] else None,
        base_path=Path(info["base_path"]) if info["base_path"] else None,
        venv_root=Path(info["venv_root"]) if info["venv_root"] else None,
        script_dir=Path(info["script_dir"]),
        paths_added=info["paths_added"],
    )
