#!/usr/bin/env python
"""Common MCP runner setup functionality.

THIS IS THE SINGLE SOURCE OF TRUTH FOR ALL MCP RUNNER SCRIPTS!
All run_stdio.py and run_websocket.py scripts MUST use this.
"""

import subprocess
import sys
from pathlib import Path


def find_module_root(script_path: Path) -> Path:
    """Find the module root (directory with .git or backend/).

    Args:
        script_path: Path to the calling script (__file__)

    Returns:
        Path to module root

    Raises:
        RuntimeError: If module root cannot be found
    """
    current = Path(script_path).resolve().parent
    while current != current.parent:
        # Found a module root - has backend/ directory
        if (current / "backend").exists() and ((current / ".git").exists() or (current / "requirements.txt").exists()):
            return current
        current = current.parent

    raise RuntimeError(f"Could not find module root for {script_path}")


def find_base_module(start_path: Path) -> Path:
    """Find the base module directory.

    Args:
        start_path: Path to start searching from

    Returns:
        Path to base module

    Raises:
        RuntimeError: If base module cannot be found
    """
    current = Path(start_path).resolve()

    # First check if we ARE in base
    if (current / "backend" / "utils" / "mcp_runner_setup.py").exists():
        return current

    # Search upwards for main orchestrator with base/
    while current != current.parent:
        if (current / "base").exists() and (current / ".git").exists():
            return current / "base"
        current = current.parent

    raise RuntimeError(f"Could not find base module from {start_path}")


def ensure_venv_exists(module_root: Path) -> Path:
    """Ensure .venv exists for a module, create if missing.

    Args:
        module_root: Root directory of the module

    Returns:
        Path to Python executable in venv

    Raises:
        RuntimeError: If venv creation fails
    """
    venv_dir = module_root / ".venv"
    venv_python = venv_dir / "Scripts" / "python.exe"

    if venv_dir.exists() and venv_python.exists():
        return venv_python

    print(f"Creating virtual environment at {venv_dir}...")

    try:
        # Create venv with uv
        subprocess.run(["uv", "venv", str(venv_dir)], cwd=str(module_root), check=True, capture_output=True, text=True)

        # Install requirements
        requirements_file = module_root / "requirements.txt"
        if requirements_file.exists():
            print(f"Installing requirements from {requirements_file}...")
            subprocess.run(
                ["uv", "pip", "install", "--python", str(venv_python), "-r", str(requirements_file)],
                cwd=str(module_root),
                check=True,
                capture_output=True,
                text=True,
            )

        # If this is NOT base, also install base requirements
        base_module = find_base_module(module_root)
        if base_module != module_root:
            base_requirements = base_module / "requirements.txt"
            if base_requirements.exists():
                print(f"Installing base requirements from {base_requirements}...")
                subprocess.run(
                    ["uv", "pip", "install", "--python", str(venv_python), "-r", str(base_requirements)],
                    cwd=str(module_root),
                    check=True,
                    capture_output=True,
                    text=True,
                )

        # Install pre-commit for all modules (needed for git hooks)
        print("Installing pre-commit for git hooks...")
        subprocess.run(
            ["uv", "pip", "install", "--python", str(venv_python), "pre-commit"],
            cwd=str(module_root),
            check=True,
            capture_output=True,
            text=True,
        )

        # ACTUALLY INSTALL THE FUCKING GIT HOOKS
        print("Installing git hooks...")
        subprocess.run([str(venv_python), "-m", "pre_commit", "install"], cwd=str(module_root), check=True)
        subprocess.run([str(venv_python), "-m", "pre_commit", "install", "--hook-type", "pre-push"], cwd=str(module_root), check=True)

        print(f"Virtual environment created successfully at {venv_dir}")
        return venv_python

    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to create venv: {e.stderr}") from e
    except FileNotFoundError as e:
        raise RuntimeError("uv not found. Please install uv first.") from e


def setup_mcp_runner(script_path: Path) -> tuple[Path, Path, Path]:
    """Set up environment for any MCP runner script.

    This function:
    1. Finds the module root (where .venv should be)
    2. Ensures .venv exists (creates if missing)
    3. Finds the base module
    4. Sets up sys.path correctly

    Args:
        script_path: Path to the calling script (__file__)

    Returns:
        Tuple of (module_root, base_path, venv_python)

    Raises:
        RuntimeError: If setup fails
    """
    # Find module root
    module_root = find_module_root(script_path)
    print(f"Module root: {module_root}")

    # Ensure .venv exists
    venv_python = ensure_venv_exists(module_root)

    # Check if we're running with the correct Python
    current_python = Path(sys.executable).resolve()
    expected_python = venv_python.resolve()

    if current_python != expected_python:
        print("Wrong Python detected. Re-executing with correct Python...")
        print(f"  Current: {current_python}")
        print(f"  Expected: {expected_python}")

        # Re-execute the script with the correct Python using subprocess
        result = subprocess.run([str(expected_python)] + sys.argv, check=False)
        sys.exit(result.returncode)

    # Find base module
    base_path = find_base_module(module_root)
    print(f"Base module: {base_path}")

    # Set up sys.path correctly
    # Clear any existing entries to avoid conflicts
    for p in [str(module_root), str(base_path.parent), str(base_path)]:
        while p in sys.path:
            sys.path.remove(p)

    # Add paths in correct order
    # 1. Module root first (for module's own imports)
    sys.path.insert(0, str(module_root))

    # 2. Main orchestrator root (for "from base..." imports)
    if base_path.parent != module_root:
        sys.path.insert(1, str(base_path.parent))

    # 3. Base module (for direct base imports)
    if base_path != module_root:
        sys.path.insert(2, str(base_path))

    return module_root, base_path, venv_python
