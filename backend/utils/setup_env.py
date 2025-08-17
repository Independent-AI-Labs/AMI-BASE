#!/usr/bin/env python
"""CENTRAL environment setup using uv.

This script sets up .venv for any module and returns the Python path.
"""

import subprocess
import sys
from pathlib import Path


def find_module_root(start_path: Path) -> Path:
    """Find module root (has backend/ and requirements.txt)."""
    current = Path(start_path).resolve()
    while current != current.parent:
        if (current / "backend").exists() and (current / "requirements.txt").exists():
            return current
        current = current.parent
    raise RuntimeError(f"Could not find module root from {start_path}")


def find_base_path(start_path: Path) -> Path:
    """Find base module."""
    current = Path(start_path).resolve()

    # Check if we ARE base
    if (current / "backend" / "utils" / "setup_env.py").exists():
        return current

    # Search for main/base
    while current != current.parent:
        if (current / "base").exists() and (current / ".git").exists():
            return current / "base"
        current = current.parent

    raise RuntimeError(f"Could not find base from {start_path}")


def setup_venv(module_root: Path) -> Path:
    """Set up .venv for a module and return Python path."""
    venv_dir = module_root / ".venv"
    venv_python = venv_dir / "Scripts" / "python.exe"

    # If exists and working, return it
    if venv_python.exists():
        try:
            result = subprocess.run([str(venv_python), "--version"], capture_output=True, text=True, timeout=5, check=False)
            if result.returncode == 0:
                return venv_python
        except Exception:  # noqa: S110
            pass  # Will recreate below

    # Create venv
    print(f"Creating .venv at {venv_dir}...")
    subprocess.run(["uv", "venv", str(venv_dir)], cwd=str(module_root), check=True)

    # Install module requirements
    requirements = module_root / "requirements.txt"
    if requirements.exists():
        print(f"Installing {requirements}...")
        subprocess.run(["uv", "pip", "install", "--python", str(venv_python), "-r", str(requirements)], cwd=str(module_root), check=True)

    # If not base, install base requirements too
    base_path = find_base_path(module_root)
    if base_path != module_root:
        base_reqs = base_path / "requirements.txt"
        if base_reqs.exists():
            print(f"Installing base requirements from {base_reqs}...")
            subprocess.run(["uv", "pip", "install", "--python", str(venv_python), "-r", str(base_reqs)], cwd=str(module_root), check=True)

    # Install pre-commit for git hooks
    print("Installing pre-commit...")
    subprocess.run(["uv", "pip", "install", "--python", str(venv_python), "pre-commit"], cwd=str(module_root), check=True)

    # ACTUALLY INSTALL THE FUCKING GIT HOOKS
    print("Installing git hooks...")
    subprocess.run([str(venv_python), "-m", "pre_commit", "install"], cwd=str(module_root), check=True)
    subprocess.run([str(venv_python), "-m", "pre_commit", "install", "--hook-type", "pre-push"], cwd=str(module_root), check=True)

    print(f"Environment ready: {venv_python}")
    return venv_python


def get_python_for_script(script_path: Path) -> Path:
    """Get the correct Python for a script, setting up .venv if needed."""
    module_root = find_module_root(script_path)
    return setup_venv(module_root)


if __name__ == "__main__":
    # When run directly, set up env for current directory
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path.cwd()

    python = get_python_for_script(path)
    print(f"Python: {python}")
