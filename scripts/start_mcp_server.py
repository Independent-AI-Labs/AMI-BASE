#!/usr/bin/env python
"""Base MCP Server launcher with automatic .venv creation.

This is the BASE implementation that all modules should use.
It ensures the module has a .venv and creates it if missing.
"""

import subprocess
import sys
from pathlib import Path


def _create_and_setup_venv(project_root: Path, venv_dir: Path, venv_python: Path) -> int:
    """Create virtual environment and install all dependencies.

    Args:
        project_root: Root directory of the module
        venv_dir: Path to .venv directory
        venv_python: Path to python.exe in venv

    Returns:
        0 on success, 1 on error
    """
    print(f"ERROR: Virtual environment not found at {venv_dir}")
    print("Creating virtual environment...")
    try:
        subprocess.run(["uv", "venv", str(venv_dir)], cwd=str(project_root), check=True)
        print("Virtual environment created")

        # ALWAYS install base requirements FIRST - modules depend on base!
        base_root = Path(__file__).parent.parent  # base directory
        base_requirements = base_root / "requirements.txt"
        if base_requirements.exists():
            print(f"Installing BASE dependencies from {base_requirements}...")
            subprocess.run(["uv", "pip", "install", "--python", str(venv_python), "-r", str(base_requirements)], cwd=str(project_root), check=True)
            print("Base dependencies installed")
        else:
            print(f"ERROR: Base requirements.txt not found at {base_requirements}")
            return 1

        # Then install module's own requirements
        requirements_file = project_root / "requirements.txt"
        if requirements_file.exists():
            print(f"Installing MODULE dependencies from {requirements_file}...")
            subprocess.run(["uv", "pip", "install", "--python", str(venv_python), "-r", "requirements.txt"], cwd=str(project_root), check=True)
            print("Module dependencies installed")
        else:
            print(f"WARNING: No requirements.txt found at {requirements_file}")

        # Install base test requirements
        base_test_requirements = base_root / "requirements-test.txt"
        if base_test_requirements.exists():
            print(f"Installing BASE test dependencies from {base_test_requirements}...")
            subprocess.run(["uv", "pip", "install", "--python", str(venv_python), "-r", str(base_test_requirements)], cwd=str(project_root), check=True)
            print("Base test dependencies installed")

        # Also install module's test dependencies if they exist
        test_requirements = project_root / "requirements-test.txt"
        if test_requirements.exists():
            print(f"Installing MODULE test dependencies from {test_requirements}...")
            subprocess.run(["uv", "pip", "install", "--python", str(venv_python), "-r", "requirements-test.txt"], cwd=str(project_root), check=True)
            print("Module test dependencies installed")

        # Install essential test packages
        print("Installing essential packages for pre-commit...")
        subprocess.run(
            [
                "uv",
                "pip",
                "install",
                "--python",
                str(venv_python),
                "pre-commit",
                "pytest",
                "pytest-asyncio",
                "pytest-timeout",
                "pytest-mock",
                "pytest-cov",
                "websockets",
            ],
            cwd=str(project_root),
            check=True,
        )
        print("Essential packages installed")
        return 0

    except subprocess.CalledProcessError as e:
        print(f"ERROR: Failed to create virtual environment: {e}")
        return 1
    except FileNotFoundError:
        print("ERROR: 'uv' command not found. Please install uv first.")
        return 1


def main(project_root: Path, mcp_base_path: str, project_name: str) -> int:  # noqa: PLR0911
    """Run MCP server for any module with automatic .venv creation.

    Args:
        project_root: Root directory of the module (where .venv should be)
        mcp_base_path: Path to the MCP server directory (e.g., backend/mcp/filesys)
        project_name: Name of the project for display

    Returns:
        Exit code
    """
    print(f"Starting {project_name}")
    print(f"Project root: {project_root}")
    print(f"MCP base path: {mcp_base_path}")

    # Ensure project_root is a Path object
    project_root = Path(project_root)

    # Check for .venv and CREATE IT if missing - NO FALLBACKS!
    venv_dir = project_root / ".venv"
    venv_python = venv_dir / "Scripts" / "python.exe"

    if not venv_dir.exists():
        result = _create_and_setup_venv(project_root, venv_dir, venv_python)
        if result != 0:
            return result

    if not venv_python.exists():
        print(f"ERROR: Python executable not found at {venv_python}")
        print("Virtual environment exists but is corrupted. Please delete .venv and try again.")
        return 1

    # Find the run_stdio.py script
    run_stdio_path = Path(mcp_base_path) / "run_stdio.py"
    if not run_stdio_path.exists():
        print(f"ERROR: MCP server script not found at {run_stdio_path}")
        return 1

    # Run the MCP server with the MODULE'S OWN PYTHON
    python_exe = str(venv_python)
    print(f"Python: {python_exe}")
    print(f"Script: {run_stdio_path}")
    print("=" * 60)

    try:
        proc = subprocess.run([python_exe, str(run_stdio_path)], cwd=str(project_root), check=False)
        return proc.returncode
    except KeyboardInterrupt:
        print(f"\n{project_name} stopped by user")
        return 0
    except Exception as e:
        print(f"ERROR: Failed to start MCP server: {e}")
        return 1


if __name__ == "__main__":
    print("This is a base module - import and call main() from your module's script")
    sys.exit(1)
