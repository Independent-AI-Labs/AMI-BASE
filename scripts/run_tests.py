#!/usr/bin/env python
"""Generic test runner using consolidated path utilities."""

import subprocess
import sys
from pathlib import Path

# Add base to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.utils.path_utils import EnvironmentSetup, ModuleSetup, PathFinder  # noqa: E402


class TestRunner:
    """Generic test runner for AMI modules."""

    def __init__(self, project_root: Path | None = None, project_name: str | None = None):
        """Initialize test runner.

        Args:
            project_root: Path to project root (defaults to current location)
            project_name: Name of the project for display
        """
        # Use consolidated path utilities
        if project_root:
            self.project_root = Path(project_root).resolve()
        else:
            self.project_root = PathFinder.find_module_root(Path.cwd())

        self.project_name = project_name or self.project_root.name
        self.venv_python = EnvironmentSetup.get_venv_python(self.project_root)

        # Ensure we're in the correct virtual environment
        ModuleSetup.ensure_running_in_venv(Path(__file__))

    def run_pytest(self, pytest_args):
        """Run pytest with the provided arguments.

        Args:
            pytest_args: List of arguments to pass to pytest

        Returns:
            Exit code from pytest
        """
        # Build the command
        cmd = [str(self.venv_python), "-m", "pytest"] + pytest_args

        # Set working directory to project root
        print(f"Running tests in {self.project_root}")
        print(f"Command: {' '.join(cmd)}")

        # Run pytest
        result = subprocess.run(cmd, cwd=str(self.project_root), check=False)
        return result.returncode

    def run(self, args):
        """Main run method.

        Args:
            args: Command line arguments

        Returns:
            Exit code
        """
        print("=" * 60)
        print(f"Running {self.project_name} Tests")
        print("=" * 60)

        # Run pytest with all provided arguments
        return self.run_pytest(args)


def main(project_root: Path | None = None, project_name: str | None = None):
    """Main entry point for test runner.

    Args:
        project_root: Path to project root
        project_name: Name of the project

    Returns:
        Exit code
    """
    runner = TestRunner(project_root=project_root, project_name=project_name)

    # Get command line arguments (skip script name)
    args = sys.argv[1:]

    # Add default timeout if not specified
    if "--timeout" not in " ".join(args):
        args.extend(["--timeout", "600"])  # 10 minute default timeout

    return runner.run(args)


if __name__ == "__main__":
    sys.exit(main())
