#!/usr/bin/env python
"""Generic test runner that ensures correct environment and handles all test execution."""

import os
import subprocess
import sys
from pathlib import Path


class TestRunner:
    """Generic test runner for submodules."""

    def __init__(self, project_root=None):
        """Initialize test runner.

        Args:
            project_root: Path to project root. If None, uses parent of script location.
        """
        if project_root:
            self.project_root = Path(project_root).resolve()
        else:
            # Default to parent of where this script is located
            self.project_root = Path(__file__).parent.parent.resolve()

        self.venv_path = self.project_root / ".venv"
        self.venv_python = self._get_venv_python()

    def _get_venv_python(self):
        """Get the path to the virtual environment Python executable."""
        if sys.platform == "win32":
            return self.venv_path / "Scripts" / "python.exe"
        else:
            return self.venv_path / "bin" / "python"

    def setup_environment(self):
        """Set up the virtual environment by running setup.py if needed."""
        # Check if venv exists
        if not self.venv_path.exists():
            print(f"Setting up environment in {self.project_root}...")
            # Check if setup.py exists
            setup_py = self.project_root / "setup.py"
            if setup_py.exists():
                # Import and run the setup directly
                sys.path.insert(0, str(self.project_root))
                try:
                    from setup import run_environment_setup

                    result = run_environment_setup()
                    if result != 0:
                        print("ERROR: Failed to set up environment")
                        sys.exit(1)
                except ImportError:
                    print("WARNING: setup.py found but no run_environment_setup function")
                    # Try running setup.py directly
                    result = subprocess.run([sys.executable, str(setup_py)], cwd=self.project_root, check=False)
                    if result.returncode != 0:
                        print("ERROR: Failed to run setup.py")
                        sys.exit(1)
            else:
                print(f"ERROR: No setup.py found in {self.project_root}")
                print("Please create a virtual environment manually")
                sys.exit(1)

        # Verify venv Python exists
        if not self.venv_python.exists():
            print(f"ERROR: Virtual environment Python not found at {self.venv_python}")
            print(f"Run: python setup.py in {self.project_root}")
            sys.exit(1)

        return self.venv_python

    def ensure_base_module(self):
        """Ensure base module is available in PYTHONPATH."""
        base_paths = []

        # Check if we have a setup.py with ensure_base_module
        setup_py = self.project_root / "setup.py"
        if setup_py.exists():
            sys.path.insert(0, str(self.project_root))
            try:
                from setup import ensure_base_module

                base_path = ensure_base_module()
                if base_path:
                    base_paths.append(base_path)
            except ImportError:
                pass

        # Check for parent base directory
        parent_base = self.project_root.parent / "base"
        if parent_base.exists() and str(parent_base) not in base_paths:
            base_paths.append(str(parent_base))

        # Check for sibling base directory
        sibling_base = self.project_root / "base"
        if sibling_base.exists() and str(sibling_base) not in base_paths:
            base_paths.append(str(sibling_base))

        return base_paths

    def run_tests(self, test_args):
        """Run tests with the virtual environment Python.

        Args:
            test_args: List of arguments to pass to pytest

        Returns:
            Exit code from pytest
        """
        venv_python = self.setup_environment()

        # Get base module paths
        base_paths = self.ensure_base_module()

        # Build the test command
        cmd = [str(venv_python), "-m", "pytest"]

        # Add default args if none provided
        if not test_args:
            test_args = ["tests/", "-v", "--tb=short"]

        cmd.extend(test_args)

        # Set environment variables
        env = os.environ.copy()

        # Build PYTHONPATH
        python_paths = [str(self.project_root)]
        python_paths.extend(base_paths)

        # Add parent directory if we're a submodule
        if self.project_root.parent.exists():
            parent_path = str(self.project_root.parent)
            if parent_path not in python_paths:
                python_paths.append(parent_path)

        env["PYTHONPATH"] = os.pathsep.join(python_paths)

        # Run the tests
        print(f"\nRunning: {' '.join(cmd)}")
        print(f"Project Root: {self.project_root}")
        print(f"PYTHONPATH: {env['PYTHONPATH']}")
        print("=" * 60)

        result = subprocess.run(cmd, cwd=self.project_root, env=env, check=False)
        return result.returncode

    def clean_environment(self):
        """Clean the virtual environment."""
        print(f"Cleaning environment in {self.project_root}...")
        import shutil

        if self.venv_path.exists():
            shutil.rmtree(self.venv_path)
        print("Environment cleaned. Re-run to rebuild.")
        return 0

    def open_shell(self):
        """Open a shell in the test environment."""
        print(f"Opening shell in test environment at {self.project_root}...")
        venv_python = self.setup_environment()

        # Get base module paths
        base_paths = self.ensure_base_module()

        env = os.environ.copy()
        # Build PYTHONPATH
        python_paths = [str(self.project_root)]
        python_paths.extend(base_paths)
        env["PYTHONPATH"] = os.pathsep.join(python_paths)

        if sys.platform == "win32":
            # On Windows, activate the venv and open cmd
            activate_script = self.venv_path / "Scripts" / "activate.bat"
            subprocess.run(["cmd", "/k", str(activate_script)], cwd=self.project_root, env=env, check=False)
        else:
            # On Unix, start a shell with activated venv
            subprocess.run([str(venv_python)], cwd=self.project_root, env=env, check=False)
        return 0

    def show_help(self, project_name="Project"):
        """Show help message.

        Args:
            project_name: Name of the project for the help message
        """
        print(f"Test Runner for {project_name}")
        print("=" * 40)
        print("\nUsage:")
        print("  python run_tests.py [pytest args]")
        print("\nExamples:")
        print("  python run_tests.py                    # Run all tests")
        print("  python run_tests.py tests/unit/        # Run unit tests")
        print("  python run_tests.py -k test_properties # Run tests matching pattern")
        print("  python run_tests.py -x                 # Stop on first failure")
        print("  python run_tests.py --lf               # Run last failed tests")
        print("\nSpecial commands:")
        print("  python run_tests.py --clean            # Clean and rebuild environment")
        print("  python run_tests.py --shell            # Open shell in test environment")
        return 0


def main(project_root=None, project_name="Project"):
    """Main entry point for test runner.

    Args:
        project_root: Optional path to project root
        project_name: Name of the project for help message

    Returns:
        Exit code
    """
    # Create runner
    runner = TestRunner(project_root)

    # Get test arguments from command line
    test_args = sys.argv[1:]

    # Handle special commands
    if test_args and test_args[0] == "--help":
        return runner.show_help(project_name)

    if test_args and test_args[0] == "--clean":
        return runner.clean_environment()

    if test_args and test_args[0] == "--shell":
        return runner.open_shell()

    # Run the tests
    return runner.run_tests(test_args)


if __name__ == "__main__":
    # When run directly, use current directory as project root
    sys.exit(main())
