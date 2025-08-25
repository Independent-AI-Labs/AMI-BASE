#!/usr/bin/env python
"""Unified setup script for AMI modules using the consolidated path utilities.

This script provides a standard way to set up any AMI module's development
environment, including virtual environment, dependencies, and pre-commit hooks.
"""

import argparse
import subprocess
import sys
from pathlib import Path

# Add base to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from backend.utils.path_utils import EnvironmentSetup, PathFinder  # noqa: E402


class AMIModuleSetup:
    """Unified setup class for AMI modules."""

    def __init__(self, project_root: Path | None = None, project_name: str | None = None):
        """Initialize the setup.

        Args:
            project_root: Root directory of the project (defaults to current)
            project_name: Name of the project for display (defaults to directory name)
        """
        self.project_root = project_root or Path.cwd()
        self.project_name = project_name or self.project_root.name

        # Find paths using consolidated utilities
        try:
            self.module_root = PathFinder.find_module_root(self.project_root)
        except RuntimeError:
            self.module_root = self.project_root

        self.orchestrator_root = PathFinder.find_orchestrator_root(self.module_root)

        try:
            self.base_path = PathFinder.find_base_module(self.module_root)
        except RuntimeError:
            self.base_path = None

    def check_uv(self) -> bool:
        """Check if uv is installed."""
        try:
            subprocess.run(["uv", "--version"], capture_output=True, check=True)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("[ERROR] uv is not installed!")
            print("Install it with: pip install uv")
            return False

    def create_venv(self) -> bool:
        """Create virtual environment if it doesn't exist."""
        venv_path = self.module_root / ".venv"

        if not venv_path.exists():
            print(f"\n[1/5] Creating virtual environment at {venv_path}...")
            try:
                self.venv_python = EnvironmentSetup.ensure_venv_exists(self.module_root)
                print("[OK] Virtual environment created")
                return True
            except Exception as e:
                print(f"[ERROR] Failed to create virtual environment: {e}")
                return False
        else:
            print("\n[1/5] Virtual environment already exists")
            self.venv_python = EnvironmentSetup.get_venv_python(self.module_root)
            return True

    def install_requirements(self) -> bool:
        """Install dependencies from requirements.txt."""
        print("\n[2/5] Installing dependencies...")

        success = EnvironmentSetup.install_requirements(self.module_root, self.venv_python, self.base_path, "requirements.txt")

        if success:
            print("[OK] Dependencies installed")
        else:
            print("[ERROR] Failed to install dependencies")

        return success

    def install_test_requirements(self) -> bool:
        """Install test dependencies from requirements-test.txt."""
        print("\n[3/5] Installing test dependencies...")

        success = EnvironmentSetup.install_requirements(self.module_root, self.venv_python, self.base_path, "requirements-test.txt")

        if success:
            print("[OK] Test dependencies installed")
        else:
            # Not having test requirements is not fatal
            print("[INFO] No test dependencies to install")
            return True

        return success

    def install_package_editable(self) -> bool:
        """Install the package in editable mode if setup.py or pyproject.toml exists."""
        setup_py = self.module_root / "setup.py"
        pyproject_toml = self.module_root / "pyproject.toml"

        # Don't try to install setup.py itself
        if setup_py == Path(__file__).resolve():
            print("\n[4/5] Skipping package installation (this is setup.py)")
            return True

        if setup_py.exists() or pyproject_toml.exists():
            print("\n[4/5] Installing package in editable mode...")
            try:
                subprocess.run(["uv", "pip", "install", "--python", str(self.venv_python), "-e", "."], check=True, cwd=str(self.module_root))
                print("[OK] Package installed in editable mode")
                return True
            except subprocess.CalledProcessError as e:
                print(f"[ERROR] Failed to install package: {e}")
                return False
        else:
            print("\n[4/5] No setup.py or pyproject.toml found - skipping package installation")
            return True

    def install_precommit_hooks(self) -> bool:
        """Install pre-commit hooks."""
        print("\n[5/5] Installing pre-commit hooks...")

        # Copy platform-specific config if we have base
        if self.base_path:
            EnvironmentSetup.copy_platform_precommit_config(self.module_root, self.base_path)

        # Install hooks
        success = EnvironmentSetup.install_precommit_hooks(self.module_root, self.venv_python)

        if success:
            print("[OK] Pre-commit hooks installed")
        else:
            print("[WARN] Could not install pre-commit hooks")
            return True  # Not fatal

        return success

    def print_completion_message(self):
        """Print completion message with next steps."""
        print("\n" + "=" * 60)
        print(f"{self.project_name} Development Environment Setup Complete!")
        print("=" * 60)
        print("\nTo activate the virtual environment:")

        if sys.platform == "win32":
            print(f"  {self.module_root}\\.venv\\Scripts\\activate")
        else:
            print(f"  source {self.module_root}/.venv/bin/activate")

        # Check for common scripts
        if (self.module_root / "scripts" / "run_tests.py").exists():
            print("\nTo run tests:")
            print("  python scripts/run_tests.py")

        if (self.module_root / "Makefile").exists():
            print("\nMakefile targets available. Run:")
            print("  make help")

        print("=" * 60)

    def setup(self) -> int:
        """Run the complete setup process.

        Returns:
            0 on success, 1 on failure
        """
        print("=" * 60)
        print(f"Setting up {self.project_name} Development Environment")
        print(f"Module root: {self.module_root}")
        if self.orchestrator_root:
            print(f"Orchestrator root: {self.orchestrator_root}")
        if self.base_path:
            print(f"Base module: {self.base_path}")
        print("=" * 60)

        # Check prerequisites
        if not self.check_uv():
            return 1

        # Run setup steps
        steps = [
            self.create_venv,
            self.install_requirements,
            self.install_test_requirements,
            self.install_package_editable,
            self.install_precommit_hooks,
        ]

        for step in steps:
            if not step():
                print(f"\n[ERROR] Setup failed at step: {step.__name__}")
                return 1

        self.print_completion_message()
        return 0


def main():
    """Main entry point for the setup script."""
    parser = argparse.ArgumentParser(description="Set up development environment for AMI modules")
    parser.add_argument("--project-dir", type=Path, default=Path.cwd(), help="Project directory (default: current directory)")
    parser.add_argument("--project-name", type=str, help="Project name for display (default: directory name)")

    args = parser.parse_args()

    # Create setup instance
    setup = AMIModuleSetup(project_root=args.project_dir.resolve(), project_name=args.project_name)

    # Run setup
    return setup.setup()


if __name__ == "__main__":
    sys.exit(main())
