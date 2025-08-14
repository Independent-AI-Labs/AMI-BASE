#!/usr/bin/env python
"""Generic setup script for AMI submodules with automatic environment setup."""

import os
import subprocess
import sys
from pathlib import Path
from typing import Any


class GenericSetup:
    """Generic setup class for AMI submodules."""

    def __init__(
        self,
        project_root: Path | None = None,
        project_name: str = "AMI-BASE",
        base_repo_url: str = "https://github.com/Independent-AI-Labs/AMI-BASE.git",
        additional_package_info: dict[str, Any] | None = None,
    ):
        """Initialize generic setup.

        Args:
            project_root: Path to project root. If None, uses parent of script location.
            project_name: Name of the project for display.
            base_repo_url: URL to clone base module from if needed.
            additional_package_info: Additional package info to merge with defaults.
        """
        if project_root:
            self.project_root = Path(project_root).resolve()
        else:
            # Default to parent of where this script is located
            self.project_root = Path(__file__).parent.resolve()

        self.project_name = project_name
        self.base_repo_url = base_repo_url
        self.venv_path = self.project_root / ".venv"
        self.venv_python = self._get_venv_python()
        self.additional_package_info = additional_package_info or {}

        # Base module paths
        self.parent_base = self.project_root.parent / "base"
        self.local_base = self.project_root / "base"

    def _get_venv_python(self) -> Path:
        """Get the path to the virtual environment Python executable."""
        if sys.platform == "win32":
            return self.venv_path / "Scripts" / "python.exe"
        return self.venv_path / "bin" / "python"

    def ensure_base_module(self) -> str | None:
        """Ensure the base module is available.

        Logic:
        1. Check parent directory for base (submodule deployment)
        2. If not found, check/clone locally (standalone deployment)

        Returns:
            Path where base module was added to sys.path, or None if failed.
        """
        # First check parent directory (submodule scenario)
        if self.parent_base.exists() and (self.parent_base / "scripts" / "setup_env.py").exists():
            sys.path.insert(0, str(self.parent_base.parent))
            print(f"Using base module from parent (submodule mode): {self.parent_base}")
            return str(self.parent_base.parent)

        # Parent base not found - assume standalone deployment
        if self.local_base.exists() and (self.local_base / "scripts" / "setup_env.py").exists():
            sys.path.insert(0, str(self.project_root))
            print(f"Using local base module (standalone mode): {self.local_base}")
            return str(self.project_root)

        # Need to clone base module for standalone deployment
        print("Standalone deployment detected. Cloning base module...")
        try:
            subprocess.run(["git", "clone", self.base_repo_url, str(self.local_base)], check=True, cwd=self.project_root, capture_output=True)
            sys.path.insert(0, str(self.project_root))
            print(f"Base module cloned successfully to: {self.local_base}")
            return str(self.project_root)
        except subprocess.CalledProcessError:
            print(f"[ERROR] Failed to clone base module from {self.base_repo_url}")
            return None
        except FileNotFoundError:
            print("[ERROR] git is not installed")
            return None

    def run_environment_setup(self) -> int:
        """Run the environment setup using base module.

        Returns:
            Exit code (0 for success, 1 for failure).
        """
        base_path = self.ensure_base_module()
        if not base_path:
            return 1

        try:
            from base.scripts.setup_env import EnvironmentSetup

            setup = EnvironmentSetup(project_root=self.project_root, project_name=self.project_name)
            return setup.setup()
        except ImportError as e:
            print(f"[ERROR] Failed to import base.scripts.setup_env: {e}")
            return 1

    def install_pre_commit_hooks(self):
        """Install pre-commit hooks automatically."""
        # First check if there's a custom install_hooks.py script
        install_hooks_script = self.project_root / "scripts" / "install_hooks.py"
        if install_hooks_script.exists():
            # Use custom script if available
            python_exe = self.venv_python if self.venv_python.exists() else Path(sys.executable)
            try:
                result = subprocess.run([str(python_exe), str(install_hooks_script)], capture_output=True, text=True, check=False, cwd=self.project_root)
                if result.stdout:
                    print(result.stdout)
                if result.stderr and result.returncode != 0:
                    print(result.stderr)
                return
            except Exception as e:
                print(f"[WARN] Failed to run install_hooks.py: {e}")

        # Fallback to direct pre-commit installation if .pre-commit-config.yaml exists
        pre_commit_config = self.project_root / ".pre-commit-config.yaml"
        if pre_commit_config.exists():
            print("Installing pre-commit hooks...")

            # Find pre-commit executable
            if sys.platform == "win32":
                pre_commit_exe = self.venv_path / "Scripts" / "pre-commit.exe"
            else:
                pre_commit_exe = self.venv_path / "bin" / "pre-commit"

            if pre_commit_exe.exists():
                try:
                    # Install pre-commit hooks
                    subprocess.run([str(pre_commit_exe), "install"], check=True, cwd=self.project_root, capture_output=True)
                    print("[OK] Pre-commit hooks installed")

                    # Install pre-push hooks if supported
                    subprocess.run([str(pre_commit_exe), "install", "--hook-type", "pre-push"], check=True, cwd=self.project_root, capture_output=True)
                    print("[OK] Pre-push hooks installed")
                except subprocess.CalledProcessError as e:
                    print(f"[WARN] Failed to install pre-commit hooks: {e}")
            else:
                print("[WARN] pre-commit not found in virtual environment")

    def get_package_info(self) -> dict[str, Any]:
        """Get package information for setuptools.

        Override this method in subclasses to provide custom package info.

        Returns:
            Dictionary with package information for setuptools.
        """
        base_info = {
            "name": self.project_name.lower().replace(" ", "-"),
            "version": "0.1.0",
            "python_requires": ">=3.12",
            "install_requires": [],
            "author": "AMI",
        }
        # Merge with additional package info if provided
        return {**base_info, **self.additional_package_info}

    def setup_package(self):
        """Run the package setup with setuptools."""
        # Ensure setuptools is available
        try:
            from setuptools import find_packages, setup
            from setuptools.command.develop import develop
            from setuptools.command.egg_info import egg_info
            from setuptools.command.install import install
        except ImportError:
            print("ERROR: setuptools is required for package installation")
            print("Installing setuptools...")

            # Try to install setuptools using pip
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", "setuptools"])
                print("setuptools installed successfully. Please run setup.py again.")
            except subprocess.CalledProcessError:
                # If pip fails, try uv
                try:
                    subprocess.check_call(["uv", "pip", "install", "--python", sys.executable, "setuptools"])
                    print("setuptools installed successfully with uv. Please run setup.py again.")
                except (subprocess.CalledProcessError, FileNotFoundError):
                    print("\nFailed to install setuptools automatically.")
                    print("Please install it manually with one of:")
                    print("  pip install setuptools")
                    print("  uv pip install setuptools")

            sys.exit(1)

        # Create command classes for post-install hooks
        parent_self = self

        class PostDevelopCommand(develop):
            """Post-installation for development mode."""

            def run(self):
                develop.run(self)
                self.execute(parent_self.install_pre_commit_hooks, [], msg="Installing pre-commit hooks...")

        class PostInstallCommand(install):
            """Post-installation for installation mode."""

            def run(self):
                install.run(self)
                self.execute(parent_self.install_pre_commit_hooks, [], msg="Installing pre-commit hooks...")

        class PostEggInfoCommand(egg_info):
            """Post-processing after egg_info."""

            def run(self):
                egg_info.run(self)
                # Install hooks after egg_info (used by pip install -e .)
                if os.environ.get("INSTALLING_FOR_DEVELOPMENT"):
                    parent_self.install_pre_commit_hooks()

        # Get package info
        package_info = self.get_package_info()

        # Add packages if not specified
        if "packages" not in package_info:
            # Find packages, typically backend and its subpackages
            backend_dir = self.project_root / "backend"
            if backend_dir.exists():
                package_info["packages"] = find_packages(include=["backend", "backend.*"])
            else:
                package_info["packages"] = find_packages()

        # Add command classes
        package_info["cmdclass"] = {
            "develop": PostDevelopCommand,
            "install": PostInstallCommand,
            "egg_info": PostEggInfoCommand,
        }

        # Run setup
        setup(**package_info)


def main(project_root: Path | None = None, project_name: str = "AMI Module", package_info: dict[str, Any] | None = None):
    """Main entry point for generic setup.

    Args:
        project_root: Path to project root.
        project_name: Name of the project.
        package_info: Optional package information for setuptools.

    Returns:
        Exit code.
    """
    # Create setup instance with additional package info
    setup_instance = GenericSetup(project_root, project_name, additional_package_info=package_info)

    # Check if running without arguments - run environment setup
    if len(sys.argv) == 1:
        return setup_instance.run_environment_setup()

    # Otherwise run the package setup
    setup_instance.setup_package()
    return 0


if __name__ == "__main__":
    # When run directly, this is the base module itself
    sys.exit(main(project_name="AMI Base"))
