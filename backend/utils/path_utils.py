"""Consolidated path discovery and environment setup utilities for AMI ecosystem.

This is the SINGLE authoritative source for all path discovery, traversal,
and environment setup functionality. All other modules should import from here.
"""

import contextlib
import os
import subprocess
import sys
from pathlib import Path


class PathFinder:
    """Centralized path discovery utilities."""

    @staticmethod
    def find_git_root(start_path: Path | None = None) -> Path | None:
        """Find the nearest .git directory by traversing up.

        Args:
            start_path: Starting path (defaults to current directory)

        Returns:
            Path to directory containing .git, or None if not found
        """
        current = (start_path or Path.cwd()).resolve()

        while current != current.parent:
            if (current / ".git").exists():
                return current
            current = current.parent

        return None

    @staticmethod
    def find_module_root(start_path: Path | None = None) -> Path:
        """Find module root (has backend/ and requirements.txt).

        Args:
            start_path: Starting path (defaults to current directory)

        Returns:
            Path to module root

        Raises:
            RuntimeError: If module root cannot be found
        """
        current = (start_path or Path.cwd()).resolve()

        while current != current.parent:
            # Module root has both backend/ and requirements.txt
            if (current / "backend").exists() and (current / "requirements.txt").exists():
                return current
            # Also check for .venv as a module indicator
            if (current / ".venv").exists() and (current / "backend").exists():
                return current
            current = current.parent

        raise RuntimeError(f"Could not find module root from {start_path}")

    @staticmethod
    def find_orchestrator_root(start_path: Path | None = None) -> Path | None:
        """Find main AMI-ORCHESTRATOR root (has base/ directory).

        Args:
            start_path: Starting path (defaults to current directory)

        Returns:
            Path to orchestrator root, or None if not found
        """
        current = (start_path or Path.cwd()).resolve()

        while current != current.parent:
            # Main orchestrator has base/ directory and .git
            if (current / "base").exists() and (current / ".git").exists():
                return current
            current = current.parent

        return None

    @staticmethod
    def find_base_module(start_path: Path | None = None) -> Path:
        """Find base module path.

        Args:
            start_path: Starting path (defaults to current directory)

        Returns:
            Path to base module

        Raises:
            RuntimeError: If base module cannot be found
        """
        current = (start_path or Path.cwd()).resolve()

        # If we're in base module
        if (current / "backend" / "utils" / "path_utils.py").exists():
            return current

        # Find orchestrator root and get base from there
        orchestrator = PathFinder.find_orchestrator_root(current)
        if orchestrator and (orchestrator / "base").exists():
            return orchestrator / "base"

        # Check parent directory
        parent_base = current.parent / "base"
        if parent_base.exists() and (parent_base / "backend").exists():
            return parent_base

        raise RuntimeError(f"Could not find base module from {start_path}")


class EnvironmentSetup:
    """Centralized environment and virtual environment setup."""

    @staticmethod
    def get_venv_python(module_root: Path) -> Path:
        """Get the Python executable path for a module's virtual environment.

        Args:
            module_root: Root directory of the module

        Returns:
            Path to Python executable
        """
        venv_dir = module_root / ".venv"

        if sys.platform == "win32":
            return venv_dir / "Scripts" / "python.exe"
        return venv_dir / "bin" / "python"

    @staticmethod
    def ensure_venv_exists(module_root: Path, python_version: str = "3.12") -> Path:
        """Ensure virtual environment exists, create if needed.

        Args:
            module_root: Root directory of the module
            python_version: Python version to use (default: 3.12)

        Returns:
            Path to Python executable
        """
        venv_python = EnvironmentSetup.get_venv_python(module_root)

        if not venv_python.exists():
            print(f"Creating virtual environment at {module_root / '.venv'}...")
            venv_dir = module_root / ".venv"

            try:
                # Try uv first (preferred)
                subprocess.run(["uv", "venv", str(venv_dir), "--python", f"python{python_version}"], check=True, cwd=str(module_root), capture_output=True)
                print("Virtual environment created with uv")
            except (subprocess.CalledProcessError, FileNotFoundError):
                # Fallback to standard venv
                print("uv not available, using standard venv")
                subprocess.run([sys.executable, "-m", "venv", str(venv_dir)], check=True, cwd=str(module_root))

        return venv_python

    @staticmethod
    def install_requirements(module_root: Path, venv_python: Path, base_path: Path | None = None, requirements_file: str = "requirements.txt") -> bool:
        """Install requirements for a module.

        Args:
            module_root: Root directory of the module
            venv_python: Path to virtual environment Python
            base_path: Path to base module (for dependency installation)
            requirements_file: Name of requirements file

        Returns:
            True if successful, False otherwise
        """
        try:
            # Install base requirements first if not in base module
            if base_path and base_path != module_root:
                base_req = base_path / requirements_file
                if base_req.exists():
                    print(f"Installing base {requirements_file}...")
                    subprocess.run(
                        ["uv", "pip", "install", "--python", str(venv_python), "-r", str(base_req)], check=True, cwd=str(module_root), capture_output=True
                    )

            # Install module requirements
            module_req = module_root / requirements_file
            if module_req.exists():
                print(f"Installing module {requirements_file}...")
                subprocess.run(
                    ["uv", "pip", "install", "--python", str(venv_python), "-r", str(module_req)], check=True, cwd=str(module_root), capture_output=True
                )

            return True

        except subprocess.CalledProcessError as e:
            print(f"Failed to install requirements: {e}")
            return False

    @staticmethod
    def setup_python_paths(module_root: Path, orchestrator_root: Path | None = None) -> None:
        """Set up Python import paths.

        Args:
            module_root: Root directory of the module
            orchestrator_root: Root directory of orchestrator (optional)
        """
        # Add module root
        if str(module_root) not in sys.path:
            sys.path.insert(0, str(module_root))

        # Add orchestrator root if available
        if orchestrator_root and str(orchestrator_root) not in sys.path:
            sys.path.insert(0, str(orchestrator_root))

        # Add base module if available
        if orchestrator_root:
            base_path = orchestrator_root / "base"
            if base_path.exists() and str(base_path) not in sys.path:
                sys.path.insert(0, str(base_path))

    @staticmethod
    def copy_platform_precommit_config(module_root: Path, base_path: Path) -> bool:
        """Copy platform-specific pre-commit config from base/configs.

        Args:
            module_root: Root directory of the module
            base_path: Path to base module

        Returns:
            True if successful, False otherwise
        """
        configs_dir = base_path / "configs"
        if not configs_dir.exists():
            return False

        # Determine platform-specific config
        if sys.platform == "win32":
            source_config = configs_dir / ".pre-commit-config.win.yaml"
        else:
            source_config = configs_dir / ".pre-commit-config.unix.yaml"

        if not source_config.exists():
            return False

        # Copy to module root
        target_config = module_root / ".pre-commit-config.yaml"

        try:
            import shutil

            shutil.copy2(source_config, target_config)
            print(f"Copied platform-specific pre-commit config from {source_config.name}")
            return True
        except Exception as e:
            print(f"Failed to copy pre-commit config: {e}")
            return False

    @staticmethod
    def install_precommit_hooks(module_root: Path, venv_python: Path) -> bool:
        """Install pre-commit hooks for a module.

        Args:
            module_root: Root directory of the module
            venv_python: Path to virtual environment Python

        Returns:
            True if successful, False otherwise
        """
        if not (module_root / ".pre-commit-config.yaml").exists():
            return True  # No config, nothing to install

        try:
            # Install pre-commit hooks
            subprocess.run([str(venv_python), "-m", "pre_commit", "install"], check=True, cwd=str(module_root), capture_output=True)

            # Install pre-push hooks
            subprocess.run([str(venv_python), "-m", "pre_commit", "install", "--hook-type", "pre-push"], check=True, cwd=str(module_root), capture_output=True)

            print("Pre-commit hooks installed")
            return True

        except subprocess.CalledProcessError:
            return False


class ModuleSetup:
    """High-level module setup orchestration."""

    @staticmethod
    def setup_module_environment(
        script_path: Path | None = None,
        require_venv: bool = True,
        install_requirements: bool = True,
        install_test_requirements: bool = True,
        setup_precommit: bool = True,
    ) -> tuple[Path, Path, Path | None]:
        """Complete environment setup for any module.

        This is the main entry point for setting up a module's environment.
        It handles path discovery, virtual environment creation, dependency
        installation, and pre-commit setup.

        Args:
            script_path: Path to the calling script (defaults to current location)
            require_venv: Whether to require/create virtual environment
            install_requirements: Whether to install requirements.txt
            install_test_requirements: Whether to install requirements-test.txt
            setup_precommit: Whether to set up pre-commit hooks

        Returns:
            Tuple of (module_root, venv_python, orchestrator_root)
        """
        # Path discovery
        start_path = script_path or Path.cwd()
        module_root = PathFinder.find_module_root(start_path)
        orchestrator_root = PathFinder.find_orchestrator_root(module_root)

        # Set up Python paths
        EnvironmentSetup.setup_python_paths(module_root, orchestrator_root)

        # Virtual environment setup
        venv_python = EnvironmentSetup.get_venv_python(module_root)

        if require_venv:
            venv_python = EnvironmentSetup.ensure_venv_exists(module_root)

            # Get base path
            base_path = None
            with contextlib.suppress(RuntimeError):
                base_path = PathFinder.find_base_module(module_root)

            # Install dependencies
            if install_requirements:
                EnvironmentSetup.install_requirements(module_root, venv_python, base_path, "requirements.txt")

            if install_test_requirements:
                EnvironmentSetup.install_requirements(module_root, venv_python, base_path, "requirements-test.txt")

            # Pre-commit setup
            if setup_precommit and base_path:
                EnvironmentSetup.copy_platform_precommit_config(module_root, base_path)
                EnvironmentSetup.install_precommit_hooks(module_root, venv_python)

        return module_root, venv_python, orchestrator_root

    @staticmethod
    def setup_for_script(script_path: Path) -> tuple[Path, Path, Path | None]:
        """Quick setup for a script that just needs paths configured.

        Args:
            script_path: Path to the calling script

        Returns:
            Tuple of (module_root, venv_python, orchestrator_root)
        """
        return ModuleSetup.setup_module_environment(
            script_path=script_path, require_venv=False, install_requirements=False, install_test_requirements=False, setup_precommit=False
        )

    @staticmethod
    def ensure_running_in_venv(script_path: Path) -> None:
        """Ensure script is running in the correct virtual environment.

        If not running in venv, re-executes the script with the correct Python.

        Args:
            script_path: Path to the current script
        """
        module_root = PathFinder.find_module_root(script_path)
        venv_python = EnvironmentSetup.get_venv_python(module_root)

        # Check if we're using the right Python
        if Path(sys.executable).resolve() != venv_python.resolve():
            # Re-run with correct Python
            print(f"Re-running with correct Python: {venv_python}")

            # Set up environment
            env = dict(os.environ)
            orchestrator_root = PathFinder.find_orchestrator_root(module_root)

            pythonpath = [str(module_root)]
            if orchestrator_root:
                pythonpath.extend([str(orchestrator_root), str(orchestrator_root / "base")])

            env["PYTHONPATH"] = os.pathsep.join(pythonpath)

            # Re-execute
            result = subprocess.run([str(venv_python)] + sys.argv, env=env, check=False)
            sys.exit(result.returncode)


# Convenience functions for backward compatibility and easy imports
def find_module_root(start_path: Path | None = None) -> Path:
    """Find module root. Backward compatibility wrapper."""
    return PathFinder.find_module_root(start_path)


def find_base_module(start_path: Path | None = None) -> Path:
    """Find base module. Backward compatibility wrapper."""
    return PathFinder.find_base_module(start_path)


def setup_paths() -> tuple[Path, Path | None]:
    """Set up Python paths. Backward compatibility wrapper."""
    module_root = PathFinder.find_module_root()
    orchestrator_root = PathFinder.find_orchestrator_root()
    EnvironmentSetup.setup_python_paths(module_root, orchestrator_root)
    return module_root, orchestrator_root


def auto_setup(require_venv: bool = True) -> tuple[Path, Path, Path | None]:
    """Auto-setup environment. Backward compatibility wrapper."""
    import inspect

    frame = inspect.currentframe()
    script_path = Path(frame.f_back.f_code.co_filename).resolve() if frame and frame.f_back else Path.cwd()

    return ModuleSetup.setup_module_environment(script_path=script_path, require_venv=require_venv)
