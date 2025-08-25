"""High-level module setup orchestration."""

import contextlib
import os
import subprocess
import sys
from pathlib import Path

from .environment_setup import EnvironmentSetup
from .path_finder import PathFinder


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
