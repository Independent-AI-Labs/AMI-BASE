"""Environment and virtual environment setup utilities."""

import subprocess
import sys
from pathlib import Path


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
