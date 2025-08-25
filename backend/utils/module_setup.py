"""
Centralized module setup utility for consistent import handling across ALL modules.
THIS IS THE ONLY WAY TO SET UP MODULE IMPORTS - NO EXCEPTIONS!
"""

import sys
from pathlib import Path


class ModuleImportSetup:
    """Single source of truth for module import setup."""

    @staticmethod
    def setup_module_paths(current_file: Path | str) -> tuple[Path, Path]:
        """
        Set up Python paths for ANY module in the orchestrator.

        Args:
            current_file: __file__ from the calling module

        Returns:
            Tuple of (orchestrator_root, module_root)

        Usage:
            from base.backend.utils.module_setup import ModuleImportSetup
            orchestrator_root, module_root = ModuleImportSetup.setup_module_paths(__file__)
        """
        current_file = Path(current_file).resolve()

        # Find orchestrator root by looking for .git and base directory
        current = current_file.parent
        orchestrator_root = None

        while current != current.parent:
            if (current / ".git").exists() and (current / "base").exists():
                orchestrator_root = current
                break
            current = current.parent

        if not orchestrator_root:
            raise RuntimeError(f"Could not find orchestrator root from {current_file}. " "Ensure you're running from within the AMI-ORCHESTRATOR repository.")

        # Find module root (parent directory containing the module)
        # Modules are: base, browser, files, compliance, domains, streams
        module_root = current_file
        module_names = {"base", "browser", "files", "compliance", "domains", "streams"}

        while module_root != orchestrator_root:
            if module_root.name in module_names:
                break
            module_root = module_root.parent
        else:
            # We're in the orchestrator root itself
            module_root = orchestrator_root

        # Add paths in the CORRECT order
        # 1. Orchestrator root (for imports like "base.xxx")
        if str(orchestrator_root) not in sys.path:
            sys.path.insert(0, str(orchestrator_root))

        # 2. Module root (for imports like "backend.xxx" within a module)
        if module_root != orchestrator_root and str(module_root) not in sys.path:
            sys.path.insert(0, str(module_root))

        return orchestrator_root, module_root

    @staticmethod
    def get_venv_python(module_root: Path) -> Path:
        """
        Get the correct Python executable for the module's virtual environment.

        Args:
            module_root: Root directory of the module

        Returns:
            Path to the Python executable
        """
        if sys.platform == "win32":
            python_exe = module_root / ".venv" / "Scripts" / "python.exe"
        else:
            python_exe = module_root / ".venv" / "bin" / "python"

        if not python_exe.exists():
            raise RuntimeError(f"Virtual environment not found at {module_root / '.venv'}. " f"Please run setup.py for the module first.")

        return python_exe

    @staticmethod
    def ensure_running_in_venv(current_file: Path | str) -> None:
        """
        Ensure we're running in the correct virtual environment.

        Args:
            current_file: __file__ from the calling module

        Raises:
            RuntimeError: If not running in the correct venv
        """
        current_file = Path(current_file).resolve()
        _, module_root = ModuleImportSetup.setup_module_paths(current_file)

        # Check if we're in a venv at all
        if not hasattr(sys, "real_prefix") and not (hasattr(sys, "base_prefix") and sys.base_prefix != sys.prefix):
            raise RuntimeError("Not running in a virtual environment! " f"Please activate the venv at {module_root / '.venv'}")

        # Check if it's the RIGHT venv
        venv_path = Path(sys.prefix)
        expected_venv = module_root / ".venv"

        if venv_path.resolve() != expected_venv.resolve():
            raise RuntimeError(
                f"Running in wrong virtual environment!\n" f"Current: {venv_path}\n" f"Expected: {expected_venv}\n" f"Please activate the correct venv."
            )


# For backward compatibility with existing code that uses ModuleSetup
class ModuleSetup:
    """Backward compatibility wrapper."""

    @staticmethod
    def ensure_running_in_venv(current_file: Path | str) -> None:
        """Backward compatibility method."""
        ModuleImportSetup.ensure_running_in_venv(current_file)
