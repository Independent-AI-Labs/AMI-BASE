"""Path discovery utilities for AMI ecosystem."""

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
