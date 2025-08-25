"""Consolidated path discovery and environment setup utilities for AMI ecosystem.

This module re-exports all functionality from the split modules for backward compatibility.
All other modules should import from here.

New code should consider importing directly from the specific modules:
- path_finder: Path discovery utilities
- environment_setup: Virtual environment and Python path setup
- module_orchestration: High-level module setup orchestration
"""

import inspect
from pathlib import Path

# Import all classes from split modules
from .environment_setup import EnvironmentSetup
from .module_orchestration import ModuleSetup
from .path_finder import PathFinder

# Re-export all classes for backward compatibility
__all__ = [
    "PathFinder",
    "EnvironmentSetup",
    "ModuleSetup",
    # Convenience functions
    "find_module_root",
    "find_base_module",
    "setup_paths",
    "auto_setup",
]


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
    frame = inspect.currentframe()
    script_path = Path(frame.f_back.f_code.co_filename).resolve() if frame and frame.f_back else Path.cwd()

    return ModuleSetup.setup_module_environment(script_path=script_path, require_venv=require_venv)
