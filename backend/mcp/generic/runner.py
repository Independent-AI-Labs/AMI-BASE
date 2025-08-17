#!/usr/bin/env python
"""Base runner module that handles all environment setup with venv guarantee."""

import sys
from pathlib import Path

# Bootstrap to find base
current = Path(__file__).resolve().parent
while current != current.parent:
    if (current / "backend" / "utils" / "smart_path.py").exists():
        # Found base, add to path
        if str(current) not in sys.path:
            sys.path.insert(0, str(current))
        break
    current = current.parent

from backend.utils.smart_path import auto_setup  # noqa: E402


def init_environment(script_path: Path):  # noqa: C901, PLR0912
    """Initialize environment for any MCP script with venv guarantee.

    Args:
        script_path: Path to the calling script (__file__)

    Returns:
        Tuple of (module_root, config_file)
    """
    script_path = script_path.resolve()

    # Save original sys.path[0] which contains the script directory
    original_script_dir = script_path.parent

    # Use smart_path auto_setup to ensure venv and paths
    paths = auto_setup(require_venv=True)

    module_root = paths.project_root
    if not module_root:
        raise RuntimeError(f"Could not find module root for {script_path}")

    # Look for config file in module root
    config_file = None
    for config_name in ["config.yaml", "config.test.yaml", "config.sample.yaml"]:
        config_path = module_root / config_name
        if config_path.exists():
            config_file = config_path
            break

    # Ensure module root is first in path for imports
    if str(module_root) not in sys.path:
        sys.path.insert(0, str(module_root))

    # Also ensure script directory is in path for local imports
    if str(original_script_dir) not in sys.path:
        sys.path.insert(0, str(original_script_dir))

    return module_root, config_file
