#!/usr/bin/env python
"""Base runner module that handles all environment setup."""

import sys
from pathlib import Path


def init_environment(script_path: Path):  # noqa: C901, PLR0912
    """Initialize environment for any MCP script.

    Args:
        script_path: Path to the calling script (__file__)

    Returns:
        Tuple of (module_root, config_file)
    """
    script_path = script_path.resolve()

    # Find orchestrator root by scanning up for .gitmodules
    orchestrator_root = None
    current = script_path.parent
    while current != current.parent:
        if (current / ".gitmodules").exists():
            orchestrator_root = current
            break
        current = current.parent

    if not orchestrator_root:
        raise RuntimeError("Could not find orchestrator root (.gitmodules not found)")

    # Read .gitmodules to get submodule paths
    gitmodules_content = (orchestrator_root / ".gitmodules").read_text()
    submodule_paths = []
    for line in gitmodules_content.split("\n"):
        if line.strip().startswith("path = "):
            path = line.split("=", 1)[1].strip()
            submodule_paths.append(path)

    # Find which submodule we're in by checking our path against submodule paths
    module_root = None
    for submodule_path in submodule_paths:
        submodule_dir = orchestrator_root / submodule_path
        try:
            # Check if script is inside this submodule
            script_path.relative_to(submodule_dir)
            module_root = submodule_dir
            break
        except ValueError:
            continue

    if not module_root:
        # Not in a submodule, must be in orchestrator root
        module_root = orchestrator_root

    # Add module root to path
    if str(module_root) not in sys.path:
        sys.path.insert(0, str(module_root))

    # Import and run module's setup if needed
    try:
        import setup  # type: ignore

        # Check venv
        venv_path = module_root / ".venv"
        if not venv_path.exists() and hasattr(setup, "run_environment_setup"):
            print(f"Setting up environment in {module_root.name}...", file=sys.stderr)
            result = setup.run_environment_setup()
            if result != 0:
                print("Failed to set up environment", file=sys.stderr)
                sys.exit(1)

        # Ensure base module is available
        if hasattr(setup, "ensure_base_module"):
            setup.ensure_base_module()

        # Re-add module root to front of path (ensure_base_module may have modified path)
        if str(module_root) in sys.path:
            sys.path.remove(str(module_root))
        sys.path.insert(0, str(module_root))

    except ImportError:
        # No setup.py in this module, that's fine
        pass

    # Find config file
    config_file = module_root / "config.yaml"
    if not config_file.exists():
        config_file = None

    return module_root, config_file
