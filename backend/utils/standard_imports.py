"""
Standard import setup header for ALL Python files in the orchestrator.
Copy this block to the top of any file that needs cross-module imports.
"""

# STANDARD IMPORT SETUP - DO NOT MODIFY
import sys
from pathlib import Path


def setup_imports():
    """Set up module imports the STANDARD way."""
    current_file = Path(__file__).resolve()
    orchestrator_root = current_file

    # Find orchestrator root
    while orchestrator_root != orchestrator_root.parent:
        if (orchestrator_root / ".git").exists() and (orchestrator_root / "base").exists():
            break
        orchestrator_root = orchestrator_root.parent
    else:
        raise RuntimeError(f"Could not find orchestrator root from {current_file}")

    # Add to path if not already there
    if str(orchestrator_root) not in sys.path:
        sys.path.insert(0, str(orchestrator_root))

    # Find module root
    module_names = {"base", "browser", "files", "compliance", "domains", "streams"}
    module_root = current_file.parent
    while module_root != orchestrator_root:
        if module_root.name in module_names:
            if str(module_root) not in sys.path:
                sys.path.insert(0, str(module_root))
            break
        module_root = module_root.parent

    return orchestrator_root, module_root


# Execute setup
ORCHESTRATOR_ROOT, MODULE_ROOT = setup_imports()
