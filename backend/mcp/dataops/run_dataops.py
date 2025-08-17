#!/usr/bin/env python
"""Run DataOps MCP server."""

import sys
from pathlib import Path

# Bootstrap path discovery - find base WITHOUT hardcoded parent counts
current = Path(__file__).resolve().parent
while current != current.parent:
    if (current / ".git").exists():
        if (current / "base").exists() and (current / "base" / "backend" / "utils" / "path_finder.py").exists():
            sys.path.insert(0, str(current / "base"))
            break
        elif current.name == "base" and (current / "backend" / "utils" / "path_finder.py").exists():
            sys.path.insert(0, str(current))
            break
    current = current.parent

# Now we can import the proper path finder
from backend.utils.path_finder import setup_base_import  # noqa: E402

setup_base_import(Path(__file__))

from backend.mcp.run_server import setup_environment  # noqa: E402

if __name__ == "__main__":
    # Setup environment first (will re-exec if needed)
    module_root, python = setup_environment(Path(__file__))

    # NOW import after environment is set up
    from backend.dataops.bpmn_model import Event, Gateway, Process, Task  # noqa: E402
    from backend.mcp.dataops.server import DataOpsMCPServer  # noqa: E402
    from backend.mcp.run_server import run_server  # noqa: E402

    # Parse transport from args
    transport = "stdio"
    if len(sys.argv) > 1 and sys.argv[1] in ["websocket", "ws"]:
        transport = "websocket"

    # Create server with registered models
    class ConfiguredDataOpsServer(DataOpsMCPServer):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.register_model(Process)
            self.register_model(Task)
            self.register_model(Event)
            self.register_model(Gateway)

    run_server(
        server_class=ConfiguredDataOpsServer,
        transport=transport,
        port=8767,  # DataOps uses 8767
    )
