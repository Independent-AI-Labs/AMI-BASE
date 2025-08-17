#!/usr/bin/env python
"""Run SSH MCP server."""

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
from services.utils.path_finder import setup_base_import  # noqa: E402

setup_base_import(Path(__file__))

from services.mcp.run_server import setup_environment  # noqa: E402

if __name__ == "__main__":
    # Setup environment first (will re-exec if needed)
    module_root, python = setup_environment(Path(__file__))

    # NOW import after environment is set up
    from services.mcp.run_server import run_server  # noqa: E402
    from services.mcp.ssh.server import SSHMCPServer  # noqa: E402

    # Parse transport from args
    transport = "stdio"
    if len(sys.argv) > 1 and sys.argv[1] in ["websocket", "ws"]:
        transport = "websocket"

    # Get config file if exists
    config_file = None
    for name in ["ssh_config.yaml", "config.yaml"]:
        path = module_root / name
        if path.exists():
            config_file = str(path)
            break

    server_args = {"config_file": config_file} if config_file else {}

    run_server(
        server_class=SSHMCPServer,
        server_args=server_args,
        transport=transport,
        port=8766,  # SSH uses 8766
    )
