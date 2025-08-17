"""Tool definitions for SSH MCP server."""

from .registry import MCPTool, ToolRegistry

# SSH connection and command tools
SSH_TOOLS = [
    MCPTool(
        name="ssh_execute",
        description="Execute a command on the remote SSH server",
        category="ssh",
        parameters={
            "properties": {
                "server": {"type": "string", "description": "Server name/identifier to execute on"},
                "command": {"type": "string", "description": "Command to execute"},
                "timeout": {"type": "number", "description": "Command timeout in seconds (default: 30)"},
            },
            "required": ["server", "command"],
        },
    ),
    MCPTool(
        name="ssh_list_servers",
        description="List all configured SSH servers",
        category="ssh",
        parameters={"properties": {}, "required": []},
    ),
    MCPTool(
        name="ssh_test_connection",
        description="Test SSH connection to a specific server",
        category="ssh",
        parameters={
            "properties": {
                "server": {"type": "string", "description": "Server name/identifier to test"},
            },
            "required": ["server"],
        },
    ),
    MCPTool(
        name="ssh_upload_file",
        description="Upload a file to the remote server",
        category="ssh",
        parameters={
            "properties": {
                "server": {"type": "string", "description": "Server name/identifier"},
                "local_path": {"type": "string", "description": "Local file path"},
                "remote_path": {"type": "string", "description": "Remote destination path"},
            },
            "required": ["server", "local_path", "remote_path"],
        },
    ),
    MCPTool(
        name="ssh_download_file",
        description="Download a file from the remote server",
        category="ssh",
        parameters={
            "properties": {
                "server": {"type": "string", "description": "Server name/identifier"},
                "remote_path": {"type": "string", "description": "Remote file path"},
                "local_path": {"type": "string", "description": "Local destination path"},
            },
            "required": ["server", "remote_path", "local_path"],
        },
    ),
]

# Privileged tools for runtime server management (disabled by default)
PRIVILEGED_TOOLS = [
    MCPTool(
        name="ssh_connect_server",
        description="Connect to a new SSH server at runtime",
        category="privileged",
        parameters={
            "properties": {
                "name": {"type": "string", "description": "Server name/identifier"},
                "host": {"type": "string", "description": "Host address or IP"},
                "port": {"type": "number", "description": "SSH port (default: 22)"},
                "username": {"type": "string", "description": "Username for authentication"},
                "password": {"type": "string", "description": "Password for authentication"},
                "key_filename": {"type": "string", "description": "Path to SSH private key (optional)"},
            },
            "required": ["name", "host", "username"],
        },
    ),
    MCPTool(
        name="ssh_disconnect_server",
        description="Disconnect from an SSH server",
        category="privileged",
        parameters={
            "properties": {
                "server": {"type": "string", "description": "Server name/identifier to disconnect"},
            },
            "required": ["server"],
        },
    ),
]


def register_all_tools(registry: ToolRegistry, enable_privileged: bool = False) -> None:
    """Register all SSH tools with the registry.
    
    Args:
        registry: Tool registry to register tools with
        enable_privileged: Whether to enable privileged runtime management tools
    """
    for tool in SSH_TOOLS:
        registry.register(tool)
    
    if enable_privileged:
        for tool in PRIVILEGED_TOOLS:
            registry.register(tool)