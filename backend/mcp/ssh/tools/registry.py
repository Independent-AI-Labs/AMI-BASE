"""Tool registry for SSH MCP server."""

from dataclasses import dataclass
from typing import Any


@dataclass
class MCPTool:
    """Definition of an MCP tool."""

    name: str
    description: str
    category: str
    parameters: dict[str, Any]


class ToolRegistry:
    """Registry for SSH MCP tools."""

    def __init__(self):
        self.tools: dict[str, MCPTool] = {}

    def register(self, tool: MCPTool) -> None:
        """Register a tool."""
        self.tools[tool.name] = tool

    def get(self, name: str) -> MCPTool | None:
        """Get a tool by name."""
        return self.tools.get(name)

    def list_tools(self) -> list[MCPTool]:
        """List all registered tools."""
        return list(self.tools.values())