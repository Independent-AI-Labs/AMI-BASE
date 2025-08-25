"""Enhanced base MCP server with common patterns for all MCP implementations."""

import sys
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Generic, TypeVar

from loguru import logger

from .mcp_server import BaseMCPServer

# Type variable for tool registry
TRegistry = TypeVar("TRegistry")
TExecutor = TypeVar("TExecutor")


class StandardMCPServer(BaseMCPServer, ABC, Generic[TRegistry, TExecutor]):
    """Standard MCP server with tool registry and executor pattern.

    This class provides:
    1. Automatic import setup
    2. Tool registry and executor initialization
    3. Standard tool registration and execution patterns

    Subclasses need to:
    1. Provide their specific ToolRegistry and ToolExecutor types
    2. Implement get_registry_class() and get_executor_class()
    3. Implement register_tools_to_registry() to register their tools
    """

    def __init__(self, config: dict[str, Any] | None = None, **kwargs):
        """Initialize standard MCP server.

        Args:
            config: Server configuration
            **kwargs: Additional arguments for the executor
        """
        # Setup imports automatically
        self._setup_imports()

        # Initialize tool registry and executor
        self.registry = self.get_registry_class()()
        self.register_tools_to_registry(self.registry)

        # Initialize executor with any additional arguments
        executor_class = self.get_executor_class()
        self.executor = executor_class(**kwargs)

        # Initialize base MCP server
        super().__init__(config)

        logger.info(f"{self.__class__.__name__} initialized with " f"{len(self.tools)} tools")

    @abstractmethod
    def get_registry_class(self) -> type[TRegistry]:
        """Get the tool registry class for this server.

        Returns:
            The ToolRegistry class to use
        """

    @abstractmethod
    def get_executor_class(self) -> type[TExecutor]:
        """Get the tool executor class for this server.

        Returns:
            The ToolExecutor class to use
        """

    @abstractmethod
    def register_tools_to_registry(self, registry: TRegistry) -> None:
        """Register tools to the provided registry.

        Args:
            registry: The tool registry to populate
        """

    def _setup_imports(self) -> None:
        """Setup standard imports for the module."""
        current_file = Path(__file__).resolve()
        orchestrator_root = current_file

        # Find orchestrator root
        while orchestrator_root != orchestrator_root.parent:
            if (orchestrator_root / ".git").exists() and (orchestrator_root / "base").exists():
                break
            orchestrator_root = orchestrator_root.parent
        else:
            # If we're already in base, that's fine
            if "base" not in str(current_file):
                raise RuntimeError(f"Could not find orchestrator root from {current_file}")

        # Add to path if needed
        if str(orchestrator_root) not in sys.path:
            sys.path.insert(0, str(orchestrator_root))

        # Find and add module root
        module_names = {"base", "browser", "files", "compliance", "domains", "streams"}
        module_root = current_file.parent
        while module_root != orchestrator_root:
            if module_root.name in module_names:
                if str(module_root) not in sys.path:
                    sys.path.insert(0, str(module_root))
                break
            module_root = module_root.parent

    def register_tools(self) -> None:
        """Register tools from the registry to the base server.

        This is called by BaseMCPServer.__init__
        """
        if hasattr(self.registry, "get_all_tools"):
            # Get tools from registry
            tools = self.registry.get_all_tools()

            # Convert to MCP format
            for tool_name, tool_def in tools.items():
                self.tools[tool_name] = {"description": tool_def.get("description", ""), "inputSchema": tool_def.get("parameters", {})}
        elif hasattr(self.registry, "tools"):
            # Direct access to tools dict
            self.tools = self.registry.tools
        else:
            # Fallback - subclass should override
            logger.warning(f"{self.__class__.__name__} registry doesn't have " "get_all_tools() or tools attribute")

    async def execute_tool(self, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        """Execute a tool using the executor.

        Args:
            tool_name: Name of the tool to execute
            arguments: Tool arguments

        Returns:
            Tool execution result
        """
        if hasattr(self.executor, "execute"):
            return await self.executor.execute(tool_name, arguments)
        if hasattr(self.executor, "execute_tool"):
            return await self.executor.execute_tool(tool_name, arguments)
        raise NotImplementedError(f"{self.__class__.__name__} executor doesn't have " "execute() or execute_tool() method")


class SimpleMCPServer(BaseMCPServer):
    """Simple MCP server for basic tool implementations.

    This is for servers that don't need the registry/executor pattern
    and just want to implement tools directly.
    """

    def __init__(self, config: dict[str, Any] | None = None):
        """Initialize simple MCP server.

        Args:
            config: Server configuration
        """
        # Setup imports automatically
        self._setup_imports()

        # Initialize base
        super().__init__(config)

    def _setup_imports(self) -> None:
        """Setup standard imports for the module."""
        current_file = Path(__file__).resolve()
        orchestrator_root = current_file

        # Find orchestrator root
        while orchestrator_root != orchestrator_root.parent:
            if (orchestrator_root / ".git").exists() and (orchestrator_root / "base").exists():
                break
            orchestrator_root = orchestrator_root.parent

        # Add to path if needed
        if str(orchestrator_root) not in sys.path:
            sys.path.insert(0, str(orchestrator_root))

        # Find and add module root
        module_names = {"base", "browser", "files", "compliance", "domains", "streams"}
        module_root = current_file.parent
        while module_root != orchestrator_root:
            if module_root.name in module_names:
                if str(module_root) not in sys.path:
                    sys.path.insert(0, str(module_root))
                break
            module_root = module_root.parent
