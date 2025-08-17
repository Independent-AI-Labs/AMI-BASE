"""Tool executor for SSH MCP server."""

import asyncio
from typing import Any

import paramiko  # type: ignore[import-untyped]
from loguru import logger

from backend.config.network import SSHConfig


class SSHConnection:
    """Manages a single SSH connection."""

    def __init__(self, config: SSHConfig):
        """Initialize SSH connection with configuration."""
        self.config = config
        self._ssh_client: paramiko.SSHClient | None = None
        self._sftp_client: paramiko.SFTPClient | None = None

    def connect(self) -> paramiko.SSHClient:
        """Ensure SSH connection is established."""
        if self._ssh_client is None:
            self._ssh_client = paramiko.SSHClient()
            self._ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Check if already connected
        transport = self._ssh_client.get_transport()
        if transport is None or not transport.is_active():
            logger.info(f"Connecting to SSH server {self.config.name} ({self.config.host}:{self.config.port})")
            try:
                self._ssh_client.connect(**self.config.to_paramiko_config())
            except Exception as e:
                logger.error(f"Failed to connect to {self.config.name}: {e}")
                raise

        return self._ssh_client

    def get_sftp(self) -> paramiko.SFTPClient:
        """Get or create SFTP client."""
        if self._sftp_client is None:
            ssh_client = self.connect()
            self._sftp_client = ssh_client.open_sftp()
        return self._sftp_client

    def execute_command(self, command: str, timeout: int = 30) -> dict[str, Any]:
        """Execute a command synchronously."""
        try:
            client = self.connect()
            stdin, stdout, stderr = client.exec_command(command, timeout=timeout)

            # Read output
            output = stdout.read().decode("utf-8")
            error = stderr.read().decode("utf-8")
            exit_code = stdout.channel.recv_exit_status()

            return {
                "status": "success" if exit_code == 0 else "failed",
                "exit_code": exit_code,
                "output": output,
                "error": error,
                "command": command,
                "server": self.config.name,
            }
        except Exception as e:
            logger.error(f"Command execution error on {self.config.name}: {e}")
            raise

    def close(self):
        """Close SSH and SFTP connections."""
        if self._sftp_client:
            self._sftp_client.close()
            self._sftp_client = None

        if self._ssh_client:
            self._ssh_client.close()
            self._ssh_client = None


class ToolExecutor:
    """Executes SSH MCP tools across multiple servers."""

    def __init__(self, servers: dict[str, SSHConfig] | None = None):
        """Initialize the executor with SSH server configurations.

        Args:
            servers: Dictionary of server name to SSHConfig
        """
        self.servers = servers or {}
        self.connections: dict[str, SSHConnection] = {}

    def add_server(self, config: SSHConfig) -> None:
        """Add a new server configuration.

        Args:
            config: SSH server configuration
        """
        self.servers[config.name] = config
        # Close existing connection if any
        if config.name in self.connections:
            self.connections[config.name].close()
            del self.connections[config.name]

    def remove_server(self, name: str) -> None:
        """Remove a server configuration.

        Args:
            name: Server name to remove
        """
        if name in self.servers:
            del self.servers[name]
        if name in self.connections:
            self.connections[name].close()
            del self.connections[name]

    def _get_connection(self, server_name: str) -> SSHConnection:
        """Get or create connection for a server."""
        if server_name not in self.servers:
            raise ValueError(f"Unknown server: {server_name}")

        if server_name not in self.connections:
            self.connections[server_name] = SSHConnection(self.servers[server_name])

        return self.connections[server_name]

    async def execute(self, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:  # noqa: PLR0911
        """Execute a tool and return the result."""
        logger.debug(f"Executing tool: {tool_name} with arguments: {arguments}")

        # Route to appropriate handler
        if tool_name == "ssh_list_servers":
            return await self._list_servers()
        if tool_name == "ssh_test_connection":
            return await self._test_connection(arguments["server"])
        if tool_name == "ssh_execute":
            return await self._execute_command(arguments["server"], arguments["command"], arguments.get("timeout", 30))
        if tool_name == "ssh_upload_file":
            return await self._upload_file(arguments["server"], arguments["local_path"], arguments["remote_path"])
        if tool_name == "ssh_download_file":
            return await self._download_file(arguments["server"], arguments["remote_path"], arguments["local_path"])
        if tool_name == "ssh_connect_server":
            return await self._connect_server(arguments)
        if tool_name == "ssh_disconnect_server":
            return await self._disconnect_server(arguments["server"])
        raise ValueError(f"Unknown tool: {tool_name}")

    async def _list_servers(self) -> dict[str, Any]:
        """List all configured servers."""
        servers = []
        for name, config in self.servers.items():
            servers.append(
                {
                    "name": name,
                    "host": config.host,
                    "port": config.port,
                    "username": config.username,
                    "description": config.description,
                    "connected": name in self.connections,
                }
            )
        return {"servers": servers, "count": len(servers)}

    async def _test_connection(self, server_name: str) -> dict[str, Any]:
        """Test connection to a server."""
        try:
            conn = self._get_connection(server_name)
            conn.connect()
            config = self.servers[server_name]
            return {
                "status": "connected",
                "server": server_name,
                "host": config.host,
                "port": config.port,
            }
        except Exception as e:
            return {
                "status": "failed",
                "server": server_name,
                "error": str(e),
            }

    async def _execute_command(self, server_name: str, command: str, timeout: int) -> dict[str, Any]:
        """Execute a command on a server."""
        try:
            conn = self._get_connection(server_name)
            # Run in executor to avoid blocking
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, conn.execute_command, command, timeout)
        except Exception as e:
            logger.error(f"Command execution failed on {server_name}: {e}")
            return {
                "status": "failed",
                "server": server_name,
                "error": str(e),
                "command": command,
            }

    async def _upload_file(self, server_name: str, local_path: str, remote_path: str) -> dict[str, Any]:
        """Upload a file to a server."""
        try:
            conn = self._get_connection(server_name)
            sftp = conn.get_sftp()

            # Run in executor to avoid blocking
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, sftp.put, local_path, remote_path)

            return {
                "status": "uploaded",
                "server": server_name,
                "local_path": local_path,
                "remote_path": remote_path,
            }
        except Exception as e:
            return {
                "status": "failed",
                "server": server_name,
                "error": str(e),
            }

    async def _download_file(self, server_name: str, remote_path: str, local_path: str) -> dict[str, Any]:
        """Download a file from a server."""
        try:
            conn = self._get_connection(server_name)
            sftp = conn.get_sftp()

            # Run in executor to avoid blocking
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, sftp.get, remote_path, local_path)

            return {
                "status": "downloaded",
                "server": server_name,
                "remote_path": remote_path,
                "local_path": local_path,
            }
        except Exception as e:
            return {
                "status": "failed",
                "server": server_name,
                "error": str(e),
            }

    async def _connect_server(self, args: dict[str, Any]) -> dict[str, Any]:
        """Connect to a new server at runtime."""
        try:
            config = SSHConfig(
                name=args["name"],
                host=args["host"],
                port=args.get("port", 22),
                username=args["username"],
                password=args.get("password"),
                key_filename=args.get("key_filename"),
            )
            self.add_server(config)

            # Test connection
            conn = self._get_connection(args["name"])
            conn.connect()

            return {
                "status": "connected",
                "server": args["name"],
                "host": args["host"],
            }
        except Exception as e:
            # Remove server if connection failed
            if args["name"] in self.servers:
                self.remove_server(args["name"])
            return {
                "status": "failed",
                "error": str(e),
            }

    async def _disconnect_server(self, server_name: str) -> dict[str, Any]:
        """Disconnect from a server."""
        if server_name not in self.servers:
            return {
                "status": "failed",
                "error": f"Server {server_name} not found",
            }

        self.remove_server(server_name)
        return {
            "status": "disconnected",
            "server": server_name,
        }

    def close_all(self):
        """Close all SSH connections."""
        for conn in self.connections.values():
            conn.close()
        self.connections.clear()
