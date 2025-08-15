#!/usr/bin/env python
"""Generic MCP Server launcher that ensures correct environment."""

import os
import subprocess
import sys
from enum import Enum
from pathlib import Path


class ServerMode(Enum):
    """Server mode enumeration."""

    STDIO = "stdio"
    WEBSOCKET = "websocket"


class MCPServerLauncher:
    """Generic MCP server launcher for submodules."""

    def __init__(self, project_root=None, mcp_base_path=None):
        """Initialize MCP server launcher.

        Args:
            project_root: Path to project root. If None, uses parent of script location.
            mcp_base_path: Base path for MCP server scripts. If None, uses backend/mcp/<module_name>
        """
        if project_root:
            self.project_root = Path(project_root).resolve()
        else:
            # Default to parent of where this script is located
            self.project_root = Path(__file__).parent.parent.resolve()

        self.venv_path = self.project_root / ".venv"
        self.venv_python = self._get_venv_python()

        # Determine MCP base path
        if mcp_base_path:
            self.mcp_base_path = Path(mcp_base_path)
        else:
            # Try to auto-detect based on project structure
            module_name = self.project_root.name.lower()
            self.mcp_base_path = self.project_root / "backend" / "mcp" / module_name

            # If not found, try without module name
            if not self.mcp_base_path.exists():
                self.mcp_base_path = self.project_root / "backend" / "mcp"

    def _get_venv_python(self):
        """Get the path to the virtual environment Python executable."""
        if sys.platform == "win32":
            return self.venv_path / "Scripts" / "python.exe"
        return self.venv_path / "bin" / "python"

    def check_uv(self):
        """Check if uv is installed."""
        try:
            subprocess.run(["uv", "--version"], capture_output=True, check=True)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("ERROR: uv is not installed!")
            print("Install it with: pip install uv")
            return False

    def run_setup_if_needed(self):
        """Run setup.py if virtual environment doesn't exist or setuptools is missing."""
        setup_py = self.project_root / "setup.py"

        # Check if we need to run setup
        needs_setup = False

        if not self.venv_path.exists():
            print("Virtual environment not found. Running setup...")
            needs_setup = True
        else:
            # Check if setuptools is installed
            try:
                result = subprocess.run([str(self.venv_python), "-c", "import setuptools"], capture_output=True, check=False)
                if result.returncode != 0:
                    print("setuptools not found. Running setup...")
                    needs_setup = True
            except (ImportError, OSError, RuntimeError, subprocess.CalledProcessError) as e:
                print(f"Error checking setuptools: {e}")
                needs_setup = True

        if needs_setup:
            if setup_py.exists():
                print(f"Running setup.py in {self.project_root}...")
                result = subprocess.run([sys.executable, str(setup_py)], cwd=self.project_root, check=False)
                if result.returncode != 0:
                    print("ERROR: setup.py failed")
                    sys.exit(1)
                print("Setup completed successfully")
            else:
                print(f"ERROR: setup.py not found in {self.project_root}")
                sys.exit(1)

    def setup_environment(self):
        """Set up the virtual environment if needed."""
        # First run setup.py if needed
        self.run_setup_if_needed()

        if not self.check_uv():
            sys.exit(1)

        # Create venv if it doesn't exist (backup if setup.py didn't create it)
        if not self.venv_path.exists():
            print(f"Creating virtual environment with uv in {self.project_root}...")
            subprocess.run(["uv", "venv", str(self.venv_path)], check=True)

        # Install/update dependencies
        print("Ensuring dependencies are installed...")

        # Install from requirements files
        requirements_file = self.project_root / "requirements.txt"
        if requirements_file.exists():
            subprocess.run(["uv", "pip", "install", "-r", str(requirements_file)], check=True, cwd=self.project_root)
        else:
            print(f"ERROR: requirements.txt not found in {self.project_root}!")
            sys.exit(1)

        if not self.venv_python.exists():
            print(f"ERROR: Virtual environment Python not found at {self.venv_python}")
            sys.exit(1)

        return self.venv_python

    def get_mcp_script(self, mode):
        """Get the path to the MCP server script.

        Args:
            mode: ServerMode enum value

        Returns:
            Path to the MCP server script
        """
        if not isinstance(mode, ServerMode):
            raise TypeError(f"Mode must be ServerMode enum, got {type(mode)}")

        script_name = f"run_{mode.value}.py"
        script_path = self.mcp_base_path / script_name

        if not script_path.exists():
            raise FileNotFoundError(f"MCP server script '{script_name}' not found at {self.mcp_base_path}\n" f"Expected location: {script_path}")

        return script_path

    def build_environment(self):
        """Build environment variables for the MCP server."""
        env = os.environ.copy()

        # Add project root to PYTHONPATH
        python_paths = [str(self.project_root)]

        # Add parent directory if we're a submodule
        parent_dir = self.project_root.parent
        if parent_dir.exists():
            python_paths.append(str(parent_dir))

            # Add parent/base if it exists
            parent_base = parent_dir / "base"
            if parent_base.exists():
                python_paths.append(str(parent_base))

        env["PYTHONPATH"] = os.pathsep.join(python_paths)

        # Set LOG_LEVEL if not already set
        if "LOG_LEVEL" not in env:
            env["LOG_LEVEL"] = "INFO"

        return env

    def run_server(self, mode="stdio", host="localhost", port=8765, custom_script=None, custom_args=None):
        """Run the MCP server with the virtual environment.

        Args:
            mode: ServerMode enum or string ("stdio" or "websocket")
            host: Host for WebSocket mode (ignored in stdio mode)
            port: Port for WebSocket mode (ignored in stdio mode)
            custom_script: Optional custom script path to run instead of default
            custom_args: Optional custom arguments to pass to the script

        Returns:
            Exit code
        """
        # Convert string mode to enum if needed
        if isinstance(mode, str):
            try:
                mode = ServerMode(mode.lower())
            except ValueError:
                print(f"ERROR: Invalid mode '{mode}'. Use 'stdio' or 'websocket'")
                sys.exit(1)
        venv_python = self.setup_environment()

        # Get environment
        env = self.build_environment()

        # Determine script and command
        if custom_script:
            mcp_script = Path(custom_script)
            cmd = [str(venv_python), str(mcp_script)]
            if custom_args:
                cmd.extend(custom_args)
            server_type = f"Custom MCP Server ({mcp_script.name})"
        else:
            # Get default script
            try:
                mcp_script = self.get_mcp_script(mode)
            except FileNotFoundError as e:
                print(f"ERROR: {e}")
                sys.exit(1)

            # Build command based on mode
            if mode == ServerMode.STDIO:
                cmd = [str(venv_python), str(mcp_script)]
                server_type = "MCP Stdio Server (for Claude Desktop)"
            elif mode == ServerMode.WEBSOCKET:
                cmd = [str(venv_python), str(mcp_script), host, str(port)]
                server_type = f"MCP WebSocket Server at ws://{host}:{port}"
            else:
                # This shouldn't happen due to enum validation
                print(f"ERROR: Unhandled mode '{mode}'")
                sys.exit(1)

        # Print startup info
        self.print_startup_info(mode, server_type, venv_python, mcp_script, env)

        # Run the server
        try:
            result = subprocess.run(cmd, cwd=self.project_root, env=env, check=False)
            return result.returncode
        except KeyboardInterrupt:
            print(f"\n{mode.value.capitalize()} server stopped by user")
            return 0
        except (ImportError, OSError, RuntimeError, subprocess.CalledProcessError) as e:
            print(f"\nERROR: Failed to run {mode.value} server: {e}")
            return 1

    def print_startup_info(self, mode, server_type, venv_python, mcp_script, env):
        """Print server startup information."""
        print("=" * 60)
        print("MCP Server Launcher")
        print("=" * 60)
        print(f"Mode: {mode.value}")
        print(f"Server: {server_type}")
        print(f"Python: {venv_python}")
        print(f"Script: {mcp_script}")
        print(f"Working Directory: {self.project_root}")
        print(f"Log Level: {env.get('LOG_LEVEL', 'INFO')}")
        print("=" * 60)
        print(f"\nStarting {mode.value} server...")
        print("Press Ctrl+C to stop\n")


def main(project_root=None, mcp_base_path=None, project_name="MCP Server"):
    """Main entry point for MCP server launcher.

    Args:
        project_root: Optional path to project root
        mcp_base_path: Optional base path for MCP server scripts
        project_name: Name of the project for display

    Returns:
        Exit code
    """
    import argparse

    # Create launcher
    launcher = MCPServerLauncher(project_root, mcp_base_path)

    parser = argparse.ArgumentParser(
        description=f"{project_name} Launcher",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python start_mcp_server.py              # Start stdio server (default)
  python start_mcp_server.py stdio        # Explicitly start stdio server
  python start_mcp_server.py websocket    # Start WebSocket server on localhost:8765
  python start_mcp_server.py websocket --host 0.0.0.0 --port 8080  # Custom host/port

Environment Variables:
  LOG_LEVEL  - Set logging level (DEBUG, INFO, WARNING, ERROR)

The server will automatically:
  1. Create a virtual environment if needed
  2. Install all required dependencies
  3. Start the appropriate MCP server
""",
    )

    parser.add_argument(
        "mode",
        nargs="?",
        default="stdio",
        choices=["stdio", "websocket"],
        help="Server mode: 'stdio' for Claude Desktop (default) or 'websocket' for WebSocket server",
    )

    parser.add_argument("--host", default="localhost", help="Host for WebSocket server (default: localhost)")

    parser.add_argument("--port", type=int, default=8765, help="Port for WebSocket server (default: 8765)")

    parser.add_argument("--script", help="Custom script to run instead of default MCP server")

    parser.add_argument("--args", nargs="*", help="Additional arguments to pass to custom script")

    args = parser.parse_args()

    return launcher.run_server(mode=args.mode, host=args.host, port=args.port, custom_script=args.script, custom_args=args.args)


if __name__ == "__main__":
    # When run directly, use current directory as project root
    sys.exit(main())
