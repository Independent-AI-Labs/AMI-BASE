"""Process pool implementation using UV for venv management."""
import asyncio
import json
import logging
import os
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any

from .base import WorkerPool
from .types import PoolConfig, TaskInfo

logger = logging.getLogger(__name__)


class UVProcessRunner:
    """Runner that uses UV to manage Python environments and spawn processes."""

    def __init__(self, venv_path: Path | None = None, python_version: str | None = None, requirements: list[str] | None = None):
        """
        Initialize UV process runner.

        Args:
            venv_path: Custom venv location. If None, uses current venv.
            python_version: Python version for new venv (e.g., "3.12", "3.11.5")
            requirements: List of packages to install in new venv
        """
        self.venv_path = venv_path
        self.python_version = python_version
        self.requirements = requirements or []
        self.python_exe: Path | None = None
        self._initialized = False

        # Find base directory
        self.base_dir = Path(__file__).parent.parent.parent

    async def initialize(self):
        """Initialize the runner, creating venv if needed."""
        if self._initialized:
            return

        if self.venv_path:
            # Create custom venv with UV
            await self._create_custom_venv()
        else:
            # Use current venv or find hierarchically
            self.python_exe = self._find_current_venv()

        self._initialized = True

    def _find_current_venv(self) -> Path:
        """Find the current venv Python executable."""
        # First check if we're already in a venv
        if hasattr(sys, "real_prefix") or (hasattr(sys, "base_prefix") and sys.base_prefix != sys.prefix):
            return Path(sys.executable)

        # Search hierarchically for .venv
        current = Path.cwd()
        while current != current.parent:
            venv_dir = current / ".venv"
            if venv_dir.exists():
                if sys.platform == "win32":
                    python_exe = venv_dir / "Scripts" / "python.exe"
                else:
                    python_exe = venv_dir / "bin" / "python"

                if python_exe.exists():
                    return python_exe
            current = current.parent

        # Fallback to system Python
        return Path(sys.executable)

    async def _create_custom_venv(self):
        """Create a custom venv using UV."""
        if not self.venv_path:
            raise ValueError("venv_path required for custom venv")

        # Ensure UV is available
        try:
            subprocess.run(["uv", "--version"], check=True, capture_output=True)
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            raise RuntimeError("UV is not installed or not in PATH") from e

        # Create venv with specified Python version
        cmd = ["uv", "venv", str(self.venv_path)]
        if self.python_version:
            cmd.extend(["--python", self.python_version])

        process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            raise RuntimeError(f"Failed to create venv: {stderr.decode()}")

        # Set Python executable path
        if sys.platform == "win32":
            self.python_exe = self.venv_path / "Scripts" / "python.exe"
        else:
            self.python_exe = self.venv_path / "bin" / "python"

        # Install requirements if any
        if self.requirements:
            await self._install_requirements()

        # Always install base dependencies for worker pool
        base_deps = ["aiomultiprocess==0.9.1"]
        for dep in base_deps:
            if dep not in self.requirements:
                await self._install_package(dep)

    async def _install_requirements(self):
        """Install requirements in the venv using UV."""
        for req in self.requirements:
            await self._install_package(req)

    async def _install_package(self, package: str):
        """Install a package in the venv using UV."""
        cmd = ["uv", "pip", "install", "--python", str(self.python_exe), package]

        process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            raise RuntimeError(f"Failed to install {package}: {stderr.decode()}")

    async def run_function(self, func_ref: str, args: tuple, kwargs: dict) -> Any:
        """
        Run a function in a subprocess with proper environment.

        Args:
            func_ref: Function reference as "module:function"
            args: Positional arguments
            kwargs: Keyword arguments

        Returns:
            Function result
        """
        if not self._initialized:
            await self.initialize()

        # Create a temporary script that will run the function
        script = f"""
import sys
import json
import os

# Add base directory to path
sys.path.insert(0, r'{self.base_dir}')
os.environ['PYTHONPATH'] = r'{self.base_dir}'

# Import and run the function
import importlib

func_ref = {json.dumps(func_ref)}
args = {json.dumps(args)}
kwargs = {json.dumps(kwargs)}

module_name, func_name = func_ref.rsplit(':', 1)
module = importlib.import_module(module_name)
func = getattr(module, func_name)

result = func(*args, **kwargs)
print(json.dumps({{'success': True, 'result': result}}))
"""

        # Run the script in subprocess
        process = await asyncio.create_subprocess_exec(
            str(self.python_exe),
            "-c",
            script,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={**os.environ, "PYTHONPATH": str(self.base_dir)},
        )

        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            raise RuntimeError(f"Subprocess failed: {stderr.decode()}")

        try:
            result = json.loads(stdout.decode())
            if result["success"]:
                return result["result"]
            raise RuntimeError(result.get("error", "Unknown error"))
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Failed to parse result: {stdout.decode()}") from e

    async def cleanup(self):
        """Clean up custom venv if it was created temporarily."""
        # Only cleanup if it's a temp venv (implement logic as needed)


class UVProcessWorker:
    """Process worker using UV runner."""

    def __init__(self, worker_id: str, runner: UVProcessRunner):
        self.worker_id = worker_id
        self.runner = runner
        self.busy = False

    async def execute(self, func_ref: str, args: tuple, kwargs: dict) -> Any:
        """Execute a function using the UV runner."""
        self.busy = True
        try:
            return await self.runner.run_function(func_ref, args, kwargs)
        finally:
            self.busy = False

    def is_busy(self) -> bool:
        """Check if worker is busy."""
        return self.busy


class UVProcessPool(WorkerPool[UVProcessWorker, Any]):
    """Process pool using UV for environment management."""

    def __init__(self, config: PoolConfig, venv_path: Path | None = None, python_version: str | None = None, requirements: list[str] | None = None):
        """
        Initialize UV process pool.

        Args:
            config: Pool configuration
            venv_path: Custom venv location. If None, uses current venv.
            python_version: Python version for new venv (e.g., "3.12")
            requirements: List of packages to install in new venv
        """
        super().__init__(config)
        self.venv_path = venv_path
        self.python_version = python_version
        self.requirements = requirements
        self._workers: dict[str, UVProcessWorker] = {}
        self._runner: UVProcessRunner | None = None

    async def initialize(self) -> None:
        """Initialize the process pool."""
        # Create UV runner
        self._runner = UVProcessRunner(venv_path=self.venv_path, python_version=self.python_version, requirements=self.requirements)
        await self._runner.initialize()

        await super().initialize()

    async def shutdown(self) -> None:
        """Shutdown the process pool."""
        await super().shutdown()

        if self._runner:
            await self._runner.cleanup()
            self._runner = None

    async def _create_worker(self, **_kwargs) -> UVProcessWorker:
        """Create a new process worker."""
        if not self._runner:
            raise RuntimeError("Pool not initialized")

        worker_id = str(uuid.uuid4())
        return UVProcessWorker(worker_id, self._runner)

    async def _destroy_worker(self, worker: UVProcessWorker) -> None:
        """Destroy a process worker."""
        # Nothing special needed

    async def _execute_task(self, worker: UVProcessWorker, task: TaskInfo) -> Any:
        """Execute a task on a process worker."""
        # Convert function to string reference if needed
        if callable(task.func):
            if hasattr(task.func, "__module__") and hasattr(task.func, "__name__"):
                func_ref = f"{task.func.__module__}:{task.func.__name__}"
            else:
                raise ValueError("Cannot execute lambda or local function in subprocess")
        else:
            func_ref = task.func

        # Apply timeout if specified
        if task.timeout:
            return await asyncio.wait_for(worker.execute(func_ref, task.args, task.kwargs), timeout=task.timeout)
        return await worker.execute(func_ref, task.args, task.kwargs)

    async def _check_worker_health(self, worker: UVProcessWorker) -> bool:
        """Check if a process worker is healthy."""
        try:
            # Try to execute a simple task
            result = await worker.execute("builtins:bool", (True,), {})
            return result is True
        except (RuntimeError, OSError, ValueError, TypeError) as e:
            logger.warning(f"Worker health check failed: {e}")
            return False

    async def _reset_worker(self, worker: UVProcessWorker) -> None:
        """Reset process worker to clean state."""
        # Nothing needed for stateless workers

    async def _hibernate_worker(self, worker: UVProcessWorker) -> None:
        """Put process worker into hibernation."""
        # Not supported

    async def _wake_worker(self, worker: UVProcessWorker) -> None:
        """Wake process worker from hibernation."""
        # Not supported

    def _store_worker_instance(self, worker_id: str, worker: UVProcessWorker) -> None:
        """Store a worker instance."""
        self._workers[worker_id] = worker

    async def _get_worker_instance(self, worker_id: str) -> UVProcessWorker | None:
        """Get a worker instance by ID."""
        return self._workers.get(worker_id)

    def _remove_worker_instance(self, worker_id: str) -> None:
        """Remove a worker instance from storage."""
        self._workers.pop(worker_id, None)
