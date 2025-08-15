"""Simple process pool that avoids pickle issues."""
import asyncio
import json
import logging
import os
import sys
import uuid
from pathlib import Path
from typing import Any

from .base import WorkerPool
from .types import PoolConfig, TaskInfo

logger = logging.getLogger(__name__)


class SimpleProcessWorker:
    """A simple process worker."""

    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.process: asyncio.subprocess.Process | None = None

    async def start(self, python_exe: str, base_dir: str):
        """Start the worker process."""
        env = os.environ.copy()
        env["PYTHONPATH"] = base_dir

        # Start a Python subprocess that can execute commands
        self.process = await asyncio.create_subprocess_exec(
            python_exe,
            "-c",
            f"""
import sys
sys.path.insert(0, r'{base_dir}')
import json
import importlib

while True:
    try:
        line = input()
        if line == 'EXIT':
            break
        data = json.loads(line)
        module_name, func_name = data['func'].rsplit(':', 1)
        module = importlib.import_module(module_name)
        func = getattr(module, func_name)
        result = func(*data.get('args', []), **data.get('kwargs', {{}}))
        print(json.dumps({{'success': True, 'result': result}}))
    except (RuntimeError, OSError, ValueError, TypeError) as e:
        print(json.dumps({{'success': False, 'error': str(e)}}))
""",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )

    async def execute(self, func_str: str, *args, **kwargs) -> Any:
        """Execute a function in the worker process."""
        if not self.process or self.process.returncode is not None:
            raise RuntimeError("Worker process not running")

        # Send command to process
        command = json.dumps({"func": func_str, "args": args, "kwargs": kwargs})
        self.process.stdin.write(f"{command}\n".encode())
        await self.process.stdin.drain()

        # Read response
        line = await self.process.stdout.readline()
        result = json.loads(line.decode())

        if result["success"]:
            return result["result"]
        raise Exception(result["error"])

    async def shutdown(self):
        """Shutdown the worker."""
        if self.process and self.process.returncode is None:
            self.process.stdin.write(b"EXIT\n")
            await self.process.stdin.drain()
            await self.process.wait()


class SimpleProcessPool(WorkerPool[SimpleProcessWorker, Any]):
    """Simple process pool implementation."""

    def __init__(self, config: PoolConfig):
        super().__init__(config)
        self._workers: dict[str, SimpleProcessWorker] = {}
        self._python_exe = sys.executable
        self._base_dir = str(Path(__file__).parent.parent.parent)

    async def _create_worker(self, **kwargs) -> SimpleProcessWorker:  # noqa: ARG002
        """Create a new worker."""
        worker_id = str(uuid.uuid4())
        worker = SimpleProcessWorker(worker_id)
        await worker.start(self._python_exe, self._base_dir)
        return worker

    async def _destroy_worker(self, worker: SimpleProcessWorker) -> None:
        """Destroy a worker."""
        await worker.shutdown()

    async def _execute_task(self, worker: SimpleProcessWorker, task: TaskInfo) -> Any:
        """Execute a task on a worker."""
        func_str = f"{task.func.__module__}:{task.func.__name__}" if not isinstance(task.func, str) else task.func

        return await worker.execute(func_str, *task.args, **task.kwargs)

    async def _check_worker_health(self, worker: SimpleProcessWorker) -> bool:
        """Check if worker is healthy."""
        try:
            result = await worker.execute("builtins:bool", True)
            return result is True
        except (RuntimeError, OSError, ValueError, TypeError) as e:
            logger.warning(f"Simple process worker health check failed: {e}")
            return False

    async def _reset_worker(self, worker: SimpleProcessWorker) -> None:
        """Reset worker to clean state."""
        # For simple process workers, we don't need to reset

    async def _hibernate_worker(self, worker: SimpleProcessWorker) -> None:
        """Put worker into hibernation."""
        # Not supported for simple process workers

    async def _wake_worker(self, worker: SimpleProcessWorker) -> None:
        """Wake worker from hibernation."""
        # Not supported for simple process workers

    def _store_worker_instance(self, worker_id: str, worker: SimpleProcessWorker) -> None:
        """Store a worker instance."""
        self._workers[worker_id] = worker

    async def _get_worker_instance(self, worker_id: str) -> SimpleProcessWorker | None:
        """Get a worker instance by ID."""
        return self._workers.get(worker_id)

    def _remove_worker_instance(self, worker_id: str) -> None:
        """Remove a worker instance from storage."""
        self._workers.pop(worker_id, None)
