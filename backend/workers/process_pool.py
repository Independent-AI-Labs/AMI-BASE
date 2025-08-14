"""Process-based worker pool implementation using aiomultiprocess."""
import os
import sys
import uuid
from pathlib import Path
from typing import Any

import aiomultiprocess

from .base import WorkerPool
from .types import PoolConfig, TaskInfo

# Ensure base is in path for subprocess imports
BASE_DIR = Path(__file__).parent.parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))


def pool_initializer():
    """Initialize the pool process with proper paths."""
    import sys
    from pathlib import Path

    # Add base directory to path
    base_dir = Path(__file__).parent.parent.parent
    if str(base_dir) not in sys.path:
        sys.path.insert(0, str(base_dir))

    # Set environment
    os.environ["PYTHONPATH"] = str(base_dir)


# Module-level functions for process pool testing
test_value = 0


def modify_global():
    """Modify global state - for process isolation testing."""
    global test_value  # noqa: PLW0603
    test_value += 1
    return test_value


def execute_function_by_name(func_str: str, args: tuple, kwargs: dict):
    """Execute a function by its module:name reference."""
    # Ensure paths are set
    pool_initializer()

    import importlib

    module_name, func_name = func_str.rsplit(":", 1)
    module = importlib.import_module(module_name)
    func = getattr(module, func_name)
    return func(*args, **kwargs)


class ProcessWorker:
    """A process worker wrapper."""

    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.process: aiomultiprocess.Process | None = None

    async def execute(self, func, *args, **kwargs):
        """Execute a function in this worker's process."""

        # Create a process to run the function
        async def run_func():
            """Wrapper to run the function."""
            pool_initializer()  # Ensure paths are set

            if callable(func):
                return func(*args, **kwargs)
            # It's a string like "module:function"
            import importlib

            module_name, func_name = func.rsplit(":", 1)
            module = importlib.import_module(module_name)
            actual_func = getattr(module, func_name)
            return actual_func(*args, **kwargs)

        # Run in a separate process
        process = aiomultiprocess.Process(target=run_func)
        process.start()
        return await process.join()

    def shutdown(self):
        """Shutdown the worker."""
        if self.process:
            self.process.terminate()


class ProcessWorkerPool(WorkerPool[ProcessWorker, Any]):
    """Process-based worker pool using aiomultiprocess."""

    def __init__(self, config: PoolConfig):
        super().__init__(config)
        self._pool: aiomultiprocess.Pool | None = None
        self._workers: dict[str, ProcessWorker] = {}

        # Ensure PYTHONPATH is set for subprocess
        os.environ["PYTHONPATH"] = str(BASE_DIR)

        # Set spawn mode for consistency across platforms
        aiomultiprocess.set_start_method("spawn")

    async def initialize(self) -> None:
        """Initialize the process pool."""
        # Create the aiomultiprocess pool with initializer
        self._pool = aiomultiprocess.Pool(
            processes=self.config.max_workers,
            childconcurrency=10,  # Allow multiple coroutines per process
            initializer=pool_initializer,  # Set up paths in each process
        )

        await super().initialize()

    async def shutdown(self) -> None:
        """Shutdown the process pool."""
        await super().shutdown()

        if self._pool:
            self._pool.close()
            await self._pool.join()
            self._pool.terminate()
            self._pool = None

    async def _create_worker(self, **kwargs) -> ProcessWorker:  # noqa: ARG002
        """Create a new process worker."""
        worker_id = str(uuid.uuid4())
        return ProcessWorker(worker_id)

    async def _destroy_worker(self, worker: ProcessWorker) -> None:
        """Destroy a process worker."""
        worker.shutdown()

    async def _execute_task(self, worker: ProcessWorker, task: TaskInfo) -> Any:  # noqa: ARG002
        """Execute a task on a process worker."""
        if self._pool is None:
            raise RuntimeError("Process pool not initialized")

        # Handle both callable and string functions
        if callable(task.func):
            # For regular functions, we need to ensure they're importable
            # Try to get the module path
            if hasattr(task.func, "__module__") and hasattr(task.func, "__name__"):
                func_str = f"{task.func.__module__}:{task.func.__name__}"
                # Use the module-level function to execute by string reference
                return await self._pool.apply(execute_function_by_name, (func_str, task.args, task.kwargs))
            # For lambdas or local functions, use the pool directly
            # This might fail for non-pickleable functions
            return await self._pool.apply(task.func, task.args, task.kwargs)
        # It's already a string reference
        return await self._pool.apply(execute_function_by_name, (task.func, task.args, task.kwargs))

    async def _check_worker_health(self, worker: ProcessWorker) -> bool:
        """Check if a process worker is healthy."""
        try:
            # Try to execute a simple task
            result = await worker.execute(lambda: True)
            return result is True
        except Exception:
            return False

    async def _reset_worker(self, worker: ProcessWorker) -> None:
        """Reset process worker to clean state."""
        # For processes, we might need to restart them completely

    async def _hibernate_worker(self, worker: ProcessWorker) -> None:
        """Put process worker into hibernation."""
        # Not supported for process workers

    async def _wake_worker(self, worker: ProcessWorker) -> None:
        """Wake process worker from hibernation."""
        # Not supported for process workers

    def _store_worker_instance(self, worker_id: str, worker: ProcessWorker) -> None:
        """Store a worker instance."""
        self._workers[worker_id] = worker

    async def _get_worker_instance(self, worker_id: str) -> ProcessWorker | None:
        """Get a worker instance by ID."""
        return self._workers.get(worker_id)

    def _remove_worker_instance(self, worker_id: str) -> None:
        """Remove a worker instance from storage."""
        self._workers.pop(worker_id, None)

    def get_process_info(self) -> dict[str, dict]:
        """Get information about all processes."""
        info = {}
        for worker_id, worker in self._workers.items():
            info[worker_id] = {
                "id": worker_id,
                "alive": worker.process.is_alive() if worker.process else False,
            }
        return info
