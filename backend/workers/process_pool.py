"""Process-based worker pool implementation"""
import asyncio
import concurrent.futures
import importlib
import multiprocessing
import os
import pickle
import signal
import sys
from collections.abc import Callable
from typing import Any

from .base import WorkerPool
from .types import PoolConfig, TaskInfo

# Global worker state for process workers
_worker_state: dict[str, Any] = {}


def _worker_initializer(init_func_str: str | None, env_vars: dict, *args, **kwargs):
    """Initialize a process worker"""
    # Ensure backend module is importable
    from pathlib import Path

    base_dir = Path(__file__).parent.parent.parent
    if str(base_dir) not in sys.path:
        sys.path.insert(0, str(base_dir))

    # Set environment variables
    os.environ.update(env_vars)

    # Store worker state in global variable
    global _worker_state  # noqa: PLW0603
    _worker_state = {}

    # Run initialization function if provided
    if init_func_str:
        module_name, func_name = init_func_str.rsplit(":", 1)
        module = importlib.import_module(module_name)
        init_func = getattr(module, func_name)
        result = init_func(*args, **kwargs)
        if isinstance(result, dict):
            _worker_state.update(result)  # noqa: PLW0602

    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, _worker_signal_handler)
    signal.signal(signal.SIGINT, _worker_signal_handler)


def _worker_signal_handler(signum, frame):  # noqa: ARG001
    """Handle signals in worker process"""
    sys.exit(0)


def _execute_in_process(func_str: str | Callable, *args, **kwargs):
    """Execute a function in the process"""
    # Import function if it's a string
    if isinstance(func_str, str):
        module_name, func_name = func_str.rsplit(":", 1)
        module = importlib.import_module(module_name)
        func = getattr(module, func_name)
    else:
        func = func_str

    # Pass worker state if function expects it
    import inspect

    sig = inspect.signature(func)
    if "worker_state" in sig.parameters:
        kwargs["worker_state"] = _worker_state  # noqa: PLW0602

    return func(*args, **kwargs)


class ProcessWorker:
    """A process worker wrapper"""

    def __init__(self, worker_id: str, executor: concurrent.futures.ProcessPoolExecutor):
        self.worker_id = worker_id
        self.executor = executor
        self.process: multiprocessing.Process | None = None
        self.pid: int | None = None

    def execute(self, func: Callable | str, *args, **kwargs) -> concurrent.futures.Future:
        """Execute a function in this worker's process"""
        future = self.executor.submit(_execute_in_process, func, *args, **kwargs)

        # Try to get the process ID
        try:
            # This is a bit hacky but works for getting the PID
            if hasattr(future, "_process"):
                self.process = future._process
                self.pid = future._process.pid
        except Exception as e:
            # Process info not available in all cases, log but don't fail
            import logging

            logging.debug(f"Could not get process info for worker: {e}")

        return future

    def shutdown(self):
        """Shutdown the worker (handled by pool)"""


class ProcessWorkerPool(WorkerPool[ProcessWorker, Any]):
    """Process-based worker pool"""

    def __init__(self, config: PoolConfig):
        super().__init__(config)
        self._executor: concurrent.futures.ProcessPoolExecutor | None = None
        self._workers: dict[str, ProcessWorker] = {}
        self._loop: asyncio.AbstractEventLoop | None = None

        # Prepare initialization parameters
        self._init_args = config.worker_args
        self._init_kwargs = config.worker_kwargs
        self._env_vars = config.worker_env.copy()

        # Ensure subprocesses can import backend modules
        from pathlib import Path

        base_dir = Path(__file__).parent.parent.parent
        if "PYTHONPATH" in self._env_vars:
            self._env_vars["PYTHONPATH"] = f"{base_dir}{os.pathsep}{self._env_vars['PYTHONPATH']}"
        else:
            self._env_vars["PYTHONPATH"] = str(base_dir)

    async def initialize(self) -> None:
        """Initialize the process pool"""
        # Create the process pool executor
        # Note: initargs is the proper way to pass arguments to initializer
        self._executor = concurrent.futures.ProcessPoolExecutor(
            max_workers=self.config.max_workers,
            initializer=_worker_initializer,  # type: ignore[arg-type]
            initargs=(
                self.config.worker_init_func,
                self._env_vars,
                *self._init_args,
            ),
            mp_context=multiprocessing.get_context("spawn"),  # Use spawn for better isolation
        )

        await super().initialize()

    async def shutdown(self) -> None:
        """Shutdown the process pool"""
        await super().shutdown()

        if self._executor:
            self._executor.shutdown(wait=True, cancel_futures=True)
            self._executor = None

    async def _create_worker(self, **kwargs) -> ProcessWorker:  # noqa: ARG002
        """Create a new process worker"""
        if not self._executor:
            raise RuntimeError("Process pool not initialized")

        worker_id = str(len(self._workers))
        return ProcessWorker(worker_id, self._executor)

    async def _destroy_worker(self, worker: ProcessWorker) -> None:
        """Destroy a process worker"""
        # Process termination is handled by the executor
        if worker.process and worker.process.is_alive():
            worker.process.terminate()
            worker.process.join(timeout=5)
            if worker.process.is_alive():
                worker.process.kill()

    async def _execute_task(self, worker: ProcessWorker, task: TaskInfo) -> Any:
        """Execute a task on a process worker"""
        # Get event loop for async execution
        self._loop = asyncio.get_event_loop()

        # Serialize function if it's not a string
        func: str | bytes = task.func  # type: ignore[assignment]
        if not isinstance(func, str):
            # Try to get the module:function string or pickle it
            func = f"{func.__module__}:{func.__name__}" if hasattr(func, "__module__") and hasattr(func, "__name__") else pickle.dumps(func)

        # Execute in process and wait for result
        # Type ignore because func can be bytes (pickled) which is valid but not in type hint
        future = worker.execute(func, *task.args, **task.kwargs)  # type: ignore[arg-type]

        # Convert concurrent.futures.Future to asyncio.Future
        async_future = asyncio.wrap_future(future, loop=self._loop)

        # Apply timeout if specified
        if task.timeout:
            try:
                result = await asyncio.wait_for(async_future, timeout=task.timeout)
            except asyncio.TimeoutError as e:
                future.cancel()
                # Try to kill the process if it's stuck
                if worker.process and worker.process.is_alive():
                    worker.process.terminate()
                raise TimeoutError(f"Task {task.id} timed out after {task.timeout}s") from e
        else:
            result = await async_future

        return result

    async def _check_worker_health(self, worker: ProcessWorker) -> bool:
        """Check if a process worker is healthy"""
        try:
            # Try to execute a simple task
            future = worker.execute(lambda: True)
            result = future.result(timeout=1)
            return result is True
        except Exception:
            return False

    async def _reset_worker(self, worker: ProcessWorker) -> None:
        """Reset process worker to clean state"""
        # For processes, we might need to restart them completely
        # This is more complex and depends on the use case

    async def _hibernate_worker(self, worker: ProcessWorker) -> None:
        """Put process worker into hibernation"""
        # Send SIGSTOP on Unix-like systems to pause the process

        if worker.process and hasattr(signal, "SIGSTOP"):
            try:
                os.kill(worker.pid, signal.SIGSTOP)
            except OSError as e:
                import logging

                logging.warning(f"Could not hibernate process {worker.pid}: {e}")

    async def _wake_worker(self, worker: ProcessWorker) -> None:
        """Wake process worker from hibernation"""
        # Send SIGCONT on Unix-like systems to resume the process

        if worker.process and hasattr(signal, "SIGCONT"):
            try:
                os.kill(worker.pid, signal.SIGCONT)
            except OSError as e:
                import logging

                logging.warning(f"Could not wake process {worker.pid}: {e}")

    def _store_worker_instance(self, worker_id: str, worker: ProcessWorker) -> None:
        """Store a worker instance"""
        self._workers[worker_id] = worker

    async def _get_worker_instance(self, worker_id: str) -> ProcessWorker | None:
        """Get a worker instance by ID"""
        return self._workers.get(worker_id)

    def _remove_worker_instance(self, worker_id: str) -> None:
        """Remove a worker instance from storage"""
        self._workers.pop(worker_id, None)

    def get_process_info(self) -> dict[str, dict]:
        """Get information about all processes"""
        info = {}
        for worker_id, worker in self._workers.items():
            if worker.process:
                info[worker_id] = {
                    "pid": worker.pid,
                    "alive": worker.process.is_alive() if worker.process else False,
                }
        return info
