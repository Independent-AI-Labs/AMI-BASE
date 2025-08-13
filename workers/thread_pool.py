"""Thread-based worker pool implementation"""
import asyncio
import concurrent.futures
import importlib
import threading
from collections.abc import Callable
from typing import Any

from .base import WorkerPool
from .types import PoolConfig, TaskInfo


class ThreadWorker:
    """A thread worker wrapper"""

    def __init__(self, worker_id: str, init_func: Callable | None = None, **kwargs):
        self.worker_id = worker_id
        self.thread_id: int | None = None
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.state_lock = threading.Lock()
        self.state: dict[str, Any] = {}

        # Initialize worker if init function provided
        if init_func:
            future = self.executor.submit(init_func, **kwargs)
            result = future.result(timeout=10)
            if isinstance(result, dict):
                self.state.update(result)

    def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Execute a function in this worker's thread"""
        # If func is a string, import it
        if isinstance(func, str):
            module_name, func_name = func.rsplit(":", 1)
            module = importlib.import_module(module_name)
            func = getattr(module, func_name)

        # Pass worker state as first argument if function expects it
        import inspect

        sig = inspect.signature(func)
        if "worker_state" in sig.parameters:
            kwargs["worker_state"] = self.state

        future = self.executor.submit(func, *args, **kwargs)
        self.thread_id = future._state  # Store thread ID for monitoring
        return future

    def shutdown(self):
        """Shutdown the worker's executor"""
        self.executor.shutdown(wait=True, cancel_futures=True)


class ThreadWorkerPool(WorkerPool[ThreadWorker, Any]):
    """Thread-based worker pool"""

    def __init__(self, config: PoolConfig):
        super().__init__(config)
        self._workers: dict[str, ThreadWorker] = {}
        self._loop = None

        # Parse init function if provided
        self._init_func = None
        if config.worker_init_func:
            module_name, func_name = config.worker_init_func.rsplit(":", 1)
            module = importlib.import_module(module_name)
            self._init_func = getattr(module, func_name)

    async def _create_worker(self, **kwargs) -> ThreadWorker:
        """Create a new thread worker"""
        worker_id = str(len(self._workers))
        worker = ThreadWorker(worker_id, init_func=self._init_func, **{**self.config.worker_kwargs, **kwargs})
        return worker

    async def _destroy_worker(self, worker: ThreadWorker) -> None:
        """Destroy a thread worker"""
        worker.shutdown()

    async def _execute_task(self, worker: ThreadWorker, task: TaskInfo) -> Any:
        """Execute a task on a thread worker"""
        # Get or create event loop for async execution
        if not self._loop:
            self._loop = asyncio.get_event_loop()

        # Execute in thread and wait for result
        future = worker.execute(task.func, *task.args, **task.kwargs)

        # Convert concurrent.futures.Future to asyncio.Future
        async_future = asyncio.wrap_future(future, loop=self._loop)

        # Apply timeout if specified
        if task.timeout:
            try:
                result = await asyncio.wait_for(async_future, timeout=task.timeout)
            except asyncio.TimeoutError:
                future.cancel()
                raise TimeoutError(f"Task {task.id} timed out after {task.timeout}s")
        else:
            result = await async_future

        return result

    async def _check_worker_health(self, worker: ThreadWorker) -> bool:
        """Check if a thread worker is healthy"""
        try:
            # Try to execute a simple task
            future = worker.execute(lambda: True)
            result = future.result(timeout=1)
            return result is True
        except Exception:
            return False

    async def _reset_worker(self, worker: ThreadWorker) -> None:
        """Reset thread worker to clean state"""
        # Clear any worker state
        with worker.state_lock:
            worker.state.clear()

    async def _hibernate_worker(self, worker: ThreadWorker) -> None:
        """Put thread worker into hibernation (no-op for threads)"""
        # Threads don't really hibernate, but we can clear their state
        await self._reset_worker(worker)

    async def _wake_worker(self, worker: ThreadWorker) -> None:
        """Wake thread worker from hibernation"""
        # Re-initialize if needed
        if self._init_func:
            future = worker.executor.submit(self._init_func, **self.config.worker_kwargs)
            result = future.result(timeout=self.config.initialization_timeout)
            if isinstance(result, dict):
                with worker.state_lock:
                    worker.state.update(result)

    def _store_worker_instance(self, worker_id: str, worker: ThreadWorker) -> None:
        """Store a worker instance"""
        self._workers[worker_id] = worker

    async def _get_worker_instance(self, worker_id: str) -> ThreadWorker | None:
        """Get a worker instance by ID"""
        return self._workers.get(worker_id)

    def _remove_worker_instance(self, worker_id: str) -> None:
        """Remove a worker instance from storage"""
        self._workers.pop(worker_id, None)
