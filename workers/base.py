"""Base classes for the generic worker pool system"""
import asyncio
import time
import uuid
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Callable
from datetime import datetime
from typing import Generic, TypeVar

from .types import PoolConfig, PoolStats, PoolType, TaskInfo, WorkerInfo, WorkerState

T = TypeVar("T")  # Worker type
R = TypeVar("R")  # Result type


class WorkerPool(ABC, Generic[T, R]):
    """Abstract base class for all worker pools"""

    def __init__(self, config: PoolConfig):
        self.config = config
        self.pool_id = str(uuid.uuid4())
        self.created_at = datetime.now()

        # Worker management
        self.available: deque[WorkerInfo] = deque()
        self.busy: dict[str, WorkerInfo] = {}
        self.hibernating: dict[str, WorkerInfo] = {}
        self.all_workers: dict[str, WorkerInfo] = {}

        # Task management
        self.pending_tasks: deque[TaskInfo] = deque()
        self.active_tasks: dict[str, TaskInfo] = {}
        self.completed_tasks: list[TaskInfo] = []
        self.failed_tasks: list[TaskInfo] = []

        # Statistics
        self.stats = PoolStats(
            name=config.name,
            pool_type=config.pool_type,
            total_workers=0,
            idle_workers=0,
            busy_workers=0,
            hibernating_workers=0,
            pending_tasks=0,
            completed_tasks=0,
            failed_tasks=0,
            average_task_time=0.0,
            uptime_seconds=0.0,
        )

        # Control
        self._lock = asyncio.Lock()
        self._shutdown = False
        self._worker_available = asyncio.Condition()  # Event for worker availability
        self._health_check_task: asyncio.Task | None = None
        self._warmup_task: asyncio.Task | None = None
        self._hibernation_task: asyncio.Task | None = None

    @abstractmethod
    async def _create_worker(self, **kwargs) -> T:
        """Create a new worker instance"""

    @abstractmethod
    async def _destroy_worker(self, worker: T) -> None:
        """Destroy a worker instance"""

    @abstractmethod
    async def _execute_task(self, worker: T, task: TaskInfo) -> R:
        """Execute a task on a worker"""

    @abstractmethod
    async def _check_worker_health(self, worker: T) -> bool:
        """Check if a worker is healthy"""

    @abstractmethod
    async def _reset_worker(self, worker: T) -> None:
        """Reset worker to clean state for reuse"""

    @abstractmethod
    async def _hibernate_worker(self, worker: T) -> None:
        """Put worker into hibernation state"""

    @abstractmethod
    async def _wake_worker(self, worker: T) -> None:
        """Wake worker from hibernation"""

    async def initialize(self) -> None:
        """Initialize the worker pool"""
        await self._ensure_min_workers()

        # Start background tasks
        if self.config.health_check_interval > 0:
            self._health_check_task = asyncio.create_task(self._health_check_loop())

        self._warmup_task = asyncio.create_task(self._warmup_loop())

        if self.config.enable_hibernation:
            self._hibernation_task = asyncio.create_task(self._hibernation_loop())

    async def shutdown(self) -> None:
        """Shutdown the worker pool"""
        self._shutdown = True

        # Cancel background tasks
        tasks = [
            self._health_check_task,
            self._warmup_task,
            self._hibernation_task,
        ]

        for task in tasks:
            if task and not task.done():
                task.cancel()

        await asyncio.gather(*[t for t in tasks if t], return_exceptions=True)

        # Destroy all workers
        for worker_id in list(self.all_workers.keys()):
            await self._remove_worker(worker_id)

        self.available.clear()
        self.busy.clear()
        self.hibernating.clear()
        self.all_workers.clear()

    async def submit(
        self,
        func: Callable[..., R] | str,
        *args,
        **kwargs,
    ) -> str:
        """Submit a task to the pool"""
        task_id = str(uuid.uuid4())
        task = TaskInfo(
            id=task_id,
            func=func,
            args=args,
            kwargs=kwargs,
            created_at=datetime.now(),
            priority=kwargs.pop("_priority", 0),
            timeout=kwargs.pop("_timeout", None),
        )

        async with self._lock:
            self.pending_tasks.append(task)
            self.stats.pending_tasks = len(self.pending_tasks)

        # Try to process immediately
        asyncio.create_task(self._process_pending_tasks())

        return task_id

    async def get_result(self, task_id: str, timeout: float | None = None) -> R:
        """Get the result of a task"""
        start_time = time.time()

        while True:
            # Check completed tasks
            for task in self.completed_tasks:
                if task.id == task_id:
                    if task.error:
                        raise task.error
                    return task.result

            # Check failed tasks
            for task in self.failed_tasks:
                if task.id == task_id:
                    raise task.error or Exception(f"Task {task_id} failed")

            # Check timeout
            if timeout and (time.time() - start_time) > timeout:
                raise TimeoutError(f"Task {task_id} timed out")

            await asyncio.sleep(0.1)

    async def acquire_worker(self, timeout: float | None = None) -> str:  # noqa: C901
        """Acquire a worker from the pool"""
        timeout = timeout or self.config.acquire_timeout

        async def _try_acquire() -> str | None:
            """Try to acquire a worker without waiting"""
            async with self._lock:
                # Try to get an available worker
                if self.available:
                    worker_info = self.available.popleft()
                    worker_id = worker_info.id
                    self.busy[worker_id] = worker_info
                    worker_info.state = WorkerState.BUSY
                    worker_info.last_activity = datetime.now()
                    self.stats.idle_workers -= 1
                    self.stats.busy_workers += 1
                    return worker_id

                # Try to wake a hibernating worker
                if self.hibernating and self.config.enable_hibernation:
                    worker_id = next(iter(self.hibernating))
                    worker_info = self.hibernating.pop(worker_id)
                    worker = await self._get_worker_instance(worker_id)
                    await self._wake_worker(worker)
                    self.busy[worker_id] = worker_info
                    worker_info.state = WorkerState.BUSY
                    worker_info.last_activity = datetime.now()
                    self.stats.hibernating_workers -= 1
                    self.stats.busy_workers += 1
                    return worker_id
            return None

        # First attempt - immediate check
        worker_id = await _try_acquire()
        if worker_id:
            return worker_id

        # Try to create a new worker if under max
        if len(self.all_workers) < self.config.max_workers:
            worker_id = await self._add_worker()
            if worker_id:
                async with self._lock:
                    worker_info = self.all_workers[worker_id]
                    if worker_info in self.available:
                        self.available.remove(worker_info)
                    self.busy[worker_id] = worker_info
                    worker_info.state = WorkerState.BUSY
                    worker_info.last_activity = datetime.now()
                    self.stats.idle_workers = max(0, self.stats.idle_workers - 1)
                    self.stats.busy_workers += 1
                return worker_id

        # Wait for a worker to become available
        try:
            async with self._worker_available:
                await asyncio.wait_for(self._worker_available.wait_for(lambda: bool(self.available or self.hibernating or self._shutdown)), timeout=timeout)
        except asyncio.TimeoutError as e:
            raise TimeoutError(f"Failed to acquire worker within {timeout} seconds") from e

        if self._shutdown:
            raise RuntimeError("Pool is shutting down")

        # Try again after notification
        worker_id = await _try_acquire()
        if worker_id:
            return worker_id

        raise RuntimeError("Failed to acquire worker")

    async def release_worker(self, worker_id: str) -> None:
        """Release a worker back to the pool"""
        async with self._lock:
            worker_info = self.busy.pop(worker_id, None)
            if not worker_info:
                return

            worker_info.state = WorkerState.IDLE
            worker_info.current_task = None

            # Check if worker should be retired
            if self._should_retire_worker(worker_info):
                await self._remove_worker(worker_id)
            else:
                # Reset and return to available pool
                worker = await self._get_worker_instance(worker_id)
                if worker:
                    await self._reset_worker(worker)
                    self.available.append(worker_info)
                    self.stats.busy_workers -= 1
                    self.stats.idle_workers += 1

        # Notify waiters that a worker is available (outside lock to avoid deadlock)
        async with self._worker_available:
            self._worker_available.notify()

    async def _add_worker(self) -> str | None:
        """Add a new worker to the pool"""
        try:
            worker = await self._create_worker(**self.config.worker_kwargs)
            # Use the worker's own ID if it has one, otherwise generate a new one
            worker_id = getattr(worker, "id", str(uuid.uuid4()))

            worker_info = WorkerInfo(
                id=worker_id,
                state=WorkerState.IDLE,
                created_at=datetime.now(),
                last_activity=datetime.now(),
            )

            async with self._lock:
                self.all_workers[worker_id] = worker_info
                self.available.append(worker_info)
                self._store_worker_instance(worker_id, worker)
                self.stats.total_workers += 1
                self.stats.idle_workers += 1

            # Notify waiters that a worker is available
            async with self._worker_available:
                self._worker_available.notify()

            return worker_id
        except Exception as e:
            print(f"Failed to create worker: {e}")
            return None

    async def _remove_worker(self, worker_id: str) -> None:
        """Remove a worker from the pool"""
        async with self._lock:
            worker_info = self.all_workers.pop(worker_id, None)
            if not worker_info:
                return

            # Remove from all collections
            if worker_info in self.available:
                self.available.remove(worker_info)
                self.stats.idle_workers -= 1

            self.busy.pop(worker_id, None)
            self.hibernating.pop(worker_id, None)

            # Destroy the actual worker
            worker = await self._get_worker_instance(worker_id)
            if worker:
                await self._destroy_worker(worker)
                self._remove_worker_instance(worker_id)

            self.stats.total_workers -= 1
            if worker_info.state == WorkerState.BUSY:
                self.stats.busy_workers -= 1
            elif worker_info.state == WorkerState.HIBERNATING:
                self.stats.hibernating_workers -= 1

    def _should_retire_worker(self, worker_info: WorkerInfo) -> bool:
        """Check if a worker should be retired"""
        # Check TTL
        age = (datetime.now() - worker_info.created_at).total_seconds()
        if age > self.config.worker_ttl:
            return True

        # Check task count
        if self.config.max_tasks_per_worker and worker_info.task_count >= self.config.max_tasks_per_worker:
            return True

        # Check error rate
        error_rate_threshold = 0.5
        if worker_info.task_count > 0 and worker_info.error_count / worker_info.task_count > error_rate_threshold:
            return True

        return False

    async def _ensure_min_workers(self) -> None:
        """Ensure minimum number of workers are available"""
        current = len(self.all_workers)
        needed = max(0, self.config.min_workers - current)

        for _ in range(needed):
            await self._add_worker()

    async def _ensure_warm_workers(self) -> None:
        """Ensure warm workers are available"""
        available = len(self.available) + len(self.hibernating)
        needed = max(0, self.config.warm_workers - available)

        for _ in range(needed):
            if len(self.all_workers) >= self.config.max_workers:
                break
            await self._add_worker()

    async def _process_pending_tasks(self) -> None:
        """Process pending tasks"""
        while self.pending_tasks and not self._shutdown:
            try:
                worker_id = await self.acquire_worker(timeout=1)
            except TimeoutError:
                continue

            async with self._lock:
                if not self.pending_tasks:
                    # No tasks left, release the worker
                    await self.release_worker(worker_id)
                    break

                # Get highest priority task
                task = self.pending_tasks.popleft()
                self.active_tasks[task.id] = task
                self.stats.pending_tasks = len(self.pending_tasks)

            # Execute task asynchronously
            asyncio.create_task(self._execute_task_wrapper(worker_id, task))

    async def _execute_task_wrapper(self, worker_id: str, task: TaskInfo) -> None:
        """Wrapper to execute a task and handle results"""
        worker_info = self.all_workers.get(worker_id)
        if not worker_info:
            return

        worker = await self._get_worker_instance(worker_id)
        if not worker:
            return

        worker_info.current_task = task.id
        task.worker_id = worker_id
        task.started_at = datetime.now()

        try:
            # Execute the task
            result = await self._execute_task(worker, task)
            task.result = result
            task.completed_at = datetime.now()

            # Update statistics
            worker_info.task_count += 1
            self.completed_tasks.append(task)
            self.stats.completed_tasks += 1

        except Exception as e:
            task.error = e
            task.completed_at = datetime.now()
            worker_info.error_count += 1
            self.failed_tasks.append(task)
            self.stats.failed_tasks += 1

        finally:
            self.active_tasks.pop(task.id, None)
            await self.release_worker(worker_id)

            # Update average task time
            if task.started_at and task.completed_at:
                task_time = (task.completed_at - task.started_at).total_seconds()
                if self.stats.completed_tasks > 0:
                    total_time = self.stats.average_task_time * (self.stats.completed_tasks - 1)
                    self.stats.average_task_time = (total_time + task_time) / self.stats.completed_tasks
                else:
                    self.stats.average_task_time = task_time

    async def _health_check_loop(self) -> None:
        """Background task for health checking"""
        while not self._shutdown:
            try:
                await asyncio.sleep(self.config.health_check_interval)
                await self._check_all_workers()
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Health check error: {e}")

    async def _check_all_workers(self) -> None:
        """Check health of all workers"""
        async with self._lock:
            to_remove = []

            for worker_id, worker_info in list(self.all_workers.items()):
                # Skip busy workers
                if worker_info.state == WorkerState.BUSY:
                    continue

                worker = await self._get_worker_instance(worker_id)
                if not worker or not await self._check_worker_health(worker):
                    to_remove.append(worker_id)

            for worker_id in to_remove:
                await self._remove_worker(worker_id)

        await self._ensure_min_workers()

    async def _warmup_loop(self) -> None:
        """Background task for maintaining warm workers"""
        while not self._shutdown:
            try:
                await asyncio.sleep(10)
                await self._ensure_warm_workers()
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Warmup error: {e}")

    async def _hibernation_loop(self) -> None:
        """Background task for hibernating idle workers"""
        while not self._shutdown:
            try:
                await asyncio.sleep(self.config.hibernation_delay)
                await self._hibernate_idle_workers()
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Hibernation error: {e}")

    async def _hibernate_idle_workers(self) -> None:
        """Hibernate workers that have been idle too long"""
        if not self.config.enable_hibernation:
            return

        async with self._lock:
            to_hibernate = []
            now = datetime.now()

            for worker_info in list(self.available):
                idle_time = (now - worker_info.last_activity).total_seconds()
                if idle_time > self.config.hibernation_delay:
                    to_hibernate.append(worker_info)

            for worker_info in to_hibernate:
                worker = await self._get_worker_instance(worker_info.id)
                if worker:
                    await self._hibernate_worker(worker)
                    self.available.remove(worker_info)
                    self.hibernating[worker_info.id] = worker_info
                    worker_info.state = WorkerState.HIBERNATING
                    self.stats.idle_workers -= 1
                    self.stats.hibernating_workers += 1

    def get_stats(self) -> PoolStats:
        """Get current pool statistics"""
        self.stats.uptime_seconds = (datetime.now() - self.created_at).total_seconds()
        self.stats.last_health_check = datetime.now()
        return self.stats

    # Abstract methods for storing worker instances (implementation-specific)
    @abstractmethod
    def _store_worker_instance(self, worker_id: str, worker: T) -> None:
        """Store a worker instance"""

    @abstractmethod
    async def _get_worker_instance(self, worker_id: str) -> T | None:
        """Get a worker instance by ID"""

    @abstractmethod
    def _remove_worker_instance(self, worker_id: str) -> None:
        """Remove a worker instance from storage"""


class WorkerPoolManager:
    """Manager for multiple named worker pools"""

    def __init__(self):
        self.pools: dict[str, WorkerPool] = {}
        self._lock = asyncio.Lock()

    async def create_pool(self, config: PoolConfig) -> WorkerPool:
        """Create a new worker pool"""
        async with self._lock:
            if config.name in self.pools:
                raise ValueError(f"Pool '{config.name}' already exists")

            # Create appropriate pool based on type
            pool: WorkerPool
            if config.pool_type == PoolType.THREAD:
                from .thread_pool import ThreadWorkerPool

                pool = ThreadWorkerPool(config)
            elif config.pool_type == PoolType.PROCESS:
                from .process_pool import ProcessWorkerPool

                pool = ProcessWorkerPool(config)
            else:
                raise ValueError(f"Unsupported pool type: {config.pool_type}")

            await pool.initialize()
            self.pools[config.name] = pool
            return pool

    async def get_pool(self, name: str) -> WorkerPool | None:
        """Get a pool by name"""
        return self.pools.get(name)

    async def remove_pool(self, name: str) -> None:
        """Remove and shutdown a pool"""
        async with self._lock:
            pool = self.pools.pop(name, None)
            if pool:
                await pool.shutdown()

    async def shutdown_all(self) -> None:
        """Shutdown all pools"""
        async with self._lock:
            for pool in self.pools.values():
                await pool.shutdown()
            self.pools.clear()

    def list_pools(self) -> list[str]:
        """List all pool names"""
        return list(self.pools.keys())

    def get_all_stats(self) -> dict[str, PoolStats]:
        """Get statistics for all pools"""
        return {name: pool.get_stats() for name, pool in self.pools.items()}
