"""Type definitions for the worker pool system"""
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, TypeVar

from pydantic import BaseModel, Field


class PoolType(Enum):
    """Type of worker pool"""

    THREAD = "thread"
    PROCESS = "process"
    ASYNC = "async"  # For future asyncio-based pools
    HYBRID = "hybrid"  # Can spawn both threads and processes


class WorkerState(Enum):
    """State of a worker"""

    IDLE = "idle"
    BUSY = "busy"
    STARTING = "starting"
    STOPPING = "stopping"
    DEAD = "dead"
    HIBERNATING = "hibernating"  # Warm but not actively consuming resources


class PoolConfig(BaseModel):
    """Configuration for a worker pool"""

    name: str = Field(description="Pool name for identification")
    pool_type: PoolType = Field(default=PoolType.THREAD)
    min_workers: int = Field(default=1, ge=0, description="Minimum number of workers")
    max_workers: int = Field(default=10, ge=1, description="Maximum number of workers")
    warm_workers: int = Field(default=2, ge=0, description="Number of warm/ready workers to maintain")
    worker_ttl: int = Field(default=3600, description="Worker time-to-live in seconds")
    idle_timeout: int = Field(default=300, description="Time before idle worker is terminated")
    health_check_interval: int = Field(default=30, description="Health check interval in seconds")
    acquire_timeout: int = Field(default=30, description="Timeout for acquiring a worker")
    initialization_timeout: int = Field(default=10, description="Timeout for worker initialization")
    max_tasks_per_worker: int | None = Field(default=None, description="Max tasks before worker restart")
    enable_hibernation: bool = Field(default=True, description="Enable worker hibernation")
    hibernation_delay: int = Field(default=60, description="Delay before hibernating idle workers")
    worker_init_func: str | None = Field(default=None, description="Module:function for worker initialization")
    worker_cleanup_func: str | None = Field(default=None, description="Module:function for worker cleanup")
    worker_env: dict[str, str] = Field(default_factory=dict, description="Environment variables for workers")
    worker_args: list[Any] = Field(default_factory=list, description="Arguments for worker initialization")
    worker_kwargs: dict[str, Any] = Field(default_factory=dict, description="Keyword arguments for worker initialization")
    enable_stats: bool = Field(default=True, description="Enable statistics collection")
    enable_persistence: bool = Field(default=False, description="Enable worker state persistence")
    persistence_path: str | None = Field(default=None, description="Path for persistence storage")
    resource_limits: dict[str, Any] = Field(default_factory=dict, description="Resource limits (memory, cpu, etc.)")


@dataclass
class WorkerInfo:
    """Information about a worker"""

    id: str
    state: WorkerState
    created_at: datetime
    last_activity: datetime
    task_count: int = 0
    error_count: int = 0
    current_task: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    pid: int | None = None  # For process workers
    tid: int | None = None  # For thread workers
    memory_usage: int | None = None  # In bytes
    cpu_percent: float | None = None


@dataclass
class TaskInfo:
    """Information about a task"""

    id: str
    func: Callable | str
    args: tuple
    kwargs: dict
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    result: Any = None
    error: Exception | None = None
    worker_id: str | None = None
    retry_count: int = 0
    timeout: float | None = None
    priority: int = 0  # Higher priority = executed first


class PoolStats(BaseModel):
    """Statistics for a worker pool"""

    name: str
    pool_type: PoolType
    total_workers: int
    idle_workers: int
    busy_workers: int
    hibernating_workers: int
    pending_tasks: int
    completed_tasks: int
    failed_tasks: int
    average_task_time: float
    uptime_seconds: float
    memory_usage: int | None = None
    cpu_percent: float | None = None
    worker_states: dict[str, WorkerState] = Field(default_factory=dict)
    last_health_check: datetime | None = None


# Type variables for generic worker pool
T = TypeVar("T")  # Worker type
R = TypeVar("R")  # Result type
