"""Pytest configuration and shared fixtures for workers tests."""

import sys
import time
from pathlib import Path

import pytest
import pytest_asyncio
from loguru import logger

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from workers import PoolConfig, PoolType, WorkerPoolManager  # noqa: E402

# Configure logging
logger.remove()
logger.add(sys.stderr, level="INFO")

# Configure pytest-asyncio
pytest_plugins = ("pytest_asyncio",)


# Module-level functions for process pool testing (must be pickleable)
def fibonacci(n):
    """Calculate fibonacci number."""
    if n <= 1:
        return n
    return fibonacci(n - 1) + fibonacci(n - 2)


def process_data(data, multiplier=2):
    """Process data with optional multiplier."""
    return [x * multiplier for x in data]


def simple_add(x, y):
    """Simple addition function."""
    return x + y


def slow_task(duration):
    """Sleep for specified duration."""
    time.sleep(duration)
    return f"Slept for {duration}s"


def error_task():
    """Task that raises an error."""
    return 1 / 0


def cpu_intensive(n):
    """CPU intensive calculation."""
    return sum(i * i for i in range(n))


def memory_intensive(size):
    """Create large list."""
    return [0] * size


@pytest_asyncio.fixture(scope="session")
async def pool_manager():
    """Create a pool manager for the test session."""
    manager = WorkerPoolManager()
    yield manager
    await manager.shutdown_all()
    logger.info("Shut down all worker pools")


@pytest_asyncio.fixture
async def thread_pool(pool_manager):
    """Create a thread pool for testing."""
    import uuid

    # Use unique name for each test to avoid reuse
    pool_name = f"test_thread_pool_{uuid.uuid4().hex[:8]}"
    config = PoolConfig(
        name=pool_name,
        pool_type=PoolType.THREAD,
        min_workers=1,
        max_workers=5,
        warm_workers=2,
        worker_ttl=60,  # Short TTL for testing
        idle_timeout=10,
        health_check_interval=5,
        acquire_timeout=10,
        enable_hibernation=True,
        hibernation_delay=5,
    )
    pool = await pool_manager.create_pool(config)
    yield pool
    await pool_manager.remove_pool(config.name)


@pytest_asyncio.fixture
async def process_pool(pool_manager):
    """Create a process pool for testing."""
    import uuid

    # Use unique name for each test to avoid reuse
    pool_name = f"test_process_pool_{uuid.uuid4().hex[:8]}"
    config = PoolConfig(
        name=pool_name,
        pool_type=PoolType.PROCESS,
        min_workers=1,
        max_workers=3,
        warm_workers=1,
        worker_ttl=60,
        idle_timeout=10,
        health_check_interval=5,
        acquire_timeout=10,
        enable_hibernation=False,  # Process hibernation is OS-specific
    )
    pool = await pool_manager.create_pool(config)
    yield pool
    await pool_manager.remove_pool(config.name)


@pytest.fixture
def sample_tasks():
    """Provide sample tasks for testing."""
    return {
        "simple": lambda x, y: x + y,
        "slow": lambda duration: __import__("time").sleep(duration) or f"Slept for {duration}s",
        "error": lambda: 1 / 0,
        "cpu_intensive": lambda n: sum(i * i for i in range(n)),
        "memory_intensive": lambda size: [0] * size,
    }


@pytest.fixture
def worker_functions():
    """Functions that can be used as worker tasks."""

    def fibonacci(n):
        """Calculate fibonacci number."""
        if n <= 1:
            return n
        return fibonacci(n - 1) + fibonacci(n - 2)

    def process_data(data, multiplier=2):
        """Process data with optional multiplier."""
        return [x * multiplier for x in data]

    def stateful_counter(worker_state=None):
        """Use worker state to maintain a counter."""
        if worker_state is None:
            return 1
        count = worker_state.get("count", 0) + 1
        worker_state["count"] = count
        return count

    return {
        "fibonacci": fibonacci,
        "process_data": process_data,
        "stateful_counter": stateful_counter,
    }


# Markers for test categorization
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line("markers", "slow: marks tests as slow")
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
    config.addinivalue_line("markers", "thread_pool: marks tests for thread pool")
    config.addinivalue_line("markers", "process_pool: marks tests for process pool")
    config.addinivalue_line("markers", "stress: marks stress tests")
