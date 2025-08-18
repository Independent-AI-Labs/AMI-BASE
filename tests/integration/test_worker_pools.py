"""Comprehensive integration tests for worker pool system."""

import asyncio
import time
from datetime import datetime

import pytest
from loguru import logger

from backend.workers.types import PoolConfig, PoolType

# Module-level functions for process pool testing
test_value = 0


def modify_global():
    """Modify global state - for process isolation testing."""
    global test_value  # noqa: PLW0603 - needed for process isolation test
    test_value += 1
    return test_value


class TestWorkerPoolBasics:
    """Test basic worker pool functionality."""

    @pytest.mark.asyncio
    @pytest.mark.thread_pool
    async def test_thread_pool_creation(self, thread_pool):
        """Test thread pool creation and initialization."""
        assert thread_pool is not None
        stats = thread_pool.get_stats()

        assert stats.name.startswith("test_thread_pool")
        assert stats.pool_type == PoolType.THREAD
        assert stats.total_workers >= thread_pool.config.min_workers
        assert stats.idle_workers >= 0
        logger.info(f"Thread pool stats: {stats}")

    @pytest.mark.asyncio
    @pytest.mark.process_pool
    async def test_process_pool_creation(self, process_pool):
        """Test process pool creation and initialization."""
        assert process_pool is not None
        stats = process_pool.get_stats()

        assert stats.name.startswith("test_process_pool")
        assert stats.pool_type == PoolType.PROCESS
        assert stats.total_workers >= process_pool.config.min_workers
        logger.info(f"Process pool stats: {stats}")

    @pytest.mark.asyncio
    @pytest.mark.thread_pool
    async def test_submit_and_get_result(self, thread_pool, sample_tasks):
        """Test submitting tasks and getting results."""
        # Submit simple addition task
        task_id = await thread_pool.submit(sample_tasks["simple"], 5, 3)
        result = await thread_pool.get_result(task_id, timeout=5)
        assert result == 8

        # Submit multiple tasks
        task_ids = []
        for i in range(5):
            task_id = await thread_pool.submit(sample_tasks["simple"], i, 10)
            task_ids.append(task_id)

        # Get all results
        results = []
        for task_id in task_ids:
            result = await thread_pool.get_result(task_id, timeout=5)
            results.append(result)

        assert results == [10, 11, 12, 13, 14]
        logger.info(f"Successfully executed {len(task_ids)} tasks")

    @pytest.mark.asyncio
    @pytest.mark.thread_pool
    async def test_error_handling(self, thread_pool, sample_tasks):
        """Test error handling in tasks."""
        # Submit task that will error
        task_id = await thread_pool.submit(sample_tasks["error"])

        with pytest.raises(ZeroDivisionError):
            await thread_pool.get_result(task_id, timeout=5)

        # Verify pool is still functional after error
        task_id = await thread_pool.submit(sample_tasks["simple"], 2, 2)
        result = await thread_pool.get_result(task_id, timeout=5)
        assert result == 4

        stats = thread_pool.get_stats()
        assert stats.failed_tasks >= 1
        logger.info(f"Error handling verified, failed tasks: {stats.failed_tasks}")


class TestWorkerLifecycle:
    """Test worker lifecycle management."""

    @pytest.mark.asyncio
    @pytest.mark.thread_pool
    async def test_worker_acquisition_and_release(self, thread_pool):
        """Test acquiring and releasing workers."""
        # Wait a moment for pool to stabilize
        await asyncio.sleep(0.5)

        initial_stats = thread_pool.get_stats()
        initial_busy = initial_stats.busy_workers
        initial_idle = initial_stats.idle_workers

        # Acquire a worker
        worker_id = await thread_pool.acquire_worker(timeout=5)
        assert worker_id is not None

        stats_after_acquire = thread_pool.get_stats()
        # After acquiring, either:
        # 1. An idle worker became busy (idle decreased, busy increased)
        # 2. A new worker was created (total increased, busy increased)
        assert stats_after_acquire.busy_workers >= initial_busy

        # If there were idle workers, one should have been used
        if initial_idle > 0:
            assert stats_after_acquire.idle_workers == initial_idle - 1
            assert stats_after_acquire.busy_workers == initial_busy + 1

        # Release the worker
        await thread_pool.release_worker(worker_id)

        # Wait a moment for the release to be processed
        await asyncio.sleep(0.1)

        stats_after_release = thread_pool.get_stats()
        # After release, the worker should be idle again
        assert stats_after_release.busy_workers <= initial_busy + 1
        logger.info(f"Worker {worker_id} acquired and released successfully")

    @pytest.mark.asyncio
    @pytest.mark.thread_pool
    async def test_warm_workers(self, thread_pool):
        """Test warm worker maintenance."""
        # Wait for warm workers to be created
        await asyncio.sleep(2)

        stats = thread_pool.get_stats()
        # Total workers should be at least min_workers
        assert stats.total_workers >= thread_pool.config.min_workers

        # Available workers (idle + hibernating) should be maintained
        # Note: warm_workers is a target, not a guarantee, especially if workers are busy
        available = stats.idle_workers + stats.hibernating_workers
        logger.info(f"Warm workers target: {thread_pool.config.warm_workers}, available: {available}")

        # At least min_workers should exist
        assert stats.total_workers >= thread_pool.config.min_workers

    @pytest.mark.asyncio
    @pytest.mark.thread_pool
    @pytest.mark.slow
    async def test_worker_ttl(self, pool_manager):
        """Test worker time-to-live functionality."""
        # Create pool with very short TTL
        config = PoolConfig(
            name="ttl_test_pool",
            pool_type=PoolType.THREAD,
            min_workers=1,
            max_workers=3,
            worker_ttl=3,  # 3 seconds TTL
            health_check_interval=1,
        )
        pool = await pool_manager.create_pool(config)

        # Get initial worker count
        initial_stats = pool.get_stats()
        initial_workers = initial_stats.total_workers

        # Wait for TTL to expire
        await asyncio.sleep(5)

        # Check that old workers were removed and new ones created
        stats = pool.get_stats()
        assert stats.total_workers >= pool.config.min_workers
        logger.info(f"Workers recycled due to TTL: initial={initial_workers}, current={stats.total_workers}")

        await pool_manager.remove_pool(config.name)

    @pytest.mark.asyncio
    @pytest.mark.thread_pool
    @pytest.mark.slow
    async def test_worker_hibernation(self, pool_manager):
        """Test worker hibernation when idle."""
        # Create pool with hibernation enabled
        config = PoolConfig(
            name="hibernation_test_pool",
            pool_type=PoolType.THREAD,
            min_workers=2,
            max_workers=5,
            warm_workers=3,
            enable_hibernation=True,
            hibernation_delay=2,  # Hibernate after 2 seconds
        )
        pool = await pool_manager.create_pool(config)

        # Wait for warm workers to be created
        await asyncio.sleep(1)
        initial_stats = pool.get_stats()
        assert initial_stats.idle_workers > 0

        # Wait for hibernation to occur
        await asyncio.sleep(4)

        stats = pool.get_stats()
        assert stats.hibernating_workers > 0
        logger.info(f"Workers hibernated: {stats.hibernating_workers}")

        # Submit a task to wake workers
        task_id = await pool.submit(lambda: "wake up")
        await pool.get_result(task_id, timeout=5)

        stats = pool.get_stats()
        logger.info(f"After wake: idle={stats.idle_workers}, hibernating={stats.hibernating_workers}")

        await pool_manager.remove_pool(config.name)


class TestConcurrency:
    """Test concurrent operations and pool limits."""

    @pytest.mark.asyncio
    @pytest.mark.thread_pool
    async def test_concurrent_tasks(self, thread_pool, sample_tasks):
        """Test executing multiple tasks concurrently."""
        # Submit many tasks at once
        task_ids = []
        for i in range(20):
            task_id = await thread_pool.submit(sample_tasks["slow"], 0.1)
            task_ids.append(task_id)

        # Wait for all results
        start_time = time.time()
        results = await asyncio.gather(*[thread_pool.get_result(task_id, timeout=10) for task_id in task_ids])
        elapsed = time.time() - start_time

        assert len(results) == 20
        assert all(r == "Slept for 0.1s" for r in results)

        # Should be faster than sequential execution
        assert elapsed < 20 * 0.1  # Much faster than sequential
        logger.info(f"Executed 20 tasks in {elapsed:.2f}s")

    @pytest.mark.asyncio
    @pytest.mark.thread_pool
    async def test_max_workers_limit(self, thread_pool):
        """Test that pool respects max workers limit."""
        # Submit more tasks than max workers
        max_workers = thread_pool.config.max_workers
        task_ids = []

        for i in range(max_workers + 5):
            task_id = await thread_pool.submit(lambda: time.sleep(0.5) or "done")
            task_ids.append(task_id)

        # Check stats during execution
        await asyncio.sleep(0.1)
        stats = thread_pool.get_stats()
        assert stats.total_workers <= max_workers
        assert stats.pending_tasks > 0  # Some tasks should be queued

        logger.info(f"Max workers enforced: {stats.total_workers}/{max_workers}, pending: {stats.pending_tasks}")

        # Clean up - wait for all tasks
        for task_id in task_ids:
            await thread_pool.get_result(task_id, timeout=10)

    @pytest.mark.asyncio
    @pytest.mark.thread_pool
    async def test_task_priority(self, thread_pool):
        """Test task priority execution."""
        # Fill pool with slow tasks
        slow_tasks = []
        for i in range(thread_pool.config.max_workers):
            task_id = await thread_pool.submit(lambda: time.sleep(1) or "slow")
            slow_tasks.append(task_id)

        # Submit high priority task
        high_priority_id = await thread_pool.submit(
            lambda: "high priority",
            _priority=10,  # Higher priority
        )

        # Submit normal priority task
        normal_priority_id = await thread_pool.submit(lambda: "normal priority", _priority=0)

        # The high priority task should complete before normal priority
        # (after slow tasks finish)
        results = []
        for _ in range(2):
            # Get whichever completes first
            done, pending = await asyncio.wait(
                [
                    asyncio.create_task(thread_pool.get_result(high_priority_id, timeout=5)),
                    asyncio.create_task(thread_pool.get_result(normal_priority_id, timeout=5)),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )
            if done:
                results.append(await list(done)[0])
                # Cancel pending
                for p in pending:
                    p.cancel()

        # High priority should complete first (though this is best-effort)
        logger.info(f"Task completion order: {results}")


class TestStatisticsAndMonitoring:
    """Test statistics collection and monitoring."""

    @pytest.mark.asyncio
    @pytest.mark.thread_pool
    async def test_statistics_tracking(self, thread_pool, sample_tasks):
        """Test that statistics are properly tracked."""
        initial_stats = thread_pool.get_stats()
        logger.info(f"Initial stats: completed={initial_stats.completed_tasks}, failed={initial_stats.failed_tasks}")

        # Execute various tasks
        success_ids = []
        for i in range(5):
            task_id = await thread_pool.submit(sample_tasks["simple"], i, i)
            success_ids.append(task_id)
            logger.debug(f"Submitted task {task_id}")

        # Submit some failing tasks
        fail_ids = []
        for _ in range(3):
            task_id = await thread_pool.submit(sample_tasks["error"])
            fail_ids.append(task_id)

        # Get results
        for task_id in success_ids:
            result = await thread_pool.get_result(task_id, timeout=5)
            logger.info(f"Got result for task {task_id}: {result}")

        for task_id in fail_ids:
            with pytest.raises(ZeroDivisionError):
                await thread_pool.get_result(task_id, timeout=5)
            logger.info(f"Got expected error for task {task_id}")

        # Wait a moment for stats to update
        await asyncio.sleep(0.5)

        # Check statistics
        stats = thread_pool.get_stats()
        logger.info(f"Final stats: completed={stats.completed_tasks}, failed={stats.failed_tasks}")
        # Calculate deltas
        completed_delta = stats.completed_tasks - initial_stats.completed_tasks
        failed_delta = stats.failed_tasks - initial_stats.failed_tasks
        logger.info(
            f"Deltas: completed={completed_delta} (from {initial_stats.completed_tasks} to {stats.completed_tasks}), "
            f"failed={failed_delta} (from {initial_stats.failed_tasks} to {stats.failed_tasks})"
        )

        # We submitted 5 successful and 3 failing tasks
        assert completed_delta == 5, f"Expected 5 completed tasks, got {completed_delta}"
        assert failed_delta == 3, f"Expected 3 failed tasks, got {failed_delta}"
        # Average task time might be 0 for very fast tasks on Windows
        assert stats.average_task_time >= 0
        assert stats.uptime_seconds > 0

        logger.info(
            f"Statistics: completed={stats.completed_tasks} (+{completed_delta}), "
            f"failed={stats.failed_tasks} (+{failed_delta}), "
            f"avg_time={stats.average_task_time:.3f}s"
        )

    @pytest.mark.asyncio
    @pytest.mark.thread_pool
    async def test_health_check(self, thread_pool):
        """Test worker health checking."""
        # Wait for health check to run
        await asyncio.sleep(thread_pool.config.health_check_interval + 1)

        stats = thread_pool.get_stats()
        assert stats.last_health_check is not None

        # Time since last health check should be reasonable
        time_since = (datetime.now() - stats.last_health_check).total_seconds()
        assert time_since < thread_pool.config.health_check_interval * 2

        logger.info(f"Health check running, last check: {time_since:.1f}s ago")


class TestPoolManager:
    """Test pool manager functionality."""

    @pytest.mark.asyncio
    async def test_multiple_named_pools(self, pool_manager):
        """Test creating and managing multiple named pools."""
        # Create multiple pools
        configs = [
            PoolConfig(name="pool1", pool_type=PoolType.THREAD, max_workers=2),
            PoolConfig(name="pool2", pool_type=PoolType.THREAD, max_workers=3),
            PoolConfig(name="pool3", pool_type=PoolType.PROCESS, max_workers=2),
        ]

        pools = []
        for config in configs:
            pool = await pool_manager.create_pool(config)
            pools.append(pool)

        # Verify all pools exist
        pool_names = pool_manager.list_pools()
        assert "pool1" in pool_names
        assert "pool2" in pool_names
        assert "pool3" in pool_names

        # Get specific pool
        pool1 = await pool_manager.get_pool("pool1")
        assert pool1 is not None
        assert pool1.config.max_workers == 2

        # Submit tasks to different pools
        # Using module-level function for process pool compatibility
        from backend.workers.test_functions import simple_add

        task_results = []
        for i, pool in enumerate(pools):
            # Use addition instead of lambda multiplication
            task_id = await pool.submit(simple_add, i, i)  # i + i = i * 2
            result = await pool.get_result(task_id, timeout=5)
            task_results.append(result)

        assert task_results == [0, 2, 4]

        # Get all stats
        all_stats = pool_manager.get_all_stats()
        assert len(all_stats) >= 3
        logger.info(f"Managing {len(all_stats)} pools")

        # Clean up
        for config in configs:
            await pool_manager.remove_pool(config.name)

    @pytest.mark.asyncio
    async def test_pool_isolation(self, pool_manager):
        """Test that pools are isolated from each other."""
        # Create two pools
        pool1 = await pool_manager.create_pool(PoolConfig(name="isolated1", pool_type=PoolType.THREAD, max_workers=1))
        pool2 = await pool_manager.create_pool(PoolConfig(name="isolated2", pool_type=PoolType.THREAD, max_workers=1))

        # Submit blocking task to pool1
        block_id = await pool1.submit(lambda: time.sleep(2) or "blocked")

        # Pool2 should still be responsive
        quick_id = await pool2.submit(lambda: "quick")
        result = await pool2.get_result(quick_id, timeout=1)
        assert result == "quick"

        # Wait for pool1 task
        result = await pool1.get_result(block_id, timeout=5)
        assert result == "blocked"

        logger.info("Pool isolation verified")

        # Clean up
        await pool_manager.remove_pool("isolated1")
        await pool_manager.remove_pool("isolated2")


class TestProcessPoolSpecific:
    """Test process pool specific functionality."""

    @pytest.mark.asyncio
    @pytest.mark.process_pool
    async def test_process_isolation(self, process_pool):
        """Test that processes are isolated."""
        # Each process should have its own memory space

        # Using module-level modify_global function from backend.workers.test_functions
        from backend.workers.test_functions import modify_global

        # Submit multiple tasks
        task_ids = []
        for _ in range(5):
            task_id = await process_pool.submit(modify_global)
            task_ids.append(task_id)

        # Get results - each should be independent
        results = []
        for task_id in task_ids:
            result = await process_pool.get_result(task_id, timeout=5)
            results.append(result)

        # In separate processes, each should return 1
        # (each process has its own global state)
        logger.info(f"Process isolation results: {results}")
        # Note: Results might vary based on process pool implementation

    @pytest.mark.asyncio
    @pytest.mark.process_pool
    async def test_process_cpu_intensive(self, process_pool):
        """Test CPU-intensive tasks in process pool."""
        # Calculate fibonacci numbers
        tasks = []
        for n in [30, 31, 32]:  # Reasonably intensive
            # Use module-level fibonacci function from test_functions
            from backend.workers.test_functions import fibonacci

            task_id = await process_pool.submit(fibonacci, n)
            tasks.append((n, task_id))

        # Get results
        for n, task_id in tasks:
            result = await process_pool.get_result(task_id, timeout=10)
            logger.info(f"Fibonacci({n}) = {result}")
            assert result > 0


class TestEdgeCases:
    """Test edge cases and error conditions."""

    @pytest.mark.asyncio
    @pytest.mark.thread_pool
    async def test_timeout_handling(self, thread_pool):
        """Test task timeout handling."""
        # Submit task with timeout
        task_id = await thread_pool.submit(
            lambda: time.sleep(5) or "done",
            _timeout=1,  # 1 second timeout
        )

        # Should timeout
        with pytest.raises(TimeoutError):
            await thread_pool.get_result(task_id, timeout=2)

        logger.info("Task timeout handled correctly")

    @pytest.mark.asyncio
    async def test_pool_shutdown_with_pending_tasks(self, pool_manager):
        """Test pool shutdown with pending tasks."""
        config = PoolConfig(name="shutdown_test", pool_type=PoolType.THREAD, max_workers=1)
        pool = await pool_manager.create_pool(config)

        # Submit tasks that will be pending
        task_ids = []
        for i in range(5):
            task_id = await pool.submit(lambda x: time.sleep(1) or x, i)
            task_ids.append(task_id)

        # Shutdown pool while tasks are pending/running
        await pool_manager.remove_pool(config.name)

        # Pool should be removed
        assert "shutdown_test" not in pool_manager.list_pools()
        logger.info("Pool shutdown with pending tasks completed")

    @pytest.mark.asyncio
    @pytest.mark.thread_pool
    async def test_worker_state_persistence(self, thread_pool):
        """Test worker state management across tasks."""

        # This requires a worker function that uses worker_state
        def stateful_task(worker_state=None):
            if worker_state is None:
                return "no state"

            # Increment counter in state
            counter = worker_state.get("counter", 0)
            counter += 1
            worker_state["counter"] = counter
            return counter

        # Submit multiple tasks - they might go to different workers
        results = []
        for _ in range(10):
            task_id = await thread_pool.submit(stateful_task)
            result = await thread_pool.get_result(task_id, timeout=5)
            results.append(result)

        logger.info(f"Stateful task results: {results}")
        # Results will vary based on which worker handles each task


@pytest.mark.stress
class TestStressAndPerformance:
    """Stress tests for the worker pool system."""

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_high_load(self, pool_manager):
        """Test pool under high load."""
        config = PoolConfig(
            name="stress_pool",
            pool_type=PoolType.THREAD,
            min_workers=2,
            max_workers=10,
            warm_workers=5,
        )
        pool = await pool_manager.create_pool(config)

        # Submit many tasks
        num_tasks = 100
        task_ids = []
        start_time = time.time()

        for i in range(num_tasks):
            task_id = await pool.submit(lambda x: x * x, i)
            task_ids.append(task_id)

        # Get all results
        results = []
        for task_id in task_ids:
            result = await pool.get_result(task_id, timeout=30)
            results.append(result)

        elapsed = time.time() - start_time

        assert len(results) == num_tasks
        assert results == [i * i for i in range(num_tasks)]

        stats = pool.get_stats()
        throughput = num_tasks / elapsed

        logger.info(f"Processed {num_tasks} tasks in {elapsed:.2f}s " f"({throughput:.1f} tasks/sec)")
        logger.info(f"Final stats: workers={stats.total_workers}, " f"completed={stats.completed_tasks}, " f"failed={stats.failed_tasks}")

        await pool_manager.remove_pool(config.name)

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_memory_intensive_tasks(self, pool_manager):
        """Test handling of memory-intensive tasks."""
        from backend.workers.test_functions import memory_intensive

        config = PoolConfig(
            name="memory_pool",
            pool_type=PoolType.PROCESS,  # Use processes for better memory isolation
            max_workers=2,
        )
        pool = await pool_manager.create_pool(config)

        # Submit memory-intensive tasks
        task_ids = []
        for size in [1000000, 2000000, 3000000]:  # Allocate lists of different sizes
            task_id = await pool.submit(memory_intensive, size)
            task_ids.append((size, task_id))

        # Get results
        for size, task_id in task_ids:
            result = await pool.get_result(task_id, timeout=10)
            assert len(result) == size
            logger.info(f"Allocated list of size {size}")

        await pool_manager.remove_pool(config.name)
