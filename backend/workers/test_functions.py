"""Test functions for process pool testing."""

# Global state for testing process isolation
test_value = 0


def modify_global():
    """Modify global state - for process isolation testing."""
    global test_value  # noqa: PLW0603
    test_value += 1
    return test_value


def simple_add(a: int, b: int) -> int:
    """Simple addition function."""
    return a + b


def fibonacci(n: int) -> int:
    """Calculate fibonacci number."""
    if n <= 1:
        return n
    return fibonacci(n - 1) + fibonacci(n - 2)


def memory_intensive(size: int) -> list:
    """Create a large list for memory testing."""
    return list(range(size))
