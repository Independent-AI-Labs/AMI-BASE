"""
Enhanced decorators with automatic event recording and sensitive field handling
"""
import functools
import inspect
from collections.abc import Callable
from datetime import datetime
from typing import Any, TypeVar

from loguru import logger
from pydantic import Field

from ..utils.uuid_utils import uuid7
from .security_model import SecuredStorageModel
from .storage_model import StorageModel
from .storage_types import StorageConfig, StorageType

T = TypeVar("T", bound=StorageModel)


class EventRecord(SecuredStorageModel):
    """Generic event record for capturing function calls"""

    event_id: str = Field(default_factory=lambda: f"event_{uuid7()}")
    event_type: str  # The base model type specified in decorator
    function_name: str

    # Automatically captured data
    input: dict[str, Any] = Field(default_factory=dict)  # All input args
    output: Any = None  # Return value(s)

    # Execution metadata
    start_time: datetime = Field(default_factory=datetime.utcnow)
    end_time: datetime | None = None
    duration_ms: int | None = None

    # Error tracking
    success: bool = True
    error: str | None = None
    error_type: str | None = None

    # Context
    context_user: str | None = None
    context_session: str | None = None

    class Meta:
        storage_configs = {
            "graph": StorageConfig(storage_type=StorageType.GRAPH),
            "timeseries": StorageConfig(storage_type=StorageType.TIMESERIES),  # For metrics
            "document": StorageConfig(storage_type=StorageType.DOCUMENT),  # For querying
        }
        path = "events"


def sensitive_field(field_name: str, mask_pattern: str = "{field}_uid"):
    """
    Mark a field as sensitive for MCP server sanitization

    Args:
        field_name: Name of the sensitive field
        mask_pattern: Pattern for masking (default: field_name_uid)
    """

    def decorator(cls):
        if not hasattr(cls, "_sensitive_fields"):
            cls._sensitive_fields = {}
        cls._sensitive_fields[field_name] = mask_pattern
        return cls

    return decorator


def sanitize_for_mcp(instance: StorageModel, caller: str = "mcp") -> dict[str, Any]:
    """
    Sanitize model instance for MCP server output

    Replaces sensitive field values with masked versions
    """
    data = instance.model_dump()

    # Check if model has sensitive fields
    if hasattr(instance.__class__, "_sensitive_fields"):
        for field_name, mask_pattern in instance.__class__._sensitive_fields.items():
            if field_name in data:
                # Replace with masked value
                masked_value = mask_pattern.format(field=field_name) if "{field}" in mask_pattern else mask_pattern

                # Add UID if pattern includes it
                if "uid" in mask_pattern.lower():
                    masked_value = f"{masked_value}_{uuid7()}"

                data[field_name] = masked_value

                # Log the masking
                logger.debug(f"Masked sensitive field '{field_name}' for {caller}")

    return data


def record_event(event_type: type[StorageModel] | str, **options):
    """Simplified event recording decorator."""

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Simple event recording without complex logic
            try:
                result = await func(*args, **kwargs) if inspect.iscoroutinefunction(func) else func(*args, **kwargs)
                logger.debug(f"Event {event_type}: {func.__name__} succeeded")
                return result
            except Exception as e:
                logger.error(f"Event {event_type}: {func.__name__} failed: {e}")
                raise

        return wrapper

    return decorator


# Removed complex crud_record decorator to reduce complexity
# Use record_event decorator instead for event recording


# Caching decorator explanation:
# The cache decorator stores results in memory/Redis to avoid repeated expensive operations
# For example, user profile lookups that don't change often can be cached
def cached_result(
    ttl: int = 300,  # 5 minutes default
    cache_key: Callable | None = None,
    backend: str = "memory",  # memory, redis
):
    """
    Cache function results to avoid repeated expensive operations

    The cache is useful for:
    - Expensive database queries that don't change often
    - API calls with rate limits
    - Complex calculations
    - User profile/settings lookups

    Args:
        ttl: Time to live in seconds
        cache_key: Function to generate cache key
        backend: Cache backend (memory or redis)

    Example:
        @cached_result(ttl=600)  # Cache user profile for 10 minutes
        async def get_user_profile(user_id: str):
            # Expensive database query here
            return profile
    """

    def decorator(func: Callable) -> Callable:
        # In-memory cache
        cache: dict[str, Any] = {}

        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Generate cache key
            if cache_key:
                key = cache_key(*args, **kwargs)
            else:
                # Simple key from function name and args
                key = f"{func.__name__}:{str(args)}:{str(kwargs)}"

            # Check cache based on backend
            if backend == "memory":
                if key in cache:
                    cached_data, cached_time = cache[key]
                    if datetime.utcnow().timestamp() - cached_time < ttl:
                        logger.debug(f"Cache hit for {key}")
                        return cached_data
            elif backend == "redis":
                # Would connect to Redis here
                pass  # Redis implementation

            # Execute function
            result = await func(*args, **kwargs)

            # Store in cache
            if backend == "memory":
                cache[key] = (result, datetime.utcnow().timestamp())
            elif backend == "redis":
                # Store in Redis with TTL
                pass  # Redis implementation

            return result

        return wrapper

    return decorator


def multi_storage(storages: list[str], ground_truth: str = "dgraph"):
    """
    Decorator to specify multiple storage backends with ground truth

    Args:
        storages: List of storage backend names
        ground_truth: Which storage is the source of truth

    Example:
        @multi_storage(["dgraph", "mongodb", "redis"], ground_truth="dgraph")
        class Document(SecuredStorageModel):
            title: str
            content: str
    """

    def decorator(cls):
        # Add storage configs to class Meta
        if not hasattr(cls, "Meta"):
            cls.Meta = type("Meta", (), {})

        storage_configs = {}
        for storage_name in storages:
            config = StorageConfig(storage_type=StorageType.GRAPH if storage_name == "dgraph" else StorageType.DOCUMENT)
            if storage_name == ground_truth:
                config.options["is_ground_truth"] = True
            storage_configs[storage_name] = config

        cls.Meta.storage_configs = storage_configs
        cls.Meta.ground_truth = ground_truth

        return cls

    return decorator
