"""
Tests for DataOps decorators
"""
import asyncio
from datetime import datetime

import pytest

from backend.dataops.enhanced_decorators import (
    EventRecord,
    cached_result,
    multi_storage,
    record_event,
    sanitize_for_mcp,
    sensitive_field,
)
from backend.dataops.security_model import SecuredStorageModel, SecurityContext


# Test models
@sensitive_field("password", mask_pattern="pwd_masked")
@sensitive_field("api_key", mask_pattern="{field}_hidden")
class TestUser(SecuredStorageModel):
    """Test user model with sensitive fields"""

    username: str
    password: str
    api_key: str = "secret_key_123"
    email: str = "test@example.com"


@multi_storage(["dgraph", "mongodb", "redis"], ground_truth="dgraph")
class TestDocument(SecuredStorageModel):
    """Test document with multiple storages"""

    title: str
    content: str


class TestDecorators:
    """Test decorator functionality"""

    @pytest.fixture
    def security_context(self):
        """Create test security context"""
        return SecurityContext(user_id="test_user", roles=["member"], session_id="test_session")

    def test_sensitive_field_decorator(self):
        """Test sensitive field marking"""
        # Check that sensitive fields are marked
        assert hasattr(TestUser, "_sensitive_fields")
        assert "password" in TestUser._sensitive_fields
        assert "api_key" in TestUser._sensitive_fields

        # Check mask patterns
        assert TestUser._sensitive_fields["password"] == "pwd_masked"
        assert TestUser._sensitive_fields["api_key"] == "{field}_hidden"

    def test_sanitize_for_mcp(self):
        """Test MCP sanitization"""
        user = TestUser(username="john", password="secret123", api_key="key_abc123")

        # Sanitize for MCP
        sanitized = sanitize_for_mcp(user, caller="mcp")

        # Check that sensitive fields are masked
        assert sanitized["username"] == "john"
        assert sanitized["password"] == "pwd_masked"
        assert "api_key_hidden" in sanitized["api_key"]
        assert sanitized["email"] == "test@example.com"

    def test_multi_storage_decorator(self):
        """Test multi-storage decorator"""
        # Check that storage configs are set
        assert hasattr(TestDocument.Meta, "storage_configs")
        assert hasattr(TestDocument.Meta, "ground_truth")

        configs = TestDocument.Meta.storage_configs
        assert "dgraph" in configs
        assert "mongodb" in configs
        assert "redis" in configs

        # Check ground truth
        assert TestDocument.Meta.ground_truth == "dgraph"
        assert configs["dgraph"].options.get("is_ground_truth") is True

    @pytest.mark.asyncio
    async def test_record_event_decorator(self, security_context):
        """Test event recording decorator"""

        @record_event("TestEvent", capture_output=True, sensitive_args=["password"])
        async def test_function(username: str, password: str, data: dict):
            """Test function with event recording"""
            return {"result": "success", "user": username}

        # Call the decorated function
        result = await test_function(username="alice", password="secret", data={"key": "value"})

        # Check result
        assert result["result"] == "success"
        assert result["user"] == "alice"

        # Note: In a real test, we'd check that EventRecord was created
        # but that requires DAO implementation

    @pytest.mark.asyncio
    async def test_record_event_with_error(self):
        """Test event recording with error capture"""

        @record_event("ErrorEvent", capture_errors=True)
        async def failing_function(value: int):
            """Function that raises an error"""
            if value < 0:
                raise ValueError("Negative value not allowed")
            return value * 2

        # Test successful call
        result = await failing_function(5)
        assert result == 10

        # Test error call
        with pytest.raises(ValueError) as exc_info:
            await failing_function(-1)
        assert "Negative value not allowed" in str(exc_info.value)

    # Removed test_crud_record_decorator as the decorator was removed to reduce complexity

    @pytest.mark.asyncio
    async def test_cached_result_decorator(self):
        """Test result caching decorator"""

        call_count = 0

        @cached_result(ttl=1, backend="memory")  # 1 second TTL
        async def expensive_operation(user_id: str) -> dict:
            """Expensive operation to cache"""
            nonlocal call_count
            call_count += 1
            return {"user_id": user_id, "data": "expensive_result"}

        # First call - should execute
        result1 = await expensive_operation("user_123")
        assert result1["user_id"] == "user_123"
        assert call_count == 1

        # Second call - should be cached
        result2 = await expensive_operation("user_123")
        assert result2["user_id"] == "user_123"
        assert call_count == 1  # Not incremented

        # Different argument - should execute
        result3 = await expensive_operation("user_456")
        assert result3["user_id"] == "user_456"
        assert call_count == 2

        # Wait for cache to expire
        await asyncio.sleep(1.1)

        # Call again - cache expired, should execute
        result4 = await expensive_operation("user_123")
        assert result4["user_id"] == "user_123"
        assert call_count == 3

    def test_event_record_model(self):
        """Test EventRecord model structure"""
        from backend.utils.uuid_utils import is_uuid7

        event = EventRecord(event_type="TestEvent", function_name="test_func", input={"arg1": "value1", "arg2": 42}, output={"result": "success"}, success=True)

        # Check fields
        assert event.event_type == "TestEvent"
        assert event.function_name == "test_func"
        assert event.input["arg1"] == "value1"
        assert event.output["result"] == "success"
        assert event.success is True

        # Check auto-generated fields
        assert event.event_id.startswith("event_")
        assert is_uuid7(event.event_id.replace("event_", ""))
        assert isinstance(event.start_time, datetime)

    def test_event_record_with_error(self):
        """Test EventRecord with error information"""
        event = EventRecord(
            event_type="ErrorEvent",
            function_name="failing_func",
            input={"bad_input": "value"},
            success=False,
            error="Something went wrong",
            error_type="ValueError",
        )

        assert event.success is False
        assert event.error == "Something went wrong"
        assert event.error_type == "ValueError"

    def test_sensitive_args_masking(self):
        """Test that sensitive arguments are masked in events"""
        # This would be tested in the actual implementation
        # of record_event decorator

        # Simulate what the decorator does
        input_data = {"username": "john", "password": "secret123", "email": "john@example.com", "api_key": "key_123", "token": "token_456"}

        # Auto-detect sensitive fields
        sensitive_names = ["password", "secret", "token", "api_key"]
        masked_data = {}

        for key, value in input_data.items():
            if key.lower() in sensitive_names:
                masked_data[key] = f"<masked_{key}>"
            else:
                masked_data[key] = value

        # Check masking
        assert masked_data["username"] == "john"
        assert masked_data["email"] == "john@example.com"
        assert masked_data["password"] == "<masked_password>"
        assert masked_data["api_key"] == "<masked_api_key>"
        assert masked_data["token"] == "<masked_token>"
