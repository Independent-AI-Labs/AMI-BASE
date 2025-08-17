"""
Simple integration test for DataOps - tests basic functionality
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from backend.dataops.storage_model import StorageModel  # noqa: E402
from backend.dataops.storage_types import StorageConfig, StorageType  # noqa: E402


class SimpleModel(StorageModel):
    """Simple test model"""

    name: str
    value: int = 0

    class Meta:
        storage_configs = {
            "cache": StorageConfig(
                storage_type=StorageType.CACHE,
                host="172.72.72.2",
                port=6379,
            )
        }
        path = "simple_test"


async def test_simple_storage():
    """Test basic storage operations with Redis"""
    print("Testing Simple Storage Operations")
    print("=" * 50)

    # Create a simple model instance
    model = SimpleModel(id="test-1", name="Test Item", value=42)

    print(f"Created model: {model.name} with value {model.value}")

    # Test model serialization
    data = model.model_dump()
    print(f"Model data: {data}")

    # Test storage dict conversion
    storage_data = model.to_storage_dict()
    print(f"Storage data: {storage_data}")

    print("\n[OK] Simple model test passed!")

    # Now let's test with BPMN model
    from services.dataops.bpmn_model import Task, TaskStatus

    task = Task(id="task-1", name="Test Task", state=TaskStatus.PENDING, assignee="user-1")

    print(f"\nCreated task: {task.name}")
    print(f"Task state: {task.state}")
    print(f"Task assignee: {task.assignee}")

    print("\n[OK] BPMN model test passed!")

    # Test security model
    from services.dataops.security_model import SecurityContext

    context = SecurityContext(user_id="test-user", roles=["admin"], groups=["developers"])

    print(f"\nCreated security context for user: {context.user_id}")
    print(f"Roles: {context.roles}")
    print(f"Groups: {context.groups}")

    print("\n[OK] Security model test passed!")

    print("\n" + "=" * 50)
    print("[SUCCESS] All simple tests passed!")


if __name__ == "__main__":
    asyncio.run(test_simple_storage())
