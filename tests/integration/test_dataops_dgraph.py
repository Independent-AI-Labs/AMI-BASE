"""
Integration tests for DataOps with real Dgraph instance.
Requires Dgraph running on localhost:9080 (gRPC) and 8080 (HTTP).
"""

import asyncio
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from backend.dataops.bpmn_model import Process, Task, TaskStatus  # noqa: E402
from backend.dataops.dao import get_dao  # noqa: E402
from backend.dataops.implementations.dgraph_dao import DgraphDAO  # noqa: E402
from backend.dataops.security_model import (  # noqa: E402
    ACLEntry,
    Permission,
    SecuredStorageModel,
    SecurityContext,
)
from backend.dataops.storage_model import StorageModel  # noqa: E402
from backend.dataops.storage_types import StorageConfig, StorageType  # noqa: E402
from backend.dataops.unified_crud import SyncStrategy, UnifiedCRUD  # noqa: E402


# Test Models
class SampleUser(SecuredStorageModel):
    """Test user model with security"""

    username: str
    email: str
    created_at: datetime = None

    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        if self.created_at is None:
            self.created_at = datetime.now(UTC)

    class Meta:
        storage_configs = {
            "dgraph": StorageConfig(
                storage_type=StorageType.GRAPH,
                host="172.72.72.2",  # Docker VM
                port=9080,
            )
        }
        path = "users"


class SampleDocument(StorageModel):
    """Test document model"""

    title: str
    content: str
    author_id: str
    tags: list[str] = []
    created_at: datetime = None

    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        if self.created_at is None:
            self.created_at = datetime.now(UTC)

    class Meta:
        storage_configs = {
            "dgraph": StorageConfig(
                storage_type=StorageType.GRAPH,
                host="172.72.72.2",
                port=9080,
            )
        }
        path = "documents"


@pytest.fixture
async def dgraph_dao():
    """Create Dgraph DAO instance"""
    config = StorageConfig(
        storage_type=StorageType.GRAPH,
        host="172.72.72.2",
        port=9080,
    )
    dao = DgraphDAO(SampleDocument, config)
    await dao.connect()
    yield dao
    await dao.disconnect()


@pytest.fixture
async def security_context():
    """Create security context for tests"""
    return SecurityContext(
        user_id="test-admin",
        roles=["admin", "user"],
        permissions=[Permission.READ, Permission.WRITE, Permission.DELETE],
    )


class TestDgraphConnection:
    """Test basic Dgraph connectivity"""

    @pytest.mark.asyncio
    async def test_connect_to_dgraph(self):
        """Test connection to Dgraph server"""
        config = StorageConfig(
            storage_type=StorageType.GRAPH,
            host="172.72.72.2",
            port=9080,
        )

        dao = get_dao(SampleDocument, config)
        assert isinstance(dao, DgraphDAO)

        await dao.connect()
        assert dao.client is not None

        await dao.disconnect()
        assert dao.client is None

    @pytest.mark.asyncio
    async def test_dgraph_health_check(self, dgraph_dao):
        """Test Dgraph health check"""
        # Simple query to verify connection
        result = await dgraph_dao.find({})
        assert isinstance(result, list)


class TestDgraphCRUD:
    """Test CRUD operations with Dgraph"""

    @pytest.mark.asyncio
    async def test_create_document(self, dgraph_dao):
        """Test creating a document in Dgraph"""
        doc = SampleDocument(
            id=str(uuid.uuid4()),
            title="Test Document",
            content="This is test content",
            author_id="user-123",
            tags=["test", "integration"],
        )

        created_id = await dgraph_dao.create(doc)
        assert created_id is not None

        # Read back to verify
        retrieved = await dgraph_dao.find_by_id(created_id)
        assert retrieved is not None
        assert retrieved.title == doc.title

        # Cleanup
        await dgraph_dao.delete(created_id)

    @pytest.mark.asyncio
    async def test_read_document(self, dgraph_dao):
        """Test reading a document from Dgraph"""
        # Create document
        doc = SampleDocument(
            id=str(uuid.uuid4()),
            title="Read Test",
            content="Content to read",
            author_id="user-456",
        )
        created_id = await dgraph_dao.create(doc)

        # Read it back
        retrieved = await dgraph_dao.find_by_id(created_id)
        assert retrieved is not None
        assert retrieved.id == doc.id
        assert retrieved.title == doc.title

        # Cleanup
        await dgraph_dao.delete(created_id)

    @pytest.mark.asyncio
    async def test_update_document(self, dgraph_dao):
        """Test updating a document in Dgraph"""
        # Create document
        doc = SampleDocument(
            id=str(uuid.uuid4()),
            title="Original Title",
            content="Original content",
            author_id="user-789",
        )
        created_id = await dgraph_dao.create(doc)

        # Update it
        update_data = {"title": "Updated Title", "tags": ["updated", "modified"]}
        success = await dgraph_dao.update(created_id, update_data)

        assert success is True

        # Verify update
        retrieved = await dgraph_dao.find_by_id(created_id)
        assert retrieved.title == "Updated Title"
        assert "updated" in retrieved.tags

        # Cleanup
        await dgraph_dao.delete(created_id)

    @pytest.mark.asyncio
    async def test_delete_document(self, dgraph_dao):
        """Test deleting a document from Dgraph"""
        # Create document
        doc = SampleDocument(
            id=str(uuid.uuid4()),
            title="Delete Test",
            content="To be deleted",
            author_id="user-999",
        )
        created_id = await dgraph_dao.create(doc)

        # Delete it
        result = await dgraph_dao.delete(created_id)
        assert result is True

        # Verify deletion
        retrieved = await dgraph_dao.find_by_id(created_id)
        assert retrieved is None

    @pytest.mark.asyncio
    async def test_find_documents(self, dgraph_dao):
        """Test finding documents with filters"""
        # Create multiple documents
        doc_ids = []
        for i in range(3):
            doc = SampleDocument(
                id=str(uuid.uuid4()),
                title=f"Find Test {i}",
                content=f"Content {i}",
                author_id="user-find",
                tags=["findable", f"tag-{i}"],
            )
            created_id = await dgraph_dao.create(doc)
            doc_ids.append(created_id)

        # Find by author
        results = await dgraph_dao.find({"author_id": "user-find"})
        assert len(results) >= 3

        # Find by tag
        results = await dgraph_dao.find({"tags": "findable"})
        assert len(results) >= 3

        # Cleanup
        for doc_id in doc_ids:
            await dgraph_dao.delete(doc_id)


class TestSecurityModel:
    """Test security features with Dgraph"""

    @pytest.mark.asyncio
    async def test_secured_create_with_acl(self, security_context):
        """Test creating secured model with ACL"""
        config = StorageConfig(
            storage_type=StorageType.GRAPH,
            host="172.72.72.2",
            port=9080,
        )

        dao = get_dao(SampleUser, config)
        await dao.connect()

        try:
            # Create user with security context
            user = SampleUser(
                id=str(uuid.uuid4()),
                username="testuser",
                email="test@example.com",
            )

            # Set security fields manually
            user.owner_id = security_context.user_id
            user.created_by = security_context.user_id
            user.modified_by = security_context.user_id

            # Add ACL entry
            owner_acl = ACLEntry(principal_id=security_context.user_id, principal_type="user", permissions=[Permission.ADMIN], granted_by="system")
            user.acl.append(owner_acl)

            # Create in database
            created_id = await dao.create(user)

            # Read back to verify
            created = await dao.find_by_id(created_id)
            assert created is not None
            assert created.owner_id == "test-admin"
            assert created.created_by == "test-admin"
            assert len(created.acl) > 0

            # Verify ACL entry
            admin_acl = next((a for a in created.acl if a.principal_id == "test-admin"), None)
            assert admin_acl is not None
            assert Permission.ADMIN in admin_acl.permissions

            # Cleanup
            await dao.delete(created_id)

        finally:
            await dao.disconnect()

    @pytest.mark.asyncio
    async def test_permission_check(self, security_context):
        """Test permission checking on secured models"""
        user = SampleUser(
            id=str(uuid.uuid4()),
            username="secured",
            email="secured@test.com",
        )

        # Apply security manually
        user.owner_id = security_context.user_id
        user.created_by = security_context.user_id
        user.acl.append(ACLEntry(principal_id=security_context.user_id, principal_type="user", permissions=[Permission.ADMIN], granted_by="system"))

        # Check permissions
        assert user.has_permission(security_context, Permission.READ)
        assert user.has_permission(security_context, Permission.WRITE)
        assert user.has_permission(security_context, Permission.DELETE)

        # Test with different context
        other_context = SecurityContext(
            user_id="other-user",
            roles=["user"],
            permissions=[Permission.READ],
        )

        # Should not have write permission
        assert not user.has_permission(other_context, Permission.WRITE)


class TestBPMNModel:
    """Test BPMN process model with Dgraph"""

    @pytest.mark.asyncio
    async def test_bpmn_process_creation(self):
        """Test creating BPMN process in Dgraph"""
        config = StorageConfig(
            storage_type=StorageType.GRAPH,
            host="172.72.72.2",
            port=9080,
        )

        dao = get_dao(Process, config)
        await dao.connect()

        try:
            # Create process with tasks
            process = Process(
                id=str(uuid.uuid4()),
                name="Test Process",
                description="Integration test process",
                tasks=[
                    Task(
                        id=str(uuid.uuid4()),
                        name="Task 1",
                        status=TaskStatus.PENDING,
                        assignee="user-1",
                    ),
                    Task(
                        id=str(uuid.uuid4()),
                        name="Task 2",
                        status=TaskStatus.PENDING,
                        assignee="user-2",
                        dependencies=["task-1"],
                    ),
                ],
            )

            created_id = await dao.create(process)

            # Read back to verify
            created = await dao.find_by_id(created_id)
            assert created is not None
            assert created.name == "Test Process"
            assert len(created.tasks) == 2

            # Cleanup
            await dao.delete(created_id)

        finally:
            await dao.disconnect()

    @pytest.mark.asyncio
    async def test_bpmn_task_update(self):
        """Test updating BPMN task status"""
        config = StorageConfig(
            storage_type=StorageType.GRAPH,
            host="172.72.72.2",
            port=9080,
        )

        dao = get_dao(Process, config)
        await dao.connect()

        try:
            # Create process
            task = Task(
                id="task-1",
                name="Update Test Task",
                status=TaskStatus.PENDING,
                assignee="user-1",
            )

            process = Process(
                id=str(uuid.uuid4()),
                name="Update Test Process",
                tasks=[task],
            )

            created_id = await dao.create(process)

            # Update task status
            update_data = {
                "tasks": [
                    {
                        "id": "task-1",
                        "name": "Update Test Task",
                        "status": TaskStatus.IN_PROGRESS.value,
                        "assignee": "user-1",
                        "started_at": datetime.now(UTC).isoformat(),
                    }
                ]
            }

            success = await dao.update(created_id, update_data)
            assert success is True

            # Verify update
            updated = await dao.find_by_id(created_id)
            assert updated.tasks[0].status == TaskStatus.IN_PROGRESS
            assert updated.tasks[0].started_at is not None

            # Cleanup
            await dao.delete(created_id)

        finally:
            await dao.disconnect()


class TestUnifiedCRUD:
    """Test UnifiedCRUD with Dgraph backend"""

    @pytest.mark.asyncio
    async def test_unified_crud_operations(self):
        """Test UnifiedCRUD with single Dgraph backend"""
        crud = UnifiedCRUD(SampleDocument, sync_strategy=SyncStrategy.PRIMARY_FIRST)

        # Create document
        doc = SampleDocument(
            id=str(uuid.uuid4()),
            title="Unified CRUD Test",
            content="Testing unified operations",
            author_id="unified-user",
        )

        created = await crud.create(doc)
        assert created.id == doc.id

        # Read
        retrieved = await crud.get(doc.id)
        assert retrieved is not None
        assert retrieved.title == doc.title

        # Update
        doc.title = "Updated via UnifiedCRUD"
        updated = await crud.update(doc)
        assert updated.title == "Updated via UnifiedCRUD"

        # Delete
        result = await crud.delete(doc.id)
        assert result is True

        # Verify deletion
        retrieved = await crud.get(doc.id)
        assert retrieved is None

    @pytest.mark.asyncio
    async def test_unified_crud_with_security(self, security_context):
        """Test UnifiedCRUD with security enabled"""
        crud = UnifiedCRUD(SampleUser, security_enabled=True)

        # Create secured user
        user = SampleUser(
            id=str(uuid.uuid4()),
            username="unified_secure",
            email="unified@secure.com",
        )

        created = await crud.create(user, context=security_context)
        assert created.owner_id == security_context.user_id

        # Update with permission check
        user.email = "updated@secure.com"
        updated = await crud.update(user, context=security_context)
        assert updated.email == "updated@secure.com"

        # Delete with permission
        result = await crud.delete(user.id, context=security_context)
        assert result is True


@pytest.mark.asyncio
async def test_dataops_end_to_end():
    """End-to-end test of DataOps functionality with Dgraph"""

    # Setup
    config = StorageConfig(
        storage_type=StorageType.GRAPH,
        host="172.72.72.2",
        port=9080,
    )

    # Test security context
    context = SecurityContext(
        user_id="e2e-test-user",
        roles=["admin"],
        permissions=[Permission.READ, Permission.WRITE, Permission.DELETE],
    )

    # Create DAO
    dao = get_dao(SampleUser, config)
    await dao.connect()

    try:
        # 1. Create user with security
        user = SampleUser(
            id=str(uuid.uuid4()),
            username="e2e_user",
            email="e2e@test.com",
        )
        # Apply security manually
        user.owner_id = context.user_id
        user.created_by = context.user_id
        user.acl.append(ACLEntry(principal_id=context.user_id, principal_type="user", permissions=[Permission.ADMIN], granted_by="system"))

        created_user_id = await dao.create(user)
        assert created_user_id is not None
        print(f"[OK] Created user: {user.username}")

        # 2. Create documents owned by user
        doc_dao = get_dao(SampleDocument, config)
        await doc_dao.connect()

        docs = []
        for i in range(3):
            doc = SampleDocument(
                id=str(uuid.uuid4()),
                title=f"E2E Doc {i}",
                content=f"Content for doc {i}",
                author_id=user.id,
                tags=["e2e", f"batch-{i//2}"],
            )
            created_doc_id = await doc_dao.create(doc)
            doc.id = created_doc_id  # Store the ID for cleanup
            docs.append(doc)
            print(f"[OK] Created document: {doc.title}")

        # 3. Query documents by author
        user_docs = await doc_dao.find({"author_id": user.id})
        assert len(user_docs) >= 3
        print(f"[OK] Found {len(user_docs)} documents by user")

        # 4. Update a document
        update_data = {"title": "E2E Updated Doc"}
        success = await doc_dao.update(docs[0].id, update_data)
        assert success is True

        # Verify update
        updated_doc = await doc_dao.find_by_id(docs[0].id)
        assert updated_doc.title == "E2E Updated Doc"
        print(f"[OK] Updated document: {updated_doc.title}")

        # 5. Create BPMN process for document workflow
        process_dao = get_dao(Process, config)
        await process_dao.connect()

        workflow = Process(
            id=str(uuid.uuid4()),
            name="Document Review Workflow",
            description=f"Review workflow for {docs[0].title}",
            tasks=[
                Task(
                    id="review-1",
                    name="Initial Review",
                    status=TaskStatus.PENDING,
                    assignee=user.id,
                ),
                Task(
                    id="approve-1",
                    name="Approval",
                    status=TaskStatus.PENDING,
                    assignee="manager-1",
                    dependencies=["review-1"],
                ),
            ],
        )

        created_workflow_id = await process_dao.create(workflow)
        print(f"[OK] Created workflow: {workflow.name}")

        # 6. Cleanup
        for doc in docs:
            await doc_dao.delete(doc.id)
        await process_dao.delete(created_workflow_id)
        await doc_dao.disconnect()
        await process_dao.disconnect()
        await dao.delete(created_user_id)

        print("\n[SUCCESS] End-to-end test completed successfully!")

    finally:
        await dao.disconnect()


if __name__ == "__main__":
    # Run the end-to-end test
    asyncio.run(test_dataops_end_to_end())
