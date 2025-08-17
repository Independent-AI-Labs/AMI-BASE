"""
Tests for the DataOps security model and unified CRUD
"""
from datetime import datetime, timedelta

import pytest

from backend.dataops.security_model import (
    ACLEntry,
    Permission,
    SecuredStorageModel,
    SecurityContext,
)
from backend.dataops.storage_types import StorageConfig, StorageType
from backend.dataops.unified_crud import SyncStrategy, UnifiedCRUD, get_crud
from backend.utils.uuid_utils import is_uuid7, uuid7


class TestModel(SecuredStorageModel):
    """Test model for security tests"""

    name: str
    value: int = 0

    class Meta:
        storage_configs = {
            "graph": StorageConfig(storage_type=StorageType.GRAPH),
            "document": StorageConfig(storage_type=StorageType.DOCUMENT),
        }
        path = "test_models"


class TestSecurityModel:
    """Test security features"""

    @pytest.fixture
    def security_context(self):
        """Create a test security context"""
        return SecurityContext(user_id="user_123", roles=["member"], groups=["developers"], session_id="session_456")

    @pytest.fixture
    def admin_context(self):
        """Create an admin security context"""
        return SecurityContext(user_id="admin_user", roles=["admin"], groups=["administrators"], session_id="admin_session")

    def test_security_context_creation(self, security_context):
        """Test security context creation"""
        assert security_context.user_id == "user_123"
        assert "member" in security_context.roles
        assert "developers" in security_context.groups
        assert security_context.session_id == "session_456"

        # Test principal_ids property
        principal_ids = security_context.principal_ids
        assert "user_123" in principal_ids
        assert "member" in principal_ids
        assert "developers" in principal_ids

    def test_acl_entry_creation(self):
        """Test ACL entry creation"""
        acl = ACLEntry(principal_id="user_123", principal_type="user", permissions=[Permission.READ, Permission.WRITE], granted_by="admin")

        assert acl.principal_id == "user_123"
        assert Permission.READ in acl.permissions
        assert Permission.WRITE in acl.permissions
        assert acl.granted_by == "admin"

        # Test has_permission
        assert acl.has_permission(Permission.READ)
        assert acl.has_permission(Permission.WRITE)
        assert not acl.has_permission(Permission.DELETE)

    def test_acl_entry_with_admin(self):
        """Test that ADMIN permission grants all"""
        acl = ACLEntry(principal_id="admin_user", principal_type="user", permissions=[Permission.ADMIN], granted_by="system")

        # Admin should have all permissions
        assert acl.has_permission(Permission.READ)
        assert acl.has_permission(Permission.WRITE)
        assert acl.has_permission(Permission.DELETE)
        assert acl.has_permission(Permission.ADMIN)

    def test_acl_entry_expiration(self):
        """Test ACL entry expiration"""
        # Create expired ACL
        expired_acl = ACLEntry(principal_id="user_123", principal_type="user", permissions=[Permission.READ], expires_at=datetime.utcnow() - timedelta(hours=1))

        # Create valid ACL
        valid_acl = ACLEntry(principal_id="user_123", principal_type="user", permissions=[Permission.READ], expires_at=datetime.utcnow() + timedelta(hours=1))

        assert expired_acl.expires_at < datetime.utcnow()
        assert valid_acl.expires_at > datetime.utcnow()

    @pytest.mark.asyncio
    async def test_secured_model_creation(self, security_context):
        """Test secured model creation with context"""
        # Note: This would need actual DAO implementations to work
        # For now, test the model structure

        model = TestModel(name="test", value=42, owner_id=security_context.user_id, created_by=security_context.user_id)

        assert model.name == "test"
        assert model.value == 42
        assert model.owner_id == "user_123"
        assert model.created_by == "user_123"
        assert model.acl == []

    @pytest.mark.asyncio
    async def test_check_permission_owner(self, security_context):
        """Test that owner has all permissions"""
        model = TestModel(name="test", owner_id=security_context.user_id)

        # Owner should have all permissions
        assert await model.check_permission(security_context, Permission.READ)
        assert await model.check_permission(security_context, Permission.WRITE)
        assert await model.check_permission(security_context, Permission.DELETE)
        assert await model.check_permission(security_context, Permission.ADMIN)

    @pytest.mark.asyncio
    async def test_check_permission_with_acl(self, security_context):
        """Test permission checking with ACL"""
        model = TestModel(
            name="test",
            owner_id="other_user",
            acl=[ACLEntry(principal_id="user_123", principal_type="user", permissions=[Permission.READ], granted_by="owner")],
        )

        # Should have READ permission
        assert await model.check_permission(security_context, Permission.READ)

        # Should NOT have WRITE permission
        assert not await model.check_permission(security_context, Permission.WRITE)

    @pytest.mark.asyncio
    async def test_check_permission_with_role(self, security_context):
        """Test permission checking with role-based ACL"""
        model = TestModel(
            name="test",
            owner_id="other_user",
            acl=[ACLEntry(principal_id="member", principal_type="role", permissions=[Permission.READ, Permission.WRITE], granted_by="owner")],
        )

        # Should have permissions via role
        assert await model.check_permission(security_context, Permission.READ)
        assert await model.check_permission(security_context, Permission.WRITE)
        assert not await model.check_permission(security_context, Permission.DELETE)

    @pytest.mark.asyncio
    async def test_check_permission_with_group(self, security_context):
        """Test permission checking with group-based ACL"""
        model = TestModel(
            name="test",
            owner_id="other_user",
            acl=[ACLEntry(principal_id="developers", principal_type="group", permissions=[Permission.READ], granted_by="owner")],
        )

        # Should have permissions via group
        assert await model.check_permission(security_context, Permission.READ)
        assert not await model.check_permission(security_context, Permission.WRITE)


class TestUnifiedCRUD:
    """Test unified CRUD operations"""

    @pytest.fixture
    def crud(self):
        """Create CRUD instance"""
        return UnifiedCRUD(model_cls=TestModel, sync_strategy=SyncStrategy.PRIMARY_FIRST, security_enabled=True)

    def test_crud_initialization(self, crud):
        """Test CRUD initialization"""
        assert crud.model_cls == TestModel
        assert crud.sync_strategy == SyncStrategy.PRIMARY_FIRST
        assert crud.security_enabled is True
        assert crud._operations_log == []

    def test_sync_strategy_enum(self):
        """Test sync strategy enum values"""
        assert SyncStrategy.SEQUENTIAL.value == "sequential"
        assert SyncStrategy.PARALLEL.value == "parallel"
        assert SyncStrategy.PRIMARY_FIRST.value == "primary_first"
        assert SyncStrategy.EVENTUAL.value == "eventual"

    def test_get_crud_singleton(self):
        """Test that get_crud returns singleton"""
        crud1 = get_crud(TestModel)
        crud2 = get_crud(TestModel)

        # Should be the same instance
        assert crud1 is crud2

    def test_operations_log(self, crud):
        """Test operations log management"""
        from backend.dataops.unified_crud import StorageOperation

        # Add operation to log
        op = StorageOperation(storage_name="graph", operation="create", data={"name": "test"}, status="success")
        crud._operations_log.append(op)

        # Check log
        log = crud.get_operations_log()
        assert len(log) == 1
        assert log[0].storage_name == "graph"
        assert log[0].operation == "create"
        assert log[0].status == "success"

        # Clear log
        crud.clear_operations_log()
        assert len(crud.get_operations_log()) == 0


class TestUUIDv7:
    """Test UUID v7 utilities"""

    def test_uuid7_generation(self):
        """Test UUID v7 generation"""
        uuid = uuid7()

        # Should be a valid UUID string
        assert isinstance(uuid, str)
        assert len(uuid) == 36  # Standard UUID format with hyphens
        assert uuid.count("-") == 4

    def test_uuid7_is_valid(self):
        """Test UUID v7 validation"""
        uuid = uuid7()
        assert is_uuid7(uuid)

        # Test with prefix
        prefixed = f"user_{uuid}"
        assert is_uuid7(prefixed)

        # Test invalid
        assert not is_uuid7("not-a-uuid")
        assert not is_uuid7("123e4567-e89b-12d3-a456-426614174000")  # UUID v1

    def test_uuid7_time_ordering(self):
        """Test that UUID v7 is time-ordered"""
        uuid1 = uuid7()
        # Small delay to ensure different timestamp
        import time

        time.sleep(0.001)
        uuid2 = uuid7()

        # Later UUID should be "greater" when compared as strings
        assert uuid2 > uuid1

    def test_uuid7_extract_timestamp(self):
        """Test timestamp extraction from UUID v7"""
        from backend.utils.uuid_utils import extract_timestamp_from_uuid7

        uuid = uuid7()
        timestamp = extract_timestamp_from_uuid7(uuid)

        # Should be close to current time (within 1 second)
        import time

        current_ms = int(time.time() * 1000)
        assert abs(timestamp - current_ms) < 1000

    def test_uuid7_with_prefix(self):
        """Test UUID v7 with prefix"""
        from backend.utils.uuid_utils import uuid7_prefix

        prefixed = uuid7_prefix("user")
        assert prefixed.startswith("user_")
        assert is_uuid7(prefixed)


class TestStorageModel:
    """Test base storage model"""

    def test_model_id_generation(self):
        """Test that model IDs use UUID v7"""
        model = TestModel(name="test")

        # ID should be auto-generated
        assert model.id is not None
        assert is_uuid7(model.id)

    def test_model_timestamps(self):
        """Test model timestamp fields"""
        model = TestModel(name="test")

        assert model.created_at is not None
        assert model.updated_at is not None
        assert isinstance(model.created_at, datetime)
        assert isinstance(model.updated_at, datetime)

    def test_model_metadata(self):
        """Test model metadata"""
        metadata = TestModel.get_metadata()

        assert metadata.path == "test_models"
        assert "graph" in metadata.storage_configs
        assert "document" in metadata.storage_configs

        # Check storage types
        graph_config = metadata.storage_configs["graph"]
        assert graph_config.storage_type == StorageType.GRAPH

        doc_config = metadata.storage_configs["document"]
        assert doc_config.storage_type == StorageType.DOCUMENT

    def test_model_collection_name(self):
        """Test model collection name"""
        assert TestModel.get_collection_name() == "test_models"

    def test_model_storage_configs(self):
        """Test model storage configurations"""
        configs = TestModel.get_storage_configs()

        assert len(configs) == 2
        assert "graph" in configs
        assert "document" in configs

    def test_model_primary_storage(self):
        """Test getting primary storage config"""
        primary = TestModel.get_primary_storage_config()

        # Should return first config (graph)
        assert primary.storage_type == StorageType.GRAPH
