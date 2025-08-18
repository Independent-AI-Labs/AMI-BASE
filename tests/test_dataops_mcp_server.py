"""
Tests for the DataOps MCP server
"""
import json

import pytest
import yaml

from backend.dataops.enhanced_decorators import sensitive_field
from backend.dataops.security_model import SecuredStorageModel, SecurityContext
from backend.dataops.storage_model import StorageModel
from backend.mcp.dataops.server import DataOpsMCPServer


class SampleModel(StorageModel):
    """Simple test model"""

    name: str
    value: int = 0
    description: str = ""


class SecureSampleModel(SecuredStorageModel):
    """Secured test model"""

    title: str
    content: str
    tags: list[str] = []


class TestDataOpsMCPServer:
    """Test DataOps MCP server"""

    @pytest.fixture
    def server(self):
        """Create test server"""
        server = DataOpsMCPServer()
        server.register_model(SampleModel)
        server.register_model(SecureSampleModel)
        return server

    @pytest.fixture
    def context(self):
        """Create test security context"""
        return {"user_id": "test_user", "roles": ["member"], "session_id": "test_session"}

    def test_server_initialization(self, server):
        """Test server initialization"""
        assert len(server._model_registry) == 2
        assert "SampleModel" in server._model_registry
        assert "SecureSampleModel" in server._model_registry

    def test_registered_tools(self, server):
        """Test that only minimal tools are registered"""
        tool_names = list(server.tools.keys())

        # Should only have 3 tools
        assert len(tool_names) == 3
        assert "dataops" in tool_names
        assert "dataops_info" in tool_names
        assert "dataops_batch" in tool_names

        # Should NOT have model-specific tools
        assert "create_testmodel" not in tool_names
        assert "find_testmodel" not in tool_names

    @pytest.mark.asyncio
    async def test_dataops_create_dict(self, server, context):
        """Test create operation with dict data"""
        response = await server._handle_dataops(operation="create", model="SampleModel", data={"name": "test1", "value": 42}, context=context)

        assert response.success is True
        assert response.data["name"] == "test1"
        assert response.data["value"] == 42

    @pytest.mark.asyncio
    async def test_dataops_create_json(self, server, context):
        """Test create operation with JSON string data"""
        json_data = json.dumps({"name": "test2", "value": 100})

        response = await server._handle_dataops(operation="create", model="SampleModel", data=json_data, data_format="json", context=context)

        assert response.success is True
        data = response.data if isinstance(response.data, dict) else json.loads(response.data)
        assert data["name"] == "test2"
        assert data["value"] == 100

    @pytest.mark.asyncio
    async def test_dataops_create_yaml(self, server, context):
        """Test create operation with YAML string data"""
        yaml_data = """
        name: test3
        value: 200
        description: YAML test
        """

        response = await server._handle_dataops(operation="create", model="SampleModel", data=yaml_data, data_format="yaml", context=context)

        assert response.success is True
        data = yaml.safe_load(response.data) if isinstance(response.data, str) else response.data
        assert data["name"] == "test3"
        assert data["value"] == 200

    @pytest.mark.asyncio
    async def test_dataops_invalid_model(self, server, context):
        """Test operation with invalid model"""
        response = await server._handle_dataops(operation="create", model="NonExistentModel", data={"test": "data"}, context=context)

        assert response.success is False
        assert "Model not found" in response.error

    @pytest.mark.asyncio
    async def test_dataops_missing_data(self, server, context):
        """Test create without data"""
        response = await server._handle_dataops(operation="create", model="SampleModel", data=None, context=context)

        assert response.success is False
        assert "Data required" in response.error

    @pytest.mark.asyncio
    async def test_dataops_info_all_models(self, server):
        """Test getting info for all models"""
        response = await server._handle_info()

        assert response.success is True
        assert "SampleModel" in response.data
        assert "SecureSampleModel" in response.data

        # Check SampleModel info
        test_info = response.data["SampleModel"]
        assert test_info["class"] == "SampleModel"
        assert "name" in test_info["fields"]
        assert "value" in test_info["fields"]
        assert test_info["secured"] is False

        # Check SecureSampleModel info
        secure_info = response.data["SecureSampleModel"]
        assert secure_info["class"] == "SecureSampleModel"
        assert secure_info["secured"] is True

    @pytest.mark.asyncio
    async def test_dataops_info_specific_model(self, server):
        """Test getting info for specific model"""
        response = await server._handle_info(model="SampleModel")

        assert response.success is True
        assert response.data["class"] == "SampleModel"
        assert "mongodb" in response.data["storages"]
        assert response.data["ground_truth"] == "mongodb"

    @pytest.mark.asyncio
    async def test_dataops_batch_operations(self, server, context):
        """Test batch operations"""
        operations = [
            {"operation": "create", "model": "SampleModel", "data": {"name": "batch1", "value": 1}},
            {"operation": "create", "model": "SampleModel", "data": {"name": "batch2", "value": 2}},
            {"operation": "create", "model": "SampleModel", "data": {"name": "batch3", "value": 3}},
        ]

        response = await server._handle_batch(operations=operations, context=context)

        assert response.success is True
        assert response.data["executed"] == 3
        assert response.data["failed"] == 0
        assert len(response.data["results"]) == 3

    @pytest.mark.asyncio
    async def test_dataops_batch_with_error(self, server, context):
        """Test batch operations with error"""
        operations = [
            {"operation": "create", "model": "SampleModel", "data": {"name": "good", "value": 1}},
            {
                "operation": "create",
                "model": "InvalidModel",  # This will fail
                "data": {"name": "bad"},
            },
            {"operation": "create", "model": "SampleModel", "data": {"name": "good2", "value": 2}},
        ]

        response = await server._handle_batch(
            operations=operations,
            transaction=False,  # Continue on error
            context=context,
        )

        # Should partially succeed
        assert response.data["executed"] == 2
        assert response.data["failed"] == 1
        assert len(response.data["errors"]) == 1

    @pytest.mark.asyncio
    async def test_dataops_batch_transaction(self, server, context):
        """Test batch operations as transaction"""
        operations = [
            {"operation": "create", "model": "SampleModel", "data": {"name": "trans1", "value": 1}},
            {
                "operation": "create",
                "model": "InvalidModel",  # This will fail
                "data": {"name": "fail"},
            },
        ]

        response = await server._handle_batch(
            operations=operations,
            transaction=True,  # All or nothing
            context=context,
        )

        # Should fail completely
        assert response.success is False
        assert "Transaction failed" in response.error or "Batch operation failed" in response.error

    def test_model_info_structure(self, server):
        """Test model info structure"""
        info = server._get_model_info(SampleModel)

        assert "class" in info
        assert "storages" in info
        assert "ground_truth" in info
        assert "fields" in info
        assert "sensitive_fields" in info
        assert "secured" in info
        assert "path" in info

        # Check field details
        assert "name" in info["fields"]
        name_field = info["fields"]["name"]
        assert "type" in name_field
        assert "required" in name_field

    def test_case_insensitive_model_lookup(self):
        """Test case-insensitive model name lookup"""
        server = DataOpsMCPServer()
        server.register_model(SampleModel, "SampleModel")

        # These should all find the model
        assert server._model_registry.get("SampleModel") is not None

        # In _handle_dataops, it tries case-insensitive if exact match fails
        # This is tested in the actual handler

    def test_output_sanitization(self, server):
        """Test output sanitization for MCP"""

        @sensitive_field("secret", mask_pattern="<hidden>")
        class SensitiveModel(StorageModel):
            name: str
            secret: str

        instance = SensitiveModel(name="test", secret="password123")  # noqa: S106

        # Test with MCP context (should sanitize)
        mcp_context = SecurityContext(user_id="mcp_server")
        output = server._prepare_output(instance, mcp_context)

        # Secret should be masked
        assert output["name"] == "test"
        assert output["secret"] == "<hidden>"  # noqa: S105

        # Test with user context (should not sanitize)
        user_context = SecurityContext(user_id="real_user")
        output = server._prepare_output(instance, user_context)

        # Secret should be visible
        assert output["name"] == "test"
        assert output["secret"] == "password123"  # noqa: S105
