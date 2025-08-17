"""
Enhanced Security Model with Dgraph as single source of truth
"""
from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field

from .storage_model import StorageModel
from .storage_types import StorageConfig, StorageType


class Permission(str, Enum):
    """Unix-style permissions for ACL"""

    READ = "r"
    WRITE = "w"
    MODIFY = "m"
    DELETE = "d"
    EXECUTE = "x"
    ADMIN = "a"


class RoleType(str, Enum):
    """Built-in role types"""

    OWNER = "owner"
    ADMIN = "admin"
    MEMBER = "member"
    VIEWER = "viewer"
    GUEST = "guest"
    SERVICE = "service"  # For service accounts


class AuthDirective(BaseModel):
    """@auth directive configuration for Dgraph GraphQL"""

    rule: str  # GraphQL query rule
    operations: list[str] = Field(default_factory=lambda: ["query", "add", "update", "delete"])
    and_rules: list["AuthDirective"] = Field(default_factory=list)
    or_rules: list["AuthDirective"] = Field(default_factory=list)
    not_rule: Optional["AuthDirective"] = None


class ACLEntry(BaseModel):
    """Access Control List entry"""

    principal_id: str  # User, role, or group ID
    principal_type: str = "user"  # user, role, group, service
    permissions: list[Permission]
    resource_path: str | None = None  # Specific resource path (e.g., /users/123/profile)
    conditions: dict[str, Any] = Field(default_factory=dict)  # Additional conditions
    granted_by: str | None = None
    granted_at: datetime = Field(default_factory=datetime.utcnow)
    expires_at: datetime | None = None

    def has_permission(self, permission: Permission) -> bool:
        """Check if this ACL entry grants a specific permission"""
        return permission in self.permissions or Permission.ADMIN in self.permissions


class SecurityContext(BaseModel):
    """Security context for operations"""

    user_id: str
    roles: list[str] = Field(default_factory=list)
    groups: list[str] = Field(default_factory=list)
    jwt_claims: dict[str, Any] = Field(default_factory=dict)
    session_id: str | None = None
    ip_address: str | None = None
    device_id: str | None = None

    @property
    def principal_ids(self) -> list[str]:
        """Get all principal IDs for this context"""
        return [self.user_id] + self.roles + self.groups


class SecuredStorageModel(StorageModel):
    """Enhanced storage model with built-in security"""

    # Security fields - ACL replication is enough, no security key needed
    owner_id: str | None = None
    acl: list[ACLEntry] = Field(default_factory=list)
    auth_rules: list[AuthDirective] = Field(default_factory=list)

    # Audit fields
    created_by: str | None = None
    modified_by: str | None = None

    # Graph reference (always exists)
    graph_id: str | None = None  # Dgraph UID

    class Meta:
        # All secured models have a Dgraph entry as primary
        storage_configs = {
            "graph": StorageConfig(storage_type=StorageType.GRAPH),
            # Additional storages can be added in subclasses
        }

    @classmethod
    async def create_with_security(cls, context: SecurityContext, **kwargs) -> "SecuredStorageModel":
        """Create instance with security context"""
        # Set security fields
        kwargs["owner_id"] = context.user_id
        kwargs["created_by"] = context.user_id
        kwargs["modified_by"] = context.user_id

        # Create default ACL for owner
        owner_acl = ACLEntry(principal_id=context.user_id, principal_type="user", permissions=[Permission.ADMIN], granted_by="system")
        kwargs.setdefault("acl", []).append(owner_acl)

        # Create instance
        instance = cls(**kwargs)

        # First create in Dgraph to get security key
        graph_dao = cls.get_dao("graph")
        await graph_dao.connect()  # Ensure connected
        graph_id = await graph_dao.create(instance)
        instance.graph_id = graph_id

        # Then create in other storages with same security key
        for storage_name, dao in cls.get_all_daos().items():
            if storage_name != "graph":
                await dao.connect()  # Ensure connected
                await dao.create(instance)

        return instance

    async def check_permission(self, context: SecurityContext, permission: Permission) -> bool:
        """Check if context has permission for this resource"""
        # Admin always has access
        if context.user_id == self.owner_id:
            return True

        # Check ACL entries
        for principal_id in context.principal_ids:
            for acl_entry in self.acl:
                if acl_entry.principal_id == principal_id and acl_entry.has_permission(permission):
                    # Check expiration
                    if acl_entry.expires_at and acl_entry.expires_at < datetime.utcnow():
                        continue
                    return True

        return False

    async def grant_permission(
        self, context: SecurityContext, principal_id: str, permissions: list[Permission], principal_type: str = "user", expires_at: datetime | None = None
    ) -> ACLEntry:
        """Grant permissions to a principal"""
        # Check if granter has admin permission
        if not await self.check_permission(context, Permission.ADMIN):
            raise PermissionError("No admin permission to grant access")

        # Create ACL entry
        acl_entry = ACLEntry(
            principal_id=principal_id, principal_type=principal_type, permissions=permissions, granted_by=context.user_id, expires_at=expires_at
        )

        # Add to ACL
        self.acl.append(acl_entry)

        # Update in all storages
        await self.sync_security()

        return acl_entry

    async def revoke_permission(self, context: SecurityContext, principal_id: str) -> bool:
        """Revoke all permissions for a principal"""
        # Check if revoker has admin permission
        if not await self.check_permission(context, Permission.ADMIN):
            raise PermissionError("No admin permission to revoke access")

        # Remove ACL entries
        original_count = len(self.acl)
        self.acl = [entry for entry in self.acl if entry.principal_id != principal_id]

        # Update in all storages
        if len(self.acl) < original_count:
            await self.sync_security()
            return True

        return False

    async def sync_security(self) -> None:
        """Sync security settings across all storages"""
        # Update Dgraph first (source of truth)
        graph_dao = self.get_dao("graph")
        await graph_dao.update(self.graph_id, self.model_dump())

        # Then update other storages
        for storage_name, dao in self.get_all_daos().items():
            if storage_name != "graph":
                await dao.update(self.id, {"acl": self.acl})

    @classmethod
    async def find_with_security(
        cls, context: SecurityContext, query: dict[str, Any] = None, check_permission: Permission = Permission.READ, **kwargs
    ) -> list["SecuredStorageModel"]:
        """Find instances with security check"""
        # Build security-aware query
        security_query = {"$or": [{"owner_id": context.user_id}, {"acl.principal_id": {"$in": context.principal_ids}}]}

        query = {"$and": [query, security_query]} if query else security_query

        # Find instances
        instances = await cls.find(query, **kwargs)

        # Filter by permission
        permitted: list[SecuredStorageModel] = []
        for instance in instances:
            if isinstance(instance, SecuredStorageModel) and await instance.check_permission(context, check_permission):
                permitted.append(instance)
            # Skip non-secured models as they shouldn't be in this list

        return permitted

    async def update_with_security(self, context: SecurityContext, **kwargs) -> "SecuredStorageModel":
        """Update instance with security check"""
        # Check write permission
        if not await self.check_permission(context, Permission.WRITE):
            raise PermissionError("No write permission")

        # Update modified_by
        kwargs["modified_by"] = context.user_id

        # Update instance
        return await self.update(**kwargs)  # type: ignore[return-value]

    async def delete_with_security(self, context: SecurityContext) -> bool:
        """Delete instance with security check"""
        # Check delete permission
        if not await self.check_permission(context, Permission.DELETE):
            raise PermissionError("No delete permission")

        # Delete from all storages
        success = True
        for storage_name, dao in self.get_all_daos().items():
            if storage_name == "graph":
                # Delete from Dgraph last (source of truth)
                continue
            success = success and await dao.delete(self.id)

        # Finally delete from Dgraph
        graph_dao = self.get_dao("graph")
        return success and await graph_dao.delete(self.graph_id)


class Role(SecuredStorageModel):
    """Role model with permissions"""

    name: str
    role_type: RoleType
    permissions: list[Permission]
    description: str | None = None

    class Meta:
        storage_configs = {
            "graph": StorageConfig(storage_type=StorageType.GRAPH),
            "cache": StorageConfig(storage_type=StorageType.CACHE),  # Cache for fast lookup
        }
        path = "roles"


class SecurityGroup(SecuredStorageModel):
    """Security group for organizing users"""

    name: str
    description: str | None = None
    member_ids: list[str] = Field(default_factory=list)
    role_ids: list[str] = Field(default_factory=list)
    parent_group_id: str | None = None  # For nested groups

    class Meta:
        storage_configs = {
            "graph": StorageConfig(storage_type=StorageType.GRAPH),
            "document": StorageConfig(storage_type=StorageType.DOCUMENT),
        }
        path = "security_groups"


class AuthRule(BaseModel):
    """Reusable authentication rule"""

    name: str
    description: str | None = None
    rule_type: str = "jwt"  # jwt, api_key, graph_traversal
    rule_config: dict[str, Any]

    def to_dgraph_rule(self) -> str:
        """Convert to Dgraph @auth rule string"""
        if self.rule_type == "jwt":
            # Example: query($USER: String!) { queryUser(filter: { id: { eq: $USER } }) { id } }
            return self.rule_config.get("query", "")
        if self.rule_type == "graph_traversal":
            # Custom graph traversal rule
            return self.rule_config.get("traversal", "")
        return ""


# Convenience functions for common security patterns
def owner_only_auth() -> AuthDirective:
    """Create auth directive for owner-only access"""
    return AuthDirective(rule="query($USER: String!) { queryUser(filter: { id: { eq: $USER } }) { id } }", operations=["query", "update", "delete"])


def role_based_auth(required_roles: list[str]) -> AuthDirective:
    """Create auth directive for role-based access"""
    role_checks = " OR ".join([f'roles: {{ name: {{ eq: "{role}" }} }}' for role in required_roles])
    return AuthDirective(
        rule=f"query($USER: String!) {{ queryUser(filter: {{ id: {{ eq: $USER }}, {role_checks} }}) {{ id }} }}",
        operations=["query", "add", "update", "delete"],
    )


def group_member_auth(group_name: str) -> AuthDirective:
    """Create auth directive for group member access"""
    return AuthDirective(
        rule=f'query($USER: String!) {{ queryUser(filter: {{ id: {{ eq: $USER }}, groups: {{ name: {{ eq: "{group_name}" }} }} }}) {{ id }} }}',
        operations=["query"],
    )
