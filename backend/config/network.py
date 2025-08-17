"""Network configuration base classes."""

from typing import Any

from pydantic import BaseModel, Field, field_validator


class IPConfig(BaseModel):
    """Base configuration for network-based services."""
    
    host: str = Field(default="localhost", description="Host address or IP")
    port: int | None = Field(default=None, description="Port number")
    username: str | None = Field(default=None, description="Username for authentication")
    password: str | None = Field(default=None, description="Password for authentication")
    timeout: int = Field(default=30, description="Connection timeout in seconds")
    options: dict[str, Any] = Field(default_factory=dict, description="Additional connection options")
    
    @field_validator("host")
    @classmethod
    def validate_host(cls, v: str) -> str:
        """Validate host is not empty."""
        if not v or not v.strip():
            raise ValueError("Host cannot be empty")
        return v.strip()
    
    @field_validator("port")
    @classmethod
    def validate_port(cls, v: int | None) -> int | None:
        """Validate port is in valid range."""
        if v is not None and (v < 1 or v > 65535):
            raise ValueError(f"Port must be between 1 and 65535, got {v}")
        return v
    
    @field_validator("timeout")
    @classmethod
    def validate_timeout(cls, v: int) -> int:
        """Validate timeout is positive."""
        if v <= 0:
            raise ValueError(f"Timeout must be positive, got {v}")
        return v


class SSHConfig(IPConfig):
    """Configuration for SSH connections."""
    
    name: str = Field(..., description="Server name/identifier")
    description: str | None = Field(default=None, description="Server description")
    port: int = Field(default=22, description="SSH port")
    key_filename: str | None = Field(default=None, description="Path to SSH private key")
    passphrase: str | None = Field(default=None, description="Passphrase for SSH key")
    known_hosts_file: str | None = Field(default=None, description="Path to known_hosts file")
    allow_agent: bool = Field(default=True, description="Allow SSH agent for authentication")
    look_for_keys: bool = Field(default=True, description="Look for SSH keys in default locations")
    compression: bool = Field(default=False, description="Enable SSH compression")
    
    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Validate server name."""
        if not v or not v.strip():
            raise ValueError("Server name cannot be empty")
        # Ensure name is valid for use as identifier
        import re
        if not re.match(r"^[a-zA-Z0-9_-]+$", v):
            raise ValueError(f"Server name must contain only alphanumeric characters, underscores, and hyphens: {v}")
        return v.strip()
    
    def to_paramiko_config(self) -> dict[str, Any]:
        """Convert to Paramiko connection parameters."""
        config = {
            "hostname": self.host,
            "port": self.port,
            "username": self.username,
            "timeout": self.timeout,
            "compress": self.compression,
            "allow_agent": self.allow_agent,
            "look_for_keys": self.look_for_keys,
        }
        
        if self.password:
            config["password"] = self.password
        if self.key_filename:
            config["key_filename"] = self.key_filename
        if self.passphrase:
            config["passphrase"] = self.passphrase
        if self.known_hosts_file:
            config["known_hosts_filename"] = self.known_hosts_file
            
        return config