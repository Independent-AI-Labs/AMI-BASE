"""Configuration loader for storage-config.yaml."""

import os
from pathlib import Path
from typing import Any

import yaml

# Find the config file
CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "storage-config.yaml"


class StorageConfigLoader:
    """Load and manage storage configurations from YAML."""

    _instance = None
    _config = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if self._config is None:
            self._load_config()

    def _load_config(self):
        """Load the YAML configuration with environment variable substitution."""
        if not CONFIG_PATH.exists():
            raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")

        with CONFIG_PATH.open() as f:
            content = f.read()

        # Expand environment variables
        content = os.path.expandvars(content)

        self._config = yaml.safe_load(content)

    def _expand_env_var(self, value: str) -> Any:
        """Expand environment variable and convert to appropriate type."""
        import re

        pattern = r"\$\{([^:}]+)(:-([^}]+))?\}"

        def replacer(match):
            var_name = match.group(1)
            default_val = match.group(3) if match.group(3) else ""
            return os.environ.get(var_name, default_val)

        result = re.sub(pattern, replacer, value)
        # Try to convert to appropriate type
        if result.isdigit():
            return int(result)
        if result.lower() in ("true", "false"):
            return result.lower() == "true"
        return result

    def _expand_vars(self, obj: Any) -> Any:
        """Recursively expand environment variables."""
        if isinstance(obj, dict):
            return {k: self._expand_vars(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._expand_vars(item) for item in obj]
        if isinstance(obj, str):
            if "${" in obj:
                return self._expand_env_var(obj)
            return os.path.expandvars(obj)
        return obj

    def get_storage_config(self, storage_name: str) -> dict[str, Any]:
        """Get configuration for a specific storage backend."""
        if self._config is None:
            raise RuntimeError("Configuration not loaded")
        if storage_name not in self._config["storage_configs"]:
            raise ValueError(f"Unknown storage: {storage_name}")

        config = self._config["storage_configs"][storage_name].copy()
        return self._expand_vars(config)

    def get_model_defaults(self) -> dict[str, Any]:
        """Get default model configurations."""
        if self._config is None:
            return {}
        return self._config.get("model_defaults", {})

    def get_connection_pools(self) -> dict[str, Any]:
        """Get connection pool settings."""
        if self._config is None:
            return {}
        return self._config.get("connection_pools", {})

    def get_performance_settings(self) -> dict[str, Any]:
        """Get performance settings."""
        if self._config is None:
            return {}
        return self._config.get("performance", {})


# Singleton instance
storage_config = StorageConfigLoader()
