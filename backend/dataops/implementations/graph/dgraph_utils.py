"""Dgraph utility and helper methods."""

import json
from datetime import datetime
from typing import Any

from loguru import logger

from ...storage_model import StorageModel


class DgraphUtilsMixin:
    """Mixin for Dgraph utility operations."""

    def _get_dgraph_type(self, python_type: Any) -> str:
        """Map Python type to Dgraph type.

        Args:
            python_type: Python type annotation

        Returns:
            Dgraph type string
        """
        type_map = {
            str: "string",
            int: "int",
            float: "float",
            bool: "bool",
            datetime: "datetime",
            dict: "string",  # JSON stored as string
            list: "string",  # JSON stored as string
        }

        # Handle optional types
        origin = getattr(python_type, "__origin__", None)
        if origin is not None:
            # Get the actual type from Optional/Union
            args = getattr(python_type, "__args__", ())
            if args:
                for arg in args:
                    if arg is not type(None):
                        return self._get_dgraph_type(arg)

        # Direct type lookup
        for py_type, dg_type in type_map.items():
            if python_type is py_type or (isinstance(python_type, type) and issubclass(python_type, py_type)):
                return dg_type

        # Default to string for unknown types
        return "string"

    def _to_dgraph_format(self, instance: StorageModel) -> dict[str, Any]:
        """Convert model instance to Dgraph format.

        Args:
            instance: Model instance

        Returns:
            Dgraph-formatted dictionary
        """
        data = instance.model_dump()

        # Convert datetime objects to ISO format
        for key, value in data.items():
            if isinstance(value, datetime):
                data[key] = value.isoformat()
            elif isinstance(value, (dict, list)):
                # Store complex types as JSON strings
                data[key] = json.dumps(value)

        return data

    def _parse_json_field(self, field_name: str, value: Any) -> Any:
        """Parse JSON field value.

        Args:
            field_name: Field name
            value: Field value

        Returns:
            Parsed value
        """
        if value is None:
            return None

        # Check if field should be JSON based on model definition
        field_info = self.model_cls.model_fields.get(field_name)
        if field_info:
            field_type = field_info.annotation
            origin = getattr(field_type, "__origin__", None)

            # Check if it's a dict or list type
            if origin in [dict, list] or field_type in [dict, list]:
                if isinstance(value, str):
                    try:
                        return json.loads(value)
                    except json.JSONDecodeError:
                        logger.warning(f"Failed to parse JSON field {field_name}: {value}")

        return value

    def _process_dgraph_value(self, value: Any) -> Any:
        """Process value from Dgraph response.

        Args:
            value: Raw value from Dgraph

        Returns:
            Processed value
        """
        if isinstance(value, list):
            # Process list items
            return [self._process_dgraph_value(item) for item in value]
        if isinstance(value, dict):
            # Check if it's a node reference
            if "uid" in value and len(value) == 1:
                return value["uid"]
            # Process nested dict
            return {k: self._process_dgraph_value(v) for k, v in value.items()}
        # Return scalar values as-is
        return value

    def _from_dgraph_format(self, data: dict[str, Any]) -> StorageModel | None:
        """Convert Dgraph format to model instance.

        Args:
            data: Dgraph-formatted dictionary

        Returns:
            Model instance or None
        """
        if not data:
            return None

        # Remove Dgraph-specific fields and unprefix field names
        clean_data = {}
        prefix = f"{self.collection_name}."

        for key, value in data.items():
            if key in ["uid", "dgraph.type"]:
                continue
            if key.startswith(prefix):
                field_name = key[len(prefix) :]
                # Parse JSON fields if needed
                clean_data[field_name] = self._parse_json_field(field_name, value)
            elif not key.startswith("~"):  # Skip reverse edges
                clean_data[key] = value

        try:
            return self.model_cls(**clean_data)
        except Exception as e:
            logger.error(f"Failed to create model instance: {e}")
            logger.debug(f"Data: {clean_data}")
            return None

    def _build_dql_query(self, query: dict[str, Any], limit: int | None = None, offset: int = 0) -> str:
        """Build DQL query from dict parameters.

        Args:
            query: Query parameters
            limit: Result limit
            offset: Result offset

        Returns:
            DQL query string
        """
        # Build filter
        filter_str = self._build_filter(query) if query else ""

        # Build pagination
        pagination = ""
        if offset > 0:
            pagination += f", offset: {offset}"
        if limit:
            pagination += f", first: {limit}"

        # Build complete query
        dql = f"""
        {{
            items(func: type({self.collection_name}){pagination}) {filter_str} {{
                uid
                expand(_all_) {{
                    uid
                    expand(_all_)
                }}
            }}
        }}
        """

        return dql

    def _build_filter(self, query: dict[str, Any]) -> str:
        """Build DQL filter from query dict.

        Args:
            query: Query parameters

        Returns:
            DQL filter string
        """
        if not query:
            return ""

        filters = []
        for key, value in query.items():
            field = f"{self.collection_name}.{key}"

            if isinstance(value, dict):
                # Handle operators
                for op, val in value.items():
                    if op == "$eq":
                        filters.append(f'eq({field}, "{val}")')
                    elif op == "$ne":
                        filters.append(f'NOT eq({field}, "{val}")')
                    elif op == "$gt":
                        filters.append(f"gt({field}, {val})")
                    elif op == "$gte":
                        filters.append(f"ge({field}, {val})")
                    elif op == "$lt":
                        filters.append(f"lt({field}, {val})")
                    elif op == "$lte":
                        filters.append(f"le({field}, {val})")
                    elif op == "$in":
                        # Create OR condition for IN
                        in_filters = [f'eq({field}, "{v}")' for v in val]
                        filters.append(f"({' OR '.join(in_filters)})")
                    elif op == "$regex":
                        filters.append(f'regexp({field}, "/{val}/")')
            else:
                # Simple equality
                if isinstance(value, str):
                    filters.append(f'eq({field}, "{value}")')
                else:
                    filters.append(f"eq({field}, {value})")

        if filters:
            return f"@filter({' AND '.join(filters)})"
        return ""

    def _build_count_query(self, query: dict[str, Any]) -> str:
        """Build DQL count query.

        Args:
            query: Query parameters

        Returns:
            DQL count query string
        """
        filter_str = self._build_filter(query) if query else ""

        dql = f"""
        {{
            count(func: type({self.collection_name})) {filter_str} {{
                total: count(uid)
            }}
        }}
        """

        return dql
