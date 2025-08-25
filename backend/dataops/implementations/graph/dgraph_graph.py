"""Dgraph graph-specific operations."""

import json
from typing import Any

from loguru import logger

from ...exceptions import StorageError


class DgraphGraphMixin:
    """Mixin for Dgraph graph-specific operations."""

    async def k_hop_query(self, start_id: str, k: int, edge_types: list[str] | None = None) -> dict[str, Any]:
        """Execute k-hop traversal from a starting node.

        Args:
            start_id: Starting node ID
            k: Number of hops
            edge_types: Edge types to follow (None = all)

        Returns:
            Graph traversal results
        """
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        if k <= 0:
            raise ValueError("k must be positive")

        # Build recursive query
        depth_queries = []
        for i in range(1, k + 1):
            if edge_types:
                edges = " ".join([f"{self.collection_name}.{edge}" for edge in edge_types])
                depth_queries.append(f"@recurse(depth: {i}, loop: false)")
            else:
                depth_queries.append(f"@recurse(depth: {i}, loop: false)")

        query = f"""
        {{
            start(func: eq({self.collection_name}.id, "{start_id}")) @filter(type({self.collection_name})) {{
                uid
                {self.collection_name}.id
                expand(_all_) {depth_queries[-1] if depth_queries else ""} {{
                    uid
                    {self.collection_name}.id
                    expand(_all_)
                }}
            }}
        }}
        """

        try:
            txn = self.client.txn(read_only=True)
            response = txn.query(query)
            data = json.loads(response.json)

            if data.get("start") and len(data["start"]) > 0:
                return {"start_node": start_id, "hops": k, "traversal": data["start"][0]}

            return {"start_node": start_id, "hops": k, "traversal": {}}

        except Exception as e:
            logger.error(f"Failed to execute k-hop query: {e}")
            raise StorageError(f"Failed to execute k-hop query: {e}") from e

    async def shortest_path(self, start_id: str, end_id: str, max_depth: int = 10) -> list[str]:
        """Find shortest path between two nodes.

        Args:
            start_id: Starting node ID
            end_id: Ending node ID
            max_depth: Maximum search depth

        Returns:
            List of node IDs in the path
        """
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        # DQL query for shortest path
        query = f"""
        {{
            path as shortest(from: {start_id}, to: {end_id}, depth: {max_depth}) {{
                {self.collection_name}.id
            }}

            result(func: uid(path)) {{
                uid
                {self.collection_name}.id
            }}
        }}
        """

        try:
            txn = self.client.txn(read_only=True)
            response = txn.query(query)
            data = json.loads(response.json)

            path = []
            if data.get("result"):
                for node in data["result"]:
                    if f"{self.collection_name}.id" in node:
                        path.append(node[f"{self.collection_name}.id"])

            return path

        except Exception as e:
            logger.error(f"Failed to find shortest path: {e}")
            raise StorageError(f"Failed to find shortest path: {e}") from e

    async def find_connected_components(self, node_type: str | None = None) -> list[list[str]]:
        """Find all connected components in the graph.

        Args:
            node_type: Filter by node type (None = all types)

        Returns:
            List of components, each component is a list of node IDs
        """
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        # Get all nodes
        if node_type:
            query = f"""
            {{
                nodes(func: type({node_type})) {{
                    uid
                    {node_type}.id
                    expand(_all_)
                }}
            }}
            """
        else:
            query = f"""
            {{
                nodes(func: type({self.collection_name})) {{
                    uid
                    {self.collection_name}.id
                    expand(_all_)
                }}
            }}
            """

        try:
            txn = self.client.txn(read_only=True)
            response = txn.query(query)
            data = json.loads(response.json)

            if not data.get("nodes"):
                return []

            # Build adjacency list
            adjacency = {}
            node_ids = {}

            for node in data["nodes"]:
                uid = node.get("uid")
                node_id = node.get(f"{self.collection_name}.id") or node.get(f"{node_type}.id") if node_type else None

                if uid and node_id:
                    node_ids[uid] = node_id
                    adjacency[uid] = []

                    # Find connections
                    for key, value in node.items():
                        if isinstance(value, list):
                            for item in value:
                                if isinstance(item, dict) and "uid" in item:
                                    adjacency[uid].append(item["uid"])
                        elif isinstance(value, dict) and "uid" in value:
                            adjacency[uid].append(value["uid"])

            # Find connected components using DFS
            visited = set()
            components = []

            def dfs(uid: str, component: list[str]) -> None:
                if uid in visited or uid not in node_ids:
                    return
                visited.add(uid)
                component.append(node_ids[uid])
                for neighbor in adjacency.get(uid, []):
                    dfs(neighbor, component)

            for uid in node_ids:
                if uid not in visited:
                    component = []
                    dfs(uid, component)
                    if component:
                        components.append(component)

            return components

        except Exception as e:
            logger.error(f"Failed to find connected components: {e}")
            raise StorageError(f"Failed to find connected components: {e}") from e

    async def get_node_degree(self, node_id: str, direction: str = "all") -> dict[str, int]:
        """Get degree of a node (in/out/all).

        Args:
            node_id: Node ID
            direction: 'in', 'out', or 'all'

        Returns:
            Dictionary with degree information
        """
        if not self.client:
            raise StorageError("Not connected to Dgraph")

        query = f"""
        {{
            node(func: eq({self.collection_name}.id, "{node_id}")) @filter(type({self.collection_name})) {{
                uid
                {self.collection_name}.id
                ~has.edge @facets {{
                    count(uid)
                }}
                has.edge @facets {{
                    count(uid)
                }}
                expand(_all_) {{
                    uid
                }}
            }}
        }}
        """

        try:
            txn = self.client.txn(read_only=True)
            response = txn.query(query)
            data = json.loads(response.json)

            if not data.get("node") or len(data["node"]) == 0:
                return {"in": 0, "out": 0, "total": 0}

            node = data["node"][0]

            # Count edges
            in_degree = 0
            out_degree = 0

            # Count incoming edges (reverse edges)
            for key, value in node.items():
                if key.startswith("~"):
                    if isinstance(value, list):
                        in_degree += len(value)
                    elif isinstance(value, dict):
                        in_degree += 1

            # Count outgoing edges
            for key, value in node.items():
                if not key.startswith("~") and key not in ["uid", f"{self.collection_name}.id", "dgraph.type"]:
                    if isinstance(value, list):
                        out_degree += len([v for v in value if isinstance(v, dict) and "uid" in v])
                    elif isinstance(value, dict) and "uid" in value:
                        out_degree += 1

            result = {"in": in_degree, "out": out_degree, "total": in_degree + out_degree}

            if direction == "in":
                return {"in": result["in"]}
            if direction == "out":
                return {"out": result["out"]}

            return result

        except Exception as e:
            logger.error(f"Failed to get node degree: {e}")
            raise StorageError(f"Failed to get node degree: {e}") from e
