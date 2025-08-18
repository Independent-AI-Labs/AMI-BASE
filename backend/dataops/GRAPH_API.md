# Graph API Specification for DataOps

## Overview

This document outlines a Graph API for the DataOps infrastructure that elegantly extends the existing DgraphDAO with native Dgraph capabilities. Rather than reimplementing graph algorithms, we leverage Dgraph's powerful built-in features through DQL (Dgraph Query Language) and expose them via a clean Python API.

## Core Concepts

### Graph Model
- **Nodes**: Entities with properties (documents, sections, users, etc.)
- **Edges**: Relationships between nodes with optional properties
- **Properties**: Key-value pairs on nodes and edges
- **Types**: Node and edge type definitions with schema
- **Predicates**: Dgraph predicates for efficient querying

## API Design

### 1. Node Operations

#### Create Node
```python
async def create_node(
    type: str,
    properties: dict[str, Any],
    labels: list[str] = None
) -> str:
    """Create a new node in the graph"""
    
# Example
node_id = await graph.create_node(
    type="Document",
    properties={
        "title": "Q4 Report",
        "created_at": datetime.now(),
        "author": "John Doe"
    },
    labels=["report", "financial"]
)
```

#### Update Node
```python
async def update_node(
    node_id: str,
    properties: dict[str, Any],
    merge: bool = True
) -> bool:
    """Update node properties"""
```

#### Delete Node
```python
async def delete_node(
    node_id: str,
    cascade: bool = False
) -> bool:
    """Delete node and optionally its relationships"""
```

#### Get Node
```python
async def get_node(
    node_id: str,
    include_edges: bool = False
) -> dict:
    """Retrieve node with optional edges"""
```

### 2. Edge Operations

#### Create Edge
```python
async def create_edge(
    from_node: str,
    to_node: str,
    type: str,
    properties: dict[str, Any] = None,
    bidirectional: bool = False
) -> str:
    """Create relationship between nodes"""
    
# Example
edge_id = await graph.create_edge(
    from_node=doc1_id,
    to_node=doc2_id,
    type="references",
    properties={"weight": 0.8, "context": "financial data"}
)
```

#### Update Edge
```python
async def update_edge(
    edge_id: str,
    properties: dict[str, Any]
) -> bool:
    """Update edge properties"""
```

#### Delete Edge
```python
async def delete_edge(
    edge_id: str
) -> bool:
    """Delete relationship"""
```

### 3. Native Dgraph Traversal Operations

#### K-Hop Traversal (Using Dgraph's @recurse)
```python
async def k_hop_query(
    start_uid: str,
    depth: int = 2,
    edge_predicates: list[str] = None
) -> dict:
    """Execute k-hop traversal using Dgraph's native recursion"""
    
    dql = f"""
    {{
        traverse(func: uid({start_uid})) @recurse(depth: {depth}) {{
            uid
            {' '.join(edge_predicates) if edge_predicates else 'expand(_all_)'}
        }}
    }}
    """
    return await self.raw_read_query(dql)
```

#### Shortest Path (Using Dgraph's shortest directive)
```python
async def shortest_path(
    from_uid: str,
    to_uid: str,
    edge_predicate: str,
    max_depth: int = 10
) -> list[str]:
    """Find shortest path using Dgraph's native shortest path"""
    
    dql = f"""
    {{
        path as shortest(from: {from_uid}, to: {to_uid}, depth: {max_depth}) {{
            {edge_predicate}
        }}
        
        path(func: uid(path)) {{
            uid
            expand(_all_)
        }}
    }}
    """
    result = await self.raw_read_query(dql)
    return result.get("path", [])
```

#### All Paths (Using Dgraph's @recurse with path tracking)
```python
async def all_paths(
    from_uid: str,
    to_uid: str,
    edge_predicate: str,
    max_depth: int = 5
) -> list[list[str]]:
    """Find all paths using Dgraph's recursive queries"""
    
    dql = f"""
    {{
        var(func: uid({from_uid})) @recurse(depth: {max_depth}, loop: false) {{
            uid
            path as {edge_predicate}
        }}
        
        paths(func: uid(path)) @filter(uid({to_uid})) {{
            uid
            ~{edge_predicate} @facets(path)
        }}
    }}
    """
    return await self.raw_read_query(dql)
```

#### All Paths
```python
async def all_paths(
    from_node: str,
    to_node: str,
    max_depth: int = 5,
    edge_types: list[str] = None
) -> list[list[dict]]:
    """Find all paths between nodes"""
```

### 4. Pattern Matching

#### Match Pattern
```python
async def match_pattern(
    pattern: dict,
    limit: int = 100
) -> list[dict]:
    """Match graph patterns using declarative syntax"""
    
# Example
results = await graph.match_pattern({
    "nodes": [
        {"var": "doc", "type": "Document", "filters": {"created_at": {"$gt": "2024-01-01"}}},
        {"var": "author", "type": "User"},
        {"var": "dept", "type": "Department"}
    ],
    "edges": [
        {"from": "author", "to": "doc", "type": "created"},
        {"from": "author", "to": "dept", "type": "belongs_to"}
    ],
    "return": ["doc", "author.name", "dept.name"]
})
```

#### Subgraph Extraction
```python
async def extract_subgraph(
    center_nodes: list[str],
    radius: int = 2,
    edge_types: list[str] = None,
    node_types: list[str] = None
) -> dict:
    """Extract subgraph around specified nodes"""
    
# Example
subgraph = await graph.extract_subgraph(
    center_nodes=[document_id],
    radius=2,
    edge_types=["references", "authored_by", "tagged_with"],
    node_types=["Document", "User", "Tag"]
)
```

### 5. Aggregation and Analytics

#### Node Degree
```python
async def node_degree(
    node_id: str,
    direction: str = "both"  # in, out, both
) -> int:
    """Get degree of a node"""
```

#### Clustering Coefficient
```python
async def clustering_coefficient(
    node_id: str = None
) -> float:
    """Calculate clustering coefficient"""
```

#### PageRank
```python
async def pagerank(
    damping: float = 0.85,
    iterations: int = 20,
    node_types: list[str] = None
) -> dict[str, float]:
    """Calculate PageRank scores"""
    
# Example
scores = await graph.pagerank(
    node_types=["Document"],
    iterations=30
)
```

#### Community Detection
```python
async def detect_communities(
    algorithm: str = "louvain",
    resolution: float = 1.0
) -> dict[str, int]:
    """Detect communities in the graph"""
```

### 6. Query Builder

#### Fluent Query Interface
```python
class GraphQuery:
    def __init__(self, graph):
        self.graph = graph
        
    def match(self, node_type: str = None, **properties):
        """Start query with node match"""
        
    def where(self, **conditions):
        """Add filter conditions"""
        
    def connected_to(self, edge_type: str, node_type: str = None):
        """Add edge traversal"""
        
    def within(self, distance: int):
        """Limit traversal distance"""
        
    def order_by(self, property: str, desc: bool = False):
        """Order results"""
        
    def limit(self, count: int):
        """Limit results"""
        
    async def execute(self) -> list[dict]:
        """Execute the query"""

# Example
results = await (
    GraphQuery(graph)
    .match("Document", created_after="2024-01-01")
    .connected_to("authored_by", "User")
    .where(department="Engineering")
    .within(2)
    .order_by("created_at", desc=True)
    .limit(10)
    .execute()
)
```

### 7. Bulk Operations

#### Bulk Create
```python
async def bulk_create_nodes(
    nodes: list[dict],
    batch_size: int = 1000
) -> list[str]:
    """Create multiple nodes efficiently"""
```

#### Bulk Create Edges
```python
async def bulk_create_edges(
    edges: list[dict],
    batch_size: int = 1000
) -> list[str]:
    """Create multiple edges efficiently"""
```

#### Bulk Update
```python
async def bulk_update_nodes(
    updates: list[dict],
    batch_size: int = 1000
) -> int:
    """Update multiple nodes"""
```

### 8. Transaction Support

#### Transaction Context
```python
async with graph.transaction() as tx:
    node1 = await tx.create_node(type="Document", properties={...})
    node2 = await tx.create_node(type="User", properties={...})
    await tx.create_edge(node1, node2, "authored_by")
    # Automatically commits on success, rolls back on error
```

### 9. Schema Management

#### Define Node Type
```python
async def define_node_type(
    name: str,
    properties: dict[str, str],  # property_name -> type
    indexes: list[str] = None
) -> bool:
    """Define or update node type schema"""
    
# Example
await graph.define_node_type(
    name="Document",
    properties={
        "title": "string",
        "content": "string",
        "created_at": "datetime",
        "word_count": "int",
        "embedding": "[float]"
    },
    indexes=["title", "created_at"]
)
```

#### Define Edge Type
```python
async def define_edge_type(
    name: str,
    properties: dict[str, str] = None,
    constraints: dict = None
) -> bool:
    """Define or update edge type schema"""
```

### 10. Advanced Features

#### Graph Algorithms
```python
# Centrality measures
async def betweenness_centrality(node_types: list[str] = None) -> dict[str, float]
async def closeness_centrality(node_types: list[str] = None) -> dict[str, float]
async def eigenvector_centrality(iterations: int = 20) -> dict[str, float]

# Path algorithms
async def has_path(from_node: str, to_node: str) -> bool
async def all_simple_paths(from_node: str, to_node: str, cutoff: int = None) -> list
async def minimum_spanning_tree() -> list[dict]

# Similarity
async def jaccard_similarity(node1: str, node2: str) -> float
async def cosine_similarity(node1: str, node2: str, property: str) -> float
```

#### Graph Visualization
```python
async def export_to_graphml(
    subgraph: dict = None,
    file_path: str = None
) -> str:
    """Export graph to GraphML format"""

async def export_to_json(
    subgraph: dict = None,
    format: str = "cytoscape"  # cytoscape, d3, gephi
) -> dict:
    """Export graph to JSON format"""
```

#### Graph Statistics
```python
async def graph_stats() -> dict:
    """Get graph statistics"""
    # Returns:
    # - node_count by type
    # - edge_count by type
    # - average_degree
    # - density
    # - connected_components
    # - diameter
```

## Implementation Details

### Storage Layer Integration
```python
class DgraphGraphAPI:
    def __init__(self, dgraph_client):
        self.client = dgraph_client
        
    async def _execute_query(self, query: str, variables: dict = None) -> dict:
        """Execute DQL query"""
        
    async def _execute_mutation(self, mutation: dict) -> str:
        """Execute graph mutation"""
        
    def _build_dql(self, operation: str, params: dict) -> str:
        """Build DQL query from operation"""
```

### Caching Strategy
```python
class GraphCache:
    def __init__(self, redis_client):
        self.cache = redis_client
        
    async def get_cached_path(self, from_node: str, to_node: str) -> list:
        """Get cached shortest path"""
        
    async def cache_subgraph(self, center: str, radius: int, data: dict):
        """Cache extracted subgraph"""
        
    async def invalidate_node(self, node_id: str):
        """Invalidate cache for node changes"""
```

### Performance Optimizations
- Predicate-based indexing for fast lookups
- Batch operations for bulk inserts
- Query result pagination
- Lazy loading of node properties
- Connection pooling for Dgraph client
- Asynchronous parallel traversals

## Usage Examples

### Document Relationship Graph
```python
# Create document network
doc1 = await graph.create_node(
    type="Document",
    properties={"title": "Introduction", "path": "/docs/intro.md"}
)

doc2 = await graph.create_node(
    type="Document", 
    properties={"title": "API Guide", "path": "/docs/api.md"}
)

# Create relationship
await graph.create_edge(
    from_node=doc1,
    to_node=doc2,
    type="references",
    properties={"section": "Getting Started"}
)

# Find related documents
related = await graph.bfs(
    start_node=doc1,
    edge_types=["references", "links_to"],
    max_depth=3
)

# Find document clusters
communities = await graph.detect_communities(
    algorithm="louvain"
)
```

### User Permission Graph
```python
# Check user access path
path = await graph.shortest_path(
    from_node=user_id,
    to_node=document_id,
    edge_types=["owns", "shared_with", "member_of", "has_access"]
)

if path:
    print(f"User has access through: {' -> '.join([n['type'] for n in path])}")
else:
    print("No access path found")
```

### Knowledge Graph Query
```python
# Find experts on a topic
experts = await (
    GraphQuery(graph)
    .match("Document", tags__contains="machine-learning")
    .connected_to("authored_by", "User")
    .where(citations__gt=10)
    .order_by("citations", desc=True)
    .limit(5)
    .execute()
)
```

## Testing Strategy

### Unit Tests
- Node CRUD operations
- Edge CRUD operations
- Basic traversal algorithms
- Query builder functionality

### Integration Tests
- Complex graph patterns
- Transaction rollback
- Concurrent operations
- Large graph traversals

### Performance Tests
- Bulk operation benchmarks
- Traversal performance on large graphs
- Query optimization verification
- Cache hit rates

## Security Considerations

### Access Control
- Node-level permissions
- Edge visibility rules
- Property-level encryption
- Query result filtering

### Audit Trail
- Track all graph modifications
- Query execution logging
- Performance metrics collection
- Anomaly detection

## Future Enhancements

### Phase 1 (Immediate)
- Basic CRUD operations
- Simple traversal algorithms
- Pattern matching
- Transaction support

### Phase 2 (3 months)
- Advanced algorithms (PageRank, centrality)
- Community detection
- Query optimization
- Caching layer

### Phase 3 (6 months)
- Graph neural network integration
- Real-time graph updates
- Distributed graph processing
- GraphQL interface

## Conclusion

This Graph API provides a comprehensive interface for graph operations within the DataOps infrastructure. It abstracts the complexity of Dgraph while providing powerful graph algorithms and pattern matching capabilities. The API is designed to be intuitive for developers while maintaining high performance for large-scale graph operations.