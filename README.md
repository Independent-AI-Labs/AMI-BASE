# AMI-BASE

## Core Infrastructure Module

AMI-BASE provides the foundational infrastructure for the AMI-ORCHESTRATOR ecosystem, including unified data operations, security models, MCP server implementations, and worker pool management.

## Key Components

### DataOps Infrastructure
A unified CRUD system that seamlessly manages data across multiple storage backends with automatic synchronization.

**Features:**
- **Multi-Storage Support**: Dgraph (graph), MongoDB (document), PostgreSQL (relational + pgvector), Redis (cache), S3 (blob)
- **Automatic Synchronization**: Primary-first, parallel, and transactional sync strategies
- **Built-in Security**: ACL-based permissions with role and group support
- **BPMN 2.0 Models**: Full support for business process modeling and execution
- **UUID v7**: Time-ordered identifiers for natural sorting and tracing
- **Vector Search**: PgVectorDAO with automatic embeddings for semantic search
- **Dynamic Schema**: PostgreSQLDAO with automatic table creation and type inference
- **Advanced Caching**: RedisDAO with TTL support and field indexing

**Example:**

```python
from services.dataops.unified_crud import UnifiedCRUD, SyncStrategy
from services.dataops.security_model import SecuredStorageModel, SecurityContext


# Create a secured model with automatic Dgraph + Redis sync
class User(SecuredStorageModel):
    username: str
    email: str

    class Meta:
        storage_configs = {
            "graph": StorageConfig(storage_type=StorageType.GRAPH),
            "cache": StorageConfig(storage_type=StorageType.CACHE)
        }


# Use unified CRUD with security
crud = UnifiedCRUD(User, sync_strategy=SyncStrategy.PRIMARY_FIRST)
context = SecurityContext(user_id="admin", roles=["admin"])

# Create with automatic ACL
user = await crud.create(
    {"username": "john", "email": "john@example.com"},
    context=context
)
```

### MCP Servers
Model Context Protocol servers providing minimal, focused interfaces for specific capabilities.

**Available Servers:**
- **DataOpsMCPServer**: CRUD operations for all registered models
- **FilesysMCPServer**: File system operations with YAML output support
- **SSHMCPServer**: Remote system management via SSH

**Features:**
- YAML and JSON output formats
- Line numbering for file operations
- Batch operations with transaction support
- Automatic model discovery and registration

### Worker Pools
Advanced task execution system with intelligent resource management.

**Capabilities:**
- Thread and process pool support
- Worker hibernation and warm-up
- Priority-based task scheduling
- Health monitoring and statistics
- Automatic scaling based on load

### Security Models
Comprehensive security infrastructure with Dgraph as the source of truth.

**Components:**
- **ACL System**: Fine-grained permissions (read, write, modify, delete, execute, admin)
- **Security Context**: User, role, and group-based access control
- **Auth Directives**: GraphQL @auth rules for Dgraph
- **Audit Trail**: Automatic tracking of created_by, modified_by, and timestamps

## Installation

```bash
# Clone the base module
git clone https://github.com/Independent-AI-Labs/AMI-BASE.git
cd AMI-BASE

# Create virtual environment with uv
uv venv .venv

# Activate environment
# Windows:
.venv\Scripts\activate
# Linux/Mac:
source .venv/bin/activate

# Install dependencies
uv pip install -r requirements.txt
uv pip install -r requirements-test.txt
```

## Configuration

### Storage Configuration
Configure storage backends in `config/storage-config.yaml`:

```yaml
dgraph:
  host: 172.72.72.2  # Docker VM with Dgraph
  port: 9080
  
mongodb:
  uri: mongodb://localhost:27017
  database: ami_base
  
redis:
  host: localhost
  port: 6379
  db: 0
```

### SSH Servers
Configure SSH targets in `config/ssh-servers.yaml`:

```yaml
docker-vm:
  host: 172.72.72.2
  username: docker
  password: docker
  port: 22
```

## Testing

```bash
# Run all tests
python -m pytest

# Run specific test suites
python -m pytest tests/integration/test_dataops_dgraph.py -xvs
python -m pytest tests/test_dataops_mcp_server.py -xvs

# Run with coverage
python -m pytest --cov=backend --cov-report=html
```

### Test Results
- **Total Tests**: 114
- **Pass Rate**: 100%
- **Coverage**: Comprehensive unit and integration testing

## Usage Examples

### DataOps with Dgraph

```python
from services.dataops.bpmn_model import Process, Task
from services.dataops.dao import get_dao
from services.dataops.storage_types import StorageConfig, StorageType

# Configure Dgraph connection
config = StorageConfig(
    storage_type=StorageType.GRAPH,
    host="172.72.72.2",
    port=9080
)

# Create DAO and connect
dao = get_dao(Process, config)
await dao.connect()

# Create BPMN process
process = Process(
    name="Order Processing",
    documentation="Customer order workflow",
    flow_nodes=["validate", "payment", "shipping"]
)

# Save to Dgraph
process_id = await dao.create(process)
```

### MCP Server Usage

```python
from services.mcp.dataops.server import DataOpsMCPServer

# Initialize server
server = DataOpsMCPServer()

# Register models
server.register_model("User", User)
server.register_model("Process", Process)

# Handle CRUD operations via MCP
result = await server.handle_tool_call(
    "dataops",
    {
        "operation": "create",
        "model": "User",
        "data": {"username": "alice", "email": "alice@example.com"}
    }
)
```

### Worker Pool Management

```python
from services.utils.worker_pools import PoolManager, PoolConfig

# Configure pool
config = PoolConfig(
    min_workers=2,
    max_workers=10,
    warm_workers=3,
    ttl=300
)

# Get or create pool
pool = PoolManager.get_pool("compute", "thread", config)

# Submit tasks
future = pool.submit(expensive_computation, arg1, arg2)
result = await future.result()

# Monitor health
stats = pool.get_statistics()
print(f"Active workers: {stats['active_workers']}")
print(f"Queue size: {stats['queue_size']}")
```

## Architecture

```
AMI-BASE/
├── backend/
│   ├── dataops/           # Unified data operations
│   │   ├── implementations/   # Storage backend implementations
│   │   ├── storage_model.py   # Base model with DAO integration
│   │   ├── security_model.py  # ACL and permissions
│   │   ├── unified_crud.py    # Multi-backend CRUD
│   │   └── bpmn_model.py      # BPMN 2.0 models
│   │
│   ├── mcp/               # Model Context Protocol servers
│   │   ├── dataops/          # DataOps MCP server
│   │   ├── filesys/          # File system MCP server
│   │   ├── ssh/              # SSH MCP server
│   │   └── transports/       # Transport implementations
│   │
│   └── utils/             # Utilities
│       ├── worker_pools.py    # Worker pool management
│       ├── uuid_utils.py      # UUID v7 generation
│       └── format_utils.py    # YAML/JSON formatting
│
├── config/                # Configuration files
│   ├── storage-config.yaml   # Storage backend configs
│   └── ssh-servers.yaml      # SSH target definitions
│
└── tests/                 # Comprehensive test suite
    ├── integration/          # Integration tests
    └── unit/                 # Unit tests
```

## Development Guidelines

### Code Quality Standards
- Maximum 300 lines per class
- Maximum 50 lines per method
- No inline JavaScript
- Proper error handling with logging
- Type hints on all functions

### Security Requirements
- No hardcoded credentials
- Secure defaults only
- Input validation on all endpoints
- ACL checks on all operations

### Testing Requirements
- 100% pass rate before commit
- No test isolation issues
- Proper cleanup in fixtures
- Coverage > 80%

## Recent Updates

### v2.1.0 - IDP Storage Integration
- **PgVectorDAO**: Vector embeddings with sentence-transformers for semantic search
- **PostgreSQLDAO**: Dynamic table creation with automatic schema inference
- **RedisDAO**: Advanced caching with TTL support and field indexing
- **DgraphDAO**: Extended with graph operations (k-hop, shortest path, components)
- **SQL Injection Prevention**: Proper identifier validation in all SQL operations

### v2.0.0 - DataOps Infrastructure
- Unified CRUD with multi-backend support
- Dgraph as primary security source
- UUID v7 for time-ordered IDs
- BPMN 2.0 workflow models
- MCP server implementations
- Fixed all test failures (114/114 passing)

### Key Fixes
- Dgraph ACL/auth_rules JSON deserialization
- Async permission checking
- Per-class DAO isolation
- UnifiedCRUD dict data support
- BPMN flow_nodes field migration

## Contributing

Please follow the guidelines in `CLAUDE.md`:
- Use uv for dependency management
- Run all tests before committing
- No `--no-verify` in git operations
- Follow code quality standards
- Maintain security defaults

## License

MIT License - See LICENSE file for details

## Support

- GitHub Issues: [AMI-BASE Issues](https://github.com/Independent-AI-Labs/AMI-BASE/issues)
- Documentation: See inline docstrings and test examples
- Main Project: [AMI-ORCHESTRATOR](https://github.com/Independent-AI-Labs/AMI-ORCHESTRATOR)