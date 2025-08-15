# CODE EXCEPTIONS - BASE MODULE

This file documents legitimate code patterns that might appear problematic but are actually necessary for the module's functionality.

## 1. Broad Exception Handlers in Worker Pools

### Location:
- `backend/workers/base.py` - Lines 338, 449, 477, 507, 518
- `backend/workers/thread_pool.py` - Line 114
- `backend/workers/process_pool.py` - Line 165
- `backend/workers/simple_process_pool.py` - Lines 50, 116
- `backend/workers/uv_process_pool.py` - Line 288

### Justification:
Worker pools execute arbitrary user-provided functions. Since we cannot predict what exceptions user code might raise, we MUST catch all exceptions to:
1. Prevent worker crashes from taking down the entire pool
2. Log errors for debugging
3. Mark tasks as failed appropriately
4. Maintain pool stability

### Pattern:
```python
except (RuntimeError, OSError, ValueError, ZeroDivisionError, Exception) as e:
    # Log the error and handle gracefully
    logger.warning(f"Worker error: {e}")
```
Note: `asyncio.CancelledError` is handled separately where needed for proper task cancellation.

## 2. Infinite Loops in Worker Processes

### Location:
- `backend/workers/simple_process_pool.py:36` - Worker process main loop
- `backend/workers/base.py:198` - Worker acquisition loop
- `backend/mcp/mcp_server.py:101,134,202` - Server connection loops

### Justification:
These infinite loops are REQUIRED for:
1. **Worker processes** - Must continuously wait for and execute tasks
2. **Server connections** - Must maintain persistent connections for MCP protocol
3. **Task acquisition** - Must wait for available workers

### Pattern:
```python
while True:  # Worker main loop - REQUIRED
    task = await get_next_task()
    if task is None:  # Shutdown signal
        break
    process_task(task)
```

## 3. Accessing Private asyncio Attributes

### Location:
- `backend/workers/base.py:94,96,113` - Accessing `_loop` attribute

### Justification:
We need to check if asyncio primitives (Lock, Condition) belong to the current event loop. This is necessary because:
1. Worker pools can be used across different event loops
2. asyncio primitives are tied to specific event loops
3. Using the wrong loop's primitives causes runtime errors

### Pattern:
```python
# Check if lock belongs to current loop
if self._lock._loop != asyncio.get_running_loop():  # type: ignore[attr-defined]
    self._lock = asyncio.Lock()  # Create new lock for current loop
```

## 4. Import Order (E402) Violations

### Location:
- `scripts/start_mcp_server.py:10` - Import after path setup

### Justification:
Must modify `sys.path` BEFORE importing local modules to ensure correct module resolution.

### Pattern:
```python
import sys
sys.path.insert(0, project_root)  # Must come first
from backend.mcp import MCPServer  # Now safe to import
```

## 5. Complex Methods

### Location:
- `backend/workers/base.py:217` - `acquire_worker()` method
- `backend/mcp/generic/runner.py:8` - `init_environment()` method

### Justification:
These methods handle complex state management that cannot be easily broken down without:
1. Creating excessive parameter passing
2. Breaking atomicity of operations
3. Making the code harder to understand

## Summary

These patterns are intentional design decisions necessary for the module's core functionality. They should NOT be "fixed" as they would break the system's operation.