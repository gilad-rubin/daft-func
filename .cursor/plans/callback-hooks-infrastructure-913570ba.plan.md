<!-- 913570ba-3f5e-4a2e-902e-772d0f3898c5 1da1bf32-a900-44a7-9536-0b041ad0f795 -->
# Callback/Hooks Infrastructure Implementation

## Core Infrastructure

### 1. Create Callback Base System (`src/daft_func/callbacks.py`)

Define the callback event types and base callback class:

**Event Types** (fine-grained):

- Pipeline level: `on_pipeline_begin`, `on_pipeline_end`
- Node level: `on_node_begin`, `on_node_end`
- Cache level: `on_cache_check`, `on_cache_hit`, `on_cache_miss`, `on_cache_store`
- Batch level: `on_batch_begin`, `on_batch_end`, `on_item_begin`, `on_item_end`
- Error handling: `on_node_error`, `on_pipeline_error`

**Callback Base Class**:

```python
class Callback:
    """Base callback with all event hooks as no-ops"""
    order: int = 50  # Default priority (lower = earlier)
    async_mode: bool = False  # If True, runs in background thread
    
    def on_pipeline_begin(self, ctx: CallbackContext): pass
    def on_node_begin(self, ctx: CallbackContext): pass
    # ... all other events
```

**Callback Context**:

```python
@dataclass
class CallbackContext:
    """Provides callbacks access to everything"""
    runner: 'Runner'
    pipeline: 'Pipeline'
    current_node: Optional[NodeDef]
    inputs: Dict[str, Any]
    outputs: Dict[str, Any]
    state: Dict[str, Any]  # Shared state across callbacks
    mode: str  # 'single', 'batch', 'local_loop'
    # Cache-specific
    cache_signature: Optional[NodeSignature]
    cache_hit: Optional[bool]
    # Error handling
    exception: Optional[Exception]
```

### 2. Callback Handler (`src/daft_func/callbacks.py`)

Implement callback orchestration with ordering and async support:

**CallbackHandler**:

- Manages registration of callbacks
- Executes callbacks in priority order (sorted by `callback.order`)
- Handles async callbacks using ThreadPoolExecutor
- Provides callback lifecycle management

**Ordering Options** (choose one to implement):

- **Option A**: Priority-based (current proposal) - callbacks have numeric order field
- **Option B**: Phase-based - callbacks belong to phases (early/normal/late), explicit order within phase
- **Option C**: Explicit dependencies - callbacks declare what they run before/after

**Recommendation**: Option A (priority-based) for simplicity, with common priorities:

- 0-20: Setup/initialization callbacks
- 21-40: Pre-processing (e.g., cache check)
- 41-60: Core processing
- 61-80: Post-processing (e.g., cache store)
- 81-100: Cleanup/finalization

### 3. Refactor Caching as Callbacks (`src/daft_func/cache_callback.py`)

Move all caching logic from `runner.py` into callback classes:

**CacheCheckCallback** (order=25):

- Implements `on_node_begin`
- Checks cache hit/miss
- Loads cached value if hit
- Sets `ctx.state['cache_hit']` and `ctx.state['cached_value']`
- Skips node execution by setting `ctx.state['skip_execution']`

**CacheStoreCallback** (order=75):

- Implements `on_node_end`
- Stores results to cache if cache miss
- Computes and stores signature

**CacheStatsCallback** (order=90):

- Implements `on_node_end` and `on_pipeline_end`
- Collects statistics
- Prints summary at end

### 4. Update Runner (`src/daft_func/runner.py`)

Integrate callback system into execution:

**Changes**:

- Add `callbacks: List[Callback]` parameter to `__init__`
- Initialize `CallbackHandler` with callbacks
- Replace inline caching code with callback invocations
- Trigger callbacks at appropriate points in `_run_single`, `_run_batch`, `_run_local_loop`
- Handle `skip_execution` flag set by callbacks

**Key integration points**:

```python
# In _run_single, for each node:
ctx = CallbackContext(runner=self, pipeline=self.pipeline, current_node=node, ...)
handler.trigger('on_node_begin', ctx)

if not ctx.state.get('skip_execution', False):
    res = node.fn(**kwargs)
    outputs[node.meta.output_name] = res
else:
    # Use cached value from context
    outputs[node.meta.output_name] = ctx.state['cached_value']
    
handler.trigger('on_node_end', ctx)
```

### 5. Update CacheConfig and Backward Migration (`src/daft_func/cache.py`)

Simplify `cache.py` to only contain:

- Signature computation functions
- Store protocols and implementations
- `CacheConfig` dataclass (keep for configuration)

Remove from `cache.py`:

- `CacheStats` class (moves to `cache_callback.py`)
- Statistics tracking (handled by callback)

### 6. Update Public API (`src/daft_func/__init__.py`)

Export new components:

```python
from daft_func.callbacks import Callback, CallbackContext, CallbackHandler
from daft_func.cache_callback import CacheCheckCallback, CacheStoreCallback

__all__ = [
    # ... existing exports
    "Callback",
    "CallbackContext", 
    "CallbackHandler",
    "CacheCheckCallback",
    "CacheStoreCallback",
]
```

### 7. Update Runner Initialization

**Option A** - Auto-create cache callbacks when `cache_config` provided (recommended):

```python
def __init__(self, pipeline, mode='auto', batch_threshold=2, 
             cache_config=None, callbacks=None):
    self.callbacks = callbacks or []
    if cache_config and cache_config.enabled:
        # Auto-add cache callbacks
        self.callbacks.extend([
            CacheCheckCallback(cache_config),
            CacheStoreCallback(cache_config),
            CacheStatsCallback(cache_config) if cache_config.verbose else None
        ])
```

**Option B** - Explicit callback registration (more flexible but verbose):

```python
# User does:
runner = Runner(
    pipeline, 
    callbacks=[
        CacheCheckCallback(cache_config),
        CacheStoreCallback(cache_config)
    ]
)
```

**Recommendation**: Option A keeps API similar to today, but make both work.

## Testing & Examples

### 8. Create Tests (`tests/test_callbacks.py`)

- Test callback ordering
- Test async callbacks don't block
- Test callback context access
- Test cache callbacks work correctly
- Test custom callback implementation

### 9. Create Example (`examples/custom_callback.py`)

Demonstrate custom callback for logging/monitoring:

```python
class LoggingCallback(Callback):
    order = 10  # Run early
    
    def on_node_begin(self, ctx):
        print(f"Starting {ctx.current_node.meta.output_name}")
    
    def on_node_end(self, ctx):
        print(f"Finished {ctx.current_node.meta.output_name}")
```

## Future Extensions (Document in comments)

Add comments noting where future callbacks can hook in:

- Logfire integration: `LogfireCallback(async_mode=True)` 
- Live monitoring: `MonitoringCallback` that sends status updates
- Distributed tracing: `TracingCallback` for Modal/Coiled runs

### To-dos

- [ ] Create callback base system with event types, Callback class, CallbackContext, and CallbackHandler in src/daft_func/callbacks.py
- [ ] Implement CacheCheckCallback, CacheStoreCallback, and CacheStatsCallback in src/daft_func/cache_callback.py
- [ ] Simplify src/daft_func/cache.py to remove statistics and keep only signature computation and stores
- [ ] Update src/daft_func/runner.py to use CallbackHandler and remove inline caching code
- [ ] Update src/daft_func/__init__.py to export new callback components
- [ ] Create comprehensive tests in tests/test_callbacks.py
- [ ] Create example custom callback in examples/custom_callback.py