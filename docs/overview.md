# DAGFlow: Generic DAG Execution Framework with Automatic Batching

## Vision

DAGFlow is a lightweight, generic framework for building and executing computational DAGs (Directed Acyclic Graphs) with automatic batch processing capabilities. It seamlessly transitions between single-item Python execution and high-performance vectorized execution using Daft DataFrames based on workload characteristics.

## Core Concept

The framework allows you to:

1. **Define computational nodes** using simple Python functions with the `@dagflow` decorator
2. **Declare dependencies implicitly** through function parameters
3. **Execute automatically** with optimal batching strategy based on input size
4. **Preserve type safety** with full Pydantic model support throughout the pipeline

## Key Features

### 🎯 Declarative DAG Definition
- Define nodes as regular Python functions
- Automatic dependency resolution through parameter inspection
- Topological sorting handled automatically
- No manual graph construction needed

### ⚡ Adaptive Execution
- **Single-item mode**: Pure Python execution for individual inputs
- **Batch mode**: Daft-powered vectorized execution for multiple inputs
- **Auto mode**: Automatically chooses based on batch size threshold
- Seamless fallback to Python if Daft is unavailable

### 🔄 Map-Reduce Pattern
- Specify which parameter represents the "map axis" (per-item data)
- Automatic alignment across nodes using key attributes
- Clean handling of constants vs. per-item parameters

### 🛡️ Type Safety with Pydantic
- Full support for Pydantic models as inputs/outputs
- Automatic serialization/deserialization in batch mode
- Type hints automatically converted to Daft/PyArrow types
- No manual schema definition required

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         User Code                            │
│  @dagflow(output="result", map_axis="item")                 │
│  def process(item: MyModel, config: Config) -> Result:      │
│      return compute(item, config)                           │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                      DAG Registry                            │
│  • Collects all @dagflow decorated functions                │
│  • Inspects signatures and metadata                         │
│  • Performs topological sort                                │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                         Runner                               │
│  • Analyzes input structure                                 │
│  • Selects execution strategy (local/batch/auto)            │
│  • Routes to appropriate executor                           │
└─────────────────────────────────────────────────────────────┘
                              ↓
        ┌────────────────────┴────────────────────┐
        ↓                                         ↓
┌──────────────────┐                   ┌──────────────────┐
│  Single Runner   │                   │   Batch Runner   │
│  (Pure Python)   │                   │  (Daft/PyArrow)  │
│                  │                   │                  │
│  • Direct calls  │                   │  • Type conv.    │
│  • Simple loop   │                   │  • Batch UDFs    │
│  • Fast startup  │                   │  • Vectorized    │
└──────────────────┘                   └──────────────────┘
```

## Use Cases

### Information Retrieval Pipelines
```python
@dagflow(output="hits", map_axis="query", key_attr="query_uuid")
def retrieve(retriever: Retriever, query: Query, top_k: int) -> RetrievalResult:
    return retriever.retrieve(query, top_k=top_k)

@dagflow(output="reranked", map_axis="query", key_attr="query_uuid")
def rerank(reranker: Reranker, query: Query, hits: RetrievalResult, top_k: int) -> List[RerankedHit]:
    return reranker.rerank(query, hits, top_k=top_k)
```

### Document Processing
- Extract → Parse → Chunk → Embed pipelines
- Multi-stage transformations with dependencies
- Automatic batching for GPU-based models

### Data Transformation Pipelines
- ETL workflows with complex dependencies
- Feature engineering with multiple stages
- Model inference pipelines with preprocessing/postprocessing

## Design Principles

1. **Simplicity First**: Write normal Python functions, get DAG execution
2. **Type-Driven**: Leverage type hints for automatic schema inference
3. **Performance When Needed**: Batch automatically when it matters
4. **Graceful Degradation**: Works without Daft, excels with it
5. **Minimal Boilerplate**: Decorator + type hints = complete specification

## Execution Modes

### Local Mode (`mode="local"`)
- Pure Python execution
- No Daft dependency required
- Best for: debugging, small datasets, CPU-bound tasks

### Daft Mode (`mode="daft"`)
- Forces batch execution with Daft
- Vectorized operations
- Best for: large datasets, GPU workloads, I/O-bound tasks

### Auto Mode (`mode="auto"`, default)
- Intelligently chooses based on input size
- Configurable threshold
- Best for: production deployments, mixed workloads

## Integration Points

### Pydantic Models
- Define schemas as Pydantic BaseModels
- Automatic conversion to PyArrow/Daft types
- Validation on deserialization

### Custom Implementations
- Protocol-based interfaces (Retriever, Reranker, etc.)
- Swap implementations without changing DAG
- Easy testing with mock implementations

### Daft DataFrames
- Transparent integration when available
- UDFs automatically generated from functions
- Leverages Daft's optimization and execution

## Future Extensions

- **Caching**: Memoization of node outputs
- **Async Nodes**: Support for async/await patterns
- **Resource Management**: GPU/CPU allocation per node
- **Distributed Execution**: Multi-node execution with Daft
- **Observability**: Metrics, tracing, profiling hooks
- **Conditional Execution**: Dynamic DAG branches
- **Streaming**: Support for streaming inputs/outputs

## Philosophy

DAGFlow embodies the principle that **complex systems should be built from simple, composable primitives**. By reducing DAG construction to function decoration and leveraging Python's type system, we create a framework that is:

- **Easy to understand**: Functions and decorators, nothing more
- **Easy to test**: Pure functions with clear inputs/outputs
- **Easy to optimize**: Framework handles execution strategy
- **Easy to extend**: New nodes are just new functions

The goal is to let developers focus on *what* to compute, while the framework handles *how* to compute it efficiently.

