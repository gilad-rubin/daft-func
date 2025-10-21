# DAGFlow

**Generic DAG Execution Framework with Automatic Batching**

DAGFlow lets you build computational DAGs using simple Python functions with automatic batch processing that seamlessly transitions between single-item Python execution and high-performance vectorized execution using Daft DataFrames.

## Features

🎯 **Declarative DAG Definition** - Define nodes as regular Python functions  
⚡ **Adaptive Execution** - Automatically chooses between Python and Daft based on workload  
🔄 **Map-Reduce Pattern** - Built-in support for per-item operations with automatic alignment  
🛡️ **Type Safety** - Full Pydantic model support with automatic schema inference  
✨ **Zero Boilerplate** - Decorator + type hints = complete specification  

## Quick Start

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd dagflow

# Install dependencies
uv sync
```

### Basic Example

```python
from pydantic import BaseModel
from dagflow import dagflow, Runner

# 1. Define your data models
class Query(BaseModel):
    id: str
    text: str

class Result(BaseModel):
    id: str
    score: float

# 2. Define your DAG nodes
@dagflow(output="results", map_axis="query", key_attr="id")
def process(query: Query, threshold: float) -> Result:
    score = len(query.text) * threshold
    return Result(id=query.id, score=score)

# 3. Run your pipeline
runner = Runner(mode="auto")
outputs = runner.run(inputs={
    "query": [
        Query(id="q1", text="hello"),
        Query(id="q2", text="world"),
    ],
    "threshold": 0.5,
})

print(outputs["results"])
# [Result(id='q1', score=2.5), Result(id='q2', score=2.5)]
```

## Running the Examples

### Retrieval Pipeline Demo

```bash
uv run python examples/retrieval/demo.py
```

This demonstrates a two-stage retrieval pipeline:
- **Retrieve**: Token-overlap based document retrieval
- **Rerank**: Reranking of retrieved documents

### Running Tests

```bash
uv run pytest tests/ -v
```

Expected output: `19 passed, 1 skipped`

## Project Structure

```
dagflow/
├── src/dagflow/          # Core framework
│   ├── decorator.py      # @dagflow decorator
│   ├── registry.py       # DAG registry & topological sort
│   ├── runner.py         # Execution engine
│   └── types.py          # Type conversion utilities
│
├── examples/             # Example pipelines
│   └── retrieval/        # Information retrieval example
│
├── tests/                # Comprehensive test suite
│
└── docs/                 # Documentation
    ├── overview.md       # Architecture & vision
    ├── fixes-applied.md  # Technical fixes
    └── refactoring-summary.md  # Structure overview
```

## Execution Modes

DAGFlow supports three execution modes:

### Local Mode (Pure Python)
```python
runner = Runner(mode="local")
```
- No Daft dependency required
- Simple loop over items
- Best for: debugging, small datasets, CPU-bound tasks

### Daft Mode (Vectorized)
```python
runner = Runner(mode="daft")
```
- Forces batch execution with Daft DataFrames
- Vectorized operations
- Best for: large datasets, GPU workloads, I/O-bound tasks

### Auto Mode (Intelligent)
```python
runner = Runner(mode="auto", batch_threshold=10)
```
- Automatically chooses based on input size
- Uses Daft when >= `batch_threshold` items
- Best for: production deployments, mixed workloads

## Documentation

- **[Overview](docs/overview.md)**: High-level architecture and vision
- **[Fixes Applied](docs/fixes-applied.md)**: Technical details of Daft integration fixes
- **[Refactoring Summary](docs/refactoring-summary.md)**: Complete refactoring details

## Creating New Pipelines

1. **Create a new folder** in `examples/`:
   ```
   examples/my_pipeline/
   ├── __init__.py
   ├── models.py          # Pydantic models
   ├── implementations.py # Your business logic
   ├── nodes.py           # DAG node definitions
   └── demo.py            # Demo script
   ```

2. **Define your models** in `models.py`:
   ```python
   from pydantic import BaseModel
   
   class Input(BaseModel):
       data: str
   
   class Output(BaseModel):
       result: float
   ```

3. **Create nodes** in `nodes.py`:
   ```python
   from dagflow import dagflow
   
   @dagflow(output="output", map_axis="input", key_attr="id")
   def transform(input: Input, config: Config) -> Output:
       return Output(result=process(input, config))
   ```

4. **Run your pipeline**:
   ```python
   from dagflow import Runner
   
   runner = Runner(mode="auto")
   result = runner.run(inputs={"input": [...], "config": ...})
   ```

## Requirements

- Python 3.12+
- Pydantic 2.12+
- Daft 0.6.7+ (optional, for batch processing)

## Development

```bash
# Install development dependencies
uv add --dev pytest

# Run tests
uv run pytest tests/ -v

# Run linter
ruff check src/ tests/ examples/
```

## Philosophy

DAGFlow embodies the principle that **complex systems should be built from simple, composable primitives**. By reducing DAG construction to function decoration and leveraging Python's type system, we create a framework that is:

- **Easy to understand**: Functions and decorators, nothing more
- **Easy to test**: Pure functions with clear inputs/outputs
- **Easy to optimize**: Framework handles execution strategy
- **Easy to extend**: New nodes are just new functions

## License

[Your License Here]

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Questions?

- Check the [documentation](docs/)
- Look at [examples](examples/)
- Run the [tests](tests/) to see more usage patterns

