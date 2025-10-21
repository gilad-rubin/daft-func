# daft_func_minimal_auto_daft.py
from __future__ import annotations

import inspect
import re
from dataclasses import dataclass
from functools import wraps
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Protocol,
    Tuple,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

from pydantic import BaseModel

try:
    import daft
    import pyarrow

    DAFT_AVAILABLE = True
except ImportError:
    DAFT_AVAILABLE = False

# --------------------------
# Pydantic to Daft/PyArrow type conversion (from daft-pdf-example.md)
# --------------------------


def pyarrow_datatype(f_type: type[Any]) -> "pyarrow.DataType":
    """Convert Python/Pydantic types to PyArrow DataTypes."""
    if not DAFT_AVAILABLE:
        raise ImportError("pyarrow is required for type conversion")

    if get_origin(f_type) is Union:
        targs = get_args(f_type)
        if len(targs) == 2:
            if targs[0] is type(None) and targs[1] is not type(None):
                refined_inner = targs[1]
            elif targs[0] is not type(None) and targs[1] is type(None):
                refined_inner = targs[0]
            else:
                raise TypeError(
                    f"Cannot convert a general union type {f_type} into a pyarrow.DataType!"
                )
            inner_type = pyarrow_datatype(refined_inner)
        else:
            raise TypeError(
                f"Cannot convert a general union type {f_type} into a pyarrow.DataType!"
            )

    elif get_origin(f_type) is list:
        targs = get_args(f_type)
        if len(targs) != 1:
            raise TypeError(
                f"Expected list type {f_type} with inner element type but got {len(targs)} inner-types: {targs}"
            )
        element_type = targs[0]
        inner_type = pyarrow.list_(pyarrow_datatype(element_type))

    elif get_origin(f_type) is dict:
        targs = get_args(f_type)
        if len(targs) != 2:
            raise TypeError(
                f"Expected dict type {f_type} with inner key-value types but got {len(targs)} inner-types: {targs}"
            )
        kt, vt = targs
        pyarrow_kt = pyarrow_datatype(kt)
        pyarrow_vt = pyarrow_datatype(vt)
        inner_type = pyarrow.map_(pyarrow_kt, pyarrow_vt)

    elif get_origin(f_type) is tuple:
        raise TypeError(f"Cannot support tuple types: {f_type}")

    elif isinstance(f_type, type) and issubclass(f_type, BaseModel):
        # Get PyArrow schema from Pydantic model
        schema_dict = {}
        for field_name, field_info in f_type.model_fields.items():
            schema_dict[field_name] = pyarrow_datatype(field_info.annotation)
        inner_type = pyarrow.struct([(k, v) for k, v in schema_dict.items()])

    elif isinstance(f_type, type) and issubclass(f_type, str):
        inner_type = pyarrow.string()

    elif isinstance(f_type, type) and issubclass(f_type, int):
        inner_type = pyarrow.int64()

    elif isinstance(f_type, type) and issubclass(f_type, float):
        inner_type = pyarrow.float64()

    elif isinstance(f_type, type) and issubclass(f_type, bool):
        inner_type = pyarrow.bool_()

    elif isinstance(f_type, type) and issubclass(f_type, bytes):
        inner_type = pyarrow.binary()

    else:
        raise TypeError(f"Cannot handle general Python objects in Arrow: {f_type}")

    return inner_type


def daft_datatype(f_type: type[Any]) -> "daft.DataType":
    """Convert Python/Pydantic types to Daft DataTypes via PyArrow."""
    if not DAFT_AVAILABLE:
        raise ImportError("daft is required for type conversion")
    return daft.DataType.from_arrow_type(pyarrow_datatype(f_type))


# --------------------------
# Small domain (schemas + ops)
# --------------------------


def normalize(text: str) -> set[str]:
    toks = re.findall(r"[a-zA-Z]+", text.lower())
    return {t[:-1] if t.endswith("s") else t for t in toks}


class Query(BaseModel):
    query_uuid: str
    text: str


class RetrievalHit(BaseModel):
    doc_id: str
    score: float


class RetrievalResult(BaseModel):
    query_uuid: str
    hits: List[RetrievalHit]


class RerankedHit(BaseModel):
    query_uuid: str
    doc_id: str
    score: float


class Retriever(Protocol):
    def retrieve(self, query: Query, top_k: int) -> RetrievalResult: ...


class Reranker(Protocol):
    def rerank(
        self, query: Query, hits: RetrievalResult, top_k: int
    ) -> List[RerankedHit]: ...


class ToyRetriever(Retriever):
    def __init__(self, corpus: Dict[str, str]):
        self._doc_tokens = {doc_id: normalize(txt) for doc_id, txt in corpus.items()}

    def retrieve(self, query: Query, top_k: int) -> RetrievalResult:
        q = normalize(query.text)
        scored = [
            RetrievalHit(doc_id=d, score=float(len(q & toks)))
            for d, toks in self._doc_tokens.items()
        ]
        scored.sort(key=lambda h: h.score, reverse=True)
        return RetrievalResult(query_uuid=query.query_uuid, hits=scored[:top_k])


class IdentityReranker(Reranker):
    def rerank(
        self, query: Query, hits: RetrievalResult, top_k: int
    ) -> List[RerankedHit]:
        return [
            RerankedHit(query_uuid=query.query_uuid, doc_id=h.doc_id, score=h.score)
            for h in hits.hits[:top_k]
        ]


# --------------------------
# Generic DAG decorator + registry
# --------------------------


@dataclass(frozen=True)
class NodeMeta:
    output_name: str
    # For auto-batching: which parameter is the per-item "map axis"?
    map_axis: Optional[str] = None  # e.g., "query"
    # If nodes align by a key (same item across nodes), what attribute on the map_axis carries it?
    key_attr: Optional[str] = None  # e.g., "query_uuid"


@dataclass(frozen=True)
class NodeDef:
    fn: Callable
    meta: NodeMeta
    params: Tuple[str, ...]  # ordered parameter names (from signature)


class DagRegistry:
    def __init__(self):
        self.nodes: List[NodeDef] = []
        self.by_output: Dict[str, NodeDef] = {}

    def add(self, fn: Callable, meta: NodeMeta):
        sig = inspect.signature(fn)
        params = tuple(sig.parameters.keys())
        node = NodeDef(fn=fn, meta=meta, params=params)
        self.nodes.append(node)
        self.by_output[meta.output_name] = node

    def topo(self, initial_inputs: Dict[str, Any]) -> List[NodeDef]:
        """Very small topo-sort by parameter availability."""
        available = set(initial_inputs.keys())
        ordered: List[NodeDef] = []
        remaining = set(self.nodes)
        while remaining:
            progress = False
            for node in list(remaining):
                needed = {p for p in node.params if p not in ("self",)}
                if needed.issubset(available):
                    ordered.append(node)
                    available.add(node.meta.output_name)
                    remaining.remove(node)
                    progress = True
            if not progress:
                raise RuntimeError(
                    f"Cannot resolve dependencies; remaining: {[n.fn.__name__ for n in remaining]}"
                )
        return ordered


_REG = DagRegistry()


def daft_func(
    *, output: str, map_axis: Optional[str] = None, key_attr: Optional[str] = None
):
    """
    Register a DAG node.
    - output: name of the produced value (binds it into the DAG namespace)
    - map_axis: name of the parameter that carries the per-item object (for multi-input runs)
    - key_attr: attribute on the map_axis object that uniquely identifies items (alignment)
    """
    meta = NodeMeta(output_name=output, map_axis=map_axis, key_attr=key_attr)

    def deco(fn: Callable):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)

        wrapper._daft_func_meta = meta
        _REG.add(wrapper, meta)
        return wrapper

    return deco


# --------------------------
# DAG nodes (generic; tiny)
# --------------------------


@daft_func(output="hits", map_axis="query", key_attr="query_uuid")
def retrieve(retriever: Retriever, query: Query, top_k: int) -> RetrievalResult:
    return retriever.retrieve(query, top_k=top_k)


@daft_func(output="reranked_hits", map_axis="query", key_attr="query_uuid")
def rerank(
    reranker: Reranker, query: Query, hits: RetrievalResult, top_k: int
) -> List[RerankedHit]:
    return reranker.rerank(query, hits, top_k=top_k)


# --------------------------
# Generic Runner
# --------------------------


class Runner:
    """
    - Single item: executes locally (pure Python), returns [[RerankedHit]] for uniformity.
    - Multi items: automatically uses Daft for vectorized execution (falls back to Python if Daft not available).
    """

    def __init__(self, mode: str = "auto", batch_threshold: int = 2):
        self.mode = mode
        self.batch_threshold = batch_threshold

    def run(self, *, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the DAG given initial inputs.
        The only special key is the map_axis object (e.g., "query"); it may be a single item or a list of items.
        """
        # Determine if we are batching based on any node's map_axis param presence + list input
        map_axes = {n.meta.map_axis for n in _REG.nodes if n.meta.map_axis}
        if len(map_axes) > 1:
            raise ValueError(
                f"This minimal runner supports one map axis; found: {map_axes}"
            )
        map_axis = next(iter(map_axes)) if map_axes else None

        batching_requested = False
        if map_axis and map_axis in inputs and isinstance(inputs[map_axis], list):
            batching_requested = True

        # Determine batching strategy
        if self.mode == "local":
            batching = False
        elif self.mode == "daft":
            batching = True
        else:  # auto mode
            batching = (
                batching_requested and len(inputs[map_axis]) >= self.batch_threshold
                if map_axis
                else False
            )

        # Execute based on strategy
        if batching:
            return self._run_batch(inputs, map_axis)
        elif batching_requested:
            # We have list inputs but using local/single execution - loop manually
            items = inputs[map_axis]
            constants = {k: v for k, v in inputs.items() if k != map_axis}
            aggregated: List[Dict[str, Any]] = []
            for it in items:
                per_inputs = {**constants, map_axis: it}
                per_out = self._run_single(per_inputs)
                aggregated.append(per_out)
            # Merge structure: final outputs to lists
            merged: Dict[str, Any] = dict(constants)
            # For final output example, ensure list-of-lists
            merged["reranked_hits"] = [o["reranked_hits"][0] for o in aggregated]
            return merged
        else:
            # True single item execution
            return self._run_single(inputs)

    # ---- single item path (pure Python) ----
    def _run_single(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        outputs = dict(inputs)
        order = _REG.topo(inputs)
        for node in order:
            kwargs = {p: outputs[p] for p in node.params if p in outputs}
            res = node.fn(**kwargs)
            outputs[node.meta.output_name] = res
        # normalize to batched shape if final output is present
        if "reranked_hits" in outputs and isinstance(outputs["reranked_hits"], list):
            outputs["reranked_hits"] = [outputs["reranked_hits"]]
        return outputs

    # ---- multi item path (Daft if available; else Python loop) ----
    def _run_batch(
        self, inputs: Dict[str, Any], map_axis: Optional[str]
    ) -> Dict[str, Any]:
        # Safety: require a single map axis for this minimal demo
        assert map_axis, "No map axis configured but batching was requested."

        items: List[BaseModel] = inputs[map_axis]  # e.g., List[Query]
        constants = {k: v for k, v in inputs.items() if k != map_axis}

        try:
            import daft
        except Exception:
            # Fallback to Python loop using the same single-run logic per item
            aggregated: List[Dict[str, Any]] = []
            for it in items:
                per_inputs = {**constants, map_axis: it}
                per_out = self._run_single(per_inputs)
                aggregated.append(per_out)
            # Merge structure: final outputs to lists
            merged: Dict[str, Any] = dict(constants)
            # For final output example, ensure list-of-lists
            merged["reranked_hits"] = [o["reranked_hits"][0] for o in aggregated]
            return merged

        # Build initial Daft DF with one column for the map_axis (dictified Pydantic)
        df = daft.from_pylist([{map_axis: it.model_dump()} for it in items])

        order = _REG.topo(
            {**constants, map_axis: items[0]}
        )  # topo over a "sample" shape

        # Helper to create batch UDF with proper closure capture
        def _make_batch_udf(
            node, arg_names, series_param_indices, node_constants, items
        ):
            """Factory function to create batch UDF with proper variable capture.

            Args:
                node: The node definition
                arg_names: Parameter names for this specific node
                series_param_indices: List of (param_name, deserializer) for series params
                node_constants: Dict of only the constants needed by THIS node
                items: List of items being processed
            """

            # Get return type from the node function's type hints
            hints = get_type_hints(node.fn)
            return_type = hints.get("return", Any)

            # Convert to Daft DataType
            try:
                return_dtype = daft_datatype(return_type)
            except Exception as e:
                print(
                    f"Warning: Could not infer return dtype from {return_type}, falling back to python(): {e}"
                )
                return_dtype = daft.DataType.python()

            @daft.func.batch(return_dtype=return_dtype)
            def _apply_batch(*cols):
                # cols only includes series (constants were filtered out)
                # Convert Series to pylist per column (Daft passes daft.Series)
                ser_lists: List[List[Any]] = []
                for c in cols:
                    ser_lists.append(c.to_pylist())

                out_list: List[Any] = []
                rows = len(ser_lists[0]) if ser_lists else len(items)
                for i in range(rows):
                    kwargs: Dict[str, Any] = {}

                    # First, add only the constants that this node actually needs
                    kwargs.update(node_constants)

                    # Then, add all series-based arguments using the proper mapping
                    for ser_idx, (param_name, deser) in enumerate(series_param_indices):
                        raw = ser_lists[ser_idx][i]
                        kwargs[param_name] = deser(raw)

                    # call the original node function
                    res = node.fn(**kwargs)
                    # store as dict (Pydantic -> dict; list[Pydantic] -> list[dict])
                    if isinstance(res, BaseModel):
                        out_list.append(res.model_dump())
                    elif (
                        isinstance(res, list) and res and isinstance(res[0], BaseModel)
                    ):
                        out_list.append([r.model_dump() for r in res])
                    else:
                        out_list.append(res)
                return out_list

            return _apply_batch

        # We'll iteratively add columns to df for each node's output.
        for node in order:
            # Collect columns for this node call: mapped param -> series; constants -> literals; deps -> series
            arg_names = node.params

            # Build a list of expressions to feed into the batch UDF in the right order
            series_args: List[Any] = []
            deserializers: List[Callable[[Dict], Any]] = []

            # Figure out types from hints (for Pydantic reconstruction)
            hints = get_type_hints(node.fn)

            def _mk_deser(py_type):
                """Create deserializer for a given type, handling Pydantic models and lists."""
                # Handle List[BaseModel] types
                origin = get_origin(py_type)
                if origin is list:
                    args = get_args(py_type)
                    if args:
                        inner_type = args[0]
                        try:
                            if isinstance(inner_type, type) and issubclass(
                                inner_type, BaseModel
                            ):
                                # Return a function that validates a list of models
                                return (
                                    lambda d, t=inner_type: [
                                        t.model_validate(item) for item in d
                                    ]
                                    if isinstance(d, list)
                                    else d
                                )
                        except Exception:
                            pass

                # Handle BaseModel types
                try:
                    if isinstance(py_type, type) and issubclass(py_type, BaseModel):
                        # Capture py_type in closure properly
                        return lambda d, t=py_type: t.model_validate(d)
                except Exception:
                    pass

                # Default passthrough
                return lambda x: x

            for name in arg_names:
                if name == node.meta.map_axis:
                    series_args.append(df[map_axis])
                    deserializers.append(_mk_deser(hints.get(name, Any)))
                elif name in constants:
                    # constants captured via closure (not as series)
                    series_args.append(None)
                    deserializers.append(lambda x: x)  # not used
                elif name in df.column_names:
                    series_args.append(df[name])
                    deserializers.append(_mk_deser(hints.get(name, Any)))
                else:
                    raise RuntimeError(
                        f"Parameter '{name}' not found among inputs/columns for node {node.fn.__name__}"
                    )

            # Create a mapping of which series index corresponds to which argument
            series_param_indices = []  # List of (arg_name, deserializer) for each series in call_series
            for idx, name in enumerate(arg_names):
                if series_args[idx] is not None:  # This is a series, not a constant
                    series_param_indices.append((name, deserializers[idx]))

            # Filter constants to only those needed by this node
            node_constants = {
                name: constants[name] for name in arg_names if name in constants
            }

            # Build a new DataFrame with the new column appended
            # We must pass only the Series columns to _apply_batch
            call_series = [c for c in series_args if c is not None]

            # Create the batch UDF with proper closure capture
            batch_udf = _make_batch_udf(
                node, arg_names, series_param_indices, node_constants, items
            )
            new_col_expr = batch_udf(*call_series)

            # Keep existing columns + add the new one
            keep_cols = [df[c].alias(c) for c in df.column_names]
            df = df.select(*keep_cols, new_col_expr.alias(node.meta.output_name))

        # Finalize: collect wanted outputs
        out_py = df.to_pylist()  # list of dicts per row, includes all columns
        merged: Dict[str, Any] = dict(constants)
        # normalize final output to List[List[RerankedHit]]
        final_name = order[-1].meta.output_name
        merged[final_name] = [
            [RerankedHit.model_validate(d) for d in row[final_name]] for row in out_py
        ]
        return merged


# --------------------------
# Demo
# --------------------------

if __name__ == "__main__":
    corpus = {
        "d1": "a quick brown fox jumps",
        "d2": "brown dog sleeps",
        "d3": "five boxing wizards jump quickly",
    }
    retriever = ToyRetriever(corpus)
    reranker = IdentityReranker()
    runner = Runner(mode="auto", batch_threshold=2)

    # Single input (local)
    single_inputs = {
        "retriever": retriever,
        "reranker": reranker,
        "query": Query(query_uuid="q1", text="quick brown"),
        "top_k": 2,  # shared by both nodes
    }
    single_out = runner.run(inputs=single_inputs)
    print("SINGLE (uniform batched shape):", single_out["reranked_hits"])

    # Multi input (auto → Daft if available, else Python fallback)
    multi_inputs = {
        "retriever": retriever,
        "reranker": reranker,
        "query": [
            Query(query_uuid="q1", text="quick brown"),
            Query(query_uuid="q2", text="wizards jump"),
            Query(query_uuid="q3", text="brown dog"),
        ],
        "top_k": 2,
    }
    multi_out = runner.run(inputs=multi_inputs)
    print("MULTI:", multi_out["reranked_hits"])
