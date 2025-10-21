"""Runner for executing DAG workflows with adaptive batching."""

from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    get_args,
    get_origin,
    get_type_hints,
)

from pydantic import BaseModel

from daft_func.pipeline import Pipeline
from daft_func.types import DAFT_AVAILABLE, daft_datatype

if DAFT_AVAILABLE:
    import daft


class Runner:
    """Execute DAG workflows with adaptive batching.

    Supports three execution modes:
    - local: Pure Python, no Daft (loops over items manually)
    - daft: Forces Daft batch execution
    - auto: Automatically chooses based on batch size threshold
    """

    def __init__(
        self, pipeline: Pipeline, mode: str = "auto", batch_threshold: int = 2
    ):
        """Initialize runner with pipeline, execution mode and batch threshold.

        Args:
            pipeline: The Pipeline instance containing the DAG nodes
            mode: Execution mode ("local", "daft", or "auto")
            batch_threshold: Minimum number of items to trigger Daft batching in auto mode
        """
        self.pipeline = pipeline
        self.mode = mode
        self.batch_threshold = batch_threshold

    def run(self, *, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the DAG given initial inputs.

        Args:
            inputs: Dictionary of input values including the map_axis (if applicable)

        Returns:
            Dictionary containing all outputs including final results
        """
        pipeline = self.pipeline

        # Determine if we are batching based on any node's map_axis param presence + list input
        map_axes = {n.meta.map_axis for n in pipeline.nodes if n.meta.map_axis}
        if len(map_axes) > 1:
            raise ValueError(f"This runner supports one map axis; found: {map_axes}")
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
            return self._run_local_loop(inputs, map_axis)
        else:
            # True single item execution
            return self._run_single(inputs)

    def _run_single(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Execute DAG for a single item using pure Python."""
        pipeline = self.pipeline
        outputs = dict(inputs)
        order = pipeline.topo(inputs)

        for node in order:
            kwargs = {p: outputs[p] for p in node.params if p in outputs}
            res = node.fn(**kwargs)
            outputs[node.meta.output_name] = res

        return outputs

    def _run_local_loop(
        self, inputs: Dict[str, Any], map_axis: Optional[str]
    ) -> Dict[str, Any]:
        """Execute DAG for multiple items using Python loop (no Daft)."""
        assert map_axis, "map_axis required for local loop"

        items = inputs[map_axis]
        constants = {k: v for k, v in inputs.items() if k != map_axis}
        aggregated: List[Dict[str, Any]] = []

        for it in items:
            per_inputs = {**constants, map_axis: it}
            per_out = self._run_single(per_inputs)
            aggregated.append(per_out)

        # Merge structure: final outputs to lists
        pipeline = self.pipeline
        order = pipeline.topo({**constants, map_axis: items[0]})
        final_output_name = order[-1].meta.output_name if order else None

        merged: Dict[str, Any] = dict(constants)
        if final_output_name:
            merged[final_output_name] = [o[final_output_name] for o in aggregated]

        return merged

    def _run_batch(
        self, inputs: Dict[str, Any], map_axis: Optional[str]
    ) -> Dict[str, Any]:
        """Execute DAG for multiple items using Daft batch processing."""
        assert map_axis, "No map axis configured but batching was requested."

        items: List[BaseModel] = inputs[map_axis]
        constants = {k: v for k, v in inputs.items() if k != map_axis}

        if not DAFT_AVAILABLE:
            # Fallback to Python loop
            return self._run_local_loop(inputs, map_axis)

        # Build initial Daft DF with one column for the map_axis (dictified Pydantic)
        df = daft.from_pylist([{map_axis: it.model_dump()} for it in items])

        pipeline = self.pipeline
        order = pipeline.topo({**constants, map_axis: items[0]})

        # We'll iteratively add columns to df for each node's output
        for node in order:
            arg_names = node.params
            series_args: List[Any] = []
            deserializers: List[Callable[[Dict], Any]] = []

            # Figure out types from hints (for Pydantic reconstruction)
            hints = get_type_hints(node.fn)

            def _mk_deser(py_type):
                """Create deserializer for a given type, handling Pydantic models and lists."""
                origin = get_origin(py_type)
                if origin is list:
                    args = get_args(py_type)
                    if args:
                        inner_type = args[0]
                        try:
                            if isinstance(inner_type, type) and issubclass(
                                inner_type, BaseModel
                            ):
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
                        return lambda d, t=py_type: t.model_validate(d)
                except Exception:
                    pass

                return lambda x: x

            for name in arg_names:
                if name == node.meta.map_axis:
                    series_args.append(df[map_axis])
                    deserializers.append(_mk_deser(hints.get(name, Any)))
                elif name in constants:
                    series_args.append(None)
                    deserializers.append(lambda x: x)
                elif name in df.column_names:
                    series_args.append(df[name])
                    deserializers.append(_mk_deser(hints.get(name, Any)))
                else:
                    raise RuntimeError(
                        f"Parameter '{name}' not found among inputs/columns for node {node.fn.__name__}"
                    )

            # Create mapping of series index to parameter
            series_param_indices = []
            for idx, name in enumerate(arg_names):
                if series_args[idx] is not None:
                    series_param_indices.append((name, deserializers[idx]))

            # Filter constants to only those needed by this node
            node_constants = {
                name: constants[name] for name in arg_names if name in constants
            }

            # Build a new DataFrame with the new column appended
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
        out_py = df.to_pylist()
        merged: Dict[str, Any] = dict(constants)

        # Get final output name
        final_name = order[-1].meta.output_name if order else None
        if final_name:
            # Reconstruct Pydantic models from dicts
            hints = get_type_hints(order[-1].fn)
            return_type = hints.get("return", Any)

            # Check if return type is List[BaseModel]
            origin = get_origin(return_type)
            if origin is list:
                args = get_args(return_type)
                if (
                    args
                    and isinstance(args[0], type)
                    and issubclass(args[0], BaseModel)
                ):
                    model_cls = args[0]
                    merged[final_name] = [
                        [model_cls.model_validate(d) for d in row[final_name]]
                        for row in out_py
                    ]
                else:
                    merged[final_name] = [row[final_name] for row in out_py]
            elif isinstance(return_type, type) and issubclass(return_type, BaseModel):
                # Single BaseModel return type
                merged[final_name] = [
                    return_type.model_validate(row[final_name]) for row in out_py
                ]
            else:
                merged[final_name] = [row[final_name] for row in out_py]

        return merged


def _make_batch_udf(node, arg_names, series_param_indices, node_constants, items):
    """Factory function to create batch UDF with proper variable capture."""

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
        # Convert Series to pylist per column
        ser_lists: List[List[Any]] = []
        for c in cols:
            ser_lists.append(c.to_pylist())

        out_list: List[Any] = []
        rows = len(ser_lists[0]) if ser_lists else len(items)

        for i in range(rows):
            kwargs: Dict[str, Any] = {}
            # Add constants
            kwargs.update(node_constants)
            # Add series-based arguments
            for ser_idx, (param_name, deser) in enumerate(series_param_indices):
                raw = ser_lists[ser_idx][i]
                kwargs[param_name] = deser(raw)

            # Call the original node function
            res = node.fn(**kwargs)

            # Store as dict (Pydantic -> dict; list[Pydantic] -> list[dict])
            if isinstance(res, BaseModel):
                out_list.append(res.model_dump())
            elif isinstance(res, list) and res and isinstance(res[0], BaseModel):
                out_list.append([r.model_dump() for r in res])
            else:
                out_list.append(res)

        return out_list

    return _apply_batch
