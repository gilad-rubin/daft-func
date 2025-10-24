"""Runner for executing DAG workflows with adaptive batching."""

import time
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

from daft_func.cache import (
    CacheConfig,
    CacheStats,
    compute_signature,
    get_item_cache_key,
    make_cache_key,
    signatures_match,
)
from daft_func.pipeline import Pipeline
from daft_func.progress import ProgressConfig, create_progress_bar
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
        self,
        pipeline: Pipeline,
        mode: str = "auto",
        batch_threshold: int = 2,
        cache_config: Optional[CacheConfig] = None,
        progress_config: Optional[ProgressConfig] = None,
    ):
        """Initialize runner with pipeline, execution mode and batch threshold.

        Args:
            pipeline: The Pipeline instance containing the DAG nodes
            mode: Execution mode ("local", "daft", or "auto")
            batch_threshold: Minimum number of items to trigger Daft batching in auto mode
            cache_config: Optional caching configuration
            progress_config: Optional progress bar configuration
        """
        self.pipeline = pipeline
        self.mode = mode
        self.batch_threshold = batch_threshold
        self.cache_config = cache_config or CacheConfig()
        self.progress_config = progress_config or ProgressConfig()

        # Get cache backend (always available, even if caching disabled)
        self.cache_backend = self.cache_config.backend

        # Track signatures for current run
        self._current_signatures: Dict[str, str] = {}

        # Track cache statistics
        self._cache_stats: Optional[CacheStats] = None

        # Progress bar (created per run)
        self._progress_bar = None

    def run(self, *, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the DAG given initial inputs.

        Args:
            inputs: Dictionary of input values including the map_axis (if applicable)

        Returns:
            Dictionary containing all outputs including final results
        """
        # Reset signatures for this run
        self._current_signatures = {}

        # Initialize cache stats if verbose logging enabled
        if self.cache_config.enabled and self.cache_config.verbose:
            self._cache_stats = CacheStats()
        else:
            self._cache_stats = None

        pipeline = self.pipeline

        # Determine if we are batching based on any node's map_axis param presence + list input
        map_axes = {n.meta.map_axis for n in pipeline.nodes if n.meta.map_axis}
        if len(map_axes) > 1:
            raise ValueError(f"This runner supports one map axis; found: {map_axes}")
        map_axis = next(iter(map_axes)) if map_axes else None

        batching_requested = False
        items_count = 1
        if map_axis and map_axis in inputs and isinstance(inputs[map_axis], list):
            batching_requested = True
            items_count = len(inputs[map_axis])

        # Determine batching strategy
        if self.mode == "local":
            batching = False
        elif self.mode == "daft":
            batching = True
        else:  # auto mode
            batching = (
                batching_requested and items_count >= self.batch_threshold
                if map_axis
                else False
            )

        # Initialize progress bar with all nodes
        # Get topological order to show nodes in execution order
        try:
            order = pipeline.topo(inputs)
            node_names = [node.meta.output_name for node in order]

            # Create and initialize progress bar
            self._progress_bar = create_progress_bar(self.progress_config)
            if self._progress_bar:
                self._progress_bar.initialize_nodes(node_names, items_count)
        except Exception:
            # If topo fails, we'll let the actual execution handle the error
            pass

        try:
            # Execute based on strategy
            if batching:
                result = self._run_batch(inputs, map_axis)
            elif batching_requested:
                # We have list inputs but using local/single execution - loop manually
                result = self._run_local_loop(inputs, map_axis)
            else:
                # True single item execution
                result = self._run_single(inputs)
        finally:
            # Stop progress bar
            if self._progress_bar:
                self._progress_bar.stop()

        # Print cache summary if enabled
        if self._cache_stats:
            self._cache_stats.print_summary()

        return result

    def _run_single(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Execute DAG for a single item using pure Python."""
        pipeline = self.pipeline
        outputs = dict(inputs)
        order = pipeline.topo(inputs)

        for node in order:
            kwargs = {p: outputs[p] for p in node.params if p in outputs}

            # Check if caching is enabled for this node
            use_cache = (
                self.cache_config.enabled
                and node.meta.cache
                and self.cache_backend is not None
            )

            # Start progress tracking for this node
            if self._progress_bar:
                self._progress_bar.start_node(node.meta.output_name)

            cached_result = False

            if use_cache:
                # Determine if this is a map_axis node and get item key
                item_key = None
                if (
                    node.meta.map_axis
                    and self.cache_config.per_item_caching
                    and node.meta.map_axis in kwargs
                ):
                    item = kwargs[node.meta.map_axis]
                    item_key = get_item_cache_key(item, node.meta.key_attr)

                # Build cache key (includes item key for map_axis nodes)
                cache_key = make_cache_key(node.meta.output_name, item_key)

                # Compute signature for this node
                env_hash = node.meta.cache_key or self.cache_config.env_hash
                new_sig = compute_signature(
                    node_name=cache_key,  # Use extended key for per-item caching
                    fn=node.fn,
                    kwargs=kwargs,
                    parent_sigs=self._current_signatures,
                    env_hash=env_hash,
                    dependency_depth=self.cache_config.dependency_depth,
                    serialization_depth=self.cache_config.serialization_depth,
                )

                # Check for cache hit
                stored_sig = self.cache_backend.get_meta(cache_key)
                cache_hit = stored_sig is not None and signatures_match(
                    new_sig, stored_sig
                )

                if cache_hit:
                    # Cache hit - try to load from blob store
                    cached_value = self.cache_backend.get_blob(cache_key)
                    if cached_value is not None:
                        outputs[node.meta.output_name] = cached_value
                        # Store signature for downstream nodes
                        sig_str = f"{new_sig.code_hash}{new_sig.env_hash}{new_sig.inputs_hash}{new_sig.deps_hash}"
                        self._current_signatures[node.meta.output_name] = sig_str

                        # Record cache hit event
                        if self._cache_stats:
                            self._cache_stats.record(
                                node_name=node.meta.output_name,
                                cache_enabled=True,
                                cache_hit=True,
                                loaded=True,
                            )

                        # Mark as cached for progress bar
                        cached_result = True
                        execution_time = 0.0

                        # Complete progress for this node
                        if self._progress_bar:
                            self._progress_bar.complete_node(
                                node.meta.output_name,
                                execution_time=execution_time,
                                cached=True,
                            )
                        continue

                # Cache miss or failed to load - execute node
                start_time = time.time()
                res = node.fn(**kwargs)
                execution_time = time.time() - start_time
                outputs[node.meta.output_name] = res

                # Save to cache
                self.cache_backend.set_blob(cache_key, res)
                self.cache_backend.set_meta(new_sig)

                # Store signature for downstream nodes
                sig_str = f"{new_sig.code_hash}{new_sig.env_hash}{new_sig.inputs_hash}{new_sig.deps_hash}"
                self._current_signatures[node.meta.output_name] = sig_str

                # Record cache miss event
                if self._cache_stats:
                    self._cache_stats.record(
                        node_name=node.meta.output_name,
                        cache_enabled=True,
                        cache_hit=False,
                        loaded=False,
                        execution_time=execution_time,
                    )
            else:
                # No caching - just execute
                start_time = time.time()
                res = node.fn(**kwargs)
                execution_time = time.time() - start_time
                outputs[node.meta.output_name] = res

                # Record no-cache event
                if self._cache_stats:
                    self._cache_stats.record(
                        node_name=node.meta.output_name,
                        cache_enabled=False,
                        cache_hit=False,
                        loaded=False,
                        execution_time=execution_time,
                    )

            # Complete progress for this node (if not already done for cache hit)
            if self._progress_bar and not cached_result:
                self._progress_bar.complete_node(
                    node.meta.output_name,
                    execution_time=execution_time,
                    cached=False,
                )

        return outputs

    def _run_local_loop(
        self, inputs: Dict[str, Any], map_axis: Optional[str]
    ) -> Dict[str, Any]:
        """Execute DAG for multiple items using Python loop (no Daft)."""
        assert map_axis, "map_axis required for local loop"

        items = inputs[map_axis]
        constants = {k: v for k, v in inputs.items() if k != map_axis}
        aggregated: List[Dict[str, Any]] = []

        # Track total execution time per node
        node_times: Dict[str, float] = {}
        node_cache_hits: Dict[str, int] = {}

        # Get node order
        pipeline = self.pipeline
        order = pipeline.topo({**constants, map_axis: items[0]})

        for item_idx, it in enumerate(items):
            # Reset signatures for each item
            self._current_signatures = {}
            per_inputs = {**constants, map_axis: it}

            # Temporarily disable progress bar for inner _run_single
            # We'll handle progress updates manually here
            saved_progress = self._progress_bar
            self._progress_bar = None

            item_start_time = time.time()
            per_out = self._run_single(per_inputs)
            aggregated.append(per_out)

            # Restore progress bar
            self._progress_bar = saved_progress

            # Update progress for each node after processing this item
            if self._progress_bar:
                for node_idx, node in enumerate(order):
                    node_name = node.meta.output_name

                    # On first item, mark node as executing
                    if item_idx == 0:
                        self._progress_bar.start_node(node_name)

                    # Update progress (this shows the item count progressing)
                    self._progress_bar.update_node_progress(
                        node_name,
                        completed=item_idx + 1,
                        total=len(items),
                    )

                    # On last item, complete the node
                    if item_idx == len(items) - 1:
                        # Estimate average time per node (rough approximation)
                        avg_time = (
                            (time.time() - item_start_time) / len(order) if order else 0
                        )
                        node_times[node_name] = node_times.get(node_name, 0) + avg_time

                        # Check if any cache hits occurred
                        has_cache_hits = node_cache_hits.get(node_name, 0) > 0

                        # Complete the node
                        self._progress_bar.complete_node(
                            node_name,
                            execution_time=node_times[node_name],
                            cached=has_cache_hits,
                        )

        # Merge structure: final outputs to lists
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

        # First pass: execute non-mapped functions once and add to constants
        # (These are functions without a map_axis that don't depend on mapped data)
        for node in order:
            if node.meta.map_axis is None:
                # This is a non-mapped function - execute it once
                kwargs = {p: constants[p] for p in node.params if p in constants}
                result = node.fn(**kwargs)
                constants[node.meta.output_name] = result

        # Second pass: process only mapped functions in batch
        mapped_nodes = [n for n in order if n.meta.map_axis is not None]

        # We'll iteratively add columns to df for each mapped node's output
        for node in mapped_nodes:
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
        merged: Dict[str, Any] = dict(constants)

        # If we have mapped nodes, collect their outputs from the dataframe
        if mapped_nodes:
            out_py = df.to_pylist()

            # Get final output name from the last mapped node
            final_name = mapped_nodes[-1].meta.output_name
            # Reconstruct Pydantic models from dicts
            hints = get_type_hints(mapped_nodes[-1].fn)
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
