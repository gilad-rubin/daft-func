"""Debug instance IDs to understand the caching behavior."""

from typing import Dict

from daft_func import CacheConfig, MemoryCache, Pipeline, Runner, func
from daft_func.cache import compute_inputs_hash
from examples.retrieval import Retriever, ToyRetriever


@func(output="index_path", cache=True)
def index_str(retriever: Retriever, corpus: Dict[str, str]) -> str:
    """Returns str (not bool) - should NOT trigger force_instance_ids."""
    return retriever.index(corpus)


def test_instance_id_behavior():
    """Test how instance IDs are computed."""

    corpus = {
        "d1": "a quick brown fox jumps",
        "d2": "brown dog sleeps",
    }

    # Create three instances
    r1 = ToyRetriever()
    r2 = ToyRetriever()
    r3 = ToyRetriever()

    print("Instance IDs:")
    print(f"  r1: {id(r1)}")
    print(f"  r2: {id(r2)}")
    print(f"  r3: {id(r3)}")

    print("\nCache keys:")
    print(f"  r1.__cache_key__(): {r1.__cache_key__()}")
    print(f"  r2.__cache_key__(): {r2.__cache_key__()}")
    print(f"  r3.__cache_key__(): {r3.__cache_key__()}")

    # Test inputs_hash with force_instance_ids=False (str return type)
    print("\nInputs hash with force_instance_ids=False:")
    kwargs1 = {"retriever": r1, "corpus": corpus}
    kwargs2 = {"retriever": r2, "corpus": corpus}
    kwargs3 = {"retriever": r3, "corpus": corpus}

    hash1 = compute_inputs_hash(kwargs1, force_instance_ids=False)
    hash2 = compute_inputs_hash(kwargs2, force_instance_ids=False)
    hash3 = compute_inputs_hash(kwargs3, force_instance_ids=False)

    print(f"  hash1: {hash1}")
    print(f"  hash2: {hash2}")
    print(f"  hash3: {hash3}")
    print(f"  hash1 == hash2: {hash1 == hash2}")
    print(f"  hash2 == hash3: {hash2 == hash3}")

    # Test inputs_hash with force_instance_ids=True (bool return type)
    print("\nInputs hash with force_instance_ids=True:")
    hash1_forced = compute_inputs_hash(kwargs1, force_instance_ids=True)
    hash2_forced = compute_inputs_hash(kwargs2, force_instance_ids=True)
    hash3_forced = compute_inputs_hash(kwargs3, force_instance_ids=True)

    print(f"  hash1: {hash1_forced}")
    print(f"  hash2: {hash2_forced}")
    print(f"  hash3: {hash3_forced}")
    print(f"  hash1 == hash2: {hash1_forced == hash2_forced}")
    print(f"  hash2 == hash3: {hash2_forced == hash3_forced}")

    # Now test actual pipeline execution
    print("\n" + "=" * 70)
    print("Testing actual pipeline with MemoryCache:")
    print("=" * 70)

    pipeline = Pipeline(functions=[index_str])
    runner = Runner(
        mode="local",
        cache_config=CacheConfig(enabled=True, backend=MemoryCache()),
    )

    print("\nRun 1 (r1):")
    result1 = runner.run(pipeline, inputs={"retriever": r1, "corpus": corpus})

    print("\nRun 2 (r2):")
    result2 = runner.run(pipeline, inputs={"retriever": r2, "corpus": corpus})

    print("\nRun 3 (r3):")
    result3 = runner.run(pipeline, inputs={"retriever": r3, "corpus": corpus})


if __name__ == "__main__":
    test_instance_id_behavior()
