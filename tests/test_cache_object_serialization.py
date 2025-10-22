"""Test cache serialization strategies for objects with __cache_key__ and automatic serialization."""

import pytest

from daft_func.cache import compute_inputs_hash


class SimpleConfig:
    """Object with simple configuration for testing automatic serialization."""

    def __init__(self, value: int, name: str = "default"):
        self.value = value
        self.name = name


class NestedConfig:
    """Object with nested attributes to test depth limits."""

    def __init__(self, simple: SimpleConfig, level: int = 1):
        self.simple = simple
        self.level = level


class CustomCacheKey:
    """Object that defines its own cache key."""

    def __init__(self, config: dict = {}):
        self.config = config

    def __cache_key__(self):
        """Return deterministic cache key based on configuration."""
        import json

        return f"{self.__class__.__name__}::{json.dumps(self.config, sort_keys=True)}"


class StatelessObject:
    """Stateless object (no public attributes)."""

    def __cache_key__(self):
        """Return class name as cache key."""
        return self.__class__.__name__


def test_cache_key_method_takes_precedence():
    """Objects with __cache_key__() should use it, ignoring other attributes."""
    obj1 = CustomCacheKey({"a": 1, "b": 2})
    obj2 = CustomCacheKey({"a": 1, "b": 2})
    obj3 = CustomCacheKey({"a": 2, "b": 1})

    hash1 = compute_inputs_hash({"obj": obj1})
    hash2 = compute_inputs_hash({"obj": obj2})
    hash3 = compute_inputs_hash({"obj": obj3})

    # Same config = same hash
    assert hash1 == hash2, "Objects with same config should hash identically"
    # Different config = different hash
    assert hash1 != hash3, "Objects with different config should hash differently"


def test_automatic_serialization_simple_objects():
    """Objects without __cache_key__() should be automatically serialized."""
    obj1 = SimpleConfig(value=42, name="test")
    obj2 = SimpleConfig(value=42, name="test")
    obj3 = SimpleConfig(value=99, name="test")

    hash1 = compute_inputs_hash({"obj": obj1})
    hash2 = compute_inputs_hash({"obj": obj2})
    hash3 = compute_inputs_hash({"obj": obj3})

    # Same attributes = same hash (no object ID!)
    assert hash1 == hash2, "Objects with same attributes should hash identically"
    # Different attributes = different hash
    assert hash1 != hash3, "Objects with different attributes should hash differently"


def test_nested_object_serialization_depth():
    """Nested objects should be serialized up to the specified depth."""
    simple1 = SimpleConfig(value=42, name="inner")
    simple2 = SimpleConfig(value=42, name="inner")

    nested1 = NestedConfig(simple=simple1, level=1)
    nested2 = NestedConfig(simple=simple2, level=1)

    # Default depth is 2, should capture nested object
    hash1 = compute_inputs_hash({"obj": nested1}, serialization_depth=2)
    hash2 = compute_inputs_hash({"obj": nested2}, serialization_depth=2)

    assert hash1 == hash2, "Nested objects should hash identically at depth 2"


def test_depth_limit_prevents_deep_serialization():
    """Objects deeper than the limit should use object ID."""
    simple1 = SimpleConfig(value=42, name="inner")
    simple2 = SimpleConfig(value=42, name="inner")

    nested1 = NestedConfig(simple=simple1, level=1)
    nested2 = NestedConfig(simple=simple2, level=1)

    # At depth 1:
    # - NestedConfig at depth 0 gets serialized
    # - Its 'simple' attribute at depth 1 reaches the limit, uses object ID
    hash1 = compute_inputs_hash({"obj": nested1}, serialization_depth=1)
    hash2 = compute_inputs_hash({"obj": nested2}, serialization_depth=1)

    # Different instances at depth limit should hash differently (uses object ID)
    assert hash1 != hash2, "At depth limit, nested objects should use ID"

    # But at depth 2, they should serialize fully and match
    hash3 = compute_inputs_hash({"obj": nested1}, serialization_depth=2)
    hash4 = compute_inputs_hash({"obj": nested2}, serialization_depth=2)
    assert hash3 == hash4, "At depth 2, should fully serialize and match"


def test_depth_zero_uses_object_id():
    """Depth 0 should immediately fall back to object ID."""
    obj1 = SimpleConfig(value=42, name="test")
    obj2 = SimpleConfig(value=42, name="test")

    hash1 = compute_inputs_hash({"obj": obj1}, serialization_depth=0)
    hash2 = compute_inputs_hash({"obj": obj2}, serialization_depth=0)

    # Different instances should hash differently at depth 0
    assert hash1 != hash2, "At depth 0, different instances should hash differently"


def test_stateless_object_with_cache_key():
    """Stateless objects should use __cache_key__() if defined."""
    obj1 = StatelessObject()
    obj2 = StatelessObject()

    hash1 = compute_inputs_hash({"obj": obj1})
    hash2 = compute_inputs_hash({"obj": obj2})

    assert hash1 == hash2, (
        "Stateless objects should hash identically via __cache_key__()"
    )


def test_mixed_objects_in_dict():
    """Test hashing dict with mix of object types."""
    inputs = {
        "simple": SimpleConfig(value=42, name="test"),
        "custom": CustomCacheKey({"x": 1}),
        "stateless": StatelessObject(),
        "primitive": 123,
        "text": "hello",
    }

    hash1 = compute_inputs_hash(inputs)

    # Create new instances with same values
    inputs2 = {
        "simple": SimpleConfig(value=42, name="test"),
        "custom": CustomCacheKey({"x": 1}),
        "stateless": StatelessObject(),
        "primitive": 123,
        "text": "hello",
    }

    hash2 = compute_inputs_hash(inputs2)

    assert hash1 == hash2, "Mixed object types should hash consistently"


def test_private_attributes_excluded():
    """Private attributes (starting with _) should not affect hash."""

    class ObjectWithPrivate:
        def __init__(self, public: int, private: int):
            self.public = public
            self._private = private

    obj1 = ObjectWithPrivate(public=42, private=1)
    obj2 = ObjectWithPrivate(public=42, private=999)

    hash1 = compute_inputs_hash({"obj": obj1})
    hash2 = compute_inputs_hash({"obj": obj2})

    assert hash1 == hash2, "Private attributes should not affect hash"


def test_objects_in_lists():
    """Test that objects in lists are serialized correctly."""
    objs1 = [SimpleConfig(value=1, name="a"), SimpleConfig(value=2, name="b")]
    objs2 = [SimpleConfig(value=1, name="a"), SimpleConfig(value=2, name="b")]

    hash1 = compute_inputs_hash({"objs": objs1})
    hash2 = compute_inputs_hash({"objs": objs2})

    assert hash1 == hash2, "Lists of objects should hash identically"


def test_dict_values_serialization():
    """Test that dict values are properly serialized."""
    d1 = {"key": SimpleConfig(value=42, name="test")}
    d2 = {"key": SimpleConfig(value=42, name="test")}

    hash1 = compute_inputs_hash({"data": d1})
    hash2 = compute_inputs_hash({"data": d2})

    assert hash1 == hash2, "Dict values should be serialized consistently"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
