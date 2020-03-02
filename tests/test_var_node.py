import pytest

from dagma import VarNode


def test_var_node_value():
    v = VarNode("x")

    assert v._evaluate({"x": 123}) == 123


def test_var_node_missing_var_raises():
    v = VarNode("x")

    with pytest.raises(ValueError):
        v._evaluate({})
