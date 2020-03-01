"""
Test that nodes are not unnecessarily being recomputed during evaluation.
"""
import pytest

from .utils import call_count, Counter
from dagma import create_node, ComputeNode


@create_node
def add_one(x):
    return x + 1


@create_node
def sub_two(x):
    return x - 2


@create_node
def sum_(*x):
    return sum(x)


@pytest.fixture
def call_counter(monkeypatch):
    count = Counter()
    monkeypatch.setattr(
        ComputeNode, "_transform", call_count(count)(ComputeNode._transform)
    )

    return count


def test_transform_calls_one_node_value(call_counter):
    ao = add_one("x")
    out = ao(x=1)

    assert out.value == 2
    assert call_counter.count == 1

    call_counter.reset()

    assert out.value == 2
    assert call_counter.count == 0


def test_transform_calls_compute(call_counter):
    ao1 = add_one("x")
    st = sub_two(ao1)
    ao2 = add_one(st)
    s = sum_(st, ao2)

    assert s.compute(x=4) == 7
    assert call_counter.count == 4

    call_counter.reset()

    assert s.compute(x=4) == 7
    assert call_counter.count == 0

    call_counter.reset()

    assert s.compute(x=5) == 9
    assert call_counter.count == 4


def test_transform_calls_compute_force(call_counter):
    ao1 = add_one("x")
    st = sub_two(ao1)
    ao2 = add_one(st)
    s = sum_(st, ao2)

    assert s.compute(x=4, force=True) == 7
    assert call_counter.count == 6

    call_counter.reset()

    assert s.compute(x=4, force=True) == 7
    assert call_counter.count == 6

    call_counter.reset()

    assert s.compute(x=4, force=False) == 7
    assert call_counter.count == 0


def test_no_recompute_unrelated_var_change(call_counter):
    ao = add_one("x")

    ao.compute(x=1)
    assert call_counter.count == 1

    call_counter.reset()

    ao.compute(x=1, y=2)
    assert call_counter.count == 0


def test_no_recompute_subnode_unrelated_var_change(call_counter):
    ao = add_one("x")
    st = sub_two("y")
    s = sum_(ao, st)

    s.compute(x=1, y=2)
    assert call_counter.count == 3

    call_counter.reset()

    s.compute(x=1, y=3)
    assert call_counter.count == 2
