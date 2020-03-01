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
def compute_call_counter(monkeypatch):
    count = Counter()
    monkeypatch.setattr(
        ComputeNode, "_compute", call_count(count)(ComputeNode._compute)
    )

    return count


def test_compute_calls_one_node_value(compute_call_counter):
    ao = add_one("x")
    out = ao(x=1)

    assert out.value == 2
    assert compute_call_counter.count == 1

    assert out.value == 2
    assert compute_call_counter.count == 1


counter2 = Counter()


def test_compute_calls_compute(compute_call_counter):
    ao1 = add_one("x")
    st = sub_two(ao1)
    ao2 = add_one(st)
    s = sum_(st, ao2)

    assert s.compute(x=4) == 7
    assert compute_call_counter.count == 5

    assert s.compute(x=4) == 7
    assert compute_call_counter.count == 6

    assert s.compute(x=5) == 9
    assert compute_call_counter.count == 11


def test_compute_calls_force(compute_call_counter):
    ao1 = add_one("x")
    st = sub_two(ao1)
    ao2 = add_one(st)
    s = sum_(st, ao2)

    assert s.compute(x=4, force=True) == 7
    assert compute_call_counter.count == 6

    assert s.compute(x=4, force=True) == 7
    assert compute_call_counter.count == 12

    assert s.compute(x=4, force=False) == 7
    assert compute_call_counter.count == 13
