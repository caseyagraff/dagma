import pytest

from dagma import create_node, QueueRunner


def _add_one(x):
    return x + 1


add_one = create_node(_add_one)


@create_node
def sub_two(x):
    return x - 2


def _sum_all(*x):
    return sum(x)


sum_all = create_node(_sum_all)


def _sum_list(x):
    return sum(x)


sum_list = create_node(_sum_list)


def _multiply_two(x):
    return x * 2


multiply_two = create_node(_multiply_two)


def test_one_node_pipeline():
    out = add_one("x")

    out = out(x=1)

    out = QueueRunner(out)

    assert out.value == 2


def test_two_node_pipeline():
    out = add_one("x")
    out = multiply_two(out)

    out = out(x=2)

    out = QueueRunner(out)

    assert out.value == 6


def test_constant_pipeline():
    out = add_one(1)
    out = out()

    out = QueueRunner(out)
    assert out.value == 2


def test_multi_dep_pipeline():
    o1 = add_one("x")
    o2 = sub_two("y")
    out = sum_all(o1, o2)

    out = out(x=1, y=4)

    out = QueueRunner(out)

    assert out.value == 4


def test_full_pipeline():
    o1 = add_one("x")
    o2 = sub_two("y")
    out = sum_all(o1, o2)
    out = sum_all(o1, out)
    out = multiply_two(out)

    out = out(x=1, y=4)

    o1 = QueueRunner(o1)
    out = QueueRunner(out)

    assert o1.value == 2
    assert out.value == 12


def test_vars_dict():
    o1 = add_one("x")
    o2 = sub_two("y")
    out = sum_all(o1, o2)
    out = sum_all(o1, out)
    out = multiply_two(out)

    out = out({"x": 1, "y": 4})

    out = QueueRunner(out)

    assert out.value == 12


def test_vars_dict_override():
    o1 = add_one("x")
    o2 = sub_two("y")
    out = sum_all(o1, o2)
    out = sum_all(o1, out)
    out = multiply_two(out)

    out = out({"x": 1, "y": 4}, x=2)

    out = QueueRunner(out)

    assert out.value == 16


def test_missing_graph_inputs_throws():
    out = add_one("x")
    out = multiply_two(out)

    out = out()

    out = QueueRunner(out)

    with pytest.raises(ValueError):
        out.value


def test_mixed_deps():
    out = add_one("x")
    out = sum_all(1, out)

    out = out(x=2)

    out = QueueRunner(out)

    assert out.value == 4


def test_force_compute():
    o1 = add_one("x")
    o2 = sub_two("y")
    out = sum_all(o1, o2)
    out = sum_all(o1, out)
    out = multiply_two(out)

    out = out(y=4)

    out = QueueRunner(out)

    assert out.compute(x=2) == 16


def test_reusing_pipeline():
    ao = add_one("x")

    out = ao(x=2)

    out = QueueRunner(out)

    assert out.value == 3
    assert out.compute(x=3) == 4


def test_reusing_pipeline_nested():
    ao = add_one("x")
    mt = multiply_two(ao)

    out = mt(x=2)

    out = QueueRunner(out)

    assert out.value == 6
    assert out.compute(x=3) == 8


def test_shared_binding():
    ao = add_one("x")
    st = sub_two("y")
    s = sum_all(ao, st)

    out = s(x=2)

    out = QueueRunner(out)

    assert out.compute(y=3) == 4
    assert out.compute(y=5) == 6


def test_reusing_pipeline_binding():
    ao = add_one("x")
    mt = multiply_two(ao)

    out = mt(x=2)

    out = QueueRunner(out)
    assert out.value == 6

    out = mt(x=3)

    out = QueueRunner(out)
    assert out.value == 8


def test_foreach_pipeline():
    xs = list(range(10))

    ao = add_one("xs", foreach="xs")
    mt = multiply_two(ao, foreach=0)
    s = sum_list(mt)

    print(s.text_graph())

    out = s(xs=xs)

    print(s.text_graph())
    out = QueueRunner(out)

    correct_val = _sum_list([_multiply_two(_add_one(v)) for v in xs])

    assert out.value == correct_val
