import time
from dagma import create_node, ThreadRunner


@create_node
def add_one(x):
    return x + 1


@create_node
def sub_two(x):
    return x - 2


@create_node
def sum_(*x):
    return sum(x)


@create_node
def slow_add_one(x):
    time.sleep(0.1)
    return x + 1


@create_node
def multiply_two(x):
    return x * 2


def test_one_node_pipeline():
    out = add_one("x")

    out = out(x=1)

    out = ThreadRunner(out)

    assert out.value == 2


def test_two_node_pipeline():
    out = add_one("x")
    out = multiply_two(out)

    out = out(x=2)

    out = ThreadRunner(out)

    assert out.value == 6


def test_full_pipeline():
    o1 = add_one("x")
    o2 = sub_two("y")
    out = sum_(o1, o2)
    out = sum_(o1, out)
    out = multiply_two(out)

    out = out(x=1, y=4)

    out = ThreadRunner(out)

    assert out.value == 12


def test_full_pipeline_multi_threaded():
    o1 = add_one("x")
    o2 = sub_two("y")
    out = sum_(o1, o2)
    out = sum_(o1, out)
    out = multiply_two(out)

    out = out(x=1, y=4)

    out = ThreadRunner(out, num_workers=2)

    assert out.value == 12


def test_long_io():
    """
    Each slow add sleeps for .1 seconds, total sleep time is 1 second. Therefore,
    requires use of multithreading to compute in under 1 second.
    """
    adds = [slow_add_one("x") for _ in range(10)]
    out = sum_(*adds)

    out = out(x=1)

    out = ThreadRunner(out, num_workers=10)

    start_time = time.time()
    computed = out.compute()
    elapsed = time.time() - start_time

    assert computed == 20
    assert elapsed < 0.5
