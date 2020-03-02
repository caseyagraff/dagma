from dagma import create_node, QueueRunner


@create_node
def add_one(x):
    return x + 1


@create_node
def subtract_two(x):
    return x - 2


@create_node
def sum_all(x, y, z):
    return x + y + z


@create_node(deps=[add_one("x")])
def multiply_two(x):
    return x * 2


@create_node(deps=[add_one("x")])
def divide_two(x):
    return x // 2


@create_node(deps=[add_one("x"), subtract_two("y")])
def add_together(x, y):
    return x + y


@create_node(deps=[multiply_two, divide_two])
def subtract_together(x, y):
    return x - y


@create_node(deps=[add_one("x"), "y"])
def remainder(x, y):
    return x % y


def test_one_node_pipeline():
    """
    1 -> add_one -> 2
    """
    a = add_one("x")
    out = a(x=1)

    out = QueueRunner(out)

    assert out.value == 2


def test_multiple_inputs_source():
    """
    1 3 5 -> sum_all -> 9
    """
    s = sum_all("x", "y", "z")
    out = s(x=1, y=3, z=5)

    out = QueueRunner(out)

    assert out.value == 9


def test_two_node_pipeline():
    """
    1 -> add_one -> 2 -> multiply_two -> 4
    """
    out = multiply_two(x=1)

    out = QueueRunner(out)

    assert out.value == 4


def test_multi_dep_pipeline():
    """
    2 -> add_one      -> 3 -> add_together -> 5
    4 -> subtract_one -> 2 ->
    """
    out = add_together(x=2, y=4)

    out = QueueRunner(out)

    assert out.value == 4 + 1


def test_same_source_multi_dep_pipeline():
    """
    1 -> add_one -> 2 -> multiply_two -> 4 -> subtract_together -> 3
                      -> divide_two   -> 1 ->

    """
    out = subtract_together(x=1)

    out = QueueRunner(out)

    assert out.value == 3


def test_dep_and_input():
    """
    5 -> add_one -> 6 -> remainder -> 2
         4       ->
    """
    out = remainder(x=5, y=4)

    out = QueueRunner(out)

    assert out.value
