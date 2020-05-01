from dagma.nodes import ComputeNode, ForeachComputeNode, NodeGraphType


def zero():
    pass


def one(x):
    pass


def two(x, y):
    pass


def add_one(x):
    return x + 1


def add(x, y):
    return x + y


def sum_all(*x):
    return sum(x)


# === Evaluate ===
def test_evaluate_constants():
    const_deps = [1, 3]
    x = ComputeNode(add, deps=const_deps)

    assert x._evaluate({}, const_deps) == 4


def test_evaluate_vars():
    var_deps = {"x": 1, "y": 3}
    x = ComputeNode(add, deps=list(var_deps.keys()))

    assert x._evaluate(var_deps, list(var_deps.values())) == 4


# === Test Foreach ===
def test_foreach():
    var_deps = {"xs": list(range(10))}
    x = ForeachComputeNode(add_one, deps=["xs"], foreach="xs")

    print(x.text_graph())

    x = x(**var_deps)
    val = x._evaluate(var_deps, list(var_deps.values()))

    correct_val = [v + 1 for v in var_deps["xs"]]
    assert val == correct_val


def test_aggregate_foreach():
    var_deps = {"xs": list(range(10))}
    x = ForeachComputeNode(add_one, deps=["xs"], foreach="xs")
    s = ComputeNode(sum_all, deps=[x])

    s = s(**var_deps)

    val = x._evaluate(var_deps, list(var_deps.values()))
    val = s._evaluate(var_deps, val)

    correct_val = sum([add_one(v) for v in var_deps["xs"]])
    assert val == correct_val


def test_chain_foreach():
    var_deps = {"xs": list(range(10))}
    x = ForeachComputeNode(add_one, deps=["xs"], foreach="xs")
    y = ForeachComputeNode(add_one, deps=[x], foreach=0)

    y = y(**var_deps)

    val = x._evaluate(var_deps, list(var_deps.values()))
    val = y._evaluate(var_deps, [val])

    correct_val = [add_one(add_one(v)) for v in var_deps["xs"]]
    assert val == correct_val


# === Test Graph Display Methods ===
def test_graph_one_node():
    x = ComputeNode(zero)

    correct_graph: NodeGraphType = (x, [])
    assert correct_graph == x.graph()


def test_graph_chain():
    x = ComputeNode(zero)
    y = ComputeNode(one, deps=[x])
    z = ComputeNode(one, deps=[y])

    correct_graph: NodeGraphType = (z, [(y, [(x, [])])])
    assert correct_graph == z.graph()


def test_graph_multi_source():
    x = ComputeNode(zero)
    y1 = ComputeNode(one, deps=[x])
    y2 = ComputeNode(one, deps=[x])
    z = ComputeNode(two, deps=[y1, y2])

    correct_graph: NodeGraphType = (z, [(y1, [(x, [])]), (y2, [(x, [])])])
    assert correct_graph == z.graph()


def test_print_graph():
    x1 = ComputeNode(one, deps=["x"])
    x2 = ComputeNode(one, deps=["x"])
    y = ComputeNode(two, deps=[x1, x2])
    z = ComputeNode(two, deps=[y, x2])

    assert z.text_graph() != ""
