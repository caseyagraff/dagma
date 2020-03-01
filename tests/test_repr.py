from dagma import ConstantNode, VarNode, ComputeNode


def test_constant_node():
    val = 1
    assert str(val) in str(ConstantNode(1))


def test_var_node():
    var = "xyz"
    assert var in str(VarNode(var))


def test_compute_node():
    def foobar(x):
        return x

    assert foobar.__name__ in str(ComputeNode(foobar))
