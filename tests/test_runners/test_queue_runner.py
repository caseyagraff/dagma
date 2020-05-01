import itertools as it

from dagma.runners import QueueRunner
from dagma import create_node, ConstantNode


@create_node
def one(x):
    pass


@create_node
def two(x, y):
    pass


@create_node
def three(x, y, z):
    pass


def get_ind(l, item):
    if isinstance(item, list):
        return [l.index(i) for i in item]
    else:
        return l.index(item)


def compare_inds(l, x, y):
    x = get_ind(l, x)
    y = get_ind(l, y)

    if not isinstance(x, list):
        x = [x]
    if not isinstance(y, list):
        y = [y]

    for i, j in it.product(x, y):
        if not i < j:
            return False

    return True


def check_order(l, order):
    for i in range(1, len(order)):
        if not compare_inds(l, order[i - 1], order[i]):
            raise AssertionError(f"Index of {order[i-1]} not less than {order[i]}")


def contains(l, items):
    if len(l) != len(items):
        raise AssertionError(f"{l} and {items} do not contain the same nodes")
    for i in items:
        if i not in l:
            raise AssertionError(f"{l} does not contain {i}")


def check_reverse_dep_list(l1, l2):
    assert l1.keys() == l2.keys()
    for k in l1.keys():
        contains(l1[k], l2[k])


class TestTopoSort:
    """Test the queue runners topological sort algorithm"""

    def setup_method(self):
        self.sink = ConstantNode(1)

    def test_topo_only_sink(self):
        runner = QueueRunner(self.sink)
        node_queue, reverse_dep_list = runner._build_topo_queue(self.sink, {}, True)

        assert node_queue[0] == self.sink
        assert len(reverse_dep_list) == 0

    def test_topo_one_compute(self):
        x = one(self.sink)

        runner = QueueRunner(x)
        node_queue, reverse_dep_list = runner._build_topo_queue(x, {}, True)

        check_order(node_queue, [self.sink, x])

        correct_reverse_dep_list = {self.sink: [x]}
        check_reverse_dep_list(correct_reverse_dep_list, reverse_dep_list)

    def test_topo_multi_dep(self):
        x = one(self.sink)
        y = one(x)
        z = two(x, y)
        o = one(z)

        runner = QueueRunner(o)
        node_queue, reverse_dep_list = runner._build_topo_queue(o, {}, True)

        check_order(node_queue, [self.sink, x, y, z, o])

        correct_reverse_dep_list = {self.sink: [x], x: [y, z], y: [z], z: [o]}
        check_reverse_dep_list(correct_reverse_dep_list, reverse_dep_list)

    def test_topo_shared_source(self):
        x = one(self.sink)
        y = one(self.sink)
        z = one(self.sink)

        o = three(x, y, z)

        runner = QueueRunner(o)
        node_queue, reverse_dep_list = runner._build_topo_queue(o, {}, True)

        check_order(node_queue, [self.sink, [x, y, z], o])

        correct_reverse_dep_list = {self.sink: [x, y, z], x: [o], y: [o], z: [o]}

        check_reverse_dep_list(correct_reverse_dep_list, reverse_dep_list)

    def test_topo_other(self):
        x = one(self.sink)
        y = one(self.sink)
        z1 = two(x, y)
        z2 = two(x, z1)
        o = one(z2)

        runner = QueueRunner(o)
        node_queue, reverse_dep_list = runner._build_topo_queue(o, {}, True)

        check_order(node_queue, [self.sink, [x, y], z1, z2, o])

        correct_reverse_dep_list = {
            self.sink: [x, y],
            x: [z1, z2],
            y: [z1],
            z1: [z2],
            z2: [o],
        }

        check_reverse_dep_list(correct_reverse_dep_list, reverse_dep_list)
