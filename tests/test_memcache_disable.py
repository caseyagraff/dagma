from dagma import create_node, QueueRunner


@create_node(mem_cache=False)
def add_one(x):
    return x + 1


def test_memcache_disable():
    ao = add_one("x")

    out = QueueRunner(ao)

    assert out.compute(x=123) == 124
    assert not ao._mem_cache
    assert ao._value is None
