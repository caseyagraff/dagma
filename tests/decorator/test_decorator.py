import pytest
from dagma.decorators.nodes import compute_node


@pytest.mark.asyncio
async def test_decorator():
    def add(x: int, y: int) -> int:
        return x + y

    value = add(2, 3)

    add_decorator = compute_node()(add)

    assert await add_decorator(2, 3) == value
