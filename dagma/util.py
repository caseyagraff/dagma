import itertools
from typing import List, TypeVar

from dagma.nodes.compute import awaitable_wrap

from .nodes import AwaitableValue


Value = TypeVar("Value")


@awaitable_wrap
async def flatten_awaitable(values: List[AwaitableValue[List[Value]]]) -> List[Value]:
    return list(itertools.chain(*[await x.get() for x in values]))
