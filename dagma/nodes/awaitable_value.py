from typing import Any, Coroutine, Generic, TypeVar


Value = TypeVar("Value")


class AwaitableValue(Generic[Value]):
    _value: Value | None = None
    _coroutine: Coroutine[Any, Any, Value]

    def __init__(self, coroutine: Coroutine[Any, Any, Value]) -> None:
        super().__init__()
        self._coroutine = coroutine

    async def get(self) -> Value:
        if self._value is None:
            self._value = await self._coroutine

        return self._value
