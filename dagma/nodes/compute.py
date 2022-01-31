from datetime import datetime
import functools
import os.path
from typing import Any, Callable, Coroutine, ParamSpec, TypeVar, cast

from dagma.nodes.awaitable_value import AwaitableValue
from ..io import save_pickle, load_pickle

TReturn = TypeVar("TReturn")
TParams = ParamSpec("TParams")

TSaveFn = Callable[[Any, str], None]
TLoadFn = Callable[[str], Any]

DAGMA_DEBUG: bool = True


"""
Create a class Result that wraps any return values.
Can call .get() to extract the value, otherwise it just gets passed along.
If the class value is needed it will be awaited the first time and stored internally for
subsequent look ups.

Q: How to handle if we don't want to cache the value, but it is needed in the future? 
Require the user to either set cache=True or enable load/save, or re-execute it
themselves in the pipeline setup?
"""


def debug_wrap(
    func: Callable[TParams, Coroutine[Any, Any, TReturn]]
) -> Callable[TParams, Coroutine[Any, Any, TReturn]]:
    @functools.wraps(func)
    async def wrapper(*args: TParams.args, **kwargs: TParams.kwargs):
        print(f"Start: {func.__name__} {datetime.now()}")

        ret = await func(*args, **kwargs)

        print(f"End: {func.__name__} {datetime.now()}")

        return ret

    return wrapper


def load_save_wrap(
    func: Callable[TParams, Coroutine[Any, Any, TReturn]],
    file_name: str | Callable[TParams, str],
    save_fn: TSaveFn = save_pickle,
    load_fn: TLoadFn = load_pickle,
) -> Callable[TParams, Coroutine[Any, Any, TReturn]]:
    @functools.wraps(func)
    async def wrapper(*args: TParams.args, **kwargs: TParams.kwargs):
        file_name_str = (
            file_name if isinstance(file_name, str) else file_name(*args, **kwargs)
        )

        if os.path.exists(file_name_str):
            if DAGMA_DEBUG:
                print(f"Skipping: {func.__name__} {datetime.now()}")

            return cast(TReturn, load_fn(file_name_str))

        ret = await func(*args, **kwargs)

        save_fn(ret, file_name_str)

        return ret

    return wrapper


async def await_args(
    *args: Any | AwaitableValue[Any], **kwargs: Any | AwaitableValue[Any]
):
    args_awaited = [
        await arg.get() if isinstance(arg, AwaitableValue) else arg for arg in args
    ]
    kwargs_awaited = {
        arg_name: await arg.get() if isinstance(arg, AwaitableValue) else arg
        for (arg_name, arg) in kwargs.items()
    }

    return (args_awaited, kwargs_awaited)


def async_wrap(
    func: Callable[TParams, TReturn]
) -> Callable[TParams, Coroutine[Any, Any, TReturn]]:
    @functools.wraps(func)
    async def wrapper(*args: TParams.args, **kwargs: TParams.kwargs):

        [args_awaited, kwargs_awaited] = await await_args(*args, **kwargs)
        return func(*args_awaited, **kwargs_awaited)

    return wrapper


def awaitable_wrap(
    func: Callable[TParams, Coroutine[Any, Any, TReturn]]
) -> Callable[TParams, AwaitableValue[TReturn]]:
    @functools.wraps(func)
    def wrapper(*args: TParams.args, **kwargs: TParams.kwargs):
        ret = func(*args, **kwargs)
        return AwaitableValue(ret)

    return wrapper


def compute_node(
    file_name: str | Callable[TParams, str] | None = None,
) -> Callable[[Callable[TParams, TReturn]], Callable[..., AwaitableValue[TReturn]]]:
    def decorator(
        func: Callable[TParams, TReturn]
    ) -> Callable[TParams, AwaitableValue[TReturn]]:
        func_ = async_wrap(func)

        if DAGMA_DEBUG:
            func_ = debug_wrap(func_)

        if file_name is not None:
            func_ = load_save_wrap(func_, file_name=file_name)

        return awaitable_wrap(func_)

    return decorator
