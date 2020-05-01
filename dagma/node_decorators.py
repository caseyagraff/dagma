import hashlib
from functools import partial

from .nodes import ComputeNode, ForeachComputeNode


def create_node(
    func=None,
    mem_cache=True,
    hash_alg=hashlib.md5,
    file_path=None,
    save=None,
    load=None,
    deps=None,
    foreach=None,
):
    """
    Decorator for converting transform function into a Node.

    Can be used with or without optional args.
    """

    def create(func, *deps, foreach=foreach):
        return (
            ComputeNode(
                func,
                mem_cache=mem_cache,
                hash_alg=hash_alg,
                file_path=file_path,
                save=save,
                load=load,
                deps=deps,
            )
            if foreach is None
            else ForeachComputeNode(
                func,
                foreach,
                mem_cache=mem_cache,
                hash_alg=hash_alg,
                file_path=file_path,
                save=save,
                load=load,
                deps=deps,
            )
        )

    if func is None:
        if deps is None:
            return lambda func: partial(create, func)
        else:
            return (
                lambda func: ComputeNode(
                    func,
                    mem_cache=mem_cache,
                    hash_alg=hash_alg,
                    file_path=file_path,
                    save=save,
                    load=load,
                    deps=deps,
                )
                if foreach is None
                else ForeachComputeNode(
                    func,
                    foreach,
                    mem_cache=mem_cache,
                    hash_alg=hash_alg,
                    file_path=file_path,
                    save=save,
                    load=load,
                    deps=deps,
                )
            )

    return partial(create, func)
