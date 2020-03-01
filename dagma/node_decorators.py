from functools import partial

from .nodes import ComputeNode


def create_node(func=None, file_path=None, save=None, load=None, deps=None):
    """
    Decorator for converting transform function into a Node.

    Can be used with or without optional args.
    """

    def create(func, *deps):
        return ComputeNode(func, file_path=file_path, save=save, load=load, deps=deps)

    if func is None:
        if deps is None:
            return lambda func: partial(create, func)
        else:
            return lambda func: ComputeNode(
                func, file_path=file_path, save=save, load=load, deps=deps
            )

    return partial(create, func)
