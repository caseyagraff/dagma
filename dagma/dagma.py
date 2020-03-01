from functools import partial

from .strings import STR_MISSING_VAR_DEPS

STR_COMPUTE_NODE_REPR = "(%s, dep=%s, bound=%s)"


class Node:
    def __init__(self):
        self._var_deps = set()
        self._node_deps = {}
        self._bound_vars = {}
        self._value = None

    def bind(self, vars_dict={}, **vars_):
        """
        Set values for graph variables.
        """
        self._bound_vars = {**vars_dict, **vars_}

    def bind_all(self, vars_dict={}, **vars_):
        """
        Set values for graph variables for all nodes in dep tree.
        """
        self.bind(vars_dict, **vars_)

        for d in self._node_deps:
            d.bind_all(vars_dict, **vars_)

    @staticmethod
    def _compare_vars(vars1, vars2):
        return vars1 == vars2

    def compute(self, vars_dict={}, force=False, **vars_):
        _bound_vars = {**self._bound_vars, **vars_dict, **vars_}

        return self._compute(_bound_vars, force=force)

    def _compute(self, vars_, force=False):
        raise NotImplementedError()

    def __call__(self, vars_dict={}, **vars_):
        """
        Set values for graph variables for all nodes in dep tree.
        """
        self.bind_all(vars_dict, **vars_)

        return self

    @property
    def value(self):
        if self._value is None or not self._compare_vars(
            self._value[1], self._bound_vars
        ):
            self._value = (self._compute(self._bound_vars), self._bound_vars)

        return self._value[0]


class ConstantNode(Node):
    def __init__(self, val):
        super().__init__()

        self._val = val

    def _compute(self, vars_, force=False):
        return self._val

    def __repr__(self):
        return f"const({self._val})"


class VarNode(Node):
    def __init__(self, var):
        super().__init__()

        self._var = var
        self._var_deps = {var}

    def _compute(self, vars_, force=False):
        return vars_[self._var]

    def __repr__(self):
        return f"var({self._var})"


class ComputeNode(Node):
    def __init__(self, transform, save=None, load=None, deps=None):
        """
        Args:
            transform: function to apply to node's input
            save: save function for node's output
            load: load function for node's saved output
        """
        super().__init__()

        self._transform_fn = transform
        self._save_fn = save
        self._load_fn = load

        self._node_deps, self._var_deps = self._parse_deps(deps)

    def _compute(self, vars_, force=False):
        """
        Evaluate the graph with inputs vars. Can force to recompute all nodes.
        """
        print(self)
        # Check if any required vars are not specified in vars_
        missing_var_deps = self._get_missing_var_deps(vars_)
        if missing_var_deps:
            raise ValueError(STR_MISSING_VAR_DEPS % missing_var_deps)

        if force or not self._value or not self._compare_vars(self._value[1], vars_):
            print(force, not self._value)
            # Compute dependent values
            dep_vals = [d._compute(vars_, force=force) for d in self._node_deps]

            self._value = (self._transform_fn(*dep_vals), vars_)

        return self._value[0]

    def save(self, path: str):
        """
        Save the result of this node's transformation.
        """
        if self._save_fn:
            return self._save_fn(path)

        raise NotImplementedError()

    def load(self, path: str):
        """
        Load the result of this node's transformation.
        """
        if self._load_fn:
            return self._load_fn(path)

        raise NotImplementedError()

    def graph(self):
        """
        Create graph representation.
        """
        return (self, [d.graph() for d in self._node_deps])

    @classmethod
    def _parse_deps(cls, deps):
        if deps is None:
            return [], set()

        node_deps = [cls._convert_input(in_) for in_ in deps]
        var_deps = {vd for n in node_deps for vd in n._var_deps}

        return node_deps, var_deps

    @staticmethod
    def _convert_input(in_):
        if isinstance(in_, Node):
            return in_
        elif isinstance(in_, str):
            return VarNode(in_)
        else:
            return ConstantNode(in_)

    def _get_missing_var_deps(self, vars_):
        return self._var_deps.difference(set(vars_.keys()))

    def __repr__(self):
        return STR_COMPUTE_NODE_REPR % (
            self._transform_fn.__name__,
            self._var_deps,
            self._bound_vars,
        )


def create_node(func=None, load=None, save=None, deps=None):
    """
    Decorator for converting transform function into a Node.

    Can be used with or without optional args.
    """

    def create(func, *deps):
        return ComputeNode(func, load, save, deps)

    if func is None:
        if deps is None:
            return lambda func: partial(create, func)
        else:
            return lambda func: ComputeNode(func, load, save, deps)

    return partial(create, func)
