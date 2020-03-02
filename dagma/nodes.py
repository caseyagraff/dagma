import hashlib

from .strings import (
    STR_COMPUTE_NODE_REPR,
    STR_MISSING_VAR_DEPS,
    STR_NO_SAVE_FUNC,
    STR_NO_LOAD_FUNC,
    STR_SAVE_NOT_COMPUTED,
    STR_LOAD_NOT_VARS_MATCH,
)

from .file_io import VarsFile, CustomFile


class Node:
    def __init__(self, mem_cache=True):
        self._mem_cache = mem_cache

        self._var_deps = set()
        self._node_deps = {}
        self._bound_vars = {}
        self._value = None

    def bind(self, vars_):
        """
        Set values for graph variables.
        """
        self._bound_vars = self._remove_non_dep_var(vars_)

    def bind_all(self, vars_):
        """
        Set values for graph variables for all nodes in dep tree.
        """
        self.bind(vars_)

        # TODO(casey): this will call some nodes more than once
        for d in self._node_deps:
            d.bind_all(vars_)

    @staticmethod
    def _compare_vars(vars1, vars2):
        return vars1 == vars2

    def _evaluate(self, vars_, dep_vals=[], force=False):
        raise NotImplementedError()

    def __call__(self, vars_dict={}, **vars_):
        """
        Set values for graph variables for all nodes in dep tree.
        """
        vars_ = {**vars_dict, **vars_}
        self.bind_all(vars_)

        return self

    def _remove_non_dep_var(self, vars_):
        return {k: vars_[k] for k in set(vars_) & self._var_deps}

    def _get_value(self, vars_={}, dep_vals=[], force=False):
        if (
            force
            or not self._mem_cache
            or self._value is None
            or not self._compare_vars(self._value[1], vars_)
        ):
            return None

        return self._value[0]

    def _set_value(self, val, vars_):
        if self._mem_cache:
            self._value = (val, vars_)

    value = property(_get_value)

    def _check_missing_var_deps(self, vars_):
        # Check if any required vars are not specified in vars_
        missing_var_deps = self._var_deps.difference(set(vars_.keys()))
        if missing_var_deps:
            raise ValueError(STR_MISSING_VAR_DEPS % missing_var_deps)


class ConstantNode(Node):
    def __init__(self, val, mem_cache=True):
        super().__init__(mem_cache=mem_cache)

        self._val = val

    def _evaluate(self, vars_, dep_vals=[], force=False):
        return self._val

    def __repr__(self):
        return f"Const({self._val})"


class VarNode(Node):
    def __init__(self, var, mem_cache=True):
        super().__init__(mem_cache=mem_cache)

        self._var = var
        self._var_deps = {var}

    def _evaluate(self, vars_, dep_vals=[], force=False):
        self._check_missing_var_deps(vars_)

        return vars_[self._var]

    def __repr__(self):
        return f"Var({self._var})"


class ComputeNode(Node):
    def __init__(
        self,
        transform,
        mem_cache=True,
        hash_alg=hashlib.md5,
        file_path=None,
        save=None,
        load=None,
        deps=None,
    ):
        """
        Args:
            transform: function to apply to node's input
            mem_cache: cache computed values in memory
            hash_alg: hash alg to verify data matches what was saved (None to disable)
            file_path: the path to save/load from (string or function)
            save: save function for node's output
            load: load function for node's saved output
            deps: constants, graph vars, or nodes this node depends on
        """
        super().__init__(mem_cache=mem_cache)

        self._transform_fn = transform
        self._file = CustomFile(file_path, save, load, hash_alg=hash_alg)
        self._vars_file = VarsFile(file_path)

        self._node_deps, self._var_deps = self._parse_deps(deps)

    def _transform(self, vals, vars_):
        print("transform:", self, vals)
        val = self._load(vars_)

        if val is None:
            val = self._transform_fn(*vals)

            self._save(val, vars_)

        return val

    def _evaluate(self, vars_, dep_vals=[], force=False):
        """
        Evaluate the graph with inputs vars. Can force to recompute all nodes.
        """
        self._check_missing_var_deps(vars_)

        val = self._transform(dep_vals, vars_)
        self._set_value(val, vars_)

        return val

    def save(self):
        if not self._file.can_save():
            raise ValueError(STR_NO_SAVE_FUNC)

        if not self._value:
            raise ValueError(STR_SAVE_NOT_COMPUTED)

        self._save(self._value[0], self._value[1])

    def load(self):
        """
        TODO: consider removing load. Why not just ask for the value normally with value
        or compute(). Perhaps this is just a way to ensure the graph won't start a
        computation if there is no load file? Add compute(load_only=True)?
        """

        if not self._file.can_load():
            raise ValueError(STR_NO_LOAD_FUNC)

        vars_ = self._bound_vars
        load_val = self._load(vars_)

        if load_val:
            self._set_value(load_val, vars_)
        else:
            raise ValueError(STR_LOAD_NOT_VARS_MATCH)

    def _save(self, value, vars_):
        """
        Save the result of this node's transformation.
        """
        if not self._file.can_save():
            return

        success = self._file.save(value, path_vars=vars_)

        if success:
            self._vars_file.save((vars_, self._file.checksum), path_vars=vars_)

    def _load(self, vars_):
        """
        Load the result of this node's transformation.
        """
        if not self._file.can_load():
            return None

        saved_vars = self._vars_file.load(path_vars=vars_)

        if saved_vars is not None:
            saved_vars, prev_checksum = saved_vars

        if not self._compare_vars(vars_, saved_vars):
            return None

        val = self._file.load(path_vars=vars_)

        if self._file.checksum != prev_checksum:
            return None

        return val

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

    def __repr__(self):
        return STR_COMPUTE_NODE_REPR % (self._transform_fn.__name__, self._bound_vars)
