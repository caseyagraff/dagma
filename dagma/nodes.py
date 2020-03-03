import logging
import hashlib

from .strings import (
    STR_COMPUTE_NODE_REPR,
    STR_MISSING_VAR_DEPS,
    STR_NO_SAVE_FUNC,
    STR_SAVE_NOT_COMPUTED,
    STR_TRANSFORM_CHANGED,
    STR_CHECKSUM_CHANGED,
)

from .file_io import VarsFile, CustomFile


class Node:
    def __init__(self, mem_cache=True):
        self._mem_cache = mem_cache

        self._var_deps = set()
        self._node_deps = {}
        self._bound_vars = {}
        self._value = None

    def bind(self, var_dict):
        """
        Set values for graph variables.
        """
        self._bound_vars = self.remove_non_dep_var(var_dict)

    def bind_all(self, var_dict):
        """
        Set values for graph variables for all nodes in dep tree.
        """
        self.bind(var_dict)

        # TODO(casey): this will call some nodes more than once
        for d in self._node_deps:
            d.bind_all(var_dict)

    @staticmethod
    def _compare_vars(vars1, vars2):
        return vars1 == vars2

    def _evaluate(self, var_dict, dep_vals=[], force=False):
        raise NotImplementedError()

    def __call__(self, var_dict={}, **vars_):
        """
        Set values for graph variables for all nodes in dep tree.
        """
        var_dict = {**var_dict, **vars_}
        self.bind_all(var_dict)

        return self

    def remove_non_dep_var(self, var_dict):
        return {k: var_dict[k] for k in set(var_dict) & self._var_deps}

    def get_value(self, var_dict={}, force=False):
        var_dict = {**self._bound_vars, **self.remove_non_dep_var(var_dict)}

        # Check if value has been cached
        if not self._value_is_mem_cached(var_dict, force):
            return None

        return self._value[0]  # type: ignore

    def _value_is_mem_cached(self, var_dict={}, force=False):
        return not (
            force
            or not self._mem_cache
            or self._value is None
            or not self._compare_vars(self._value[1], var_dict)
          
    def can_get_value(self, var_dict={}, force=False):
        return self._value_is_mem_cached(var_dict, force)

    @property
    def value(self):
        return self.get_value(self._bound_vars)

    def set_value(self, val, var_dict):
        var_dict = {**self._bound_vars, **self.remove_non_dep_var(var_dict)}
        if self._mem_cache:
            self._value = (val, var_dict)

    def _check_missing_var_deps(self, var_dict):
        # Check if any required vars are not specified in var_dict
        missing_var_deps = self._var_deps.difference(set(var_dict.keys()))
        if missing_var_deps:
            raise ValueError(STR_MISSING_VAR_DEPS % missing_var_deps)


class ConstantNode(Node):
    def __init__(self, val, mem_cache=True):
        super().__init__(mem_cache=mem_cache)

        self._val = val

    def _evaluate(self, var_dict, dep_vals=[], force=False):
        var_dict = {**self._bound_vars, **self.remove_non_dep_var(var_dict)}
        return self._val

    def __repr__(self):
        return f"Const({self._val})"


class VarNode(Node):
    def __init__(self, var, mem_cache=True):
        super().__init__(mem_cache=mem_cache)

        self._var = var
        self._var_deps = {var}

    def _evaluate(self, var_dict, dep_vals=[], force=False):
        var_dict = {**self._bound_vars, **self.remove_non_dep_var(var_dict)}
        self._check_missing_var_deps(var_dict)

        return var_dict[self._var]

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
        self._transform_heuristics = self._compute_transform_heuristics()

    def can_get_value(self, var_dict={}, force=False):
        can_get = super().can_get_value(var_dict, force)

        if not can_get:
            can_get = self._can_load(var_dict)

        return can_get

    def get_value(self, var_dict={}, force=False):
        var_dict = {**self._bound_vars, **self.remove_non_dep_var(var_dict)}
        val = super().get_value(var_dict, force)

        if val is None:
            val = self._load(var_dict)

            if val is not None:
                self.set_value(val, var_dict)

        return val

    def _transform(self, vals, var_dict):
        val = self._transform_fn(*vals)

        self._save(val, var_dict)

        return val

    def _evaluate(self, var_dict, dep_vals=[], force=False):
        """
        Evaluate the graph with inputs vars. Can force to recompute all nodes.
        """
        var_dict = {**self._bound_vars, **self.remove_non_dep_var(var_dict)}
        self._check_missing_var_deps(var_dict)

        val = self._transform(dep_vals, var_dict)
        self.set_value(val, var_dict)

        return val

    def save(self):
        if not self._file.can_save():
            raise ValueError(STR_NO_SAVE_FUNC)

        if not self._value:
            raise ValueError(STR_SAVE_NOT_COMPUTED)

        self._save(self._value[0], self._value[1])

    def _save(self, value, var_dict):
        """
        Save the result of this node's transformation.
        """
        if not self._file.can_save():
            return

        success = self._file.save(value, path_vars=var_dict)

        path = self._file.get_path(path_vars=var_dict)
        checksum = self._file.compute_checksum(path)

        if success:
            self._vars_file.save(
                (var_dict, checksum, self._transform_heuristics), path_vars=var_dict
            )

    def _can_load(self, var_dict):
        if not self._file.can_load():
            return False

        if not self._file.exists(path_vars=var_dict):
            return False

        saved_vars = self._vars_file.load(path_vars=var_dict)

        if saved_vars is None:
            return False

        saved_vars, prev_checksum, prev_transform_heuristics = saved_vars

        if not self._compare_vars(var_dict, saved_vars):
            return False

        path = self._file.get_path(path_vars=var_dict)
        checksum = self._file.compute_checksum(path)

        if checksum != prev_checksum:
            logging.log(
                logging.WARN,
                STR_CHECKSUM_CHANGED,
                self._file.get_path(path_vars=var_dict),
            )
            return False

        if self._transform_heuristics != prev_transform_heuristics:
            logging.log(
                logging.WARN,
                STR_TRANSFORM_CHANGED,
                self._file.get_path(path_vars=var_dict),
            )

        return True

    def _load(self, var_dict):
        """
        Load the result of this node's transformation.
        """
        if not self._can_load(var_dict):
            return None

        return self._file.load(path_vars=var_dict)

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

    def _compute_transform_heuristics(self):
        code = self._transform_fn.__code__
        return (code.co_code, code.co_consts[1:], code.co_argcount)

    def __repr__(self):
        return STR_COMPUTE_NODE_REPR % (self._transform_fn.__name__, self._bound_vars)
