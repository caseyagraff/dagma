from typing import Tuple, List
import logging
import hashlib

from .strings import (
    STR_COMPUTE_NODE_REPR,
    STR_MISSING_VAR_DEPS,
    STR_NO_SAVE_FUNC,
    STR_SAVE_NOT_COMPUTED,
    STR_TRANSFORM_CHANGED,
    STR_CHECKSUM_CHANGED,
    STR_FILE_PATH_WRONG_TYPE_FOREACH,
)

from .file_io import VarsFile, CustomFile


class Node:
    def __init__(self, mem_cache=True):
        self._mem_cache = mem_cache

        self._var_deps = set()
        self._node_deps = []
        self._bound_vars = {}
        self._value = None

    def bind(self, var_dict):
        """
        Set values for graph variables.
        """
        self._bound_vars = self.remove_non_dep_var({**self._bound_vars, **var_dict})

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
        )

    def can_get_value(self, var_dict={}, force=False):
        var_dict = {**self._bound_vars, **var_dict}
        return self._value_is_mem_cached(var_dict, force)

    @property
    def value(self):
        return self.get_value(self._bound_vars)

    def set_value(self, val, var_dict):
        var_dict = {**self._bound_vars, **self.remove_non_dep_var(var_dict)}
        if self._mem_cache:
            self._value = (val, var_dict)


NodeGraphType = Tuple[Node, List["GraphTupleType"]]  # type: ignore


class ConstantNode(Node):
    def __init__(self, val, mem_cache=True):
        super().__init__(mem_cache=mem_cache)

        self._val = val

    def get_value(self, var_dict, force=False):
        return self._evaluate(var_dict)

    def _value_is_mem_cached(self, var_dict={}, force=False):
        return True

    def _evaluate(self, var_dict, dep_vals=[], force=False):
        var_dict = {**self._bound_vars, **self.remove_non_dep_var(var_dict)}
        return self._val

    def __eq__(self, other):
        return isinstance(other, ConstantNode) and self._val == other._val

    def __hash__(self):
        return self._val

    def __repr__(self):
        return f"Const({self._val})"


class VarNode(Node):
    def __init__(self, var, mem_cache=True):
        super().__init__(mem_cache=mem_cache)

        self._var = var
        self._var_deps = {var}

    def get_value(self, var_dict, force=False):
        return self._evaluate(var_dict)

    def _value_is_mem_cached(self, var_dict={}, force=False):
        return True

    def _evaluate(self, var_dict, dep_vals=[], force=False):
        var_dict = {**self._bound_vars, **self.remove_non_dep_var(var_dict)}
        self._check_missing_var_deps(var_dict)

        return var_dict[self._var]

    def _check_missing_var_deps(self, var_dict):
        if self._var not in var_dict:
            raise ValueError(STR_MISSING_VAR_DEPS % [self._var])

    def __eq__(self, other):
        return isinstance(other, VarNode) and self._var == other._var

    def __hash__(self):
        return hash(self._var)

    def __repr__(self):
        if self._var in self._bound_vars:
            val = self._bound_vars[self._var]
            return f"Var({self._var}={val})"
        else:
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
        var_dict = {**self._bound_vars, **var_dict}
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

    def _check_missing_var_deps(self, var_dict):
        # Is this needed (checks all child deps) -- early stopping?
        # Check if any required vars are not specified in var_dict
        direct_var_deps = set(
            [n._var for n in self._node_deps if isinstance(n, VarNode)]
        )
        missing_var_deps = direct_var_deps.difference(set(var_dict.keys()))

        if missing_var_deps:
            raise ValueError(STR_MISSING_VAR_DEPS % missing_var_deps)

    def graph(self) -> NodeGraphType:
        """
        Create graph representation.
        """
        return (
            self,
            [d.graph() if isinstance(d, ComputeNode) else d for d in self._node_deps],
        )

    @classmethod
    def _display_node_text_graph(cls, n: Node):
        if not isinstance(n, cls):
            return str(n)

        return n._transform_fn.__name__

    @classmethod
    def _text_graph(cls, buf: str, node, depth=0) -> str:
        INDENT_LEVEL = 2
        STR_INDENT = "|" + " " * (INDENT_LEVEL + 1)
        STR_LEVEL = "|" + "-" * INDENT_LEVEL + " "

        if isinstance(node, tuple):
            node, deps = node
        else:
            deps = []

        if depth > 0:
            buf += STR_INDENT * (depth - 1)
            buf += STR_LEVEL

        buf += cls._display_node_text_graph(node) + "\n"

        for d in deps:
            buf = cls._text_graph(buf, d, depth + 1)

        return buf

    def text_graph(self) -> str:
        graph = self.graph()
        return self._text_graph("", graph)

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
        if hasattr(self._transform_fn, "__code__"):
            code = self._transform_fn.__code__
            return (code.co_code, code.co_consts[1:], code.co_argcount)
        else:
            return None

    @staticmethod
    def _get_hex_id(node):
        return hex(id(node) & 0xFFFFF)

    @classmethod
    def _display_node_repr(cls, n):
        if isinstance(n, ComputeNode):
            return cls._get_hex_id(n)
        else:
            return n

    def __repr__(self):
        deps = [self._display_node_repr(d) for d in self._node_deps]

        return STR_COMPUTE_NODE_REPR % (
            self._get_hex_id(self),
            self._transform_fn.__name__,
            deps,
            self._bound_vars,
        )


class ForeachComputeNode(ComputeNode):
    def __init__(
        self,
        transform,
        foreach,
        mem_cache=True,
        hash_alg=hashlib.md5,
        file_path=None,
        save=None,
        load=None,
        deps=None,
    ):
        if file_path is not None and not callable(file_path):
            raise ValueError(STR_FILE_PATH_WRONG_TYPE_FOREACH % file_path)

        super().__init__(transform, False, hash_alg, file_path, save, load, deps)
        self._deps = deps
        self._mem_cache = mem_cache

        self._foreach = foreach

        self._foreach_input_pos = (
            deps.index(foreach) if isinstance(foreach, str) else foreach
        )

        self._foreach_nodes = {}

    def _evaluate(self, var_dict, dep_vals=[], force=False):
        """
        Evaluate the graph with inputs vars. Can force to recompute all nodes.
        """
        vals = {}

        foreach_vals = dep_vals[self._foreach_input_pos]

        self._foreach_nodes = {
            val: ComputeNode(
                self._transform_fn,
                self._mem_cache,
                self._file._hash_alg,
                self._file._path,
                self._file._save_fn,
                self._file._load_fn,
                self._deps,
            )
            for val in set(foreach_vals)
        }

        for val, node in self._foreach_nodes.items():
            if isinstance(self._foreach, str):
                var_dict_single = {**var_dict, self._foreach: val}
            else:
                var_dict_single = var_dict

            dep_vals_single = dep_vals.copy()
            dep_vals_single[self._foreach_input_pos] = val

            ret = node.get_value(var_dict_single, force)

            if ret is None:
                ret = node._evaluate(var_dict_single, dep_vals_single, force)
                node.set_value(ret, var_dict_single)

            vals[val] = ret

        return [vals[val] for val in foreach_vals]

    @classmethod
    def _display_node_text_graph(cls, n: Node) -> str:
        if not isinstance(n, cls):
            return str(n)

        return f'Foreach({n._transform_fn.__name__}, foreach="{n._foreach}")'

    def __repr__(self):
        return f'Foreach({super().__repr__()}, foreach="{self._foreach}")'
