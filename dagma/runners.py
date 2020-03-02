from typing import Any, Dict, List
from collections import defaultdict

from .nodes import Node


class Runner:
    def __init__(self, node):
        self._sink_node = node

    def compute(self, vars_dict={}, force=False, **vars_):
        var_overrides = {**vars_dict, **vars_}

        return self._compute_node(self._sink_node, var_overrides, force)

    def _compute_node(self, node, vars_dict={}, force=False):
        raise NotImplementedError()

    def _get_or_evaluate_node(self, node, vars_dict, dep_vals, force):
        vars_ = {**node._bound_vars, **node._remove_non_dep_var(vars_dict)}

        val = node._get_value(vars_, dep_vals, force)

        if val is None:
            val = node._evaluate(vars_, dep_vals, force)
            node._set_value(val, vars_)

        return val

    @property
    def value(self):
        return self.compute()


class RecursiveRunner(Runner):
    def _compute_node(self, node, vars_dict={}, force=False):
        # Compute dependent values
        dep_vals = [self._compute_node(d, vars_dict, force) for d in node._node_deps]

        val = self._get_or_evaluate_node(node, vars_dict, dep_vals, force)

        return val


class QueueRunner(Runner):
    def _compute_node(self, node, vars_dict={}, force=False):
        visited: Dict[Node, bool] = {}
        to_explore = [node]
        node_queue = []
        reverse_dep_list: Dict[Node, list] = defaultdict(list)

        while len(to_explore) > 0:
            n = to_explore.pop()

            if n in visited:
                continue

            visited[n] = True

            for dep in n._node_deps:
                reverse_dep_list[dep].append(n)
                if dep not in visited:
                    to_explore.append(dep)

            node_queue.append(n)

        node_queue.reverse()

        """
        Build delete list to remove saved values after all nodes dependent on value are
        computed. Keeps memory use similar to stack/recursion based approach.
        """
        node_to_pos = {n: i for i, n in enumerate(node_queue)}
        last_dep_list = {
            n: max([node_to_pos[d] for d in ds]) for n, ds in reverse_dep_list.items()
        }

        to_delete: Dict[int, List[Node]] = defaultdict(list)
        for n, pos in last_dep_list.items():
            to_delete[pos].append(n)

        # Compute pass on node queue
        computed_vals: Dict[Node, Any] = {}
        for i, n in enumerate(node_queue):
            dep_vals = [computed_vals[d] for d in n._node_deps]
            computed_vals[n] = self._get_or_evaluate_node(n, vars_dict, dep_vals, force)

            # Remove values no longer needed at this point in queue
            for d in to_delete[i]:
                del computed_vals[d]

        return computed_vals[node]
