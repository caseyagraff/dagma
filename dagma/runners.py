from typing import Any, Dict, List
from collections import defaultdict

from .nodes import Node


class Runner:
    def __init__(self, node):
        self._sink_node = node

    def compute(self, var_dict={}, force=False, **vars_):
        return self._compute_node(self._sink_node, {**var_dict, **vars_}, force)

    def _compute_node(self, node, var_dict={}, force=False):
        raise NotImplementedError()

    def _get_node_value(self, node, var_dict, force):
        var_dict = node.remove_non_dep_var(var_dict)
        return node.get_value(var_dict, force)

    def _evaluate_node(self, node, var_dict, dep_vals, force):
        val = node._evaluate(var_dict, dep_vals, force)
        node.set_value(val, var_dict)

        return val

    @property
    def value(self):
        return self.compute()


class RecursiveRunner(Runner):
    def _compute_node(self, node, var_dict={}, force=False):
        val = self._get_node_value(node, var_dict, force)

        if val is not None:
            return val

        # Compute dependent values
        dep_vals = [self._compute_node(d, var_dict, force) for d in node._node_deps]

        val = self._evaluate_node(node, var_dict, dep_vals, force)

        return val


class QueueRunner(Runner):
    """
    TODO: can we do better with memory for mem_cache=False objects? If a large output
    has multiple deps it may be kep in the computed_vals dict for a long time. Can we
    sacrifice recompute time for reduced memory?
    """

    def _compute_node(self, node, var_dict={}, force=False):
        visited: Dict[Node, bool] = {}
        to_explore = [node]
        node_queue = []
        reverse_dep_list: Dict[Node, list] = defaultdict(list)
        computed_vals: Dict[Node, Any] = {}

        while len(to_explore) > 0:
            n = to_explore.pop()

            if n in visited:
                continue

            visited[n] = True

            val = self._get_node_value(n, var_dict, force)

            # Node value is already saved, don't add the nodes it depends on
            if val is not None:
                computed_vals[n] = val
                continue

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
        for i, n in enumerate(node_queue):
            if n not in computed_vals:
                dep_vals = [computed_vals[d] for d in n._node_deps]
                computed_vals[n] = self._evaluate_node(n, var_dict, dep_vals, force)

            # Remove values no longer needed at this point in queue
            for d in to_delete[i]:
                del computed_vals[d]

        return computed_vals[node]
