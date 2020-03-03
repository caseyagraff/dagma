from typing import Any, Dict, List, Tuple, Optional
from collections import defaultdict
from queue import Queue

# from multiprocessing import Pool
from multiprocessing.pool import ThreadPool
from threading import Thread

from .nodes import Node


class Runner:
    def __init__(self, node):
        self._sink_node = node

    def compute(self, var_dict={}, force=False, **vars_):
        return self._compute_node(self._sink_node, {**var_dict, **vars_}, force)

    def _compute_node(self, node, var_dict={}, force=False):
        raise NotImplementedError()

    @staticmethod
    def _get_node_value(node, var_dict, force):
        var_dict = node.remove_non_dep_var(var_dict)
        return node.get_value(var_dict, force)

    @staticmethod
    def _can_get_node_value(node, var_dict, force):
        var_dict = node.remove_non_dep_var(var_dict)
        return node.can_get_value(var_dict, force)

    @staticmethod
    def _evaluate_node(node, var_dict, dep_vals, force):
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
    def _build_topo_queue(self, node, var_dict, force):

        visited: Dict[Node, bool] = {}
        to_explore = [node]
        node_queue = []
        reverse_dep_list: Dict[Node, list] = defaultdict(list)

        while len(to_explore) > 0:
            n = to_explore.pop()

            if n in visited:
                continue

            visited[n] = True

            node_queue.append(n)

            # Node value is already saved, don't add the nodes it depends on
            if self._can_get_node_value(n, var_dict, force):
                continue

            for dep in n._node_deps:
                reverse_dep_list[dep].append(n)
                if dep not in visited:
                    to_explore.append(dep)

        node_queue.reverse()

        return node_queue, reverse_dep_list

    def _build_delete_list(self, node_queue, reverse_dep_list):
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

        return to_delete

    @classmethod
    def _get_deps_and_evaluate(cls, node, var_dict, computed_vals, force):
        dep_vals = [computed_vals[d] for d in node._node_deps]
        return cls._evaluate_node(node, var_dict, dep_vals, force)

    def _compute_node(self, node, var_dict={}, force=False):
        node_queue, reverse_dep_list = self._build_topo_queue(node, var_dict, force)
        to_delete = self._build_delete_list(node_queue, reverse_dep_list)

        # Compute pass on node queue
        computed_vals: Dict[Node, Any] = {}

        for i, n in enumerate(node_queue):
            val = self._get_node_value(n, var_dict, force)

            # Node value is already saved, don't add the nodes it depends on
            if val is not None:
                computed_vals[n] = val
            else:
                computed_vals[n] = self._get_deps_and_evaluate(
                    n, var_dict, computed_vals, force
                )

            # Remove values no longer needed at this point in queue
            for d in to_delete[i]:
                del computed_vals[d]

        return computed_vals[node]

      
class ThreadRunner(QueueRunner):
    # TODO(casey): clean up this code and add a multiprocessing runner
    def __init__(self, node, num_workers=1):
        self._sink_node = node
        self._num_workers = num_workers

    @classmethod
    def _process_node(cls, node, var_dict, force, computed_vals):
        val = cls._get_node_value(node, var_dict, force)

        # Node value is already saved, don't add the nodes it depends on
        if val is None:
            val = cls._get_deps_and_evaluate(node, var_dict, computed_vals, force)

        return node, val

    def _compute_node(self, node, var_dict={}, force=False):
        node_queue, reverse_dep_list = self._build_topo_queue(node, var_dict, force)
        # to_delete = self._build_delete_list(node_queue, reverse_dep_list)

        result_queue: Queue[Tuple[Node, Any]] = Queue()
        node_process_queue: Queue[Optional[Node]] = Queue()

        for n in node_queue:
            if len(n._node_deps) == 0:
                node_process_queue.put(n)

        dep_list: Dict[Node, List[Node]] = defaultdict(list)
        for n, r_deps in reverse_dep_list.items():
            for rd in r_deps:
                dep_list[rd].append(n)

        # Compute pass on node queue
        computed_vals: Dict[Node, Any] = {}

        def store_result(ret_val):
            result_queue.put(ret_val)

        def update_queue(result_queue):
            while len(node_queue) > 0:
                node, val = result_queue.get()

                computed_vals[node] = val
                node_queue.remove(node)

                for r_dep in reverse_dep_list[node]:
                    dep_list[r_dep].remove(node)

                    if len(dep_list[r_dep]) == 0:
                        node_process_queue.put(r_dep)

                """
                for d in to_delete[i]:
                    del computed_vals[d]
                """

            node_process_queue.put(None)

        result_thread = Thread(target=update_queue, args=(result_queue,))

        result_thread.start()

        pool = ThreadPool(self._num_workers)
        while len(node_queue) > 0 or not node_process_queue.empty():
            n = node_process_queue.get()

            if n is None:
                break

            pool.apply_async(
                type(self)._process_node,
                args=(n, var_dict, force, computed_vals),
                callback=store_result,
            )

        pool.close()
        pool.join()
        result_thread.join()

        return computed_vals[node]
