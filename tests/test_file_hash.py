import pickle

from .utils import call_count, Counter
from dagma import ComputeNode, QueueRunner


def save_pickle(path, value):
    with open(path, "wb") as f_out:
        pickle.dump(value, f_out)


def load_pickle(path):
    with open(path, "rb") as f_in:
        return pickle.load(f_in)


def add_one(x):
    return x + 1


def test_file_hash_works_for_int(tmp_path):
    file_path = tmp_path / "value.pkl"

    counter = Counter()
    add_one_counter = call_count(counter)(add_one)

    n1 = ComputeNode(add_one, file_path=file_path, save=save_pickle, deps=["x"])
    n2 = ComputeNode(add_one_counter, file_path=file_path, load=load_pickle, deps=["x"])

    out = QueueRunner(n1)

    computed_val = out.compute(x=1)

    out = QueueRunner(n2)

    assert out.compute(x=1) == computed_val
    assert counter.count == 0


def test_file_hash_detects_change(tmp_path):
    file_path = tmp_path / "value.pkl"

    counter = Counter()
    add_one_counter = call_count(counter)(add_one)

    n1 = ComputeNode(add_one, file_path=file_path, save=save_pickle, deps=["x"])
    n2 = ComputeNode(add_one_counter, file_path=file_path, load=load_pickle, deps=["x"])

    out = QueueRunner(n1)

    computed_val = out.compute(x=1)

    # Must recompute because file changed, which means hashes won't match
    save_pickle(file_path, 3)

    out = QueueRunner(n2)

    assert out.compute(x=1) == computed_val
    assert counter.count == 1
