import os
import pickle
import pytest

from .utils import call_count, Counter
from dagma import ComputeNode, ForeachComputeNode, QueueRunner


def add_one(x):
    return x + 1


def save_pickle(path, value):
    with open(path, "wb") as f_out:
        pickle.dump(value, f_out)


def load_pickle(path):
    with open(path, "rb") as f_in:
        return pickle.load(f_in)


def test_save(tmp_path):
    file_path = tmp_path / "save.pkl"
    n = ComputeNode(add_one, file_path=file_path, save=save_pickle, deps=["x"])

    out = QueueRunner(n)

    computed_value = out.compute(x=1013)

    saved_value = load_pickle(file_path)

    assert computed_value == saved_value


def test_load_save(tmp_path):
    file_path = tmp_path / "save.pkl"
    n = ComputeNode(
        add_one, file_path=file_path, save=save_pickle, load=load_pickle, deps=["x"]
    )

    out = QueueRunner(n)

    value = out.compute(x=65)

    counter = Counter()
    add_one_counter = call_count(counter)(add_one)

    n = ComputeNode(
        add_one_counter,
        file_path=file_path,
        save=save_pickle,
        load=load_pickle,
        deps=["x"],
    )(x=65)

    out = QueueRunner(n)

    assert out.value == value
    assert counter.count == 0


def test_load_save_different_x(tmp_path):
    file_path = tmp_path / "save.pkl"
    n = ComputeNode(
        add_one, file_path=file_path, save=save_pickle, load=load_pickle, deps=["x"]
    )

    out = QueueRunner(n)

    value = out.compute(x=65)

    counter = Counter()
    add_one_counter = call_count(counter)(add_one)

    n = ComputeNode(
        add_one_counter,
        file_path=file_path,
        save=save_pickle,
        load=load_pickle,
        deps=["x"],
    )(x=66)

    out = QueueRunner(n)

    assert out.value != value
    assert counter.count == 1


def test_save_missing_raises():
    n = ComputeNode(add_one, deps=["x"])

    with pytest.raises(ValueError):
        n.save()


def test_manual_save_before_compute_raises(tmp_path):
    file_path = tmp_path / "bad.out"
    n = ComputeNode(add_one, file_path=file_path, save=save_pickle, deps=["x"])

    with pytest.raises(ValueError):
        n.save()


def test_manual_save_success(tmp_path):
    file_path = tmp_path / "bad.out"
    n1 = ComputeNode(add_one, file_path=file_path, save=save_pickle, deps=["x"])

    out = QueueRunner(n1)

    computed_value = out.compute(x=1)

    n1.save()

    # Pass nop for transform to ensure load is used (not recomputing)
    n2 = ComputeNode(lambda: None, file_path=file_path, load=load_pickle, deps=["x"])
    n2.bind_all({"x": 1})

    out = QueueRunner(n2)

    assert out.value == computed_value


def test_bad_save_raises(tmp_path):
    def bad_save(path, value):
        raise Exception("Failed to save.")

    file_path = tmp_path / "bad.out"

    # Expect not to raise
    with pytest.raises(Exception):
        n = ComputeNode(add_one, file_path=file_path, save=bad_save, deps=["x"])

        out = QueueRunner(n)
        out.compute(x=1)


def test_bad_load_raises(tmp_path):
    def bad_load(path):
        raise Exception("Failed to load.")

    file_path = tmp_path / "bad.out"

    n1 = ComputeNode(
        add_one, file_path=file_path, save=save_pickle, load=bad_load, deps=["x"]
    )
    n2 = ComputeNode(
        add_one, file_path=file_path, save=save_pickle, load=bad_load, deps=["x"]
    )

    out = QueueRunner(n1)

    out.compute(x=1)

    with pytest.raises(Exception):
        out = QueueRunner(n2)
        out.compute(x=1)


def test_load_file_not_found_no_exception(tmp_path):
    file_path = tmp_path / "bad.out"

    n1 = ComputeNode(
        add_one, file_path=file_path, save=save_pickle, load=load_pickle, deps=["x"]
    )
    n2 = ComputeNode(
        add_one, file_path=file_path, save=save_pickle, load=load_pickle, deps=["x"]
    )

    out = QueueRunner(n1)

    out.compute(x=1)

    os.remove(file_path)

    # Expect no exception
    out = QueueRunner(n2)
    out.compute(x=1)


# === Foreach ===
def test_save_foreach(tmp_path):
    def file_path(params):
        return tmp_path / f"save_{params['xs']}.pkl"

    n = ForeachComputeNode(
        add_one, file_path=file_path, save=save_pickle, deps=["xs"], foreach="xs"
    )

    out = QueueRunner(n)

    xs = [1013, 1, -5]
    computed_values = out.compute(xs=xs)

    for val, computed_val in zip(xs, computed_values):
        saved_val = load_pickle(file_path({"xs": val}))
        assert computed_val == saved_val


def test_load_save_foreach(tmp_path):
    def file_path(params):
        return tmp_path / f"save-{params['xs']}.pkl"

    n = ForeachComputeNode(
        add_one,
        file_path=file_path,
        save=save_pickle,
        load=load_pickle,
        deps=["xs"],
        foreach="xs",
    )

    out = QueueRunner(n)

    xs = [65, 70, 1]
    value = out.compute(xs=xs)

    counter = Counter()
    add_one_counter = call_count(counter)(add_one)

    n = ForeachComputeNode(
        add_one_counter,
        file_path=file_path,
        save=save_pickle,
        load=load_pickle,
        deps=["xs"],
        foreach="xs",
    )(xs=xs)

    out = QueueRunner(n)

    assert out.value == value
    assert counter.count == 0
