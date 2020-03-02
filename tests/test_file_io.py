from functools import partial
import pickle

from dagma.file_io import PickleFile


def make_path(base_path, path_vars):
    return base_path / f'save_{path_vars["x"]}.pkl'


def load_pickle(path):
    with open(path, "rb") as f:
        return pickle.load(f)


def test_pickle_file_path_vars(tmp_path):
    file_ = PickleFile(partial(make_path, tmp_path))

    path_vars = {"x": 123}

    file_.save(17, path_vars)

    saved_value = load_pickle(make_path(tmp_path, path_vars))

    assert saved_value == 17
