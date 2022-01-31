import pickle
from typing import Any


def save_pickle(data: Any, path: str):
    with open(path, "wb") as f_out:
        pickle.dump(data, f_out)


def load_pickle(path: str) -> Any:
    with open(path, "rb") as f_in:
        return pickle.load(f_in)
