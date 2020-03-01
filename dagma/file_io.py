from typing import Any
import logging
import pickle
import os

from .strings import STR_SAVE_FUNC_EXCEPTION, STR_LOAD_FUNC_EXCEPTION


class File:
    def __init__(self, path):
        self._path = path

    def save(self, value, path_vars=None) -> bool:
        path = self._get_path(path_vars)

        try:
            self._save(path, value)
            return True
        except Exception as e:
            logging.log(logging.ERROR, STR_SAVE_FUNC_EXCEPTION, e)
            raise e

    def load(self, path_vars=None) -> Any:
        path = self._get_path(path_vars)

        try:
            return self._load(path)
        except FileNotFoundError as e:
            logging.log(logging.INFO, e)
            return None
        except Exception as e:
            logging.log(logging.ERROR, STR_LOAD_FUNC_EXCEPTION, e)
            raise e

    def _save(self, path, value) -> None:
        raise NotImplementedError()

    def _load(self, path) -> Any:
        raise NotImplementedError()

    def _get_path(self, path_vars):
        return self._path if not callable(self._path) else self._path(path_vars)


class PickleFile(File):
    @staticmethod
    def _save(path, value) -> None:
        with open(path, "wb") as f_out:
            pickle.dump(value, f_out)

    @staticmethod
    def _load(path) -> Any:
        with open(path, "rb") as f_in:
            return pickle.load(f_in)  # nosec


class VarsFile(PickleFile):
    def _get_path(self, path_vars):
        path = super()._get_path(path_vars)

        path, file_name = os.path.split(path)

        return os.path.join(path, f".{file_name}.dagma-vars")


class CustomFile(File):
    def __init__(self, path, save_fn=None, load_fn=None):
        super().__init__(path)
        self._save_fn = save_fn
        self._load_fn = load_fn

    def _save(self, path, value) -> None:
        self._save_fn(path, value)

    def _load(self, path) -> Any:
        return self._load_fn(path)

    def can_save(self):
        return self._save_fn is not None and self._path is not None

    def can_load(self):
        return self._load_fn is not None and self._path is not None
