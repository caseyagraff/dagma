import pytest
import time
from functools import reduce
from pathlib import Path
from typing import Dict, List, Tuple
import random
from tempfile import TemporaryDirectory

from dagma.decorators.nodes import compute_node

DATA_DIR = Path(__file__).parent.joinpath("data")

IRIS_DATA_NAME = "iris.data"

IRIS_CLASS_MAP = {"Iris-setosa": 0, "Iris-versicolor": 1, "Iris-virginica": 2}

IRIS_TRAIN_SPLIT = 0.8
IRIS_LOAD_CHUNKS = 5

IrisFormattedDatum = Tuple[float, int]
IrisFormattedData = List[IrisFormattedDatum]
IrisDataAcc = Dict[int, Tuple[int, float]]
IrisModel = Dict[int, float]
IrisResults = List[Tuple[int, int]]


test_dir = Path("")


def load_data_file_name(file_name: str, chunk_ind: int) -> str:
    return str(test_dir.joinpath(f"{file_name}-{chunk_ind}.pkl"))


@compute_node(file_name=load_data_file_name)
def load_data(file_name: str, chunk_ind: int) -> List[str]:
    data_path = DATA_DIR.joinpath(file_name)
    with open(data_path, "r") as f_in:
        data = f_in.read()
        rows = data.split("\n")
        return rows[chunk_ind * len(rows) : (chunk_ind + 1) * len(rows)]


@compute_node()
def format_data(rows: List[str]) -> IrisFormattedData:
    rows_split = [row.split(",") for row in rows if row]
    data_formatted = [
        (float(row[0]), IRIS_CLASS_MAP.get(row[-1], 0)) for row in rows_split
    ]

    return data_formatted


@compute_node()
def preprocess_data(data: IrisFormattedData) -> IrisFormattedData:
    cov_avg = sum([row[0] for row in data]) / len(data)
    data_preprocessed = [(row[0] - cov_avg, row[1]) for row in data]

    random.seed(1337)

    random.shuffle(data_preprocessed)

    return data_preprocessed


def reduce_data_fn(acc: IrisDataAcc, datum: IrisFormattedDatum) -> IrisDataAcc:
    entry = acc[datum[1]]
    return {**acc, datum[1]: (entry[0] + 1, entry[1] + datum[0])}


@compute_node()
def model_train(data: IrisFormattedData) -> IrisModel:
    data_train = data[: int(len(data) * IRIS_TRAIN_SPLIT)]
    data_acc = reduce(
        reduce_data_fn, data_train, {0: (0, 0.0), 1: (0, 0.0), 2: (0, 0.0)}
    )
    model = reduce(
        lambda acc, acc_entry: {**acc, acc_entry[0]: acc_entry[1][1] / acc_entry[1][0]},
        data_acc.items(),
        {0: 0.0, 1: 0.0, 2: 0.0},
    )

    return model


def classify_instance(datum: IrisFormattedDatum, model: IrisModel) -> int:
    def classify_reduce_fn(
        current: Tuple[int, float], model_entry: Tuple[int, float]
    ) -> Tuple[int, float]:
        diff_new = abs(datum[0] - model_entry[1])
        return current if current[1] < diff_new else (model_entry[0], diff_new)

    return reduce(classify_reduce_fn, model.items(), (0, float("inf")))[0]


def file_name_model_test(data: IrisFormattedData, model: IrisModel) -> str:
    return str(test_dir.joinpath("model_test.pkl"))


@compute_node(file_name=file_name_model_test)
def model_test(data: IrisFormattedData, model: IrisModel) -> IrisResults:
    data_test = data[int(len(data) * IRIS_TRAIN_SPLIT) :]
    return list(
        map(lambda datum: (datum[1], classify_instance(datum, model)), data_test)
    )


@compute_node()
def build_summary(results: IrisResults) -> Dict[int, float]:
    def summary_reduce_fn(
        acc: Dict[int, Tuple[int, int]], result: Tuple[int, int]
    ) -> Dict[int, Tuple[int, int]]:

        return {
            **acc,
            result[0]: (
                acc[result[0]][0] + (result[0] == result[1]),
                acc[result[0]][1] + 1,
            ),
        }

    summary_acc = reduce(summary_reduce_fn, results, {0: (0, 0), 1: (0, 0), 2: (0, 0)})

    return reduce(
        lambda acc, entry: {**acc, entry[0]: round(entry[1][0] / entry[1][1], 1)},
        summary_acc.items(),
        {0: 0.0, 1: 0.0, 2: 0.0},
    )


IRIS_SUMMARY_EXPECTED = {0: 1.0, 1: 0.6, 2: 0.8}


async def logistic_regression():
    data: List[str] = []
    for chunk_ind in range(IRIS_LOAD_CHUNKS):
        data += await load_data(IRIS_DATA_NAME, chunk_ind)

    data_formatted = format_data(data)
    data_preprocessed = await preprocess_data(data_formatted)

    model = model_train(data_preprocessed)

    results = model_test(data_preprocessed, model)

    summary = await build_summary(results)

    return summary


@pytest.mark.asyncio
async def test_logistic_regression_e2e():
    global test_dir
    with TemporaryDirectory() as temp_dir:
        test_dir = Path(temp_dir)
        # Run first time without anything cached
        summary = await logistic_regression()
        assert summary == IRIS_SUMMARY_EXPECTED

        print("\n=== End 1 ===\n")

        # Run second time with cache
        summary = await logistic_regression()
        assert summary == IRIS_SUMMARY_EXPECTED
