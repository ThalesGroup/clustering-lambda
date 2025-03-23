import gzip
import io
import json
import tempfile
from typing import List, Tuple

import boto3
import pytest
from moto import mock_aws
import csv

from lambda_function import lambda_handler, get_value, get_bucket


def test_full_flow_no_args():
    result = lambda_handler({}, None)
    assert result == {"success": False}


def test_get_value(monkeypatch):
    monkeypatch.setenv("MIN_SAMPLES", "1")
    monkeypatch.setenv("OUTPUT_FOLDER", "test")
    assert get_value("convert-key-to-int")
    assert get_value("input-folder") == "temp/clustering-lambda/distances"
    assert get_value("output-folder") == "test"
    assert get_value("algorithm") == "DBSCAN"
    assert get_value("hyper-params") == {}


def test_get_bucket(monkeypatch):
    assert get_bucket() == "test"


@pytest.fixture(autouse=True)
def do_mock_aws(monkeypatch):
    monkeypatch.setenv("BUCKET", "test")
    mock = mock_aws()
    mock.start()
    yield
    mock.stop()


def test_full_flow_no_data(monkeypatch):
    day = "2024-01-01"
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket=get_bucket())
    result = lambda_handler({"day": day}, None)
    assert result == {
        "day": "2024-01-01",
        "success": True,
        "clusters": 0,
        "clustered-keys": 0,
    }


def _run_clustering(data: List[Tuple[int, int, float]]):
    day = "2024-01-01"
    with tempfile.NamedTemporaryFile(suffix=".csv.gz") as f:
        with gzip.open(f.name, "wt") as gzip_f:
            for row in data:
                gzip_f.write(f"{row[0]},{row[1]},{row[2]}\n")
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=get_bucket())
        s3.upload_file(
            f.name, get_bucket(), f"{get_value('input-folder')}/day={day}/data.csv.gz"
        )
    result = lambda_handler({"day": day}, None)
    del result["day"]
    result_object = s3.get_object(
        Bucket=get_bucket(),
        Key=f"{get_value('output-folder')}/day={day}/clusters.csv.gz",
    )
    clusters = {}
    with gzip.GzipFile(fileobj=result_object["Body"]) as out_gz:
        with io.StringIO(out_gz.read().decode()) as text_file:
            reader = csv.reader(text_file)
            next(reader)
            for row in reader:
                cluster, key = int(row[0]), int(row[1])
                if cluster not in clusters:
                    clusters[cluster] = []
                clusters[cluster].append(key)
    result["content"] = clusters
    return result


@pytest.mark.parametrize("algorithm", ["DBSCAN", "OPTICS"])
@pytest.mark.parametrize(
    "hyper_params", [{"eps": 0.4, "min_samples": 2}, {"eps": 0.5, "min_samples": 3}]
)
def test_full_flow_click(monkeypatch, algorithm: str, hyper_params: dict):
    monkeypatch.setenv("ALGORITHM", algorithm)
    monkeypatch.setenv("HYPER_PARAMS", json.dumps(hyper_params))
    result = _run_clustering([(i, j, 0.5) for i in range(5) for j in range(5)])
    assert result["success"]


def test_full_flow_dbscan(monkeypatch):
    monkeypatch.setenv("HYPER_PARAMS", '{"eps": 0.5, "min_samples": 3}')
    result = _run_clustering([(1, 2, 0.5), (1, 3, 0.5), (4, 5, 0.9)])

    assert result == {
        "success": True,
        "clusters": 1,
        "clustered-keys": 3,
        "content": {0: [1, 2, 3]},
    }
