import pytest

from lambda_function import lambda_handler, get_value, get_bucket


@pytest.fixture(autouse=True)
def set_env_vars(monkeypatch):
    monkeypatch.setenv("BUCKET", "test")


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
