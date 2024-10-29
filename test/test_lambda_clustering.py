import gzip
import tempfile

import boto3
from moto import mock_aws

from lambda_function import lambda_handler, get_value, get_bucket



def test_full_flow_no_args():
    result = lambda_handler({}, None)
    assert result == {"success": False}

def test_get_value(monkeypatch):
    monkeypatch.setenv("MIN_SAMPLES", "1")
    monkeypatch.setenv("OUTPUT_FOLDER", "test")
    assert get_value("min-samples") == 1
    assert get_value("convert-key-to-int")
    assert get_value("epsilon") == 0.5
    assert get_value("input-folder") == "temp/clustering-lambda/distances"
    assert get_value("output-folder") == "test"

def test_get_bucket(monkeypatch):
    monkeypatch.setenv("BUCKET", "test")
    assert get_bucket() == "test"


@mock_aws
def test_full_flow_no_data(monkeypatch):
    monkeypatch.setenv("BUCKET", "test")
    day = "2024-01-01"
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket=get_bucket())
    result = lambda_handler({"day": day}, None)
    assert result == {'day': '2024-01-01', 'success': True, 'clusters': 0, "clustered-keys": 0}


@mock_aws
def test_full_flow(monkeypatch):
    monkeypatch.setenv("BUCKET", "test")
    day = "2024-01-01"
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket=get_bucket())
    with tempfile.NamedTemporaryFile(suffix=".csv.gz") as f:
        with gzip.open(f.name, "wt") as gzip_f:
            gzip_f.write("1,2,0.5\n")
            gzip_f.write("1,3,0.5\n")
            gzip_f.write("4,5,0.9\n")
        s3.upload_file(f.name, get_bucket(), f"{get_value('input-folder')}/day={day}/data.csv.gz")
    result = lambda_handler({"day": day}, None)
    assert result == {'day': '2024-01-01', 'success': True, 'clusters': 1, "clustered-keys": 3}
    result_object = s3.get_object(Bucket=get_bucket(), Key=f"{get_value('output-folder')}/day={day}/clusters.csv.gz")
    with gzip.GzipFile(fileobj=result_object["Body"]) as out_gz:
        content = out_gz.read().decode()
    assert content == "cluster,key\r\n0,1\r\n0,2\r\n0,3\r\n"


