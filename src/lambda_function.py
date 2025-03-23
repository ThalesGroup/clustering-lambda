# https://github.com/imperva/clustering-lambda
import csv
import gzip
import json
import logging
import os
import tempfile
from typing import Dict
from urllib.parse import unquote

import boto3
import sklearn
from botocore.exceptions import ClientError
from scipy.sparse import csr_matrix
from sklearn.base import ClusterMixin
from sklearn.cluster import DBSCAN

_DEFAULT_VALUES = {
    "max-records": 1_000_000,
    "algorithm": "DBSCAN",
    "hyper-params": {},
    "convert-key-to-int": True,
    "input-folder": "temp/clustering-lambda/distances",
    "output-folder": "temp/clustering-lambda/clusters",
}


def get_value(prop_name: str):
    """
    Get property value from environment variables or a default value
    :param prop_name: property name
    :return: value, or default value if not found using the data type of the default value
    """
    default_value = _DEFAULT_VALUES[prop_name]
    evn_var_name = prop_name.upper().replace("-", "_")
    if isinstance(default_value, str):
        return os.environ.get(evn_var_name, default_value)
    elif isinstance(default_value, bool):
        return (
            os.environ[evn_var_name] == "true"
            if evn_var_name in os.environ
            else default_value
        )
    elif isinstance(default_value, int):
        return (
            int(os.environ[evn_var_name])
            if evn_var_name in os.environ
            else default_value
        )
    elif isinstance(default_value, float):
        return (
            float(os.environ[evn_var_name])
            if evn_var_name in os.environ
            else default_value
        )
    elif isinstance(default_value, dict):
        return (
            json.loads(os.environ[evn_var_name])
            if evn_var_name in os.environ
            else default_value
        )
    else:
        raise Exception(
            f"Unknown type for property: {prop_name}. Type: {type(default_value)}"
        )


def get_bucket() -> str:
    return os.environ["BUCKET"]


def build_mat(day: str):
    key_to_index: Dict[object, int] = {}

    def add_key(key_to_add) -> int:
        if key_to_add not in key_to_index:
            key_to_index[key_to_add] = len(key_to_index)
        return key_to_index[key_to_add]

    row_arr = []
    col_arr = []
    distances = []
    logging.info("building distance matrix..")
    limit_reached = False
    s3 = boto3.client("s3")
    convert_key_to_int = get_value("convert-key-to-int")
    objects = s3.list_objects_v2(
        Bucket=get_bucket(), Prefix=f"{get_value("input-folder")}/day={day}/"
    )
    for key in [key["Key"] for key in objects.get("Contents", [])]:
        tmp_file = tempfile.NamedTemporaryFile(suffix=".csv.gz")
        s3.download_file(get_bucket(), key, tmp_file.name)
        with gzip.open(tmp_file.name, "rt") as f:
            for idx, line in enumerate(f):
                line_data = line.split(",")
                row_arr.append(
                    add_key(int(line_data[0]) if convert_key_to_int else line_data[0])
                )
                col_arr.append(
                    add_key(int(line_data[1]) if convert_key_to_int else line_data[1])
                )
                distance = float(line_data[2][:-1])
                distances.append(0.001 if distance == 0 else distance)
                limit_reached = idx == get_value("max-records")
                if limit_reached:
                    break
        if limit_reached:
            logging.info(f"Limit reached: {get_value("max-records")}")
            break
    logging.info(f"Distances: {len(distances)}, keys: {len(key_to_index)}")
    mat = csr_matrix(
        (distances, (row_arr, col_arr)), shape=(len(key_to_index), len(key_to_index))
    )
    mat[col_arr, row_arr] = mat[row_arr, col_arr]  # make matrix symmetric
    logging.info(f"Mat size: {mat.size}")
    return {y: x for x, y in key_to_index.items()}, mat


def cluster_data(mat):
    logging.info("clustering..")
    if mat is not None and mat.shape[0] > 0 and mat.shape[1] > 0:
        model = get_clustering_algorithm()
        result = model.fit_predict(mat)
        return result
    else:
        return []


def get_clustering_algorithm() -> ClusterMixin:
    hyper_param = get_value("hyper-params")
    hyper_param["metric"] = "precomputed"
    algorithm = get_value("algorithm")
    logging.info(f"Algorithm: {algorithm}, hyper params: {hyper_param}")
    return getattr(sklearn.cluster, algorithm)(**hyper_param)


def get_output_key(day: str) -> str:
    return f"{get_value("output-folder")}/day={day}/clusters.csv.gz"


def clear_old_data(day: str):
    s3 = boto3.client("s3")
    key = get_output_key(day)
    try:
        s3.head_object(Bucket=get_bucket(), Key=key)
        object_exists = True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            object_exists = False
        else:
            raise e
    if object_exists:
        logging.info("Object exists. Deleting old data")
        s3.delete_object(Bucket=get_bucket(), Key=key)


def write_results(keys: Dict[int, object], result, day: str):
    logging.info("writing results..")
    dict_result = {}
    for idx, cluster in enumerate(result):
        if cluster != -1:
            if cluster not in dict_result:
                dict_result[cluster] = []
            dict_result[cluster].append(keys[idx])
    logging.info(f"clusters: {len(dict_result)}")
    logging.info(f"cluster sizes: {[len(dict_result[k]) for k in dict_result]}")
    tmp_file = tempfile.NamedTemporaryFile(suffix=".csv.gz")
    with gzip.open(tmp_file.name, "wt") as f:
        writer = csv.writer(f, delimiter=",")
        writer.writerow(["cluster", "key"])
        for d in dict_result:
            for key in dict_result[d]:
                writer.writerow([d, key])
    s3 = boto3.client("s3")
    output_key = get_output_key(day)
    logging.info(output_key)
    s3.upload_file(tmp_file.name, get_bucket(), output_key)
    return len(dict_result), sum([len(dict_result[k]) for k in dict_result])


# noinspection PyUnusedLocal
def lambda_handler(event, context):
    logging.getLogger().setLevel(logging.INFO)
    if "Records" in event:
        key = unquote(event["Records"][0]["s3"]["object"]["key"])
        day = key[key.rfind("day=") + 4 : key.rfind("/")]
    elif "day" in event:
        day = event["day"]
    else:
        day = None
    if day:
        logging.info(f"Going to cluster data for day: {day}")
        clear_old_data(day)
        keys, mat = build_mat(day)
        result = cluster_data(mat)
        clusters, clustered_keys = write_results(keys, result, day)
        return {
            "day": day,
            "success": True,
            "clusters": clusters,
            "clustered-keys": clustered_keys,
        }
    else:
        return {
            "success": False,
        }
