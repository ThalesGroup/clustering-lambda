# clustering-lambda

## Overview
This project provides a Lambda function for clustering data based on a distance matrix. The input is a CSV file compressed with gzip (`.csv.gz`) containing distances between points. The output is a set of clusters stored in an S3 bucket.

## Features
- Downloads distance data from an S3 bucket.
- Processes the data to build a distance matrix.
- Uses DBSCAN for clustering.
- Uploads the clustering results back to an S3 bucket.

## Requirements
- Python 3.x
- AWS Lambda
- AWS S3
- Required Python packages: `boto3`, `scikit-learn`, `scipy`, `gzip`, `tempfile`, `csv`, `logging`

## Installation

**Lambda Layer for scikit-learn**:
- Create a Lambda function with the necessary permissions to access S3. The lambda should be configured to use Python 3.x.
- Create a Lambda layer for `scikit-learn` using [this repository](https://github.com/imperva/aws-lambda-layer) and add it to your Lambda function.
- Configure the lambda environment variables:

| Environment Variable | Description                          | Required | Default Value |
|----------------------|--------------------------------------|----------|---------------|
| `bucket`             | S3 bucket name for input and output data. | Yes      | N/A           |
| `input-folder`       | S3 folder path for input data.       | Yes      | N/A           |
| `output-folder`      | S3 folder path for output data.      | Yes      | N/A           |
| `convert-key-to-int` | Whether to convert keys to integers. | No       | `False`       |
| `max-records`        | Maximum number of records to process.| No       | `1000`        |
| `epsilon`            | Epsilon parameter for DBSCAN.        | No       | `0.5`         |
| `min-samples`        | Minimum samples parameter for DBSCAN.| No       | `5`           |

## Usage
### Lambda Function
The Lambda function expects an event with either:
- An S3 event record indicating the location of the input file.
- A `day` parameter specifying the day for which to process the data.

### Input Data
The input data should be a `.csv.gz` file with the following format: