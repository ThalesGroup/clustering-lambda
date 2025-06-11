# clustering-lambda

## Overview
This project provides a Lambda function for daily batch clustering of data based on a distance matrix. The input is a CSV file compressed with gzip (`.csv.gz`) containing distances between points. The output is a set of clusters stored in an S3 bucket.

## Features
- Downloads distance data from an S3 bucket.
- Processes the data to build a sparse distance matrix.
- Uses a clustering algorithm (default to DBSCAN).
- Uploads the clustering results back to an S3 bucket.

## Requirements
- Python 3.x
- AWS Lambda
- AWS S3
- Required Python packages: `boto3`, `scikit-learn`, `scipy`, `gzip`, `tempfile`, `csv`, `logging`

## Installation

**Lambda Layer for scikit-learn**:
- Create a Lambda function with the necessary permissions to access S3. The lambda should be configured to use Python 3.x.
- Create a Lambda layer for `scikit-learn` using [aws-lambda-layer repository](https://github.com/imperva/aws-lambda-layer) and add it to your Lambda function. And configure the layer in the Lambda function settings.
- Configure the lambda environment variables:

| Environment Variable | Description                                  | Required | Default Value |
|----------------------|----------------------------------------------|----------|---------------|
| `BUCKET`             | S3 bucket name for input and output data.    | Yes      | N/A           |
| `INPUT_FOLDER`       | S3 folder path for input data.               | Yes      | N/A           |
| `OUTPUT_FOLDER`      | S3 folder path for output data.              | Yes      | N/A           |
| `ALGORITHM`          | Clustering algorithm                         | No       | `DBSCAN`      |
| `HYPER_PARAMS`       | Clustering algorithm hyper parameters (JSON) | No       | `{}`          |
| `CONVERT_KEY_TO_INT` | Whether to convert keys to integers.         | No       | `False`       |

- **IAM Role**:
- Ensure the Lambda function has an IAM role with permissions to read from and write to the specified S3 bucket. The role should also allow logging to CloudWatch.
- Update the default Lambda timeout to 5 minutes or more, depending on the expected size of the input data.
- Update the memory size of the Lambda function to at least 512 MB, depending on the size of the input data and the clustering algorithm used.
- Add a trigger to the Lambda function to trigger it automatically on S3 PUT operation.

## Usage
### Lambda Function
The Lambda function expects an event with either:
- An S3 event record indicating the location of the input file.
- A `day` parameter specifying the day for which to process the data.

### Input Data
The input data should be a `.csv.gz` file with the following format: