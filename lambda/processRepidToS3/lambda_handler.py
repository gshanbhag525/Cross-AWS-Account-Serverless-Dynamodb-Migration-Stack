import os
import boto3
import json
import logging
from typing import Optional
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

PROCESSING_BUCKET_NAME = os.getenv("PROCESSING_BUCKET_NAME")

s3 = boto3.client("s3")

"""
  This lambda does:
  1. creates repid json files in s3 for each repid present in reqbody list
"""


def lambda_handler(event, context):
    lambda_response = {"statusCode": 200, "body": ""}
    data = json.loads(event["body"])
    err = ""
    try:
        for repid in data["repid_list"]:
            updated_repid = repid.replace("/", "_")
            filename = updated_repid + ".json"
            local_filename_json = f"/tmp/{filename}"
            try:
                os.unlink(local_filename_json)
            except FileNotFoundError:
                pass

            with open(local_filename_json, "x") as f:
                json.dump({"repid": repid}, f)

            s3_file_key = upload_file_to_s3(
                PROCESSING_BUCKET_NAME, local_filename_json, updated_repid
            )
            if s3_file_key is None:
                err = f"s3 file didnt not upload for {repid=}"
                logger.error(err)

        lambda_response["body"] = json.dumps(
            {"message": "Repid json files uploaded to s3 for processing"}
        )

        return lambda_response
    except Exception as e:
        logger.info(e)
        lambda_response["body"] = json.dumps(
            {
                "message": "Error occured, pls check logs",
            }
        )

    return lambda_response


def upload_file_to_s3(bucket_name: str, filename: str, file_key: str) -> Optional[str]:
    try:
        s3.upload_file(Filename=filename, Bucket=bucket_name, Key=file_key)
        logger.info(f"Successfully uploaded the json to {bucket_name} as {file_key}")
    except ClientError as ex:
        logger.error("Failed to upload file to s3.")
        logger.error(ex)
        file_key = None
    return file_key
