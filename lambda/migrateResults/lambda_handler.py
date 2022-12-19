import os
import boto3
import json
import logging
from urllib.parse import unquote_plus

from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr, Key
from boto3.session import Session

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
client = boto3.client("sns")
SNS_ARN = os.getenv("SNS_ARN")
dynamodb = boto3.resource("dynamodb")
tablename = os.getenv("TABLE_NAME")
main_table = dynamodb.Table(tablename)

hms_tablename = os.getenv("HMS_TABLE_NAME")
hms_table = dynamodb.Table(hms_tablename)

PROCESSING_BUCKET_NAME = os.getenv("PROCESSING_BUCKET_NAME")
TARGET_ACC_DEJ_ACCESS_ROLE = os.getenv("TARGET_ACC_DEJ_ROLE_ARN")
sts_client = boto3.client("sts")
sts_session = sts_client.assume_role(
    RoleArn=TARGET_ACC_DEJ_ACCESS_ROLE, RoleSessionName="test-dynamodb-session"
)

TARGET_ACC_ACCESS_KEY_ID = sts_session["Credentials"]["AccessKeyId"]
TARGET_ACC_ACCESS_KEY = sts_session["Credentials"]["SecretAccessKey"]
TARGET_ACC_TOKEN = sts_session["Credentials"]["SessionToken"]

TARGET_ACC_REGION = os.getenv("TARGET_ACC_REGION")
TARGET_ACC_TABLE_NAME = os.getenv("TARGET_ACC_TABLE_NAME")
TARGET_ACC_HMS_TABLE_NAME = os.getenv("TARGET_ACC_HMS_TABLE_NAME")

target_acc_dynamodb_session = Session(
    aws_access_key_id=TARGET_ACC_ACCESS_KEY_ID,
    aws_secret_access_key=TARGET_ACC_ACCESS_KEY,
    aws_session_token=TARGET_ACC_TOKEN,
    region_name=TARGET_ACC_REGION,
)

target_dynamodb = target_acc_dynamodb_session.resource("dynamodb")
target_table = target_dynamodb.Table(TARGET_ACC_TABLE_NAME)
target_hms_table = target_dynamodb.Table(TARGET_ACC_HMS_TABLE_NAME)

items_arr = []

"""
  This lambda migrates:
  1. results items  
"""


def lambda_handler(event, context):
    lambda_response = {"statusCode": 200, "body": ""}
    logger.info(f"{event=}")
    s3_file_key = unquote_plus(event["Records"][0]["s3"]["object"]["key"])
    repid_json = get_json_from_s3(PROCESSING_BUCKET_NAME, key=s3_file_key)
    if not repid_json:
        logger.error("repid is empty")
        return

    repid = repid_json.get("repid")
    logger.info(f"{repid=}")

    data = {"repid_list": [repid]}
    try:
        for repid in data["repid_list"]:
            report_estimated_response = main_table.query(
                KeyConditionExpression=Key("PK").eq(repid)
                & Key("SK").begins_with("#DEF#TS"),
                FilterExpression=Attr("STATE").eq("ESTIMATED"),
            )
            logger.info(
                f"Found {len(report_estimated_response['Items'])} no of report estimated items for repid {repid}"
            )

            if (
                report_estimated_response["Items"] == None
                or report_estimated_response["Items"] == []
            ):
                continue

            for report_est_item in report_estimated_response["Items"]:
                try:
                    target_table.put_item(
                        Item=report_est_item,
                        ConditionExpression=Attr("PK").ne(report_est_item["PK"])
                        & Attr("SK").ne(report_est_item["SK"]),
                    )
                except ClientError as e:
                    # Ignore the ConditionalCheckFailedException, bubble up
                    # other exceptions.
                    if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
                        raise

            response = main_table.query(
                KeyConditionExpression=Key("PK").eq("REPORTING_INDEX")
                & Key("SK").begins_with(repid)
            )

            trimmed_repid = repid.lstrip("REPORTID#")
            logger.info(f"No of subreport: {len(response['Items'])}")

            if response["Items"] == None or response["Items"] == []:
                continue

            for response_items in response["Items"]:
                logger.info(
                    f"No of task barcodes: {len(response_items['ITEMS'])}"
                )

                result_arr = []
                for barcode in response_items["ITEMS"]:
                    results_response = main_table.query(
                        IndexName="PK-SK-index",
                        KeyConditionExpression=Key("barcode").eq(barcode)
                        & Key("SK").begins_with("#STATE_DATE#"),
                        FilterExpression=Attr("type_event").eq("REPORT_READY")
                        & Attr("REPID").eq(trimmed_repid),
                    )

                    if not results_response["Items"]:
                        logger.info("No results found for {barcode}")
                        continue

                    logger.info(f"No of result items: {len(results_response['Items'])}")

                    for item in results_response["Items"]:

                        # added additonal check if report ready is already existign in target acc
                        logger.info("here")
                        try:
                            state_date = item["SK"].split("#")[2]
                        except Exception as e:
                            logger.error(e)
                        target_results_response = target_table.query(
                            IndexName="barcode-SK-index",
                            KeyConditionExpression=Key("barcode").eq(barcode)
                            & Key("SK").begins_with(f"#STATE_DATE#{state_date}"),
                            FilterExpression=Attr("type_event").eq("REPORT_READY")
                            & Attr("REPID").eq(trimmed_repid),
                        )
                        logger.info(f"{target_results_response['Items']=}")

                        try:
                            if (
                                target_results_response["Items"] == None
                                or target_results_response["Items"] == []
                            ):
                                item["type_event"] = "REPORT_DRAFT"
                                target_table.put_item(
                                    Item=item,
                                    ConditionExpression=Attr("PK").ne(item["PK"])
                                    & Attr("SK").ne(item["SK"])
                                    & Attr("REPID").ne(item["REPID"])
                                    & Attr("type_event").ne(item["type_event"]),
                                )
                                result_arr.append(
                                    {"processed_result_barcode": item["PK"]}
                                )
                                items_arr.append(result_arr)
                        except ClientError as e:
                            # Ignore the ConditionalCheckFailedException, bubble up
                            # other exceptions.
                            if (
                                e.response["Error"]["Code"]
                                != "ConditionalCheckFailedException"
                            ):
                                raise

        response = client.publish(
            TopicArn=SNS_ARN,
            Message=json.dumps(items_arr),
            Subject="Hello Here's the processed items details",
        )
        logger.info(f"sns response: {response}")

        delete_hms_s3_obj(s3_file_key)

        lambda_response["body"] = json.dumps(
            {
                "message": "Reports processing",
            }
        )

    except Exception as e:
        logger.info(e)
        lambda_response["body"] = json.dumps(
            {
                "message": "Reports not published due to error, pls check logs",
            }
        )

    return lambda_response


def delete_hms_s3_obj(key):
    s3.delete_object(Bucket=PROCESSING_BUCKET_NAME, Key=key)


def get_json_from_s3(bucket_name: str, key: str):
    print(f"get_json_from_s3: {bucket_name=}, {key=}")
    try:
        data = s3.get_object(Bucket=bucket_name, Key=key)
        json_text_bytes = data["Body"].read().decode("utf-8")
        json_text = json.loads(json_text_bytes)
    except ClientError as e:
        logger.error("Failed to get json from s3.")
        logger.error(e)
        json_text = {}
    finally:
        print("json_text:", json_text)
    return json_text
