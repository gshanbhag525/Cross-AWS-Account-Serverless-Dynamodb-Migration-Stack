from aws_cdk import Duration, Stack, CfnOutput
from constructs import Construct

from aws_cdk.aws_lambda import Function, Runtime, Code, Tracing
import aws_cdk.aws_dynamodb as dynamodb
import aws_cdk.aws_apigateway as apigateway
from aws_cdk.aws_iam import Policy, PolicyDocument, PolicyStatement, Effect
from aws_cdk.aws_s3 import Bucket, BlockPublicAccess, BucketAccessControl, EventType
from aws_cdk.aws_s3_notifications import LambdaDestination
from aws_cdk.aws_sns import Topic
from aws_cdk.aws_sns_subscriptions import EmailSubscription

import pipeline_stack.config as config


class PipelineStackStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        config_data = config.getConfigurations()
        account_id = self.account
        region = self.region
        target_account_id = config_data["target_account_id"]
        target_account_role_name = config_data["target_account_role_name"]

        TARGET_ACC_DEJ_ROLE_ARN = (
            f"arn:aws:iam::{target_account_id}:role/{target_account_role_name}"
        )

        table = dynamodb.Table.from_table_attributes(
            self,
            "data_table",
            table_arn=config_data["table_arn"],
            global_indexes=[config_data["gsi_index_name"]],
        )
        another_table = dynamodb.Table.from_table_attributes(
            self,
            "anotherdatatable",
            table_arn=config_data["another_table_arn"],
            global_indexes=[config_data["another_table_gsi_index_name"]],
        )
        # policy to access target acc db from main acc db
        access_target_acc = PolicyDocument(
            statements=[
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=["sts:AssumeRole"],
                    resources=[TARGET_ACC_DEJ_ROLE_ARN],
                ),
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "dynamodb:PutItem",
                        "dynamodb:Query",
                    ],
                    resources=[TARGET_ACC_DEJ_ROLE_ARN],
                ),
            ]
        )

        processing_bucket = Bucket(
            self,
            "processing_bucket",
            block_public_access=BlockPublicAccess.BLOCK_ALL,
            access_control=BucketAccessControl.BUCKET_OWNER_FULL_CONTROL,
        )

        sns_topic = Topic(
            self,
            "eplite_pipeline_send_mail_topic",
            display_name="eplite_pipeline_mail_topic",
        )
        sns_topic.add_subscription(EmailSubscription("test@test.com"))

        processing_repid_function = Function(
            self,
            "processing_repid_function",
            runtime=Runtime.PYTHON_3_9,
            timeout=Duration.minutes(15),
            handler="lambda_handler.lambda_handler",
            code=Code.from_asset(path="lambda/processRepidToS3"),
            environment={
                "PROCESSING_BUCKET_NAME": processing_bucket.bucket_name,
            },
            tracing=Tracing.ACTIVE,
        )
        processing_bucket.grant_write(processing_repid_function)

        migrate_results_to_function = Function(
            self,
            "migrate_results_to_function",
            runtime=Runtime.PYTHON_3_9,
            timeout=Duration.minutes(15),
            handler="lambda_handler.lambda_handler",
            code=Code.from_asset(path="lambda/migrateResults"),
            environment={
                "TABLE_NAME": table.table_name,
                "HMS_TABLE_NAME": another_table.table_name,
                "TARGET_ACC_DEJ_ROLE_ARN": TARGET_ACC_DEJ_ROLE_ARN,
                "TARGET_ACC_TABLE_NAME": config_data["TARGET_ACC_TABLE_NAME"],
                "TARGET_ACC_REGION": config_data["TARGET_ACC_REGION"],
                "TARGET_ACC_HMS_TABLE_NAME": config_data["TARGET_ACC_HMS_TABLE_NAME"],
                "PROCESSING_BUCKET_NAME": processing_bucket.bucket_name,
                "SNS_ARN": sns_topic.topic_arn,
            },
            tracing=Tracing.ACTIVE,
        )
        table.grant_read_data(migrate_results_to_function)
        another_table.grant_read_data(migrate_results_to_function)
        processing_bucket.grant_read_write(migrate_results_to_function)
        sns_topic.grant_publish(migrate_results_to_function)

        processing_bucket.add_event_notification(
            EventType.OBJECT_CREATED,
            LambdaDestination(migrate_results_to_function),
        )

        migrate_results_to_function.role.attach_inline_policy(
            Policy(self, "access_target_acc_policy", document=access_target_acc)
        )

        migrate_result_api = apigateway.RestApi(self, "migrate_result_api")
        plan = migrate_result_api.add_usage_plan(
            "migrate_result_api_plan",
            name="migrate_result_api_plan",
            throttle=apigateway.ThrottleSettings(rate_limit=10, burst_limit=2),
        )

        key = migrate_result_api.add_api_key("migrate_result_api_key")
        CfnOutput(self, "migrate_result_api_cfnop", value=key.key_id)

        plan.add_api_key(key)
        plan.add_api_stage(stage=migrate_result_api.deployment_stage)
        result_api_resource = migrate_result_api.root.add_resource("result")
        migrate_result_api_resource = result_api_resource.add_resource("migrate")

        lamda_integration = apigateway.LambdaIntegration(processing_repid_function)
        migrate_result_api_resource.add_method(
            "POST", lamda_integration, api_key_required=True
        )
