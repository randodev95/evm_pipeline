"""Tests for CDK stack synthesis."""

import pytest
import aws_cdk as cdk
from aws_cdk.assertions import Template, Match

from stacks.storage_stack import StorageStack
from stacks.lambda_stack import LambdaStack
from stacks.orchestration_stack import OrchestrationStack


@pytest.fixture
def app():
    """Create CDK app for testing."""
    return cdk.App()


@pytest.fixture
def env():
    """Create CDK environment for testing."""
    return cdk.Environment(account="123456789012", region="us-east-1")


class TestStorageStack:
    """Tests for StorageStack."""

    def test_s3_buckets_created(self, app, env):
        """Test that all 3 S3 buckets are created."""
        stack = StorageStack(app, "TestStorage", env=env)
        template = Template.from_stack(stack)

        template.resource_count_is("AWS::S3::Bucket", 3)

    def test_s3_buckets_have_encryption(self, app, env):
        """Test that S3 buckets have server-side encryption."""
        stack = StorageStack(app, "TestStorage", env=env)
        template = Template.from_stack(stack)

        template.has_resource_properties(
            "AWS::S3::Bucket",
            {
                "BucketEncryption": {
                    "ServerSideEncryptionConfiguration": Match.any_value()
                }
            },
        )

    def test_s3_buckets_block_public_access(self, app, env):
        """Test that S3 buckets block public access."""
        stack = StorageStack(app, "TestStorage", env=env)
        template = Template.from_stack(stack)

        template.has_resource_properties(
            "AWS::S3::Bucket",
            {
                "PublicAccessBlockConfiguration": {
                    "BlockPublicAcls": True,
                    "BlockPublicPolicy": True,
                    "IgnorePublicAcls": True,
                    "RestrictPublicBuckets": True,
                }
            },
        )

    def test_dynamodb_table_created(self, app, env):
        """Test that DynamoDB table is created with correct schema."""
        stack = StorageStack(app, "TestStorage", env=env)
        template = Template.from_stack(stack)

        template.resource_count_is("AWS::DynamoDB::Table", 1)

        template.has_resource_properties(
            "AWS::DynamoDB::Table",
            {
                "TableName": "evm-pipeline-contracts",
                "KeySchema": [
                    {"AttributeName": "chainid", "KeyType": "HASH"},
                    {"AttributeName": "contract_address", "KeyType": "RANGE"},
                ],
                "BillingMode": "PAY_PER_REQUEST",
            },
        )

    def test_dynamodb_has_gsi(self, app, env):
        """Test that DynamoDB table has Global Secondary Index."""
        stack = StorageStack(app, "TestStorage", env=env)
        template = Template.from_stack(stack)

        template.has_resource_properties(
            "AWS::DynamoDB::Table",
            {
                "GlobalSecondaryIndexes": [
                    {
                        "IndexName": "chain-name-index",
                        "KeySchema": [
                            {"AttributeName": "chain_name", "KeyType": "HASH"}
                        ],
                    }
                ]
            },
        )

    def test_ssm_parameter_created(self, app, env):
        """Test that SSM parameter for API key is created."""
        stack = StorageStack(app, "TestStorage", env=env)
        template = Template.from_stack(stack)

        template.resource_count_is("AWS::SSM::Parameter", 1)
        template.has_resource_properties(
            "AWS::SSM::Parameter",
            {"Name": "/evm-pipeline/etherscan-api-key"},
        )

    def test_outputs_created(self, app, env):
        """Test that stack outputs are created."""
        stack = StorageStack(app, "TestStorage", env=env)
        template = Template.from_stack(stack)

        template.has_output("AbiBucketName", {})
        template.has_output("RawDataBucketName", {})
        template.has_output("DecodedDataBucketName", {})
        template.has_output("ContractsTableName", {})


class TestLambdaStack:
    """Tests for LambdaStack."""

    def test_lambda_functions_created(self, app, env):
        """Test that all 3 Lambda functions are created."""
        storage_stack = StorageStack(app, "TestStorage", env=env)
        lambda_stack = LambdaStack(
            app, "TestLambda", storage_stack=storage_stack, env=env
        )
        template = Template.from_stack(lambda_stack)

        template.resource_count_is("AWS::Lambda::Function", 3)

    def test_fetch_latest_block_config(self, app, env):
        """Test fetch_latest_block Lambda configuration."""
        storage_stack = StorageStack(app, "TestStorage", env=env)
        lambda_stack = LambdaStack(
            app, "TestLambda", storage_stack=storage_stack, env=env
        )
        template = Template.from_stack(lambda_stack)

        template.has_resource_properties(
            "AWS::Lambda::Function",
            {
                "FunctionName": "evm-pipeline-fetch-latest-block",
                "MemorySize": 512,
                "Timeout": 60,
            },
        )

    def test_sync_raw_data_config(self, app, env):
        """Test sync_raw_data Lambda configuration."""
        storage_stack = StorageStack(app, "TestStorage", env=env)
        lambda_stack = LambdaStack(
            app, "TestLambda", storage_stack=storage_stack, env=env
        )
        template = Template.from_stack(lambda_stack)

        template.has_resource_properties(
            "AWS::Lambda::Function",
            {
                "FunctionName": "evm-pipeline-sync-raw-data",
                "MemorySize": 1024,
                "Timeout": 600,
            },
        )

    def test_decode_data_config(self, app, env):
        """Test decode_data Lambda configuration."""
        storage_stack = StorageStack(app, "TestStorage", env=env)
        lambda_stack = LambdaStack(
            app, "TestLambda", storage_stack=storage_stack, env=env
        )
        template = Template.from_stack(lambda_stack)

        template.has_resource_properties(
            "AWS::Lambda::Function",
            {
                "FunctionName": "evm-pipeline-decode-data",
                "MemorySize": 2048,
                "Timeout": 600,
            },
        )

    def test_lambda_iam_roles_created(self, app, env):
        """Test that IAM roles are created for Lambda functions."""
        storage_stack = StorageStack(app, "TestStorage", env=env)
        lambda_stack = LambdaStack(
            app, "TestLambda", storage_stack=storage_stack, env=env
        )
        template = Template.from_stack(lambda_stack)

        # Each Lambda gets its own role
        template.resource_count_is("AWS::IAM::Role", 3)

    def test_outputs_created(self, app, env):
        """Test that Lambda ARN outputs are created."""
        storage_stack = StorageStack(app, "TestStorage", env=env)
        lambda_stack = LambdaStack(
            app, "TestLambda", storage_stack=storage_stack, env=env
        )
        template = Template.from_stack(lambda_stack)

        template.has_output("FetchLatestBlockFnArn", {})
        template.has_output("SyncRawDataFnArn", {})
        template.has_output("DecodeDataFnArn", {})


class TestOrchestrationStack:
    """Tests for OrchestrationStack."""

    def test_state_machine_created(self, app, env):
        """Test that Step Functions state machine is created."""
        storage_stack = StorageStack(app, "TestStorage", env=env)
        lambda_stack = LambdaStack(
            app, "TestLambda", storage_stack=storage_stack, env=env
        )
        orchestration_stack = OrchestrationStack(
            app,
            "TestOrchestration",
            lambda_stack=lambda_stack,
            storage_stack=storage_stack,
            env=env,
        )
        template = Template.from_stack(orchestration_stack)

        template.resource_count_is("AWS::StepFunctions::StateMachine", 1)

    def test_state_machine_name(self, app, env):
        """Test state machine has correct name."""
        storage_stack = StorageStack(app, "TestStorage", env=env)
        lambda_stack = LambdaStack(
            app, "TestLambda", storage_stack=storage_stack, env=env
        )
        orchestration_stack = OrchestrationStack(
            app,
            "TestOrchestration",
            lambda_stack=lambda_stack,
            storage_stack=storage_stack,
            env=env,
        )
        template = Template.from_stack(orchestration_stack)

        template.has_resource_properties(
            "AWS::StepFunctions::StateMachine",
            {"StateMachineName": "evm-pipeline-sync"},
        )

    def test_eventbridge_rule_created(self, app, env):
        """Test that EventBridge rule is created with 30-minute schedule."""
        storage_stack = StorageStack(app, "TestStorage", env=env)
        lambda_stack = LambdaStack(
            app, "TestLambda", storage_stack=storage_stack, env=env
        )
        orchestration_stack = OrchestrationStack(
            app,
            "TestOrchestration",
            lambda_stack=lambda_stack,
            storage_stack=storage_stack,
            env=env,
        )
        template = Template.from_stack(orchestration_stack)

        template.resource_count_is("AWS::Events::Rule", 1)
        template.has_resource_properties(
            "AWS::Events::Rule",
            {
                "Name": "evm-pipeline-schedule",
                "ScheduleExpression": "rate(30 minutes)",
            },
        )

    def test_state_machine_has_tracing(self, app, env):
        """Test that state machine has X-Ray tracing enabled."""
        storage_stack = StorageStack(app, "TestStorage", env=env)
        lambda_stack = LambdaStack(
            app, "TestLambda", storage_stack=storage_stack, env=env
        )
        orchestration_stack = OrchestrationStack(
            app,
            "TestOrchestration",
            lambda_stack=lambda_stack,
            storage_stack=storage_stack,
            env=env,
        )
        template = Template.from_stack(orchestration_stack)

        template.has_resource_properties(
            "AWS::StepFunctions::StateMachine",
            {"TracingConfiguration": {"Enabled": True}},
        )

    def test_outputs_created(self, app, env):
        """Test that orchestration outputs are created."""
        storage_stack = StorageStack(app, "TestStorage", env=env)
        lambda_stack = LambdaStack(
            app, "TestLambda", storage_stack=storage_stack, env=env
        )
        orchestration_stack = OrchestrationStack(
            app,
            "TestOrchestration",
            lambda_stack=lambda_stack,
            storage_stack=storage_stack,
            env=env,
        )
        template = Template.from_stack(orchestration_stack)

        template.has_output("StateMachineArn", {})
        template.has_output("StateMachineName", {})
        template.has_output("ScheduleRuleName", {})
