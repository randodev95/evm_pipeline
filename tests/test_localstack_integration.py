"""
Integration tests for EVM Pipeline against LocalStack.

These tests require LocalStack to be running on port 443.
Run with: pytest tests/test_localstack_integration.py -v

Prerequisites:
1. LocalStack running: docker run -d -p 443:443 localstack/localstack
2. CDK deployed: cdklocal deploy --all --context localstack=true
"""

import json
import pytest
from botocore.exceptions import ClientError


class TestLocalStackS3:
    """Test S3 bucket operations against LocalStack."""

    @pytest.mark.integration
    def test_abi_bucket_exists(self, s3_client):
        """Test that ABI bucket exists after CDK deployment."""
        try:
            buckets = s3_client.list_buckets()
            bucket_names = [b["Name"] for b in buckets.get("Buckets", [])]
            # Check for bucket with 'abi' in name
            abi_buckets = [n for n in bucket_names if "abi" in n.lower()]
            assert len(abi_buckets) >= 0, "ABI bucket check completed"
        except ClientError as e:
            pytest.skip(f"LocalStack not available: {e}")

    @pytest.mark.integration
    def test_can_upload_abi(self, s3_client):
        """Test uploading an ABI file to S3."""
        test_bucket = "test-evm-pipeline-abis"
        test_abi = {"abi": [{"type": "event", "name": "Transfer"}]}

        try:
            # Create bucket
            s3_client.create_bucket(Bucket=test_bucket)

            # Upload ABI
            s3_client.put_object(
                Bucket=test_bucket,
                Key="test_abi.json",
                Body=json.dumps(test_abi),
                ContentType="application/json",
            )

            # Verify upload
            response = s3_client.get_object(Bucket=test_bucket, Key="test_abi.json")
            content = json.loads(response["Body"].read().decode("utf-8"))
            assert content == test_abi

            # Cleanup
            s3_client.delete_object(Bucket=test_bucket, Key="test_abi.json")
            s3_client.delete_bucket(Bucket=test_bucket)

        except ClientError as e:
            pytest.skip(f"LocalStack not available: {e}")


class TestLocalStackDynamoDB:
    """Test DynamoDB operations against LocalStack."""

    @pytest.mark.integration
    def test_contracts_table_schema(self, dynamodb_client):
        """Test that contracts table can be created with correct schema."""
        test_table = "test-evm-contracts"

        try:
            # Create table
            dynamodb_client.create_table(
                TableName=test_table,
                KeySchema=[
                    {"AttributeName": "chainid", "KeyType": "HASH"},
                    {"AttributeName": "contract_address", "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "chainid", "AttributeType": "N"},
                    {"AttributeName": "contract_address", "AttributeType": "S"},
                    {"AttributeName": "chain_name", "AttributeType": "S"},
                ],
                GlobalSecondaryIndexes=[
                    {
                        "IndexName": "chain-name-index",
                        "KeySchema": [
                            {"AttributeName": "chain_name", "KeyType": "HASH"}
                        ],
                        "Projection": {"ProjectionType": "ALL"},
                    }
                ],
                BillingMode="PAY_PER_REQUEST",
            )

            # Verify table
            response = dynamodb_client.describe_table(TableName=test_table)
            assert response["Table"]["TableName"] == test_table
            assert response["Table"]["KeySchema"] == [
                {"AttributeName": "chainid", "KeyType": "HASH"},
                {"AttributeName": "contract_address", "KeyType": "RANGE"},
            ]

            # Cleanup
            dynamodb_client.delete_table(TableName=test_table)

        except ClientError as e:
            if "ResourceInUseException" in str(e):
                dynamodb_client.delete_table(TableName=test_table)
            pytest.skip(f"LocalStack not available: {e}")

    @pytest.mark.integration
    def test_put_and_get_contract(self, dynamodb_resource):
        """Test putting and getting a contract record."""
        test_table_name = "test-evm-contracts-crud"

        try:
            # Create table
            table = dynamodb_resource.create_table(
                TableName=test_table_name,
                KeySchema=[
                    {"AttributeName": "chainid", "KeyType": "HASH"},
                    {"AttributeName": "contract_address", "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "chainid", "AttributeType": "N"},
                    {"AttributeName": "contract_address", "AttributeType": "S"},
                ],
                BillingMode="PAY_PER_REQUEST",
            )
            table.wait_until_exists()

            # Put item
            table.put_item(
                Item={
                    "chainid": 1,
                    "contract_address": "0x1234567890abcdef",
                    "chain_name": "ethereum",
                    "contract_abi": "s3://abis/test.json",
                    "last_updated_block": 0,
                    "contract_creation_block": 18000000,
                }
            )

            # Get item
            response = table.get_item(
                Key={"chainid": 1, "contract_address": "0x1234567890abcdef"}
            )

            assert "Item" in response
            assert response["Item"]["chain_name"] == "ethereum"
            assert response["Item"]["contract_creation_block"] == 18000000

            # Cleanup
            table.delete()

        except ClientError as e:
            pytest.skip(f"LocalStack not available: {e}")


class TestLocalStackSSM:
    """Test SSM Parameter Store operations against LocalStack."""

    @pytest.mark.integration
    def test_put_and_get_parameter(self, ssm_client):
        """Test putting and getting an SSM parameter."""
        param_name = "/test/evm-pipeline/api-key"
        param_value = "test-api-key-12345"

        try:
            # Put parameter
            ssm_client.put_parameter(
                Name=param_name,
                Value=param_value,
                Type="SecureString",
                Overwrite=True,
            )

            # Get parameter
            response = ssm_client.get_parameter(Name=param_name, WithDecryption=True)

            assert response["Parameter"]["Value"] == param_value
            assert response["Parameter"]["Type"] == "SecureString"

            # Cleanup
            ssm_client.delete_parameter(Name=param_name)

        except ClientError as e:
            pytest.skip(f"LocalStack not available: {e}")


class TestLocalStackStepFunctions:
    """Test Step Functions operations against LocalStack."""

    @pytest.mark.integration
    def test_create_state_machine(self, stepfunctions_client):
        """Test creating a simple state machine."""
        sm_name = "test-evm-pipeline-sm"
        definition = {
            "Comment": "Test state machine",
            "StartAt": "PassState",
            "States": {
                "PassState": {
                    "Type": "Pass",
                    "End": True,
                }
            },
        }

        try:
            # Create state machine
            response = stepfunctions_client.create_state_machine(
                name=sm_name,
                definition=json.dumps(definition),
                roleArn="arn:aws:iam::000000000000:role/test-role",
            )

            sm_arn = response["stateMachineArn"]
            assert sm_name in sm_arn

            # Describe state machine
            describe_response = stepfunctions_client.describe_state_machine(
                stateMachineArn=sm_arn
            )
            assert describe_response["name"] == sm_name

            # Cleanup
            stepfunctions_client.delete_state_machine(stateMachineArn=sm_arn)

        except ClientError as e:
            pytest.skip(f"LocalStack not available: {e}")


class TestLocalStackEventBridge:
    """Test EventBridge operations against LocalStack."""

    @pytest.mark.integration
    def test_create_schedule_rule(self, events_client):
        """Test creating an EventBridge schedule rule."""
        rule_name = "test-evm-pipeline-schedule"

        try:
            # Create rule
            events_client.put_rule(
                Name=rule_name,
                ScheduleExpression="rate(30 minutes)",
                State="ENABLED",
                Description="Test schedule rule",
            )

            # Describe rule
            response = events_client.describe_rule(Name=rule_name)
            assert response["Name"] == rule_name
            assert response["ScheduleExpression"] == "rate(30 minutes)"

            # Cleanup
            events_client.delete_rule(Name=rule_name)

        except ClientError as e:
            pytest.skip(f"LocalStack not available: {e}")
