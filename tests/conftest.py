"""Pytest fixtures for EVM Pipeline infrastructure tests."""

import os
import sys
import pytest
import boto3
import warnings

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Suppress SSL warnings for LocalStack
warnings.filterwarnings("ignore", message="Unverified HTTPS request")


@pytest.fixture(scope="session")
def localstack_endpoint():
    """Return LocalStack endpoint URL (port 443 as configured)."""
    return os.environ.get("LOCALSTACK_ENDPOINT", "https://localhost:443")


@pytest.fixture(scope="session")
def aws_credentials():
    """Set mock AWS credentials for LocalStack."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    yield


@pytest.fixture(scope="session")
def dynamodb_resource(localstack_endpoint, aws_credentials):
    """Create DynamoDB resource pointing to LocalStack."""
    return boto3.resource(
        "dynamodb",
        endpoint_url=localstack_endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        verify=False,
    )


@pytest.fixture(scope="session")
def dynamodb_client(localstack_endpoint, aws_credentials):
    """Create DynamoDB client pointing to LocalStack."""
    return boto3.client(
        "dynamodb",
        endpoint_url=localstack_endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        verify=False,
    )


@pytest.fixture(scope="session")
def s3_client(localstack_endpoint, aws_credentials):
    """Create S3 client pointing to LocalStack."""
    return boto3.client(
        "s3",
        endpoint_url=localstack_endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        verify=False,
    )


@pytest.fixture(scope="session")
def ssm_client(localstack_endpoint, aws_credentials):
    """Create SSM client pointing to LocalStack."""
    return boto3.client(
        "ssm",
        endpoint_url=localstack_endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        verify=False,
    )


@pytest.fixture(scope="session")
def stepfunctions_client(localstack_endpoint, aws_credentials):
    """Create Step Functions client pointing to LocalStack."""
    return boto3.client(
        "stepfunctions",
        endpoint_url=localstack_endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        verify=False,
    )


@pytest.fixture(scope="session")
def lambda_client(localstack_endpoint, aws_credentials):
    """Create Lambda client pointing to LocalStack."""
    return boto3.client(
        "lambda",
        endpoint_url=localstack_endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        verify=False,
    )


@pytest.fixture(scope="session")
def events_client(localstack_endpoint, aws_credentials):
    """Create EventBridge client pointing to LocalStack."""
    return boto3.client(
        "events",
        endpoint_url=localstack_endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        verify=False,
    )
