#!/usr/bin/env python3
"""
AWS CDK App Entry Point for EVM Pipeline Infrastructure.

Usage:
    # Synthesize CloudFormation templates
    cdk synth

    # Deploy to AWS
    cdk deploy --all

    # Deploy to LocalStack (uses cdklocal wrapper)
    cdklocal deploy --all
"""

import os

import aws_cdk as cdk

from stacks.storage_stack import StorageStack
from stacks.lambda_stack import LambdaStack
from stacks.orchestration_stack import OrchestrationStack


app = cdk.App()

# Environment configuration
env = cdk.Environment(
    account=os.environ.get("CDK_DEFAULT_ACCOUNT", os.environ.get("AWS_ACCOUNT_ID")),
    region=os.environ.get("CDK_DEFAULT_REGION", os.environ.get("AWS_REGION", "us-east-1")),
)

# Create stacks with dependencies
storage_stack = StorageStack(
    app,
    "EvmPipelineStorage",
    env=env,
    description="EVM Pipeline - Storage resources (S3, DynamoDB, SSM)",
)

lambda_stack = LambdaStack(
    app,
    "EvmPipelineLambda",
    storage_stack=storage_stack,
    env=env,
    description="EVM Pipeline - Lambda functions",
)
lambda_stack.add_dependency(storage_stack)

orchestration_stack = OrchestrationStack(
    app,
    "EvmPipelineOrchestration",
    lambda_stack=lambda_stack,
    storage_stack=storage_stack,
    env=env,
    description="EVM Pipeline - Step Functions and EventBridge",
)
orchestration_stack.add_dependency(lambda_stack)

# Add tags to all resources
cdk.Tags.of(app).add("Project", "evm-pipeline")
cdk.Tags.of(app).add("ManagedBy", "CDK")

app.synth()
