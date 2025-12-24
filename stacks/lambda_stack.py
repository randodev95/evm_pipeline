"""Lambda Stack - Docker-based Lambda functions for EVM Pipeline."""

from pathlib import Path

from aws_cdk import (
    Stack,
    Duration,
    CfnOutput,
    aws_lambda as lambda_,
    aws_ecr_assets as ecr_assets,
)
from constructs import Construct

from .storage_stack import StorageStack


class LambdaStack(Stack):
    """Stack containing Lambda functions for EVM data pipeline."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        storage_stack: StorageStack,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Path to lambdas directory
        lambdas_dir = Path(__file__).parent.parent / "lambdas"

        # Lambda 1: Fetch Latest Block
        self.fetch_latest_block_fn = lambda_.DockerImageFunction(
            self,
            "FetchLatestBlockFn",
            function_name="evm-pipeline-fetch-latest-block",
            code=lambda_.DockerImageCode.from_image_asset(
                directory=str(lambdas_dir),
                file="fetch_latest_block/Dockerfile",
                platform=ecr_assets.Platform.LINUX_AMD64,
            ),
            memory_size=512,
            timeout=Duration.minutes(1),
            architecture=lambda_.Architecture.X86_64,
            environment={
                "DYNAMODB_TABLE": storage_stack.contracts_table.table_name,
                "SSM_API_KEY_PARAM": storage_stack.etherscan_api_key_param.parameter_name,
                "REORG_BUFFER_BLOCKS": "50",
            },
            description="Fetches latest block for each chain with reorg buffer",
        )

        # Lambda 2: Sync Raw Data
        self.sync_raw_data_fn = lambda_.DockerImageFunction(
            self,
            "SyncRawDataFn",
            function_name="evm-pipeline-sync-raw-data",
            code=lambda_.DockerImageCode.from_image_asset(
                directory=str(lambdas_dir),
                file="sync_raw_data/Dockerfile",
                platform=ecr_assets.Platform.LINUX_AMD64,
            ),
            memory_size=1024,
            timeout=Duration.minutes(10),
            architecture=lambda_.Architecture.X86_64,
            environment={
                "DYNAMODB_TABLE": storage_stack.contracts_table.table_name,
                "SSM_API_KEY_PARAM": storage_stack.etherscan_api_key_param.parameter_name,
                "RAW_DATA_BUCKET": storage_stack.raw_data_bucket.bucket_name,
                "ABI_BUCKET": storage_stack.abi_bucket.bucket_name,
            },
            description="Syncs raw event logs from Etherscan to DeltaLake",
        )

        # Lambda 3: Decode Data
        self.decode_data_fn = lambda_.DockerImageFunction(
            self,
            "DecodeDataFn",
            function_name="evm-pipeline-decode-data",
            code=lambda_.DockerImageCode.from_image_asset(
                directory=str(lambdas_dir),
                file="decode_data/Dockerfile",
                platform=ecr_assets.Platform.LINUX_AMD64,
            ),
            memory_size=2048,
            timeout=Duration.minutes(10),
            architecture=lambda_.Architecture.X86_64,
            environment={
                "DYNAMODB_TABLE": storage_stack.contracts_table.table_name,
                "RAW_DATA_BUCKET": storage_stack.raw_data_bucket.bucket_name,
                "DECODED_DATA_BUCKET": storage_stack.decoded_data_bucket.bucket_name,
                "ABI_BUCKET": storage_stack.abi_bucket.bucket_name,
            },
            description="Decodes raw event logs using contract ABIs",
        )

        # Grant permissions

        # DynamoDB permissions
        storage_stack.contracts_table.grant_read_data(self.fetch_latest_block_fn)
        storage_stack.contracts_table.grant_read_write_data(self.sync_raw_data_fn)
        storage_stack.contracts_table.grant_read_write_data(self.decode_data_fn)

        # SSM permissions for API key
        storage_stack.etherscan_api_key_param.grant_read(self.fetch_latest_block_fn)
        storage_stack.etherscan_api_key_param.grant_read(self.sync_raw_data_fn)

        # S3 permissions

        # ABI bucket - read access
        storage_stack.abi_bucket.grant_read(self.sync_raw_data_fn)
        storage_stack.abi_bucket.grant_read(self.decode_data_fn)

        # Raw data bucket - write for sync, read for decode
        storage_stack.raw_data_bucket.grant_read_write(self.sync_raw_data_fn)
        storage_stack.raw_data_bucket.grant_read(self.decode_data_fn)

        # Decoded data bucket - write access
        storage_stack.decoded_data_bucket.grant_read_write(self.decode_data_fn)

        # Outputs
        CfnOutput(
            self,
            "FetchLatestBlockFnArn",
            value=self.fetch_latest_block_fn.function_arn,
            description="ARN of the Fetch Latest Block Lambda",
        )

        CfnOutput(
            self,
            "SyncRawDataFnArn",
            value=self.sync_raw_data_fn.function_arn,
            description="ARN of the Sync Raw Data Lambda",
        )

        CfnOutput(
            self,
            "DecodeDataFnArn",
            value=self.decode_data_fn.function_arn,
            description="ARN of the Decode Data Lambda",
        )
