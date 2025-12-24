"""Storage Stack - S3 buckets and DynamoDB table for EVM Pipeline."""

from aws_cdk import (
    Stack,
    RemovalPolicy,
    Duration,
    CfnOutput,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    aws_ssm as ssm,
)
from constructs import Construct


class StorageStack(Stack):
    """Stack containing S3 buckets and DynamoDB table for EVM data pipeline."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # S3 Bucket 1: Contract ABIs
        self.abi_bucket = s3.Bucket(
            self,
            "AbiBucket",
            bucket_name=f"evm-pipeline-abis-{self.account}",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            removal_policy=RemovalPolicy.RETAIN,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        )

        # S3 Bucket 2: Raw DeltaLake Data
        self.raw_data_bucket = s3.Bucket(
            self,
            "RawDataBucket",
            bucket_name=f"evm-pipeline-raw-data-{self.account}",
            versioned=False,  # DeltaLake handles versioning via transaction log
            encryption=s3.BucketEncryption.S3_MANAGED,
            removal_policy=RemovalPolicy.RETAIN,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="IntelligentTiering",
                    enabled=True,
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                            transition_after=Duration.days(30),
                        )
                    ],
                )
            ],
        )

        # S3 Bucket 3: Decoded DeltaLake Data
        self.decoded_data_bucket = s3.Bucket(
            self,
            "DecodedDataBucket",
            bucket_name=f"evm-pipeline-decoded-data-{self.account}",
            versioned=False,
            encryption=s3.BucketEncryption.S3_MANAGED,
            removal_policy=RemovalPolicy.RETAIN,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="IntelligentTiering",
                    enabled=True,
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                            transition_after=Duration.days(30),
                        )
                    ],
                )
            ],
        )

        # DynamoDB Table: Contract Registry
        self.contracts_table = dynamodb.Table(
            self,
            "ContractsTable",
            table_name="evm-pipeline-contracts",
            partition_key=dynamodb.Attribute(
                name="chainid", type=dynamodb.AttributeType.NUMBER
            ),
            sort_key=dynamodb.Attribute(
                name="contract_address", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True
            ),
        )

        # Global Secondary Index for querying by chain_name
        self.contracts_table.add_global_secondary_index(
            index_name="chain-name-index",
            partition_key=dynamodb.Attribute(
                name="chain_name", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        # SSM Parameter for Etherscan API Key
        self.etherscan_api_key_param = ssm.StringParameter(
            self,
            "EtherscanApiKeyParam",
            parameter_name="/evm-pipeline/etherscan-api-key",
            string_value="placeholder-replace-with-actual-key",
            description="Etherscan API key for fetching blockchain data",
        )

        # Outputs
        CfnOutput(
            self,
            "AbiBucketName",
            value=self.abi_bucket.bucket_name,
            description="S3 bucket for contract ABIs",
        )

        CfnOutput(
            self,
            "RawDataBucketName",
            value=self.raw_data_bucket.bucket_name,
            description="S3 bucket for raw DeltaLake data",
        )

        CfnOutput(
            self,
            "DecodedDataBucketName",
            value=self.decoded_data_bucket.bucket_name,
            description="S3 bucket for decoded DeltaLake data",
        )

        CfnOutput(
            self,
            "ContractsTableName",
            value=self.contracts_table.table_name,
            description="DynamoDB table for contract registry",
        )

        CfnOutput(
            self,
            "EtherscanApiKeyParamName",
            value=self.etherscan_api_key_param.parameter_name,
            description="SSM parameter name for Etherscan API key",
        )
