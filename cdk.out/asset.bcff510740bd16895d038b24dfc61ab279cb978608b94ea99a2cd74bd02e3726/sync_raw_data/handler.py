"""
Lambda 2: Sync Raw Data

This Lambda fetches raw event logs from Etherscan and writes them
to S3 as DeltaLake parquet files partitioned by chainid, contract_address, and topic0.
"""

import json
import os
from decimal import Decimal
from typing import Any

import boto3
import pandas as pd

from shared.etherscan_client import EtherscanClient
from shared.delta_lake_utils import write_delta_table

# Configuration
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "evm-pipeline-contracts")
RAW_DATA_BUCKET = os.environ.get("RAW_DATA_BUCKET", "evm-pipeline-raw-data-local")
SSM_API_KEY_PARAM = os.environ.get(
    "SSM_API_KEY_PARAM", "/evm-pipeline/etherscan-api-key"
)


def get_api_key() -> str:
    """Fetch Etherscan API key from SSM Parameter Store."""
    ssm = boto3.client("ssm")
    response = ssm.get_parameter(Name=SSM_API_KEY_PARAM, WithDecryption=True)
    return response["Parameter"]["Value"]


def update_last_synced_block(
    chain_id: int, contract_address: str, block_number: int
) -> None:
    """Update the last_updated_block in DynamoDB."""
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(DYNAMODB_TABLE)

    table.update_item(
        Key={"chainid": chain_id, "contract_address": contract_address},
        UpdateExpression="SET last_updated_block = :block",
        ExpressionAttributeValues={":block": block_number},
    )


def extract_topic0(topics: list[str] | None) -> str | None:
    """Extract topic0 (event signature) from topics list."""
    if topics and len(topics) > 0:
        return topics[0]
    return None


def process_logs_to_dataframe(
    logs: list[dict[str, Any]], chain_id: int, contract_address: str
) -> pd.DataFrame:
    """
    Convert raw logs to a DataFrame with proper columns.

    Adds chainid, contract_address, and topic0 for partitioning.
    """
    if not logs:
        return pd.DataFrame()

    # Add metadata columns
    for log in logs:
        log["chainid"] = chain_id
        log["contract_address"] = contract_address
        log["topic0"] = extract_topic0(log.get("topics"))

        # Convert topics list to JSON string for storage
        if "topics" in log and isinstance(log["topics"], list):
            log["topics_json"] = json.dumps(log["topics"])

    df = pd.DataFrame(logs)

    # Ensure required columns exist
    required_columns = [
        "chainid",
        "contract_address",
        "topic0",
        "address",
        "blockNumber",
        "transactionHash",
        "data",
        "topics_json",
    ]

    for col in required_columns:
        if col not in df.columns:
            df[col] = None

    return df


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    Main Lambda handler for syncing raw data.

    Input (from Step Function Map state):
        {
            "chainid": 1,
            "contract_address": "0x...",
            "chain_name": "ethereum",
            "contract_abi": "s3://bucket/abi.json",
            "last_updated_block": 19999000,
            "contract_creation_block": 18000000,
            "target_block": 20000000
        }

    Output:
        {
            "status": "success",
            "chainid": 1,
            "contract_address": "0x...",
            "synced_from_block": 19999001,
            "synced_to_block": 20000000,
            "logs_count": 150
        }
    """
    print(f"Received event: {json.dumps(event, default=str)}")

    # Extract contract info
    chain_id = int(event.get("chainid", 0))
    contract_address = event.get("contract_address", "")
    target_block = int(event.get("target_block", 0))
    last_updated_block = int(event.get("last_updated_block", 0))
    contract_creation_block = int(event.get("contract_creation_block", 0))

    if not chain_id or not contract_address or not target_block:
        return {
            "status": "error",
            "error": "Missing required fields: chainid, contract_address, or target_block",
            "chainid": chain_id,
            "contract_address": contract_address,
        }

    # Determine sync mode
    if last_updated_block == 0:
        # Full backfill from contract creation
        from_block = contract_creation_block if contract_creation_block > 0 else 0
        print(
            f"Full backfill mode: starting from block {from_block} "
            f"(contract creation: {contract_creation_block})"
        )
    else:
        # Incremental sync
        from_block = last_updated_block + 1
        print(f"Incremental sync mode: from block {from_block} to {target_block}")

    # Check if we need to sync
    if from_block > target_block:
        print(f"No new blocks to sync (from={from_block}, target={target_block})")
        return {
            "status": "no_new_data",
            "chainid": chain_id,
            "contract_address": contract_address,
            "message": "Already synced to target block",
            "last_updated_block": last_updated_block,
            "target_block": target_block,
        }

    # Get API key and create client
    api_key = get_api_key()
    client = EtherscanClient(api_key)

    # Fetch logs
    print(f"Fetching logs for {contract_address} on chain {chain_id}")
    print(f"Block range: {from_block} to {target_block}")

    try:
        logs = client.get_logs(
            chain_id=chain_id,
            address=contract_address,
            from_block=from_block,
            to_block=target_block,
        )
        print(f"Fetched {len(logs)} logs")

    except Exception as e:
        print(f"Error fetching logs: {e}")
        return {
            "status": "error",
            "error": f"Failed to fetch logs: {str(e)}",
            "chainid": chain_id,
            "contract_address": contract_address,
        }

    # Process and write logs
    if logs:
        df = process_logs_to_dataframe(logs, chain_id, contract_address)
        print(f"Processed DataFrame with {len(df)} rows")

        # Write to DeltaLake
        table_path = f"s3://{RAW_DATA_BUCKET}/raw_logs"

        try:
            write_delta_table(
                table_path=table_path,
                df=df,
                partition_by=["chainid", "contract_address", "topic0"],
                mode="append",
            )
            print(f"Successfully wrote {len(df)} rows to {table_path}")

        except Exception as e:
            print(f"Error writing to DeltaLake: {e}")
            return {
                "status": "error",
                "error": f"Failed to write to DeltaLake: {str(e)}",
                "chainid": chain_id,
                "contract_address": contract_address,
            }

    # Update DynamoDB with last synced block
    try:
        update_last_synced_block(chain_id, contract_address, target_block)
        print(f"Updated last_updated_block to {target_block}")

    except Exception as e:
        print(f"Warning: Failed to update DynamoDB: {e}")
        # Don't fail the whole operation for this

    return {
        "status": "success",
        "chainid": chain_id,
        "contract_address": contract_address,
        "chain_name": event.get("chain_name"),
        "contract_abi": event.get("contract_abi"),
        "synced_from_block": from_block,
        "synced_to_block": target_block,
        "logs_count": len(logs),
    }
