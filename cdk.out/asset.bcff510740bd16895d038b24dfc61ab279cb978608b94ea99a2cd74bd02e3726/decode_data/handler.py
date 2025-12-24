"""
Lambda 3: Decode Data

This Lambda decodes raw event logs using contract ABIs and writes
the decoded data to S3 as DeltaLake parquet files.
"""

import json
import os
from typing import Any

import boto3
import pandas as pd

from shared.abi_decoder import decode_logs
from shared.delta_lake_utils import read_delta_table, write_delta_table

# Configuration
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "evm-pipeline-contracts")
RAW_DATA_BUCKET = os.environ.get("RAW_DATA_BUCKET", "evm-pipeline-raw-data-local")
DECODED_DATA_BUCKET = os.environ.get(
    "DECODED_DATA_BUCKET", "evm-pipeline-decoded-data-local"
)
ABI_BUCKET = os.environ.get("ABI_BUCKET", "evm-pipeline-abis-local")


def load_abi_from_s3(abi_location: str) -> list[dict]:
    """
    Load ABI JSON from S3.

    Args:
        abi_location: S3 URI (s3://bucket/key) or just the key

    Returns:
        ABI as a list of dictionaries
    """
    s3 = boto3.client("s3")

    # Parse S3 URI
    if abi_location.startswith("s3://"):
        parts = abi_location[5:].split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
    else:
        # Assume it's just a key in the ABI bucket
        bucket = ABI_BUCKET
        key = abi_location

    print(f"Loading ABI from s3://{bucket}/{key}")

    response = s3.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")
    abi = json.loads(content)

    # Handle case where ABI is wrapped in an object
    if isinstance(abi, dict) and "abi" in abi:
        abi = abi["abi"]

    return abi


def reconstruct_log_for_decoding(row: pd.Series) -> dict[str, Any]:
    """
    Reconstruct a log entry from DataFrame row for decoding.

    Args:
        row: DataFrame row with log data

    Returns:
        Dictionary in the format expected by the decoder
    """
    # Parse topics from JSON if stored as string
    topics = row.get("topics_json")
    if isinstance(topics, str):
        try:
            topics = json.loads(topics)
        except json.JSONDecodeError:
            topics = []
    elif not topics:
        topics = []

    return {
        "address": row.get("address", ""),
        "blockNumber": row.get("blockNumber", "0x0"),
        "transactionHash": row.get("transactionHash", ""),
        "transactionIndex": row.get("transactionIndex", "0x0"),
        "logIndex": row.get("logIndex", "0x0"),
        "data": row.get("data", "0x"),
        "topics": topics,
        "chainid": row.get("chainid"),
        "contract_address": row.get("contract_address"),
        "topic0": row.get("topic0"),
    }


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    Main Lambda handler for decoding raw data.

    Input (from Step Function Map state - result of sync_raw_data):
        {
            "status": "success",
            "chainid": 1,
            "contract_address": "0x...",
            "chain_name": "ethereum",
            "contract_abi": "s3://bucket/abi.json",
            "synced_from_block": 19999001,
            "synced_to_block": 20000000,
            "logs_count": 150
        }

    Output:
        {
            "status": "success",
            "chainid": 1,
            "contract_address": "0x...",
            "decoded_count": 150,
            "events_found": ["Transfer", "Approval", ...]
        }
    """
    print(f"Received event: {json.dumps(event, default=str)}")

    # Check if previous step had data
    if event.get("status") == "no_new_data":
        print("No new data from sync step, skipping decode")
        return {
            "status": "skipped",
            "reason": "no_new_data",
            "chainid": event.get("chainid"),
            "contract_address": event.get("contract_address"),
        }

    if event.get("status") == "error":
        print(f"Previous step failed: {event.get('error')}")
        return {
            "status": "skipped",
            "reason": "previous_step_failed",
            "chainid": event.get("chainid"),
            "contract_address": event.get("contract_address"),
            "error": event.get("error"),
        }

    # Extract contract info
    chain_id = int(event.get("chainid", 0))
    contract_address = event.get("contract_address", "")
    abi_location = event.get("contract_abi", "")
    synced_from_block = event.get("synced_from_block")
    synced_to_block = event.get("synced_to_block")

    if not chain_id or not contract_address:
        return {
            "status": "error",
            "error": "Missing required fields: chainid or contract_address",
            "chainid": chain_id,
            "contract_address": contract_address,
        }

    if not abi_location:
        return {
            "status": "error",
            "error": "No ABI location provided",
            "chainid": chain_id,
            "contract_address": contract_address,
        }

    # Load ABI from S3
    try:
        abi = load_abi_from_s3(abi_location)
        print(f"Loaded ABI with {len(abi)} entries")
    except Exception as e:
        print(f"Error loading ABI: {e}")
        return {
            "status": "error",
            "error": f"Failed to load ABI: {str(e)}",
            "chainid": chain_id,
            "contract_address": contract_address,
        }

    # Read raw logs from DeltaLake
    raw_table_path = f"s3://{RAW_DATA_BUCKET}/raw_logs"

    try:
        # Filter by chainid and contract_address
        filters = [
            ("chainid", "=", chain_id),
            ("contract_address", "=", contract_address),
        ]

        df = read_delta_table(raw_table_path, filters=filters)
        print(f"Read {len(df)} raw logs from DeltaLake")

    except Exception as e:
        print(f"Error reading raw logs: {e}")
        return {
            "status": "error",
            "error": f"Failed to read raw logs: {str(e)}",
            "chainid": chain_id,
            "contract_address": contract_address,
        }

    if df.empty:
        print("No raw logs found to decode")
        return {
            "status": "no_data",
            "chainid": chain_id,
            "contract_address": contract_address,
            "message": "No raw logs found for this contract",
        }

    # Reconstruct logs for decoding
    logs = [reconstruct_log_for_decoding(row) for _, row in df.iterrows()]
    print(f"Reconstructed {len(logs)} logs for decoding")

    # Decode logs using ABI
    try:
        decoded_logs = decode_logs(logs, abi)
        print(f"Decoded {len(decoded_logs)} logs")
    except Exception as e:
        print(f"Error decoding logs: {e}")
        return {
            "status": "error",
            "error": f"Failed to decode logs: {str(e)}",
            "chainid": chain_id,
            "contract_address": contract_address,
        }

    # Convert to DataFrame
    decoded_df = pd.DataFrame(decoded_logs)

    # Convert decoded_args dict to JSON string for storage
    if "decoded_args" in decoded_df.columns:
        decoded_df["decoded_args_json"] = decoded_df["decoded_args"].apply(
            lambda x: json.dumps(x) if isinstance(x, dict) else str(x)
        )

    # Get unique event names for reporting
    events_found = []
    if "event_name" in decoded_df.columns:
        events_found = (
            decoded_df["event_name"].dropna().unique().tolist()
        )
        print(f"Found events: {events_found}")

    # Write decoded logs to DeltaLake
    decoded_table_path = f"s3://{DECODED_DATA_BUCKET}/decoded_logs"

    try:
        write_delta_table(
            table_path=decoded_table_path,
            df=decoded_df,
            partition_by=["chainid", "contract_address", "topic0"],
            mode="overwrite",  # Overwrite for this contract's partition
        )
        print(f"Successfully wrote {len(decoded_df)} decoded logs")

    except Exception as e:
        print(f"Error writing decoded logs: {e}")
        return {
            "status": "error",
            "error": f"Failed to write decoded logs: {str(e)}",
            "chainid": chain_id,
            "contract_address": contract_address,
        }

    # Count decode statuses
    decode_stats = {}
    if "decode_status" in decoded_df.columns:
        decode_stats = decoded_df["decode_status"].value_counts().to_dict()

    return {
        "status": "success",
        "chainid": chain_id,
        "contract_address": contract_address,
        "decoded_count": len(decoded_logs),
        "events_found": events_found,
        "decode_stats": decode_stats,
        "synced_from_block": synced_from_block,
        "synced_to_block": synced_to_block,
    }
