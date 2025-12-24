"""
Lambda 1: Fetch Latest Block

This Lambda fetches the latest block number for each chain and returns
the contract list with block information for processing.
"""

import json
import os
from decimal import Decimal
from typing import Any

import boto3

from shared.etherscan_client import EtherscanClient, get_chain_name

# Configuration
REORG_BUFFER = int(os.environ.get("REORG_BUFFER_BLOCKS", "50"))
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "evm-pipeline-contracts")
SSM_API_KEY_PARAM = os.environ.get(
    "SSM_API_KEY_PARAM", "/evm-pipeline/etherscan-api-key"
)


def get_api_key() -> str:
    """Fetch Etherscan API key from SSM Parameter Store."""
    ssm = boto3.client("ssm")
    response = ssm.get_parameter(Name=SSM_API_KEY_PARAM, WithDecryption=True)
    return response["Parameter"]["Value"]


def get_contracts_from_dynamodb() -> list[dict[str, Any]]:
    """Scan DynamoDB for all registered contracts."""
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(DYNAMODB_TABLE)

    contracts = []
    response = table.scan()

    while True:
        items = response.get("Items", [])
        # Convert Decimal to int for JSON serialization
        for item in items:
            contracts.append(_convert_decimals(item))

        # Check for pagination
        if "LastEvaluatedKey" in response:
            response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        else:
            break

    return contracts


def _convert_decimals(obj: Any) -> Any:
    """Convert Decimal objects to int/float for JSON serialization."""
    if isinstance(obj, Decimal):
        if obj % 1 == 0:
            return int(obj)
        return float(obj)
    elif isinstance(obj, dict):
        return {k: _convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_convert_decimals(v) for v in obj]
    return obj


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    Main Lambda handler for fetching latest block information.

    Input (from EventBridge):
        {
            "triggered_at": "2024-01-01T00:00:00Z"
        }

    Output:
        {
            "latest_blocks": {
                "1": 20000000,
                "137": 50000000
            },
            "contracts": [
                {
                    "chainid": 1,
                    "contract_address": "0x...",
                    "chain_name": "ethereum",
                    "contract_abi": "s3://bucket/abi.json",
                    "last_updated_block": 19999000,
                    "contract_creation_block": 18000000,
                    "contract_creation_date": "2024-01-01"
                }
            ],
            "triggered_at": "2024-01-01T00:00:00Z"
        }
    """
    print(f"Received event: {json.dumps(event)}")

    # Get API key
    api_key = get_api_key()
    client = EtherscanClient(api_key)

    # Get all contracts from DynamoDB
    contracts = get_contracts_from_dynamodb()
    print(f"Found {len(contracts)} contracts to process")

    if not contracts:
        return {
            "latest_blocks": {},
            "contracts": [],
            "triggered_at": event.get("triggered_at"),
            "message": "No contracts registered in DynamoDB",
        }

    # Get unique chain IDs
    chain_ids = set(int(c.get("chainid", 0)) for c in contracts)
    chain_ids.discard(0)  # Remove invalid chain IDs

    # Fetch latest block for each chain with reorg buffer
    latest_blocks: dict[str, int] = {}
    errors: list[str] = []

    for chain_id in chain_ids:
        try:
            latest_block = client.get_latest_block(chain_id)
            safe_block = latest_block - REORG_BUFFER
            latest_blocks[str(chain_id)] = safe_block
            print(
                f"Chain {chain_id} ({get_chain_name(chain_id)}): "
                f"latest={latest_block}, safe={safe_block}"
            )
        except Exception as e:
            error_msg = f"Failed to get latest block for chain {chain_id}: {e}"
            print(error_msg)
            errors.append(error_msg)

    # Prepare contracts for processing
    # Add latest block info to each contract for the Map state
    processed_contracts = []
    for contract in contracts:
        chain_id = str(contract.get("chainid", 0))
        if chain_id in latest_blocks:
            processed_contracts.append(
                {
                    **contract,
                    "target_block": latest_blocks[chain_id],
                }
            )
        else:
            print(f"Skipping contract {contract.get('contract_address')} - no block info for chain {chain_id}")

    result = {
        "latest_blocks": latest_blocks,
        "contracts": processed_contracts,
        "triggered_at": event.get("triggered_at"),
        "reorg_buffer": REORG_BUFFER,
    }

    if errors:
        result["errors"] = errors

    print(f"Returning {len(processed_contracts)} contracts for processing")
    return result
