"""DeltaLake utilities for reading and writing parquet data to S3."""

import os
from typing import Any

import pandas as pd
from deltalake import DeltaTable, write_deltalake


def get_storage_options(region: str | None = None) -> dict[str, str]:
    """
    Get S3 storage options for DeltaLake.

    Args:
        region: AWS region (defaults to AWS_REGION env var or us-east-1)

    Returns:
        Dictionary of storage options for DeltaLake
    """
    region = region or os.environ.get("AWS_REGION", "us-east-1")

    storage_options = {
        "AWS_REGION": region,
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }

    # Check for LocalStack endpoint
    localstack_endpoint = os.environ.get("AWS_ENDPOINT_URL")
    if localstack_endpoint:
        storage_options["AWS_ENDPOINT_URL"] = localstack_endpoint
        storage_options["AWS_ALLOW_HTTP"] = "true"

    return storage_options


def write_delta_table(
    table_path: str,
    df: pd.DataFrame,
    partition_by: list[str] | None = None,
    mode: str = "append",
    region: str | None = None,
) -> None:
    """
    Write a DataFrame to a DeltaLake table on S3.

    Args:
        table_path: S3 path to the Delta table (e.g., s3://bucket/table)
        df: DataFrame to write
        partition_by: List of columns to partition by
        mode: Write mode - 'append', 'overwrite', or 'error'
        region: AWS region
    """
    if df.empty:
        return

    storage_options = get_storage_options(region)

    # Default partitioning for EVM logs
    if partition_by is None:
        partition_by = ["chainid", "contract_address", "topic0"]

    # Ensure partition columns exist
    available_partitions = [col for col in partition_by if col in df.columns]

    write_deltalake(
        table_path,
        df,
        mode=mode,
        partition_by=available_partitions if available_partitions else None,
        storage_options=storage_options,
    )


def read_delta_table(
    table_path: str,
    filters: list[tuple[str, str, Any]] | None = None,
    columns: list[str] | None = None,
    region: str | None = None,
) -> pd.DataFrame:
    """
    Read a DeltaLake table from S3.

    Args:
        table_path: S3 path to the Delta table
        filters: List of filter tuples (column, op, value)
        columns: List of columns to read
        region: AWS region

    Returns:
        DataFrame with the table data
    """
    storage_options = get_storage_options(region)

    try:
        dt = DeltaTable(table_path, storage_options=storage_options)

        # Apply filters if provided
        if filters:
            pyarrow_table = dt.to_pyarrow_table(filters=filters, columns=columns)
        else:
            pyarrow_table = dt.to_pyarrow_table(columns=columns)

        return pyarrow_table.to_pandas()

    except Exception as e:
        # Table doesn't exist yet
        if "not found" in str(e).lower() or "does not exist" in str(e).lower():
            return pd.DataFrame()
        raise


def table_exists(table_path: str, region: str | None = None) -> bool:
    """
    Check if a DeltaLake table exists.

    Args:
        table_path: S3 path to the Delta table
        region: AWS region

    Returns:
        True if table exists, False otherwise
    """
    storage_options = get_storage_options(region)

    try:
        DeltaTable(table_path, storage_options=storage_options)
        return True
    except Exception:
        return False


def get_max_block_number(
    table_path: str,
    chain_id: int,
    contract_address: str,
    region: str | None = None,
) -> int | None:
    """
    Get the maximum block number for a contract in the Delta table.

    Args:
        table_path: S3 path to the Delta table
        chain_id: Chain ID to filter by
        contract_address: Contract address to filter by
        region: AWS region

    Returns:
        Maximum block number or None if no data exists
    """
    storage_options = get_storage_options(region)

    try:
        dt = DeltaTable(table_path, storage_options=storage_options)

        # Read only the blockNumber column with filters
        filters = [
            ("chainid", "=", chain_id),
            ("contract_address", "=", contract_address),
        ]

        df = dt.to_pandas(filters=filters, columns=["blockNumber"])

        if df.empty:
            return None

        # Handle hex string block numbers
        block_numbers = df["blockNumber"].apply(
            lambda x: int(x, 16) if isinstance(x, str) and x.startswith("0x") else int(x)
        )

        return int(block_numbers.max())

    except Exception:
        return None
