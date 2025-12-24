"""Etherscan v2 API client for fetching blockchain data."""

import time
from typing import Any

import requests

# Mapping of chain_id to Etherscan v2 API base URL
CHAIN_URLS: dict[int, str] = {
    1: "https://api.etherscan.io/v2/api",
    5: "https://api-goerli.etherscan.io/v2/api",
    11155111: "https://api-sepolia.etherscan.io/v2/api",
    137: "https://api.polygonscan.com/v2/api",
    42161: "https://api.arbiscan.io/v2/api",
    10: "https://api-optimistic.etherscan.io/v2/api",
    8453: "https://api.basescan.org/v2/api",
}

# Chain ID to name mapping
CHAIN_NAMES: dict[int, str] = {
    1: "ethereum",
    5: "goerli",
    11155111: "sepolia",
    137: "polygon",
    42161: "arbitrum",
    10: "optimism",
    8453: "base",
}


class EtherscanClient:
    """Client for interacting with Etherscan v2 API."""

    def __init__(self, api_key: str, rate_limit_delay: float = 0.2):
        """
        Initialize Etherscan client.

        Args:
            api_key: Etherscan API key
            rate_limit_delay: Delay between API calls in seconds (default 0.2s = 5 req/s)
        """
        self.api_key = api_key
        self.rate_limit_delay = rate_limit_delay
        self._last_request_time = 0.0

    def _get_base_url(self, chain_id: int) -> str:
        """Get the base URL for a chain ID."""
        base_url = CHAIN_URLS.get(chain_id)
        if not base_url:
            raise ValueError(
                f"Unsupported chain_id: {chain_id}. "
                f"Supported chains: {list(CHAIN_URLS.keys())}"
            )
        return base_url

    def _rate_limit(self) -> None:
        """Apply rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self._last_request_time = time.time()

    def _make_request(
        self, chain_id: int, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Make a rate-limited request to Etherscan API."""
        self._rate_limit()

        base_url = self._get_base_url(chain_id)
        params["apikey"] = self.api_key

        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        return data

    def get_latest_block(self, chain_id: int) -> int:
        """
        Get the latest block number for a chain.

        Args:
            chain_id: The chain ID to query

        Returns:
            Latest block number as integer
        """
        params = {
            "module": "proxy",
            "action": "eth_blockNumber",
        }

        data = self._make_request(chain_id, params)

        if "result" not in data:
            raise ValueError(f"Invalid response from Etherscan: {data}")

        # Convert hex to int
        return int(data["result"], 16)

    def get_logs(
        self,
        chain_id: int,
        address: str,
        from_block: int,
        to_block: int,
        batch_size: int = 10000,
    ) -> list[dict[str, Any]]:
        """
        Fetch logs from Etherscan for a contract address.

        Args:
            chain_id: The chain ID to query
            address: Contract address to fetch logs for
            from_block: Starting block number
            to_block: Ending block number
            batch_size: Number of blocks per request (max 10000)

        Returns:
            List of log entries
        """
        all_logs: list[dict[str, Any]] = []
        current_from = from_block

        while current_from <= to_block:
            current_to = min(current_from + batch_size - 1, to_block)

            params = {
                "module": "logs",
                "action": "getLogs",
                "address": address,
                "fromBlock": current_from,
                "toBlock": current_to,
            }

            data = self._make_request(chain_id, params)

            if data.get("status") == "1" and data.get("result"):
                logs = data["result"]
                if isinstance(logs, list):
                    all_logs.extend(logs)

            current_from = current_to + 1

        return all_logs


def get_chain_name(chain_id: int) -> str:
    """Get the chain name for a chain ID."""
    return CHAIN_NAMES.get(chain_id, f"chain_{chain_id}")
