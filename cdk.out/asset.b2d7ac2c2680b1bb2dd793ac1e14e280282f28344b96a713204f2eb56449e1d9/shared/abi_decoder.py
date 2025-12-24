"""ABI decoder for decoding Ethereum event logs using Web3.py."""

from typing import Any

from web3 import Web3


def decode_logs(logs: list[dict[str, Any]], abi: list[dict]) -> list[dict[str, Any]]:
    """
    Decode raw event logs using a contract ABI.

    This function takes raw logs from Etherscan and decodes them using the
    provided contract ABI to extract human-readable event names and parameters.

    Args:
        logs: List of raw log entries from Etherscan
        abi: Contract ABI as a list of dictionaries

    Returns:
        List of decoded log entries with event_name and decoded_args fields
    """
    w3 = Web3()
    contract = w3.eth.contract(abi=abi)

    # Create mapping of event signature hash to event object
    event_map: dict[str, Any] = {}
    for event in contract.events:
        # Calculate the keccak256 hash of the event signature
        sig_hash = w3.keccak(text=event.event_signature).hex()
        event_map[sig_hash] = event

    decoded_logs: list[dict[str, Any]] = []

    for log in logs:
        try:
            topics = log.get("topics", [])

            if not topics:
                # No topics means we can't decode the event
                decoded_logs.append(
                    {
                        **log,
                        "event_name": None,
                        "decoded_args": {},
                        "decode_status": "no_topics",
                    }
                )
                continue

            # First topic is the event signature
            sig = topics[0]
            if isinstance(sig, bytes):
                sig = sig.hex()
            if not sig.startswith("0x"):
                sig = "0x" + sig

            event = event_map.get(sig)

            if event:
                # Prepare log for Web3 processing
                web3_log = _prepare_log_for_web3(log)

                # Decode the log using Web3
                decoded = event.process_log(web3_log)

                # Convert args to serializable dict
                decoded_args = _args_to_dict(decoded.args)

                decoded_logs.append(
                    {
                        **log,
                        "event_name": event.event_name,
                        "decoded_args": decoded_args,
                        "decode_status": "success",
                    }
                )
            else:
                # Event signature not found in ABI
                decoded_logs.append(
                    {
                        **log,
                        "event_name": None,
                        "decoded_args": {},
                        "decode_status": "unknown_event",
                    }
                )

        except Exception as e:
            # If decoding fails, keep the raw log with error info
            decoded_logs.append(
                {
                    **log,
                    "event_name": None,
                    "decoded_args": {},
                    "decode_status": "error",
                    "decode_error": str(e),
                }
            )

    return decoded_logs


def _prepare_log_for_web3(log: dict[str, Any]) -> dict[str, Any]:
    """
    Prepare a log entry for Web3 processing.

    Converts hex strings to bytes and formats the log in the expected structure.
    """
    topics = log.get("topics", [])
    processed_topics = []

    for topic in topics:
        if isinstance(topic, str):
            # Remove 0x prefix and convert to bytes
            if topic.startswith("0x"):
                processed_topics.append(bytes.fromhex(topic[2:]))
            else:
                processed_topics.append(bytes.fromhex(topic))
        elif isinstance(topic, bytes):
            processed_topics.append(topic)
        else:
            processed_topics.append(topic)

    data = log.get("data", "0x")
    if isinstance(data, str):
        if data.startswith("0x"):
            data = bytes.fromhex(data[2:]) if len(data) > 2 else b""
        else:
            data = bytes.fromhex(data) if data else b""

    return {
        "topics": processed_topics,
        "data": data,
        "address": log.get("address", ""),
        "blockNumber": (
            int(log.get("blockNumber", "0x0"), 16)
            if isinstance(log.get("blockNumber"), str)
            else log.get("blockNumber", 0)
        ),
        "transactionHash": log.get("transactionHash", ""),
        "transactionIndex": (
            int(log.get("transactionIndex", "0x0"), 16)
            if isinstance(log.get("transactionIndex"), str)
            else log.get("transactionIndex", 0)
        ),
        "logIndex": (
            int(log.get("logIndex", "0x0"), 16)
            if isinstance(log.get("logIndex"), str)
            else log.get("logIndex", 0)
        ),
    }


def _args_to_dict(args: Any) -> dict[str, Any]:
    """
    Convert Web3 decoded args to a serializable dictionary.

    Handles conversion of bytes, addresses, and other Web3 types.
    """
    if hasattr(args, "_asdict"):
        # Named tuple from Web3
        result = {}
        for key, value in args._asdict().items():
            result[key] = _convert_value(value)
        return result
    elif isinstance(args, dict):
        return {k: _convert_value(v) for k, v in args.items()}
    else:
        return {"value": _convert_value(args)}


def _convert_value(value: Any) -> Any:
    """Convert a value to a JSON-serializable format."""
    if isinstance(value, bytes):
        return "0x" + value.hex()
    elif isinstance(value, (list, tuple)):
        return [_convert_value(v) for v in value]
    elif isinstance(value, dict):
        return {k: _convert_value(v) for k, v in value.items()}
    elif hasattr(value, "hex"):
        # HexBytes or similar
        return "0x" + value.hex()
    else:
        return value
