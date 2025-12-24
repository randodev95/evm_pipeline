# Web3 EVM Data Pipeline

A modular, config-driven pipeline for ingesting, decoding, and modeling EVM-compatible blockchain data. The system is designed for **near–real-time analytics** using **Dagster** for orchestration, **Etherscan v2 APIs** for raw data ingestion, and **SQLMesh** for analytics modeling.

---

## Overview

The pipeline runs on a fixed schedule (every 15 minutes) and follows a clear separation of concerns:

1. **Orchestration** – execution, scheduling, retries
2. **Ingestion** – raw on-chain log collection
3. **Decoding** – ABI-based event decoding
4. **Modeling** – SQL-based transformations and metrics

This separation allows independent scaling, testing, and evolution of each layer.

---

## High-Level Architecture

```
Dagster (15 min schedule)
        |
        v
Etherscan v2 APIs (multi-chain)
        |
        v
Raw Event Logs
        |
        v
ABI-based Log Decoder
        |
        v
Decoded Event Tables
        |
        v
SQLMesh (staging + metrics)
```

---

## 1. Orchestration (Dagster)

Dagster is used as the orchestration layer for the entire pipeline.

Responsibilities:

* Schedule execution every **15 minutes**
* Track pipeline state and last processed blocks
* Handle retries and failures
* Enable historical backfills
* Provide observability via logs and asset lineage

Each run is deterministic and idempotent, allowing safe re-runs when needed.

---

## 2. Raw Data Ingestion (Etherscan v2)

The pipeline uses **Etherscan v2 APIs** to fetch raw event logs from multiple EVM-compatible chains.

### Config-Driven Contract Definitions

Users onboard new contracts by supplying configuration only — no code changes are required.

Example configuration:

```yaml
contracts:
  - name: uniswap_v3_pool
    chain_id: 1
    contract_address: "0x..."
    abi_path: "./abis/uniswap_v3_pool.json"
    start_block: 17000000
```

Each contract definition includes:

* `chain_id` – EVM chain identifier
* `contract_address` – target smart contract
* `abi_path` – ABI JSON used for decoding
* `start_block` – lower bound for ingestion

### Ingestion Characteristics

* Multi-chain support via Etherscan v2
* Incremental block-range fetching
* Rate-limit aware execution
* Raw logs stored without mutation for traceability

---

## 3. Log Decoding

After raw logs are fetched, they are decoded using the provided ABI definitions.

Decoding process:

* Load ABI JSON dynamically per contract
* Match event signatures against log topics
* Decode indexed and non-indexed parameters
* Normalize decoded output into structured records

### Decoded Event Structure (Example)

```json
{
  "chain_id": 1,
  "contract_address": "0x...",
  "event_name": "Swap",
  "transaction_hash": "0x...",
  "block_number": 17000123,
  "block_timestamp": "2024-01-01T12:00:00Z",
  "event_payload": {
    "sender": "0x...",
    "amount0": "123456",
    "amount1": "-78910"
  }
}
```

Decoded events are persisted in warehouse-friendly tables or files (e.g. Parquet).

---

## 4. Analytics & Modeling (SQLMesh)

SQLMesh is used to transform decoded blockchain events into analytics-ready datasets.

### Model Layers

**Staging Models**

* Apply schema normalization
* Cast EVM types (e.g. `uint256`, `address`)
* Enforce event-level constraints
* One-to-one mapping with decoded events

**Metrics Models**

* Aggregations and rollups
* Time-windowed metrics (15m / hourly / daily)
* Protocol-level KPIs

SQLMesh provides versioned SQL, testing, and safe schema evolution.

---

## Execution Flow

1. Dagster triggers a scheduled run
2. Latest unprocessed block range is calculated
3. Raw logs are fetched from Etherscan
4. Logs are decoded using ABI definitions
5. Decoded events are persisted
6. SQLMesh runs staging and metrics models
7. Data becomes available for analytics and dashboards

---

## Example Metrics

* Event counts by contract and chain
* Swap volume by pool and token
* Daily active addresses
* Gas usage and transaction frequency

---

## Extensibility

The pipeline is designed for incremental growth:

* New contracts → update config
* New chains → add chain ID and API key
* New metrics → add SQLMesh models
* Historical backfills → Dagster partitions

---

## Summary

This project provides a clean, production-oriented foundation for EVM analytics:

* **Dagster** for orchestration
* **Etherscan v2** for raw on-chain data
* **ABI decoding** for structured events
* **SQLMesh** for reliable, versioned analytics models

The result is a scalable and maintainable Web3 data pipeline suitable for real-time and historical analysis.
