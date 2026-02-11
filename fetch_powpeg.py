"""
Fetch PowPeg (Bridge) contract data from Blockscout API.

The Bridge is a precompiled contract at 0x0000000000000000000000000000000001000006.
Its events are not decoded by Blockscout (Java precompile, not Solidity).

Data sources:
1. Event logs — paginate ALL logs and classify by topic0:
   - pegin_btc(address,bytes32,int256,int256) — peg-in completed, BTC amount in satoshis
   - pegout_confirmed(bytes32,uint256) — peg-out confirmed on Bitcoin
   - release_btc(bytes32,bytes) — BTC released to user
   - batch_pegout_created(bytes32,bytes) — batch pegout created
   - release_request_received (0x1a4457a4...) — pegout request with BTC address + amount
   - update_collections(address) — maintenance, ignored

2. Internal transactions — FROM Bridge to addresses with value > 0 = RBTC credited (peg-in)
   Used to get RBTC amounts since pegin_btc event stores BTC satoshis.

Note: Blockscout's topic0 filter doesn't work for this precompiled contract,
so we fetch ALL logs and filter client-side.
"""

import json
import os
import time
import requests

BASE_URL = "https://rootstock.blockscout.com/api/v2"
BRIDGE_ADDRESS = "0x0000000000000000000000000000000001000006"
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

# Only fetch events from ~1 year ago (block ~7,230,000 ≈ Feb 2025)
MIN_BLOCK = 7_230_000

RATE_LIMIT_DELAY = 0.3

# Known Bridge event topic0 hashes
BRIDGE_EVENTS = {
    "0x1069152f4f916cbf155ee32a695d92258481944edb5b6fd649718fc1b43e515e": "update_collections",
    "0x44cdc782a38244afd68336ab92a0b39f864d6c0b2a50fa1da58cafc93cd2ae5a": "pegin_btc",
    "0xc287f602476eeef8a547a3b82e79045c827c51362ff153f728b6d839bad099ef": "pegout_confirmed",
    "0x655929b56d5c5a24f81ee80267d5151b9d680e7e703387999922e9070bc98a02": "release_btc",
    "0x483d0191cc4e784b04a41f6c4801a0766b43b1fdd0b9e3e6bfdca74e5b05c2eb": "batch_pegout_created",
    "0x9ee5d520fd5e6eaea3fd2e3ae4e35e9a9c0fb05c9d8f84b507f287da84b5117c": "pegout_transaction_created",
    "0x83b6efe3a7d95459577ec9396f5d6f1e486ca2378130e2ba4d98a4da108ca743": "add_signature",
    "0x1a4457a4460d48b40c5280955faf8e4685fa73f0866f7d8f573bdd8e64aca5b1": "release_request_received",
}


def fetch_all_logs() -> list[dict]:
    """Fetch event logs from the Bridge contract, stopping at MIN_BLOCK."""
    all_items = []
    url = f"{BASE_URL}/addresses/{BRIDGE_ADDRESS}/logs"
    params = {}
    page = 1

    while True:
        print(f"  Fetching logs page {page}...")
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        items = data.get("items", [])
        filtered = [i for i in items if i.get("block_number", 0) >= MIN_BLOCK]
        all_items.extend(filtered)

        if len(filtered) < len(items):
            print(f"  Reached block cutoff ({MIN_BLOCK}), stopping.")
            break

        next_page = data.get("next_page_params")
        if not next_page or not items:
            break

        params = dict(next_page)
        page += 1
        time.sleep(RATE_LIMIT_DELAY)

    return all_items


def fetch_internal_transactions() -> list[dict]:
    """Fetch internal transactions for the Bridge contract, stopping at MIN_BLOCK."""
    all_items = []
    url = f"{BASE_URL}/addresses/{BRIDGE_ADDRESS}/internal-transactions"
    params = {}
    page = 1

    while True:
        print(f"  Fetching internal transactions page {page}...")
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        items = data.get("items", [])
        filtered = [i for i in items if i.get("block_number", 0) >= MIN_BLOCK]
        all_items.extend(filtered)

        if len(filtered) < len(items):
            print(f"  Reached block cutoff ({MIN_BLOCK}), stopping.")
            break

        next_page = data.get("next_page_params")
        if not next_page or not items:
            break

        params = dict(next_page)
        page += 1
        time.sleep(RATE_LIMIT_DELAY)

    return all_items


def _address_from_topic(topic: str) -> str:
    """Extract address from a 32-byte topic (last 20 bytes)."""
    if not topic:
        return ""
    return "0x" + topic[-40:]


def _int256_from_hex(hex_str: str) -> int:
    """Parse a 32-byte hex value as int256 (signed)."""
    val = int(hex_str, 16)
    if val >= 2**255:
        val -= 2**256
    return val


def parse_pegin_btc(log: dict) -> dict:
    """Parse pegin_btc(address indexed, bytes32 indexed, int256, int256) event.

    - topic1: recipient address (indexed)
    - topic2: btc tx hash (indexed)
    - data[0:32]: amount in satoshis (int256)
    - data[32:64]: protocolVersion (int256)
    """
    topics = log.get("topics", [])
    data = log.get("data", "0x")

    recipient = _address_from_topic(topics[1]) if len(topics) > 1 and topics[1] else ""
    btc_tx_hash = topics[2] if len(topics) > 2 and topics[2] else ""

    # Parse data: 2 x int256
    raw = data[2:]  # strip 0x
    satoshis = _int256_from_hex(raw[0:64]) if len(raw) >= 64 else 0
    protocol_version = _int256_from_hex(raw[64:128]) if len(raw) >= 128 else 0

    return {
        "event": "pegin_btc",
        "tx_hash": log["transaction_hash"],
        "block_number": log["block_number"],
        "recipient": recipient,
        "btc_tx_hash": btc_tx_hash,
        "amount_satoshis": satoshis,
        "amount_btc": satoshis / 1e8,
        "protocol_version": protocol_version,
    }


def parse_release_request_received(log: dict) -> dict:
    """Parse release_request_received event (peg-out initiation).

    - topic1: sender address (indexed)
    - data: bytes (BTC destination address) + int256 (RBTC amount in wei)
    """
    topics = log.get("topics", [])
    data = log.get("data", "0x")
    raw = data[2:]

    sender = _address_from_topic(topics[1]) if len(topics) > 1 and topics[1] else ""

    # Parse ABI-encoded (bytes, int256):
    # offset to bytes (32 bytes) + int256 amount (32 bytes) + bytes length (32 bytes) + bytes data
    amount_wei = 0
    btc_address = ""

    if len(raw) >= 128:
        # First 32 bytes: offset to bytes parameter
        # Second 32 bytes: amount (int256)
        amount_wei = _int256_from_hex(raw[64:128])

        # Bytes parameter: length at offset, then data
        if len(raw) >= 192:
            btc_addr_len = int(raw[128:192], 16)
            btc_addr_hex = raw[192:192 + btc_addr_len * 2]
            try:
                btc_address = bytes.fromhex(btc_addr_hex).decode("ascii")
            except (ValueError, UnicodeDecodeError):
                btc_address = ""

    return {
        "event": "release_request_received",
        "tx_hash": log["transaction_hash"],
        "block_number": log["block_number"],
        "sender": sender,
        "btc_destination": btc_address,
        "amount_wei": str(amount_wei),
        "amount_rbtc": amount_wei / 1e18,
    }


def parse_pegout_confirmed(log: dict) -> dict:
    """Parse pegout_confirmed(bytes32, uint256) event."""
    topics = log.get("topics", [])
    data = log.get("data", "0x")
    raw = data[2:]

    pegout_hash = topics[1] if len(topics) > 1 and topics[1] else ""
    btc_block_height = int(raw[0:64], 16) if len(raw) >= 64 else 0

    return {
        "event": "pegout_confirmed",
        "tx_hash": log["transaction_hash"],
        "block_number": log["block_number"],
        "pegout_hash": pegout_hash,
        "btc_block_height": btc_block_height,
    }


def parse_release_btc(log: dict) -> dict:
    """Parse release_btc(bytes32, bytes) event."""
    topics = log.get("topics", [])

    release_hash = topics[1] if len(topics) > 1 and topics[1] else ""

    return {
        "event": "release_btc",
        "tx_hash": log["transaction_hash"],
        "block_number": log["block_number"],
        "release_hash": release_hash,
    }


def parse_batch_pegout_created(log: dict) -> dict:
    """Parse batch_pegout_created(bytes32, bytes) event."""
    topics = log.get("topics", [])

    batch_hash = topics[1] if len(topics) > 1 and topics[1] else ""

    return {
        "event": "batch_pegout_created",
        "tx_hash": log["transaction_hash"],
        "block_number": log["block_number"],
        "batch_hash": batch_hash,
    }


def classify_internal_tx(tx: dict) -> dict | None:
    """Classify an internal transaction as peg-in (RBTC credit from Bridge)."""
    from_addr = tx.get("from", {}).get("hash", "").lower()
    to_addr = tx.get("to", {}).get("hash", "").lower()
    value = int(tx.get("value", "0"))

    if value == 0:
        return None

    bridge_addr = BRIDGE_ADDRESS.lower()

    if from_addr == bridge_addr and to_addr != bridge_addr:
        return {
            "type": "pegin",
            "tx_hash": tx.get("transaction_hash", ""),
            "block_number": tx.get("block_number", 0),
            "to_address": to_addr,
            "value_wei": str(value),
            "value_rbtc": value / 1e18,
            "block_timestamp": tx.get("timestamp") or "",
        }

    return None


def fetch_block_timestamps(block_numbers: list[int]) -> dict[int, str]:
    """Fetch timestamps for blocks that don't have them yet."""
    timestamps = {}
    unique_blocks = sorted(set(b for b in block_numbers if b))
    if not unique_blocks:
        return timestamps

    print(f"  Fetching timestamps for {len(unique_blocks)} blocks...")
    for i, block_num in enumerate(unique_blocks):
        if i > 0 and i % 50 == 0:
            print(f"    ...{i}/{len(unique_blocks)} blocks")
        try:
            resp = requests.get(f"{BASE_URL}/blocks/{block_num}", timeout=15)
            resp.raise_for_status()
            block_data = resp.json()
            ts = block_data.get("timestamp")
            if ts:
                timestamps[block_num] = ts
            time.sleep(RATE_LIMIT_DELAY)
        except Exception as e:
            print(f"    Warning: Failed to fetch block {block_num}: {e}")

    return timestamps


def enrich_with_timestamps(events: list[dict], timestamps: dict[int, str]) -> list[dict]:
    """Add timestamp field to events missing it."""
    for event in events:
        if not event.get("block_timestamp"):
            block = event.get("block_number")
            if block and block in timestamps:
                event["block_timestamp"] = timestamps[block]
    return events


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    # --- Internal transactions (primary data source for PowPeg) ---
    # This is far more efficient than scanning thousands of update_collections logs.
    # Internal txs FROM Bridge = RBTC credited to user (peg-in completion).
    print("Fetching Bridge internal transactions...")
    internal_txs = fetch_internal_transactions()
    print(f"  Found {len(internal_txs)} internal transactions")

    pegins = []
    for tx in internal_txs:
        classified = classify_internal_tx(tx)
        if classified:
            pegins.append(classified)
    print(f"  Classified {len(pegins)} as RBTC credits (peg-ins)")

    # --- Peg-outs: we don't have a fast source for these via internal txs ---
    # release_request_received events exist but are buried in ~20k update_collections.
    # For now, we save an empty pegouts list and note this limitation.
    pegouts = []
    print(f"\n  Note: PowPeg peg-out data requires scanning all Bridge logs (~20k+ events).")
    print(f"  Peg-out tracking is limited to Flyover for now.")

    # --- Fetch timestamps ---
    blocks_needing = [e["block_number"] for e in pegins if not e.get("block_timestamp") and e.get("block_number")]
    if blocks_needing:
        print("\nFetching block timestamps...")
        timestamps = fetch_block_timestamps(blocks_needing)
        pegins = enrich_with_timestamps(pegins, timestamps)

    # --- Save to JSON ---
    pegins_path = os.path.join(DATA_DIR, "powpeg_pegins.json")
    pegouts_path = os.path.join(DATA_DIR, "powpeg_pegouts.json")

    with open(pegins_path, "w") as f:
        json.dump(pegins, f, indent=2)
    with open(pegouts_path, "w") as f:
        json.dump(pegouts, f, indent=2)

    print(f"\nSaved {len(pegins)} peg-in events to {pegins_path}")
    print(f"Saved {len(pegouts)} peg-out events to {pegouts_path}")

    # Summary
    pegin_volume = sum(e["value_rbtc"] for e in pegins)
    print(f"\n--- PowPeg Summary ---")
    print(f"Peg-ins: {len(pegins)} txs, {pegin_volume:.6f} RBTC")

    pegin_addrs = set(e["to_address"] for e in pegins)
    print(f"Unique peg-in recipients: {len(pegin_addrs)}")


if __name__ == "__main__":
    main()
