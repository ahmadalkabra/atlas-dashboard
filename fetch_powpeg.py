"""
Fetch PowPeg (Bridge) peg-in and peg-out data via Blockscout eth-rpc.

The Bridge is a precompiled contract at 0x0000000000000000000000000000000001000006.
Its events are not decoded by Blockscout (Java precompile).

Strategy (uses eth_getLogs with topic filtering — fast):
- Peg-ins:  pegin_btc events (topic0 0x44cdc782...) — amount in satoshis (data word[0])
- Peg-outs: release_request_received events (topic0 0x1a4457a4...) — amount in wei (data word[1])
"""

import json
import logging
import os
import sys
import time
import requests

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

BASE_URL = "https://rootstock.blockscout.com/api/v2"
BRIDGE_ADDRESS = "0x0000000000000000000000000000000001000006"
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
CURSOR_FILE = os.path.join(DATA_DIR, ".cursor_powpeg.json")

MIN_BLOCK = 7_430_000  # ~Feb 2025 (full year of data)
RATE_LIMIT_DELAY = 0.15

# Reorg buffer — re-fetch this many blocks before cursor to handle reorgs
REORG_BUFFER = 10


def load_cursor() -> int | None:
    """Load the last-fetched block number from cursor file."""
    if os.path.exists(CURSOR_FILE):
        try:
            with open(CURSOR_FILE) as f:
                data = json.load(f)
            return data.get("last_block")
        except (json.JSONDecodeError, KeyError):
            pass
    return None


def save_cursor(block_number: int):
    """Save the last-fetched block number to cursor file."""
    with open(CURSOR_FILE, "w") as f:
        json.dump({"last_block": block_number, "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}, f)


def load_existing_json(filename: str) -> list[dict]:
    """Load existing JSON data file, returning empty list if missing/corrupt."""
    path = os.path.join(DATA_DIR, filename)
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except (json.JSONDecodeError, ValueError):
            pass
    return []


def merge_events(existing: list[dict], new: list[dict], key: str = "tx_hash") -> list[dict]:
    """Merge new events into existing, deduplicating by key. New events win on conflict."""
    by_key = {e.get(key): e for e in existing}
    for e in new:
        by_key[e.get(key)] = e
    return list(by_key.values())


PEGIN_BTC_TOPIC0 = "0x44cdc782a38244afd68336ab92a0b39f864d6c0b2a50fa1da58cafc93cd2ae5a"


def fetch_pegin_logs(start_block: int = MIN_BLOCK) -> list[dict]:
    """Fetch pegin_btc events via eth_getLogs (fast topic filter).

    pegin_btc layout:
      topic0: pegin_btc signature
      topic1: recipient RSK address (indexed)
      topic2: BTC tx hash (indexed)
      data word[0]: amount in satoshis (int256)
      data word[1]: protocolVersion (int256)
    """
    events = []
    start = start_block

    resp = requests.post(ETH_RPC_URL, json={
        "jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1,
    }, timeout=30)
    latest = int(resp.json()["result"], 16)

    while start <= latest:
        to_block = min(start + CHUNK_SIZE - 1, latest)
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_getLogs",
            "params": [{
                "fromBlock": hex(start),
                "toBlock": hex(to_block),
                "address": BRIDGE_ADDRESS,
                "topics": [PEGIN_BTC_TOPIC0],
            }],
            "id": 1,
        }

        for attempt in range(3):
            try:
                resp = requests.post(ETH_RPC_URL, json=payload, timeout=120)
                data = resp.json()
                break
            except Exception as e:
                if attempt < 2:
                    time.sleep(2 * (attempt + 1))
                else:
                    logger.error(f"Failed at blocks {start}-{to_block}: {e}")
                    return events

        if "error" in data:
            logger.error(f"RPC error at {start}-{to_block}: {data['error']}")
            return events

        chunk_events = data.get("result", [])
        for raw in chunk_events:
            events.append({
                "block_number": int(raw["blockNumber"], 16),
                "transaction_hash": raw["transactionHash"],
                "topics": raw["topics"],
                "data": raw["data"],
            })

        logger.debug(f"Blocks {start}-{to_block}: {len(chunk_events)} events (total: {len(events)})")
        start = to_block + 1
        time.sleep(0.5)

    return events


def parse_pegin_log(log: dict) -> dict:
    """Parse a pegin_btc log into a peg-in record.

    Amount is in satoshis (data word[0]), convert to BTC (= RBTC, 1:1 peg).
    """
    topics = log.get("topics", [])
    recipient = None
    if len(topics) > 1 and topics[1]:
        recipient = "0x" + topics[1][-40:]

    raw = log.get("data", "0x")[2:]
    amount_btc = 0.0
    if len(raw) >= 64:
        amount_sat = int(raw[:64], 16)
        amount_btc = amount_sat / 1e8

    return {
        "tx_hash": log.get("transaction_hash", ""),
        "block_number": log.get("block_number", 0),
        "to_address": recipient or "",
        "value_rbtc": amount_btc,
    }


RELEASE_REQ_TOPIC0 = "0x1a4457a4460d48b40c5280955faf8e4685fa73f0866f7d8f573bdd8e64aca5b1"
ETH_RPC_URL = f"{BASE_URL.rsplit('/api', 1)[0]}/api/eth-rpc"
CHUNK_SIZE = 200_000  # blocks per eth_getLogs request


def fetch_pegout_logs(start_block: int = MIN_BLOCK) -> list[dict]:
    """Fetch release_request_received events via eth_getLogs (fast topic filter).

    Uses Blockscout's eth-rpc proxy which supports topic filtering even for the
    Bridge precompile. Takes ~10 seconds vs ~30 minutes for full log scan.
    """
    events = []
    start = start_block

    # Get latest block
    resp = requests.post(ETH_RPC_URL, json={
        "jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1,
    }, timeout=30)
    latest = int(resp.json()["result"], 16)
    logger.debug(f"Latest block: {latest}")

    while start <= latest:
        to_block = min(start + CHUNK_SIZE - 1, latest)
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_getLogs",
            "params": [{
                "fromBlock": hex(start),
                "toBlock": hex(to_block),
                "address": BRIDGE_ADDRESS,
                "topics": [RELEASE_REQ_TOPIC0],
            }],
            "id": 1,
        }

        for attempt in range(3):
            try:
                resp = requests.post(ETH_RPC_URL, json=payload, timeout=120)
                data = resp.json()
                break
            except Exception as e:
                if attempt < 2:
                    time.sleep(2 * (attempt + 1))
                else:
                    logger.error(f"Failed at blocks {start}-{to_block}: {e}")
                    return events

        if "error" in data:
            logger.error(f"RPC error at {start}-{to_block}: {data['error']}")
            return events

        chunk_events = data.get("result", [])
        # Convert from eth_getLogs format to Blockscout-like format
        for raw in chunk_events:
            events.append({
                "block_number": int(raw["blockNumber"], 16),
                "transaction_hash": raw["transactionHash"],
                "topics": raw["topics"],
                "data": raw["data"],
            })

        logger.debug(f"Blocks {start}-{to_block}: {len(chunk_events)} events (total: {len(events)})")
        start = to_block + 1
        time.sleep(0.5)

    return events


def parse_pegout_log(log: dict) -> dict:
    """Parse a release_request_received log into a peg-out record.

    Event layout:
      topic0: release_request_received signature
      topic1: sender RSK address (zero-padded)
      data word[0]: offset to bytes param (always 0x40)
      data word[1]: amount in wei (RBTC)
      data word[2..]: BTC destination address (bytes)
    """
    topics = log.get("topics", [])
    sender = None
    if len(topics) > 1 and topics[1]:
        sender = "0x" + topics[1][-40:]

    raw = log.get("data", "0x")[2:]
    amount_rbtc = 0.0
    if len(raw) >= 128:
        amount_wei = int(raw[64:128], 16)
        amount_rbtc = amount_wei / 1e18

    return {
        "tx_hash": log.get("transaction_hash", ""),
        "block_number": log.get("block_number", 0),
        "from_address": sender or "",
        "value_rbtc": amount_rbtc,
    }


def fetch_tx_timestamp(tx_hash: str) -> str:
    """Fetch timestamp for a transaction from Blockscout."""
    try:
        resp = requests.get(f"{BASE_URL}/transactions/{tx_hash}", timeout=30)
        resp.raise_for_status()
        return resp.json().get("timestamp", "")
    except Exception:
        return ""


def dedup_by_tx_hash(records: list[dict]) -> list[dict]:
    """Remove duplicate records by tx_hash, keeping the first occurrence."""
    seen = set()
    result = []
    for r in records:
        tx = r.get("tx_hash", "")
        if tx and tx not in seen:
            seen.add(tx)
            result.append(r)
    return result


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    full_mode = "--full" in sys.argv

    # Determine start block
    cursor_block = load_cursor() if not full_mode else None
    if cursor_block:
        start_block = max(cursor_block - REORG_BUFFER, MIN_BLOCK)
        logger.info(f"Incremental mode: fetching from block {start_block} (cursor={cursor_block}, buffer={REORG_BUFFER})")
    else:
        start_block = MIN_BLOCK
        logger.info(f"Full mode: fetching from block {start_block}")

    # Fetch peg-ins (pegin_btc events via eth_getLogs — fast)
    logger.info("Fetching PowPeg peg-in events via eth_getLogs...")
    pegin_logs = fetch_pegin_logs(start_block=start_block)
    new_pegins = [parse_pegin_log(log) for log in pegin_logs]
    new_pegins = dedup_by_tx_hash(new_pegins)

    # Fetch peg-outs (release_request_received via eth_getLogs — fast)
    logger.info("Fetching PowPeg peg-out events via eth_getLogs...")
    pegout_logs = fetch_pegout_logs(start_block=start_block)
    new_pegouts = [parse_pegout_log(log) for log in pegout_logs]
    new_pegouts = dedup_by_tx_hash(new_pegouts)

    # Track max block for cursor
    max_block = start_block
    for rec in new_pegins + new_pegouts:
        max_block = max(max_block, rec.get("block_number", 0))

    # Fetch timestamps for new events only (eth_getLogs doesn't include them)
    new_records = new_pegins + new_pegouts
    if new_records:
        logger.info(f"Fetching timestamps for {len(new_records)} transactions...")
        for i, rec in enumerate(new_records):
            if i % 20 == 0:
                logger.debug(f"{i}/{len(new_records)}...")
            rec["block_timestamp"] = fetch_tx_timestamp(rec["tx_hash"])
            time.sleep(RATE_LIMIT_DELAY)

    # Merge with existing data (incremental) or use new data only (full)
    if cursor_block and not full_mode:
        logger.info("Merging with existing data...")
        pegins = merge_events(load_existing_json("powpeg_pegins.json"), new_pegins)
        pegouts = merge_events(load_existing_json("powpeg_pegouts.json"), new_pegouts)
    else:
        pegins = new_pegins
        pegouts = new_pegouts

    # Sort by block
    pegins.sort(key=lambda x: x["block_number"])
    pegouts.sort(key=lambda x: x["block_number"])

    # Save
    pegins_path = os.path.join(DATA_DIR, "powpeg_pegins.json")
    pegouts_path = os.path.join(DATA_DIR, "powpeg_pegouts.json")

    with open(pegins_path, "w") as f:
        json.dump(pegins, f, indent=2)
    with open(pegouts_path, "w") as f:
        json.dump(pegouts, f, indent=2)

    # Update cursor
    save_cursor(max_block)

    pegin_vol = sum(e["value_rbtc"] for e in pegins)
    pegout_vol = sum(e["value_rbtc"] for e in pegouts)

    logger.info(f"Saved {len(pegins)} peg-ins")
    logger.info(f"Saved {len(pegouts)} peg-outs")
    logger.info(f"Cursor updated to block {max_block}")
    logger.info("--- PowPeg Summary ---")
    logger.info(f"Peg-ins:  {len(pegins)} txs, {pegin_vol:.6f} RBTC")
    logger.info(f"Peg-outs: {len(pegouts)} txs, {pegout_vol:.6f} RBTC")
    if pegins:
        logger.info(f"Peg-in range:  block {pegins[0]['block_number']} to {pegins[-1]['block_number']}")
    if pegouts:
        logger.info(f"Peg-out range: block {pegouts[0]['block_number']} to {pegouts[-1]['block_number']}")


if __name__ == "__main__":
    main()
