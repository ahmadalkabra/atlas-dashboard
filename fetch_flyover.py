"""
Fetch Flyover (LBC) contract events from Blockscout API.

Queries the LiquidityBridgeContractV2 (TransparentUpgradeableProxy) for:
- CallForUser: LP delivers RBTC to user (peg-in completion)
- PegOutDeposit: User deposits RBTC for peg-out
- PegOutRefunded: LP refund claimed for peg-out
- Penalized: LP slashed
- PegOutUserRefunded: User refunded (LP failed)
- PegInRegistered: Peg-in registered on Bridge
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
LBC_ADDRESS = "0xaa9caf1e3967600578727f975f283446a3da6612"
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
CURSOR_FILE = os.path.join(DATA_DIR, ".cursor_flyover.json")

# Fetch events from ~Feb 2025 (full year of data)
MIN_BLOCK = 7_430_000

# Reorg buffer — re-fetch this many blocks before cursor to handle reorgs
REORG_BUFFER = 10

# TeksCapital LP (sole active Flyover LP on RSK mainnet)
TEKSCAPITAL_RBTC_WALLET = "0x82A06eBdb97776a2DA4041DF8F2b2Ea8d3257852"
TEKSCAPITAL_BTC_WALLET = "1D2xucTYkxCHvaaZuaKVJTfZQWr4PUjzAy"
TEKSCAPITAL_LPS_URL = "https://lps.tekscapital.com/providers/liquidity"

# Event topic0 hashes (keccak256 of event signatures)
EVENTS = {
    "CallForUser": {
        "topic0": "0xbfc7404e6fe464f0646fe2c6ab942b92d56be722bb39f8c6bc4830d2d32fb80d",
        "sig": "CallForUser(address,address,uint256,uint256,bytes,bool,bytes32)",
    },
    "PegOutDeposit": {
        "topic0": "0xb1bc7bfc0dab19777eb03aa0a5643378fc9f186c8fc5a36620d21136fbea570f",
        "sig": "PegOutDeposit(bytes32,address,uint256,uint256)",
    },
    "PegOutRefunded": {
        "topic0": "0xb781856ec73fd0dc39351043d1634ea22cd3277b0866ab93e7ec1801766bb384",
        "sig": "PegOutRefunded(bytes32)",
    },
    "Penalized": {
        "topic0": "0x9685484093cc596fdaeab51abf645b1753dbb7d869bfd2eb21e2c646e47a36f4",
        "sig": "Penalized(address,uint256,bytes32)",
    },
    "PegOutUserRefunded": {
        "topic0": "0x9ccbeffc442024e2a6ade18ff0978af9a4c4d6562ae38adb51ccf8256cf42b41",
        "sig": "PegOutUserRefunded(bytes32,uint256,address)",
    },
    "PegInRegistered": {
        "topic0": "0x0629ae9d1dc61501b0ca90670a9a9b88daaf7504b54537b53e1219de794c63d2",
        "sig": "PegInRegistered(bytes32,int256)",
    },
}

RATE_LIMIT_DELAY = 0.3  # seconds between API calls


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


def fetch_all_logs(min_block: int = MIN_BLOCK) -> list[dict]:
    """Fetch logs from the LBC contract, stopping at min_block.

    Blockscout's topic0 filter returns 422 for this contract, so we fetch
    everything and filter client-side.
    """
    all_items = []
    url = f"{BASE_URL}/addresses/{LBC_ADDRESS}/logs"
    params = {}
    page = 1

    while True:
        logger.debug(f"Fetching LBC logs page {page}...")
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        items = data.get("items", [])
        # Filter out items below min_block and stop if all are old
        filtered = [i for i in items if i.get("block_number", 0) >= min_block]
        all_items.extend(filtered)

        if len(filtered) < len(items):
            # We've hit events older than our cutoff
            logger.debug(f"Reached block cutoff ({min_block}), stopping.")
            break

        next_page = data.get("next_page_params")
        if not next_page or not items:
            break

        params = dict(next_page)
        page += 1
        time.sleep(RATE_LIMIT_DELAY)

    return all_items


def parse_call_for_user(log: dict) -> dict:
    """Parse a CallForUser event log into structured data."""
    decoded = log.get("decoded")
    if decoded and decoded.get("parameters"):
        params = {p["name"]: p["value"] for p in decoded["parameters"]}
        return {
            "event": "CallForUser",
            "tx_hash": log["transaction_hash"],
            "block_number": log["block_number"],
            "from_address": params.get("from", ""),
            "dest_address": params.get("dest", ""),
            "gas_limit": params.get("gasLimit", "0"),
            "value_wei": params.get("value", "0"),
            "value_rbtc": int(params.get("value", "0")) / 1e18,
            "success": params.get("success", False),
            "quote_hash": params.get("quoteHash", ""),
        }
    # Fallback: parse from topics and data
    topics = log.get("topics", [])
    return {
        "event": "CallForUser",
        "tx_hash": log["transaction_hash"],
        "block_number": log["block_number"],
        "from_address": _address_from_topic(topics[1]) if len(topics) > 1 and topics[1] else "",
        "dest_address": _address_from_topic(topics[2]) if len(topics) > 2 and topics[2] else "",
        "value_wei": "0",
        "value_rbtc": 0,
        "success": False,
        "quote_hash": "",
        "raw_data": log.get("data", ""),
    }


def parse_pegout_deposit(log: dict) -> dict:
    """Parse a PegOutDeposit event log."""
    decoded = log.get("decoded")
    if decoded and decoded.get("parameters"):
        params = {p["name"]: p["value"] for p in decoded["parameters"]}
        return {
            "event": "PegOutDeposit",
            "tx_hash": log["transaction_hash"],
            "block_number": log["block_number"],
            "quote_hash": params.get("quoteHash", ""),
            "sender": params.get("sender", ""),
            "amount_wei": params.get("amount", "0"),
            "amount_rbtc": int(params.get("amount", "0")) / 1e18,
            "timestamp": int(params.get("timestamp", "0")),
        }
    topics = log.get("topics", [])
    data = log.get("data", "0x")
    return {
        "event": "PegOutDeposit",
        "tx_hash": log["transaction_hash"],
        "block_number": log["block_number"],
        "quote_hash": topics[1] if len(topics) > 1 and topics[1] else "",
        "sender": _address_from_topic(topics[2]) if len(topics) > 2 and topics[2] else "",
        "amount_wei": str(int(data[2:66], 16)) if len(data) >= 66 else "0",
        "amount_rbtc": int(data[2:66], 16) / 1e18 if len(data) >= 66 else 0,
        "timestamp": int(data[66:130], 16) if len(data) >= 130 else 0,
    }


def parse_pegout_refunded(log: dict) -> dict:
    """Parse a PegOutRefunded event log."""
    decoded = log.get("decoded")
    if decoded and decoded.get("parameters"):
        params = {p["name"]: p["value"] for p in decoded["parameters"]}
        return {
            "event": "PegOutRefunded",
            "tx_hash": log["transaction_hash"],
            "block_number": log["block_number"],
            "quote_hash": params.get("quoteHash", ""),
        }
    topics = log.get("topics", [])
    return {
        "event": "PegOutRefunded",
        "tx_hash": log["transaction_hash"],
        "block_number": log["block_number"],
        "quote_hash": topics[1] if len(topics) > 1 and topics[1] else "",
    }


def parse_penalized(log: dict) -> dict:
    """Parse a Penalized event log."""
    decoded = log.get("decoded")
    if decoded and decoded.get("parameters"):
        params = {p["name"]: p["value"] for p in decoded["parameters"]}
        return {
            "event": "Penalized",
            "tx_hash": log["transaction_hash"],
            "block_number": log["block_number"],
            "lp_address": params.get("liquidityProvider", ""),
            "penalty_wei": params.get("penalty", "0"),
            "penalty_rbtc": int(params.get("penalty", "0")) / 1e18,
            "quote_hash": params.get("quoteHash", ""),
        }
    topics = log.get("topics", [])
    return {
        "event": "Penalized",
        "tx_hash": log["transaction_hash"],
        "block_number": log["block_number"],
        "lp_address": "",
        "penalty_wei": "0",
        "penalty_rbtc": 0,
        "quote_hash": "",
        "raw_data": log.get("data", ""),
    }


def parse_pegout_user_refunded(log: dict) -> dict:
    """Parse a PegOutUserRefunded event log."""
    decoded = log.get("decoded")
    if decoded and decoded.get("parameters"):
        params = {p["name"]: p["value"] for p in decoded["parameters"]}
        return {
            "event": "PegOutUserRefunded",
            "tx_hash": log["transaction_hash"],
            "block_number": log["block_number"],
            "quote_hash": params.get("quoteHash", ""),
            "value_wei": params.get("value", "0"),
            "value_rbtc": int(params.get("value", "0")) / 1e18,
            "user_address": params.get("userAddress", ""),
        }
    topics = log.get("topics", [])
    return {
        "event": "PegOutUserRefunded",
        "tx_hash": log["transaction_hash"],
        "block_number": log["block_number"],
        "quote_hash": topics[1] if len(topics) > 1 and topics[1] else "",
        "value_wei": "0",
        "value_rbtc": 0,
        "user_address": "",
        "raw_data": log.get("data", ""),
    }


def parse_pegin_registered(log: dict) -> dict:
    """Parse a PegInRegistered event log."""
    decoded = log.get("decoded")
    if decoded and decoded.get("parameters"):
        params = {p["name"]: p["value"] for p in decoded["parameters"]}
        return {
            "event": "PegInRegistered",
            "tx_hash": log["transaction_hash"],
            "block_number": log["block_number"],
            "quote_hash": params.get("quoteHash", ""),
            "transferred_amount_wei": params.get("transferredAmount", "0"),
            "transferred_amount_rbtc": int(params.get("transferredAmount", "0")) / 1e18,
        }
    topics = log.get("topics", [])
    data = log.get("data", "0x")
    return {
        "event": "PegInRegistered",
        "tx_hash": log["transaction_hash"],
        "block_number": log["block_number"],
        "quote_hash": topics[1] if len(topics) > 1 and topics[1] else "",
        "transferred_amount_wei": str(int(data[2:66], 16)) if len(data) >= 66 else "0",
        "transferred_amount_rbtc": int(data[2:66], 16) / 1e18 if len(data) >= 66 else 0,
    }


def _address_from_topic(topic: str) -> str:
    """Extract address from a 32-byte topic (last 20 bytes)."""
    if not topic:
        return ""
    return "0x" + topic[-40:]


def fetch_block_timestamps(block_numbers: list[int]) -> dict[int, int]:
    """Fetch timestamps for a list of block numbers."""
    timestamps = {}
    unique_blocks = sorted(set(block_numbers))
    logger.info(f"Fetching timestamps for {len(unique_blocks)} blocks...")

    for i, block_num in enumerate(unique_blocks):
        if i > 0 and i % 50 == 0:
            logger.debug(f"...{i}/{len(unique_blocks)} blocks")
        try:
            resp = requests.get(f"{BASE_URL}/blocks/{block_num}", timeout=15)
            resp.raise_for_status()
            block_data = resp.json()
            ts = block_data.get("timestamp")
            if ts:
                timestamps[block_num] = ts
            time.sleep(RATE_LIMIT_DELAY)
        except Exception as e:
            logger.warning(f"Failed to fetch block {block_num}: {e}")

    return timestamps


def enrich_with_timestamps(events: list[dict], timestamps: dict[int, str]) -> list[dict]:
    """Add timestamp field to each event from block timestamps."""
    for event in events:
        block = event.get("block_number")
        if block and block in timestamps:
            event["block_timestamp"] = timestamps[block]
    return events


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

    # Build topic0 -> event_name lookup
    topic_to_event = {info["topic0"]: name for name, info in EVENTS.items()}

    parsers = {
        "CallForUser": parse_call_for_user,
        "PegOutDeposit": parse_pegout_deposit,
        "PegOutRefunded": parse_pegout_refunded,
        "Penalized": parse_penalized,
        "PegOutUserRefunded": parse_pegout_user_refunded,
        "PegInRegistered": parse_pegin_registered,
    }

    # Fetch logs from start_block
    logger.info("Fetching LBC events...")
    all_logs = fetch_all_logs(min_block=start_block)
    logger.info(f"Found {len(all_logs)} log entries")

    new_pegins = []
    new_pegouts = []
    new_penalties = []
    new_refunds = []
    new_block_numbers = []
    event_counts = {}
    max_block = start_block

    for log in all_logs:
        topic0 = log.get("topics", [None])[0]
        event_name = topic_to_event.get(topic0)
        if not event_name:
            continue

        event_counts[event_name] = event_counts.get(event_name, 0) + 1
        max_block = max(max_block, log.get("block_number", 0))

        # Skip PegInRegistered and PegOutRefunded — recognized but not saved
        if event_name in ("PegInRegistered", "PegOutRefunded"):
            continue

        parser = parsers[event_name]
        parsed = parser(log)

        new_block_numbers.append(parsed.get("block_number", 0))

        if event_name == "CallForUser":
            new_pegins.append(parsed)
        elif event_name == "PegOutDeposit":
            new_pegouts.append(parsed)
        elif event_name == "Penalized":
            new_penalties.append(parsed)
        elif event_name == "PegOutUserRefunded":
            new_refunds.append(parsed)

    logger.info("Event breakdown:")
    for name, count in sorted(event_counts.items(), key=lambda x: -x[1]):
        logger.info(f"  {name}: {count}")

    # Fetch block timestamps for new events only
    if new_block_numbers:
        logger.info("Fetching block timestamps...")
        timestamps = fetch_block_timestamps(new_block_numbers)
        new_pegins = enrich_with_timestamps(new_pegins, timestamps)
        new_pegouts = enrich_with_timestamps(new_pegouts, timestamps)
        new_penalties = enrich_with_timestamps(new_penalties, timestamps)
        new_refunds = enrich_with_timestamps(new_refunds, timestamps)

    # Merge with existing data (incremental) or use new data only (full)
    if cursor_block and not full_mode:
        logger.info("Merging with existing data...")
        all_pegins = merge_events(load_existing_json("flyover_pegins.json"), new_pegins)
        all_pegouts = merge_events(load_existing_json("flyover_pegouts.json"), new_pegouts)
        all_penalties = merge_events(load_existing_json("flyover_penalties.json"), new_penalties)
        all_refunds = merge_events(load_existing_json("flyover_refunds.json"), new_refunds)
    else:
        all_pegins = new_pegins
        all_pegouts = new_pegouts
        all_penalties = new_penalties
        all_refunds = new_refunds

    # Save to JSON
    pegins_path = os.path.join(DATA_DIR, "flyover_pegins.json")
    pegouts_path = os.path.join(DATA_DIR, "flyover_pegouts.json")
    penalties_path = os.path.join(DATA_DIR, "flyover_penalties.json")
    refunds_path = os.path.join(DATA_DIR, "flyover_refunds.json")

    with open(pegins_path, "w") as f:
        json.dump(all_pegins, f, indent=2)
    with open(pegouts_path, "w") as f:
        json.dump(all_pegouts, f, indent=2)
    with open(penalties_path, "w") as f:
        json.dump(all_penalties, f, indent=2)
    with open(refunds_path, "w") as f:
        json.dump(all_refunds, f, indent=2)

    # Update cursor
    save_cursor(max_block)

    logger.info(f"Saved {len(all_pegins)} peg-in events")
    logger.info(f"Saved {len(all_pegouts)} peg-out events")
    logger.info(f"Saved {len(all_penalties)} penalty events")
    logger.info(f"Saved {len(all_refunds)} refund events")
    logger.info(f"Cursor updated to block {max_block}")

    # Fetch TeksCapital LP real-time liquidity
    logger.info("Fetching TeksCapital LPS liquidity...")
    lp_liquidity = fetch_lp_liquidity()

    # Save LP info
    lp_info_path = os.path.join(DATA_DIR, "flyover_lp_info.json")
    with open(lp_info_path, "w") as f:
        json.dump(lp_liquidity, f, indent=2)
    logger.info("Saved LP liquidity info")

    # Summary
    pegin_volume = sum(e.get("value_rbtc", 0) for e in all_pegins if e["event"] == "CallForUser")
    pegout_volume = sum(e.get("amount_rbtc", 0) for e in all_pegouts if e["event"] == "PegOutDeposit")
    logger.info("--- Flyover Summary ---")
    logger.info(f"Peg-in (CallForUser): {sum(1 for e in all_pegins if e['event'] == 'CallForUser')} txs, {pegin_volume:.6f} RBTC")
    logger.info(f"Peg-out (PegOutDeposit): {sum(1 for e in all_pegouts if e['event'] == 'PegOutDeposit')} txs, {pegout_volume:.6f} RBTC")
    logger.info(f"Penalties: {len(all_penalties)}")
    logger.info(f"User refunds: {len(all_refunds)}")
    if lp_liquidity:
        logger.info(f"LP peg-in liquidity: {lp_liquidity.get('pegin_rbtc', 0):.6f} RBTC")
        logger.info(f"LP peg-out liquidity: {lp_liquidity.get('pegout_btc', 0):.6f} BTC")


def fetch_lp_liquidity() -> dict:
    """Fetch real-time liquidity from TeksCapital LPS API."""
    try:
        resp = requests.get(TEKSCAPITAL_LPS_URL, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        pegin_wei = int(data.get("peginLiquidityAmount", 0))
        pegout_wei = int(data.get("pegoutLiquidityAmount", 0))
        return {
            "lp_name": "TeksCapital",
            "rbtc_wallet": TEKSCAPITAL_RBTC_WALLET,
            "btc_wallet": TEKSCAPITAL_BTC_WALLET,
            "pegin_liquidity_wei": str(pegin_wei),
            "pegin_rbtc": pegin_wei / 1e18,
            "pegout_liquidity_wei": str(pegout_wei),
            "pegout_btc": pegout_wei / 1e18,
            "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
    except Exception as e:
        logger.warning(f"Failed to fetch LP liquidity: {e}")
        return {}


if __name__ == "__main__":
    main()
