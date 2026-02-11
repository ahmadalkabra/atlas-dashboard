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
import os
import time
import requests

BASE_URL = "https://rootstock.blockscout.com/api/v2"
LBC_ADDRESS = "0xaa9caf1e3967600578727f975f283446a3da6612"
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

# Only fetch events from ~1 year ago (block ~7,230,000 ≈ Feb 2025)
MIN_BLOCK = 7_230_000

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


def fetch_all_logs() -> list[dict]:
    """Fetch logs from the LBC contract, stopping at MIN_BLOCK.

    Blockscout's topic0 filter returns 422 for this contract, so we fetch
    everything and filter client-side.
    """
    all_items = []
    url = f"{BASE_URL}/addresses/{LBC_ADDRESS}/logs"
    params = {}
    page = 1

    while True:
        print(f"  Fetching LBC logs page {page}...")
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        items = data.get("items", [])
        # Filter out items below MIN_BLOCK and stop if all are old
        filtered = [i for i in items if i.get("block_number", 0) >= MIN_BLOCK]
        all_items.extend(filtered)

        if len(filtered) < len(items):
            # We've hit events older than our cutoff
            print(f"  Reached block cutoff ({MIN_BLOCK}), stopping.")
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
    """Add timestamp field to each event from block timestamps."""
    for event in events:
        block = event.get("block_number")
        if block and block in timestamps:
            event["block_timestamp"] = timestamps[block]
    return events


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

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

    # Fetch all logs and classify client-side
    print("Fetching all LBC events...")
    all_logs = fetch_all_logs()
    print(f"  Found {len(all_logs)} total log entries")

    all_pegins = []
    all_pegouts = []
    all_penalties = []
    all_refunds = []
    all_block_numbers = []
    event_counts = {}

    for log in all_logs:
        topic0 = log.get("topics", [None])[0]
        event_name = topic_to_event.get(topic0)
        if not event_name:
            continue

        event_counts[event_name] = event_counts.get(event_name, 0) + 1
        parser = parsers[event_name]
        parsed = parser(log)

        all_block_numbers.append(parsed.get("block_number", 0))

        if event_name in ("CallForUser", "PegInRegistered"):
            all_pegins.append(parsed)
        elif event_name in ("PegOutDeposit", "PegOutRefunded"):
            all_pegouts.append(parsed)
        elif event_name == "Penalized":
            all_penalties.append(parsed)
        elif event_name == "PegOutUserRefunded":
            all_refunds.append(parsed)

    print("\n  Event breakdown:")
    for name, count in sorted(event_counts.items(), key=lambda x: -x[1]):
        print(f"    {name}: {count}")

    # Fetch block timestamps
    print("\nFetching block timestamps...")
    timestamps = fetch_block_timestamps(all_block_numbers)

    # Enrich all events with timestamps
    all_pegins = enrich_with_timestamps(all_pegins, timestamps)
    all_pegouts = enrich_with_timestamps(all_pegouts, timestamps)
    all_penalties = enrich_with_timestamps(all_penalties, timestamps)
    all_refunds = enrich_with_timestamps(all_refunds, timestamps)

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

    print(f"\nSaved {len(all_pegins)} peg-in events to {pegins_path}")
    print(f"Saved {len(all_pegouts)} peg-out events to {pegouts_path}")
    print(f"Saved {len(all_penalties)} penalty events to {penalties_path}")
    print(f"Saved {len(all_refunds)} refund events to {refunds_path}")

    # Fetch TeksCapital LP real-time liquidity
    print("\nFetching TeksCapital LPS liquidity...")
    lp_liquidity = fetch_lp_liquidity()

    # Save LP info
    lp_info_path = os.path.join(DATA_DIR, "flyover_lp_info.json")
    with open(lp_info_path, "w") as f:
        json.dump(lp_liquidity, f, indent=2)
    print(f"Saved LP liquidity info to {lp_info_path}")

    # Summary
    pegin_volume = sum(e.get("value_rbtc", 0) for e in all_pegins if e["event"] == "CallForUser")
    pegout_volume = sum(e.get("amount_rbtc", 0) for e in all_pegouts if e["event"] == "PegOutDeposit")
    print(f"\n--- Flyover Summary ---")
    print(f"Peg-in (CallForUser): {sum(1 for e in all_pegins if e['event'] == 'CallForUser')} txs, {pegin_volume:.6f} RBTC")
    print(f"Peg-out (PegOutDeposit): {sum(1 for e in all_pegouts if e['event'] == 'PegOutDeposit')} txs, {pegout_volume:.6f} RBTC")
    print(f"Penalties: {len(all_penalties)}")
    print(f"User refunds: {len(all_refunds)}")
    if lp_liquidity:
        print(f"LP peg-in liquidity: {lp_liquidity.get('pegin_rbtc', 0):.6f} RBTC")
        print(f"LP peg-out liquidity: {lp_liquidity.get('pegout_btc', 0):.6f} BTC")


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
        print(f"  Warning: Failed to fetch LP liquidity: {e}")
        return {}


if __name__ == "__main__":
    main()
