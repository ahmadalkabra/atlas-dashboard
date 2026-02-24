"""
Fetch operational health data for swap routes from the RSK Swap API.

Queries the rsk-swap-api (the single source of truth for swap route
availability) and enriches with PowPeg/Flyover data from existing files.

Data sources:
  - RSK Swap API /providers  — which swap providers are enabled + their pairs
  - RSK Swap API /tokens     — supported tokens
  - RSK Swap API /swaps/limits — min/max per pair (BTC→RBTC reference pair)
  - PowPeg: always available (native bridge, checked via existing data)
  - Flyover: from flyover_lp_info.json (already fetched by fetch_flyover.py)

Provider change detection:
  Compares current enabled provider list against the previous run's list.
  Records additions and removals in provider_changes[] for alerting.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta

import requests

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
OUTPUT_FILE = os.path.join(DATA_DIR, "route_health.json")

SWAP_API_BASE = "https://rskswap.mainnet.flyover.rif.technology/api"
REQUEST_TIMEOUT = 15

# History: max 7 days at 2h intervals = 84 entries
MAX_HISTORY = 84

# Reference pair for limits check
LIMITS_PAIR = {
    "from_token": "BTC",
    "to_token": "RBTC",
    "from_network": "BTC",
    "to_network": "30",
}

# RSK mainnet chain ID (string, as returned by the API)
RSK_CHAIN_ID = "30"


# ---------------------------------------------------------------------------
# RSK Swap API fetchers
# ---------------------------------------------------------------------------

def fetch_providers():
    """GET /providers — returns list of enabled providers with supported pairs."""
    url = f"{SWAP_API_BASE}/providers"
    start = time.monotonic()
    resp = requests.get(url, timeout=REQUEST_TIMEOUT)
    elapsed_ms = round((time.monotonic() - start) * 1000)
    resp.raise_for_status()
    return resp.json(), elapsed_ms


def fetch_tokens():
    """GET /tokens — returns list of supported tokens."""
    url = f"{SWAP_API_BASE}/tokens"
    resp = requests.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def fetch_limits(from_token, to_token, from_network, to_network):
    """GET /swaps/limits — returns min/max for a pair across all providers."""
    url = f"{SWAP_API_BASE}/swaps/limits"
    params = {
        "from_token": from_token,
        "to_token": to_token,
        "from_network": from_network,
        "to_network": to_network,
    }
    resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Provider data extraction
# ---------------------------------------------------------------------------

def extract_mainnet_pairs(supported_pairs):
    """Filter to mainnet pairs involving RSK (chain 30) and count directions."""
    pairs = []
    for p in supported_pairs:
        from_net = str(p.get("fromNetwork", ""))
        to_net = str(p.get("toNetwork", ""))
        # Keep pairs where at least one side is RSK mainnet
        if from_net == RSK_CHAIN_ID or to_net == RSK_CHAIN_ID:
            pairs.append({
                "from": f"{p.get('fromToken', '?')} ({from_net})",
                "to": f"{p.get('toToken', '?')} ({to_net})",
                "from_token": p.get("fromToken", ""),
                "to_token": p.get("toToken", ""),
            })
    return pairs


def extract_tokens_from_pairs(pairs):
    """Get unique token symbols traded against RSK from pairs."""
    tokens = set()
    for p in pairs:
        tokens.add(p["from_token"])
        tokens.add(p["to_token"])
    # Remove known testnet token prefixes (tRBTC, tBTC, etc.)
    TESTNET_TOKENS = {"tRBTC", "tBTC", "tRIF", "tUSDT", "tUSDC"}
    tokens = {t for t in tokens if t not in TESTNET_TOKENS}
    return sorted(tokens)


def build_provider_snapshot(provider_dto, limits_data):
    """Build a route snapshot from a SwapProviderDTO."""
    provider_id = provider_dto["providerId"]
    mainnet_pairs = extract_mainnet_pairs(provider_dto.get("supportedPairs", []))
    tokens = extract_tokens_from_pairs(mainnet_pairs)

    inbound = [p for p in mainnet_pairs if p["to_token"] in ("RBTC", "tRBTC")]
    outbound = [p for p in mainnet_pairs if p["from_token"] in ("RBTC", "tRBTC")]

    snapshot = {
        "name": provider_dto.get("shortName") or provider_id,
        "provider_id": provider_id,
        "enabled": True,
        "pair_count": len(mainnet_pairs),
        "inbound_pairs": len(inbound),
        "outbound_pairs": len(outbound),
        "tokens": tokens,
        "pairs": mainnet_pairs,
    }

    # Add limits if available
    if limits_data and provider_id in limits_data:
        lim = limits_data[provider_id]
        snapshot["limits"] = lim
    elif limits_data and "_global" in limits_data:
        snapshot["limits"] = limits_data["_global"]

    return snapshot


# ---------------------------------------------------------------------------
# PowPeg & Flyover (from existing data files)
# ---------------------------------------------------------------------------

def load_powpeg_status():
    """PowPeg is the native bridge — always available if chain is running."""
    return {
        "name": "PowPeg",
        "provider_id": "POWPEG",
        "enabled": True,
        "type": "native",
        "pair_count": 2,
        "inbound_pairs": 1,
        "outbound_pairs": 1,
        "tokens": ["BTC", "RBTC"],
        "estimated_speed": "~16 hours",
        "fee": "Network fee only",
    }


def load_flyover_status():
    """Load Flyover status from existing flyover_lp_info.json."""
    result = {
        "name": "Flyover",
        "provider_id": "FLYOVER",
        "enabled": True,
        "type": "lp_bridge",
        "pair_count": 2,
        "inbound_pairs": 1,
        "outbound_pairs": 1,
        "tokens": ["BTC", "RBTC"],
        "estimated_speed": "20-60 min",
        "fee": "~0.15%",
    }

    lp_path = os.path.join(DATA_DIR, "flyover_lp_info.json")
    try:
        with open(lp_path) as f:
            lp = json.load(f)
        result["pegin_available"] = lp.get("is_operational_pegin", True)
        result["pegout_available"] = lp.get("is_operational_pegout", True)
        pegin_liq = lp.get("lps_pegin_rbtc") or lp.get("pegin_rbtc")
        pegout_liq = lp.get("lps_pegout_btc") or lp.get("pegout_btc")
        if pegin_liq is not None:
            result["pegin_liquidity_rbtc"] = round(float(pegin_liq), 2)
        if pegout_liq is not None:
            result["pegout_liquidity_btc"] = round(float(pegout_liq), 2)
    except (FileNotFoundError, json.JSONDecodeError):
        logger.warning("flyover_lp_info.json not available — using defaults")

    return result


# ---------------------------------------------------------------------------
# Provider change detection
# ---------------------------------------------------------------------------

def detect_provider_changes(existing, current_provider_ids):
    """Compare current providers against previous run. Returns list of changes."""
    changes = []
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    prev_ids = set(existing.get("swap_provider_ids", []))
    curr_ids = set(current_provider_ids)

    for pid in curr_ids - prev_ids:
        changes.append({"t": now, "provider": pid, "change": "added"})
        logger.info("Provider ADDED: %s", pid)

    for pid in prev_ids - curr_ids:
        changes.append({"t": now, "provider": pid, "change": "removed"})
        logger.warning("Provider REMOVED: %s", pid)

    return changes


# ---------------------------------------------------------------------------
# History management
# ---------------------------------------------------------------------------

def load_existing():
    """Load existing route_health.json, return empty structure on failure."""
    try:
        with open(OUTPUT_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def append_history(existing, providers):
    """Append a compact status snapshot to the rolling history."""
    history = existing.get("history", [])
    entry = {"t": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")}

    # API-level status
    entry["swap_api"] = "up" if providers else "down"

    # Per-provider: enabled = up
    for pid in providers:
        entry[pid.lower()] = "up"

    history.append(entry)

    # Trim to 7-day window
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    cutoff_str = cutoff.isoformat().replace("+00:00", "Z")
    history = [h for h in history if h.get("t", "") >= cutoff_str]

    if len(history) > MAX_HISTORY:
        history = history[-MAX_HISTORY:]

    return history


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    logger.info("Starting route health check via RSK Swap API...")
    os.makedirs(DATA_DIR, exist_ok=True)

    existing = load_existing()
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    # --- Fetch from RSK Swap API ---
    swap_api_status = "down"
    swap_api_response_ms = None
    providers_raw = []
    tokens_raw = []
    limits_data = {}

    try:
        providers_raw, swap_api_response_ms = fetch_providers()
        swap_api_status = "operational"
        logger.info(
            "Swap API: %d providers enabled (%dms)",
            len(providers_raw), swap_api_response_ms,
        )
    except requests.exceptions.RequestException as exc:
        logger.error("Swap API /providers failed: %s", exc)

    if swap_api_status == "operational":
        # Fetch tokens
        try:
            tokens_raw = fetch_tokens()
            logger.info("Swap API: %d tokens", len(tokens_raw))
        except requests.exceptions.RequestException as exc:
            logger.warning("Swap API /tokens failed: %s", exc)

        # Fetch limits for reference pair
        try:
            lim = fetch_limits(**LIMITS_PAIR)
            limits_data["_global"] = {
                "min_sats": lim.get("minAmount"),
                "max_sats": lim.get("maxAmount"),
                "min_btc": (lim.get("minAmount") or 0) / 1e8,
                "max_btc": (lim.get("maxAmount") or 0) / 1e8,
            }
            logger.info(
                "Swap API limits (BTC→RBTC): %s – %s BTC",
                limits_data["_global"]["min_btc"],
                limits_data["_global"]["max_btc"],
            )
        except requests.exceptions.RequestException as exc:
            logger.warning("Swap API /swaps/limits failed: %s", exc)

    # --- Build provider snapshots ---
    swap_providers = {}
    swap_provider_ids = []

    for p in providers_raw:
        pid = p.get("providerId", "UNKNOWN")
        swap_provider_ids.append(pid)
        swap_providers[pid.lower()] = build_provider_snapshot(p, limits_data)

    # --- Detect provider changes ---
    provider_changes = existing.get("provider_changes", [])
    new_changes = detect_provider_changes(existing, swap_provider_ids)
    provider_changes.extend(new_changes)
    # Keep last 30 days of changes
    cutoff_30d = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat().replace("+00:00", "Z")
    provider_changes = [c for c in provider_changes if c.get("t", "") >= cutoff_30d]

    # --- PowPeg & Flyover (from existing data, not swap API) ---
    powpeg = load_powpeg_status()
    flyover = load_flyover_status()

    # --- History ---
    history = append_history(existing, swap_provider_ids)

    # --- Assemble output ---
    result = {
        "fetched_at": now,
        "swap_api": {
            "status": swap_api_status,
            "response_ms": swap_api_response_ms,
            "base_url": SWAP_API_BASE,
        },
        "native_routes": {
            "powpeg": powpeg,
            "flyover": flyover,
        },
        "swap_providers": swap_providers,
        "swap_provider_ids": swap_provider_ids,
        "tokens": [
            {"symbol": t.get("symbol", "UNKNOWN"), "description": t.get("description", ""), "type": t.get("type", "")}
            for t in tokens_raw if t.get("symbol")
        ],
        "limits_btc_rbtc": limits_data.get("_global"),
        "provider_changes": provider_changes,
        "new_provider_changes": new_changes,
        "history": history,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(result, f, indent=2)

    logger.info("Route health written to %s", OUTPUT_FILE)

    # Summary
    total_providers = len(swap_provider_ids)
    logger.info(
        "Summary: swap API %s, %d swap providers enabled [%s], %d tokens",
        swap_api_status,
        total_providers,
        ", ".join(swap_provider_ids),
        len(tokens_raw),
    )
    if new_changes:
        for c in new_changes:
            logger.info("  Provider change: %s %s", c["provider"], c["change"])


if __name__ == "__main__":
    main()
