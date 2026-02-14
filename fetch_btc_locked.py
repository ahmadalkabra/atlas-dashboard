"""
Fetch BTC Bridged & Locked stats from Blockscout API.

Queries:
- /stats for total bridged RBTC (rootstock_locked_btc)
- /addresses for paginated address list, summing balances where is_contract=True
"""

import json
import os
import time
import requests
from datetime import datetime, timezone

BASE_URL = "https://rootstock.blockscout.com/api/v2"
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

RATE_LIMIT_DELAY = 0.3  # seconds between API calls
MAX_PAGES = 100
MIN_BALANCE_RBTC = 0.01  # stop when individual balance drops below this
MAX_RETRIES = 3


def fetch_with_retry(url, params=None, timeout=30):
    """Fetch a URL with retry and exponential backoff."""
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait = 2 * (attempt + 1)
                print(f"  Retry {attempt + 1}/{MAX_RETRIES} after {wait}s: {e}")
                time.sleep(wait)
            else:
                raise


def fetch_total_bridged():
    """Fetch total bridged RBTC from /stats endpoint."""
    print("Fetching total bridged RBTC from /stats...")
    data = fetch_with_retry(f"{BASE_URL}/stats")
    raw = data.get("rootstock_locked_btc")
    if raw is None:
        print("  Warning: rootstock_locked_btc not found in /stats response")
        return None
    # Value is returned as a string in wei — convert to RBTC
    total = int(raw) / 1e18
    print(f"  Total bridged: {total:.4f} RBTC")
    return total


def fetch_contract_balances():
    """Paginate /addresses, sum coin_balance where is_contract=True."""
    print("Fetching address balances...")
    seen_addresses = set()  # deduplicate across pages
    contracts = {}  # hash -> {balance_rbtc, name}
    pages_fetched = 0
    params = {}

    for page_num in range(1, MAX_PAGES + 1):
        print(f"  Fetching addresses page {page_num}...")
        data = fetch_with_retry(f"{BASE_URL}/addresses", params=params)

        items = data.get("items", [])
        if not items:
            print("  No more addresses, stopping.")
            break

        min_balance_on_page = float("inf")
        for addr in items:
            addr_hash = (addr.get("hash") or "").lower()
            if addr_hash in seen_addresses:
                continue
            seen_addresses.add(addr_hash)

            balance_wei = int(addr.get("coin_balance") or "0")
            balance_rbtc = balance_wei / 1e18
            min_balance_on_page = min(min_balance_on_page, balance_rbtc)

            if addr.get("is_contract"):
                contracts[addr_hash] = {
                    "hash": addr.get("hash", ""),
                    "balance_rbtc": round(balance_rbtc, 6),
                    "name": addr.get("name") or addr.get("ens_domain_name") or "",
                }

        pages_fetched = page_num

        # Stop if smallest balance on page is below threshold
        if min_balance_on_page < MIN_BALANCE_RBTC:
            print(f"  Balance dropped below {MIN_BALANCE_RBTC} RBTC, stopping.")
            break

        next_page = data.get("next_page_params")
        if not next_page:
            print("  No more pages available.")
            break

        params = dict(next_page)
        time.sleep(RATE_LIMIT_DELAY)

    total_locked = sum(c["balance_rbtc"] for c in contracts.values())
    contract_count = len(contracts)

    # Sort top contracts by balance descending, keep top 20
    top_contracts = sorted(contracts.values(), key=lambda c: c["balance_rbtc"], reverse=True)[:20]

    print(f"  Locked in contracts: {total_locked:.4f} RBTC across {contract_count} contracts")
    print(f"  Pages fetched: {pages_fetched}")

    return total_locked, contract_count, top_contracts, pages_fetched


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    total_bridged = fetch_total_bridged()
    if total_bridged is None:
        print("Failed to fetch total bridged RBTC, aborting.")
        return

    time.sleep(RATE_LIMIT_DELAY)

    locked_rbtc, contract_count, top_contracts, pages_fetched = fetch_contract_balances()

    pct_locked = (locked_rbtc / total_bridged * 100) if total_bridged > 0 else 0

    result = {
        "total_bridged_rbtc": round(total_bridged, 4),
        "locked_in_contracts_rbtc": round(locked_rbtc, 4),
        "pct_locked": round(pct_locked, 2),
        "contract_count": contract_count,
        "top_contracts": top_contracts,
        "pages_fetched": pages_fetched,
        "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    output_path = os.path.join(DATA_DIR, "btc_locked_stats.json")
    with open(output_path, "w") as f:
        json.dump(result, f, indent=2)

    print(f"\nSaved to {output_path}")
    print(f"\n--- BTC Locked Summary ---")
    print(f"Total bridged: {total_bridged:.4f} RBTC")
    print(f"Locked in contracts: {locked_rbtc:.4f} RBTC")
    print(f"Percentage locked: {pct_locked:.2f}%")
    print(f"Contract count: {contract_count}")
    print(f"Pages fetched: {pages_fetched}")


if __name__ == "__main__":
    main()
