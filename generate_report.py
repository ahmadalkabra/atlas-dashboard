"""
Generate a static HTML dashboard from Flyover and PowPeg event data.

Loads JSON data from data/, computes metrics, and renders an interactive
HTML dashboard using Plotly.js for charts and vanilla JS for filtering.
"""

import json
import os
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
PAGES_DIR = os.path.join(SCRIPT_DIR, "pages")
OUTPUT_PATH = os.path.join(PAGES_DIR, "index.html")


def load_json(filename: str) -> list | dict:
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        print(f"  Warning: {path} not found, returning empty")
        dict_files = ("flyover_lp_info.json", "btc_locked_stats.json", "web_analytics.json", "route_health.json")
        return {} if filename in dict_files else []
    with open(path) as f:
        return json.load(f)


def parse_timestamp(ts: str) -> datetime | None:
    """Parse an ISO 8601 timestamp string."""
    if not ts:
        return None
    try:
        # Blockscout returns ISO format like "2024-01-15T10:30:00.000000Z"
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def build_dashboard_data(
    flyover_pegins: list[dict],
    flyover_pegouts: list[dict],
    flyover_pegout_refunds: list[dict],
    flyover_penalties: list[dict],
    flyover_refunds: list[dict],
    powpeg_pegins: list[dict],
    powpeg_pegouts: list[dict],
    lp_info: dict | None = None,
    btc_locked_stats: dict | None = None,
    web_analytics: dict | None = None,
    route_health: dict | None = None,
) -> dict:
    """Build the full dashboard dataset for embedding in HTML."""

    def extract_events(events: list[dict], value_field: str, address_field: str, event_filter: str | None = None) -> list[dict]:
        result = []
        for e in events:
            if event_filter and e.get("event") != event_filter:
                continue
            ts = parse_timestamp(e.get("block_timestamp", ""))
            result.append({
                "tx_hash": e.get("tx_hash", ""),
                "block": e.get("block_number", 0),
                "timestamp": ts.isoformat() if ts else "",
                "value_rbtc": float(e.get(value_field, 0)),
                "address": e.get(address_field, ""),
            })
        return result

    # Flyover peg-ins (CallForUser only — fetcher already filters)
    fp_pegins = []
    for e in flyover_pegins:
        ts = parse_timestamp(e.get("block_timestamp", ""))
        fp_pegins.append({
            "tx_hash": e.get("tx_hash", ""),
            "block": e.get("block_number", 0),
            "timestamp": ts.isoformat() if ts else "",
            "value_rbtc": float(e.get("value_rbtc", 0)),
            "address": e.get("dest_address", ""),
            "lp_address": e.get("from_address", ""),
        })

    # Flyover peg-outs (PegOutDeposit only — fetcher already filters)
    fp_pegouts = []
    for e in flyover_pegouts:
        ts = parse_timestamp(e.get("block_timestamp", ""))
        fp_pegouts.append({
            "tx_hash": e.get("tx_hash", ""),
            "block": e.get("block_number", 0),
            "timestamp": ts.isoformat() if ts else "",
            "value_rbtc": float(e.get("amount_rbtc", 0)),
            "address": e.get("sender", ""),
            "quote_hash": e.get("quote_hash", ""),
        })

    # PowPeg peg-ins
    pp_pegins = []
    for e in powpeg_pegins:
        ts = parse_timestamp(e.get("block_timestamp", ""))
        pp_pegins.append({
            "tx_hash": e.get("tx_hash", ""),
            "block": e.get("block_number", 0),
            "timestamp": ts.isoformat() if ts else "",
            "value_rbtc": float(e.get("value_rbtc", 0)),
            "address": e.get("to_address", ""),
        })

    # PowPeg peg-outs
    pp_pegouts = []
    for e in powpeg_pegouts:
        ts = parse_timestamp(e.get("block_timestamp", ""))
        pp_pegouts.append({
            "tx_hash": e.get("tx_hash", ""),
            "block": e.get("block_number", 0),
            "timestamp": ts.isoformat() if ts else "",
            "value_rbtc": float(e.get("value_rbtc", 0)),
            "address": e.get("from_address", ""),
        })

    # Penalties
    penalties = []
    for e in flyover_penalties:
        ts = parse_timestamp(e.get("block_timestamp", ""))
        penalties.append({
            "tx_hash": e.get("tx_hash", ""),
            "block": e.get("block_number", 0),
            "timestamp": ts.isoformat() if ts else "",
            "lp_address": e.get("lp_address", ""),
            "penalty_rbtc": float(e.get("penalty_rbtc", 0)),
            "quote_hash": e.get("quote_hash", ""),
        })

    # User refunds
    refunds = []
    for e in flyover_refunds:
        ts = parse_timestamp(e.get("block_timestamp", ""))
        refunds.append({
            "tx_hash": e.get("tx_hash", ""),
            "block": e.get("block_number", 0),
            "timestamp": ts.isoformat() if ts else "",
            "user_address": e.get("user_address", ""),
            "value_rbtc": float(e.get("value_rbtc", 0)),
        })

    # Peg-out refunds (LP claimed BTC delivery)
    pegout_refund_hashes = set()
    for e in flyover_pegout_refunds:
        pegout_refund_hashes.add(e.get("quote_hash", ""))

    return {
        "flyover_pegins": fp_pegins,
        "flyover_pegouts": fp_pegouts,
        "pegout_refund_hashes": list(pegout_refund_hashes),
        "powpeg_pegins": pp_pegins,
        "powpeg_pegouts": pp_pegouts,
        "penalties": penalties,
        "refunds": refunds,
        "lp_info": lp_info or {},
        "btc_locked": btc_locked_stats or {},
        "web_analytics": web_analytics or {},
        "route_health": route_health or {},
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def generate_html() -> str:
    """Generate the full HTML dashboard (loads data via fetch at runtime)."""

    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Atlas Dashboard</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<script src="https://cdn.plot.ly/plotly-2.35.0.min.js"></script>
<style>
  :root {
    --bg: #0a0a0a;
    --surface: #111111;
    --surface-2: #161616;
    --border: #1e1e1e;
    --border-hover: #2a2a2a;
    --text: #FAFAF5;
    --muted: #737373;
    --flyover-pegin: #DEFF19;
    --flyover-pegout: #F0FF96;
    --powpeg-pegin: #FF9100;
    --powpeg-pegout: #FED8A7;
    --green: #22C55E;
    --red: #EF4444;
    --purple: #9E75FF;
    --radius: 12px;
    --radius-sm: 8px;
  }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background: var(--bg);
    color: var(--text);
    min-height: 100vh;
    -webkit-font-smoothing: antialiased;
  }
  .dashboard {
    max-width: 1200px;
    margin: 0 auto;
    padding: 32px 24px;
  }

  /* --- Header --- */
  header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 32px;
    flex-wrap: wrap;
    gap: 16px;
  }
  .title-group h1 {
    font-size: 22px;
    font-weight: 700;
    letter-spacing: -0.5px;
    background: linear-gradient(135deg, #FF9100, #DEFF19);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
  }
  .title-group .subtitle {
    color: var(--muted);
    font-size: 12px;
    margin-top: 2px;
  }
  .period-nav {
    display: flex;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    overflow: hidden;
  }
  .period-nav button {
    background: transparent;
    border: none;
    color: var(--muted);
    padding: 7px 16px;
    font-family: inherit;
    font-size: 13px;
    font-weight: 500;
    cursor: pointer;
    transition: all 0.15s;
  }
  .period-nav button:hover { color: var(--text); background: rgba(255,255,255,0.04); }
  .period-nav button.active { background: var(--text); color: #000; font-weight: 600; }

  /* --- Operation Summary --- */
  .op-summary {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 16px;
    margin-bottom: 28px;
  }
  .op-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 18px;
    transition: border-color 0.15s;
    border-top: 3px solid var(--border);
  }
  .op-card:hover { border-color: var(--border-hover); }
  .op-card-name {
    font-size: 12px;
    font-weight: 600;
    margin-bottom: 14px;
    letter-spacing: 0.3px;
  }
  .op-card-metrics {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 10px;
  }
  .op-metric-label {
    color: var(--muted);
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 4px;
  }
  .op-metric-value {
    font-size: 20px;
    font-weight: 700;
    letter-spacing: -0.3px;
    line-height: 1.1;
  }
  .op-metric-delta {
    font-size: 11px;
    font-weight: 600;
    margin-top: 4px;
  }
  .op-metric-delta.up { color: var(--green); }
  .op-metric-delta.down { color: var(--red); }
  .op-metric-delta.neutral { color: var(--muted); }
  .op-totals {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 16px;
    margin-bottom: 28px;
  }
  .op-total-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 14px 18px;
    display: flex;
    align-items: center;
    justify-content: space-between;
  }
  .op-total-label {
    color: var(--muted);
    font-size: 11px;
    font-weight: 500;
  }
  .op-total-value {
    font-size: 18px;
    font-weight: 700;
  }

  /* --- BTC Locked Section --- */
  .btc-locked-section {
    margin-bottom: 28px;
  }
  .btc-locked-panel {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 20px;
  }
  .btc-locked-header {
    font-size: 13px;
    font-weight: 600;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.8px;
    margin-bottom: 16px;
  }
  .btc-locked-stats {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 12px;
    margin-bottom: 16px;
  }
  .btc-locked-stat {
    background: var(--bg);
    border-radius: var(--radius-sm);
    padding: 14px;
    text-align: center;
  }
  .btc-locked-stat-label {
    color: var(--muted);
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 6px;
  }
  .btc-locked-stat-value {
    font-size: 22px;
    font-weight: 700;
    color: #08FFD1;
    line-height: 1.1;
  }
  .btc-locked-stat-sub {
    color: var(--muted);
    font-size: 11px;
    margin-top: 4px;
  }
  .btc-locked-bar-wrapper {
    position: relative;
    margin-top: 4px;
  }
  .btc-locked-bar {
    height: 10px;
    background: var(--border);
    border-radius: 5px;
    overflow: hidden;
  }
  .btc-locked-bar-fill {
    height: 100%;
    border-radius: 5px;
    background: linear-gradient(90deg, #08FFD1, #08FFD1cc);
    transition: width 0.5s ease;
  }
  .btc-locked-bar-labels {
    display: flex;
    justify-content: space-between;
    margin-top: 4px;
  }
  .btc-locked-bar-labels span {
    font-size: 10px;
    color: var(--muted);
  }
  .btc-locked-bar-pct {
    position: absolute;
    right: 0;
    top: -18px;
    font-size: 11px;
    font-weight: 600;
    color: #08FFD1;
  }

  /* --- Unique Wallets Section --- */
  .wallets-section {
    margin-bottom: 28px;
  }
  .wallets-panel {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 20px;
  }
  .wallets-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
  }
  .wallets-table th,
  .wallets-table td {
    padding: 10px 14px;
    text-align: right;
    border-bottom: 1px solid var(--border);
  }
  .wallets-table th {
    color: var(--muted);
    font-weight: 600;
    text-transform: uppercase;
    font-size: 10px;
    letter-spacing: 0.5px;
    background: var(--surface);
  }
  .wallets-table th:first-child,
  .wallets-table td:first-child {
    text-align: left;
  }
  .wallets-table tbody tr:hover td {
    background: rgba(255,255,255,0.02);
  }
  .wallets-table td.wallet-num {
    font-weight: 700;
    font-size: 16px;
    letter-spacing: -0.3px;
  }
  .wallets-table .col-flyover { color: var(--flyover-pegin); }
  .wallets-table .col-powpeg { color: var(--powpeg-pegin); }
  .wallets-table .col-combined { color: var(--purple); }

  /* --- Health Section --- */
  .health-section {
    margin-bottom: 28px;
  }
  .health-panel {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 20px;
  }
  .health-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 16px;
  }
  .health-header h3 {
    font-size: 14px;
    font-weight: 600;
    display: flex;
    align-items: center;
    gap: 10px;
  }
  .health-overall-dot {
    width: 10px;
    height: 10px;
    border-radius: 50%;
    display: inline-block;
  }
  .health-overall-dot.pulse {
    animation: healthPulse 2s ease-in-out infinite;
  }
  @keyframes healthPulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.4; }
  }
  .health-overall-label {
    font-size: 12px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }
  .health-staleness {
    font-size: 11px;
    color: var(--muted);
    background: rgba(239,68,68,0.1);
    border: 1px solid rgba(239,68,68,0.25);
    border-radius: 6px;
    padding: 4px 10px;
  }
  .health-grid {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 12px;
    align-items: stretch;
  }
  .health-indicator {
    background: var(--bg);
    border-radius: var(--radius-sm);
    padding: 14px;
    border-left: 3px solid var(--border);
    display: flex;
    flex-direction: column;
  }
  .health-indicator.status-healthy { border-left-color: var(--green); }
  .health-indicator.status-warning { border-left-color: #EAB308; }
  .health-indicator.status-critical { border-left-color: var(--red); }
  .health-indicator { position: relative; }
  .health-info-btn {
    position: absolute;
    top: 10px;
    right: 10px;
    width: 18px;
    height: 18px;
    border-radius: 50%;
    border: 1px solid var(--border-hover);
    background: transparent;
    color: var(--muted);
    font-size: 11px;
    line-height: 16px;
    text-align: center;
    cursor: pointer;
    padding: 0;
    font-family: inherit;
    transition: all 0.15s;
  }
  .health-info-btn:hover { border-color: var(--purple); color: var(--purple); }
  .health-popover {
    display: none;
    position: absolute;
    top: 32px;
    right: 6px;
    background: var(--surface);
    border: 1px solid var(--border-hover);
    border-radius: 8px;
    padding: 10px 14px;
    font-size: 11px;
    color: var(--text);
    z-index: 10;
    white-space: nowrap;
    box-shadow: 0 4px 12px rgba(0,0,0,0.4);
    line-height: 1.6;
  }
  .health-popover.open { display: block; }
  .health-popover-row { display: flex; align-items: center; gap: 6px; }
  .health-popover-dot {
    width: 6px;
    height: 6px;
    border-radius: 50%;
    display: inline-block;
    flex-shrink: 0;
  }
  .health-indicator-label {
    color: var(--muted);
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 8px;
  }
  .health-indicator-value {
    font-size: 18px;
    font-weight: 700;
    margin-bottom: 4px;
    line-height: 1.1;
  }
  .health-indicator-sub {
    color: var(--muted);
    font-size: 11px;
    margin-bottom: 8px;
  }
  .health-indicator-status {
    font-size: 10px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.3px;
    margin-top: auto;
  }
  .health-indicator-status.healthy { color: var(--green); }
  .health-indicator-status.warning { color: #EAB308; }
  .health-indicator-status.critical { color: var(--red); }
  .health-bar {
    height: 4px;
    background: var(--border);
    border-radius: 2px;
    margin: 8px 0 4px;
    overflow: hidden;
  }
  .health-bar-fill {
    height: 100%;
    border-radius: 2px;
    transition: width 0.3s ease;
  }
  .health-panel.flash {
    animation: healthFlash 0.6s ease;
  }
  @keyframes healthFlash {
    0% { border-color: var(--border); }
    30% { border-color: var(--green); }
    100% { border-color: var(--border); }
  }
  .live-badge {
    display: inline-block;
    font-size: 9px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    color: var(--green);
    background: rgba(34,197,94,0.12);
    border: 1px solid rgba(34,197,94,0.3);
    border-radius: 4px;
    padding: 2px 6px;
    margin-left: 8px;
    vertical-align: middle;
  }
  .health-updated {
    font-size: 11px;
    color: var(--muted);
    background: rgba(34,197,94,0.08);
    border: 1px solid rgba(34,197,94,0.2);
    border-radius: 6px;
    padding: 4px 10px;
  }

  /* --- Route Health Section --- */
  .route-health-panel {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 20px;
  }
  .route-health-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 16px;
  }
  .route-health-header h3 {
    font-size: 14px;
    font-weight: 600;
    display: flex;
    align-items: center;
    gap: 10px;
  }
  .route-health-grid {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 10px;
  }
  @media (max-width: 700px) {
    .route-health-grid {
      grid-template-columns: repeat(2, 1fr);
    }
  }
  @media (max-width: 450px) {
    .route-health-grid {
      grid-template-columns: 1fr;
    }
  }
  .route-card {
    background: var(--bg);
    border-radius: var(--radius-sm);
    padding: 16px;
    border-left: 3px solid var(--border);
    display: flex;
    flex-direction: column;
    gap: 2px;
  }
  .route-card.status-operational { border-left-color: #08FFD1; }
  .route-card.status-degraded { border-left-color: #DEFF19; }
  .route-card.status-down { border-left-color: #FF70E0; }
  .route-card-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 8px;
  }
  .route-card-name {
    font-size: 13px;
    font-weight: 600;
    display: flex;
    align-items: center;
    gap: 8px;
  }
  .route-card-type {
    font-size: 9px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    color: var(--muted);
    background: var(--surface-2);
    border-radius: 4px;
    padding: 2px 6px;
  }
  .route-status-dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    display: inline-block;
    flex-shrink: 0;
  }
  .route-status-dot.operational { background: #08FFD1; }
  .route-status-dot.degraded { background: #DEFF19; }
  .route-status-dot.down { background: #FF70E0; animation: healthPulse 2s ease-in-out infinite; }
  .route-card-status {
    font-size: 10px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.3px;
    margin-bottom: 8px;
  }
  .route-card-status.operational { color: #08FFD1; }
  .route-card-status.degraded { color: #DEFF19; }
  .route-card-status.down { color: #FF70E0; }
  .route-card-row {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    font-size: 11px;
    line-height: 1.8;
  }
  .route-card-row .label { color: var(--muted); }
  .route-card-row .value { color: var(--text); font-weight: 500; text-align: right; }
  .route-card-row .value.dim { color: var(--muted); font-weight: 400; }
  .route-card-divider {
    border-top: 1px solid var(--border);
    margin: 6px 0;
  }
  .route-card-details {
    margin-top: 4px;
  }
  .route-card-detail-extra {
    font-size: 10px;
    color: var(--muted);
    line-height: 1.6;
  }
  .route-card-detail-extra .up { color: var(--green); }
  .route-card-detail-extra .warn { color: #EAB308; }
  .route-card-detail-extra .err { color: #FF70E0; }
  .route-asset-list {
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
    margin-top: 4px;
  }
  .route-asset-tag {
    font-size: 9px;
    font-weight: 600;
    padding: 1px 5px;
    border-radius: 3px;
    background: var(--surface-2);
    color: var(--text);
    letter-spacing: 0.3px;
  }
  .route-pairs-toggle {
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 11px;
    color: var(--muted);
    cursor: pointer;
    padding: 6px 0 2px;
    user-select: none;
  }
  .route-pairs-toggle:hover { color: var(--text); }
  .route-pairs-toggle .arrow {
    display: inline-block;
    font-size: 9px;
    transition: transform 0.2s ease;
  }
  .route-pairs-toggle.open .arrow { transform: rotate(90deg); }
  .route-pairs-list {
    display: none;
    padding-top: 4px;
  }
  .route-pairs-list.open { display: block; }
  .route-uptime-row {
    margin-top: 12px;
    padding-top: 12px;
    border-top: 1px solid var(--border);
    font-size: 11px;
    color: var(--muted);
    display: flex;
    flex-wrap: wrap;
    gap: 6px 16px;
  }
  .route-uptime-row span { color: var(--text); font-weight: 500; }

  /* --- Traffic Section --- */
  .traffic-stats {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 12px;
    margin-bottom: 16px;
  }
  .traffic-stat {
    background: var(--bg);
    border-radius: var(--radius-sm);
    padding: 14px;
    text-align: center;
  }
  .traffic-stat-label {
    color: var(--muted);
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 6px;
  }
  .traffic-stat-value {
    font-size: 22px;
    font-weight: 700;
    color: var(--purple);
    line-height: 1.1;
  }
  .traffic-stat-sub {
    color: var(--muted);
    font-size: 11px;
    margin-top: 4px;
  }
  .funnel-step {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 8px;
  }
  .funnel-step-label {
    width: 120px;
    font-size: 12px;
    font-weight: 500;
    color: var(--text);
    text-align: right;
    flex-shrink: 0;
  }
  .funnel-step-bar-wrapper {
    flex: 1;
    position: relative;
  }
  .funnel-step-bar {
    height: 28px;
    background: var(--border);
    border-radius: 4px;
    overflow: hidden;
  }
  .funnel-step-bar-fill {
    height: 100%;
    border-radius: 4px;
    background: linear-gradient(90deg, var(--purple), rgba(158,117,255,0.6));
    transition: width 0.5s ease;
    display: flex;
    align-items: center;
    padding-left: 10px;
  }
  .funnel-step-bar-text {
    font-size: 11px;
    font-weight: 600;
    color: #fff;
    white-space: nowrap;
  }
  .funnel-step-meta {
    width: 60px;
    font-size: 11px;
    color: var(--muted);
    text-align: right;
    flex-shrink: 0;
  }
  .traffic-source {
    font-size: 11px;
    color: var(--muted);
    text-align: right;
    margin-top: 12px;
  }

  /* --- Chart Panels --- */
  .chart-section { margin-bottom: 20px; }
  .chart-grid {
    display: grid;
    grid-template-columns: 3fr 2fr;
    gap: 20px;
    margin-bottom: 20px;
  }
  .chart-grid-2col {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
    margin-bottom: 20px;
  }
  .chart-panel {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 20px;
  }
  .chart-panel-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 12px;
  }
  .chart-panel-title {
    font-size: 13px;
    font-weight: 600;
    color: var(--text);
  }
  .chart-toggle {
    display: flex;
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 6px;
    overflow: hidden;
  }
  .chart-toggle button {
    background: transparent;
    border: none;
    color: var(--muted);
    padding: 4px 10px;
    font-family: inherit;
    font-size: 11px;
    font-weight: 500;
    cursor: pointer;
    transition: all 0.15s;
  }
  .chart-toggle button:hover { color: var(--text); }
  .chart-toggle button.active { background: var(--surface-2); color: var(--text); }

  /* --- Table --- */
  .table-section { margin-bottom: 28px; }
  .table-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 12px;
  }
  .table-header .section-title { margin-bottom: 0; }
  .section-title {
    font-size: 13px;
    font-weight: 600;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.8px;
    margin-bottom: 12px;
  }
  .table-panel {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 16px;
    overflow-x: auto;
  }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th, td { padding: 10px 12px; text-align: right; border-bottom: 1px solid var(--border); }
  th {
    color: var(--muted);
    font-weight: 600;
    text-transform: uppercase;
    font-size: 10px;
    letter-spacing: 0.5px;
    position: sticky;
    top: 0;
    background: var(--surface);
    white-space: nowrap;
  }
  th:first-child, td:first-child { text-align: left; }
  tbody tr:hover td { background: rgba(255,255,255,0.02); }
  .th-group {
    text-align: center !important;
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0;
    text-transform: none;
    border-bottom: 2px solid var(--border);
  }
  .totals-row td {
    font-weight: 700;
    border-top: 2px solid var(--border);
    border-bottom: none;
  }
  .th-dot {
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    margin-right: 4px;
    vertical-align: middle;
  }
  .page-controls {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 12px;
    padding: 14px 0 2px;
  }
  .page-btn {
    background: var(--surface);
    border: 1px solid var(--border);
    color: var(--muted);
    padding: 6px 14px;
    border-radius: 6px;
    cursor: pointer;
    font-family: inherit;
    font-size: 12px;
    font-weight: 500;
    transition: all 0.15s;
  }
  .page-btn:hover:not(:disabled) { border-color: var(--purple); color: var(--purple); }
  .page-btn:disabled { opacity: 0.3; cursor: default; }
  .page-info { color: var(--muted); font-size: 12px; }

  /* --- LP Section --- */
  .lp-name { color: var(--flyover-pegin); font-weight: 600; font-size: 13px; }

  /* --- Footer --- */
  footer {
    text-align: center;
    padding: 20px 0;
    color: var(--muted);
    font-size: 11px;
    border-top: 1px solid var(--border);
  }
  footer a { color: var(--purple); text-decoration: none; }
  footer a:hover { text-decoration: underline; }

  /* --- Loading Overlay --- */
  #loading-overlay {
    position: fixed;
    inset: 0;
    background: var(--bg);
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 1000;
    color: var(--muted);
    font-size: 14px;
    font-weight: 500;
    letter-spacing: 0.3px;
  }
  #loading-overlay.hidden { display: none; }

  /* --- Responsive --- */
  @media (max-width: 1024px) {
    .chart-grid { grid-template-columns: 1fr; }
    .chart-grid-2col { grid-template-columns: 1fr; }
    .op-summary { grid-template-columns: repeat(2, 1fr); }
    .op-totals { grid-template-columns: repeat(2, 1fr); }

    .health-grid { grid-template-columns: repeat(2, 1fr); }
    .btc-locked-stats { grid-template-columns: repeat(3, 1fr); }
    .traffic-stats { grid-template-columns: repeat(2, 1fr); }
  }
  @media (max-width: 768px) {
    header { flex-direction: column; align-items: flex-start; }
  }
  @media (max-width: 480px) {
    .op-summary { grid-template-columns: 1fr; }
    .op-totals { grid-template-columns: 1fr; }

    .health-grid { grid-template-columns: 1fr; }
    .btc-locked-stats { grid-template-columns: 1fr; }
    .traffic-stats { grid-template-columns: 1fr; }
    .funnel-step-label { width: 80px; font-size: 11px; }
    .dashboard { padding: 16px 12px; }
  }
</style>
</head>
<body>

<div id="loading-overlay">Loading dashboard data...</div>

<div class="dashboard">
  <header>
    <div class="title-group">
      <h1>Atlas Dashboard</h1>
      <p class="subtitle">Rootstock Bridge Analytics <span id="generated-at"></span></p>
    </div>
    <nav class="period-nav">
      <button onclick="setPeriod('day')" id="btn-day">D</button>
      <button onclick="setPeriod('week')" id="btn-week">W</button>
      <button onclick="setPeriod('month')" id="btn-month" class="active">M</button>
      <button onclick="setPeriod('quarter')" id="btn-quarter">Q</button>
    </nav>
  </header>

  <section id="op-summary" class="op-summary"></section>

  <section class="btc-locked-section" id="btc-locked-section"></section>

  <section class="wallets-section" id="wallets-section">
    <div class="section-title">Unique Wallets</div>
    <div class="wallets-panel">
      <div id="wallets-content"></div>
    </div>
  </section>

  <section class="health-section" id="health-section-wrapper">
    <div class="section-title">Flyover Liquidity Provider</div>
    <div id="health-panel" class="health-panel"></div>
  </section>

  <section class="health-section" id="route-health-section" style="display:none">
    <div class="section-title">Route Health</div>
    <div id="route-health-panel" class="route-health-panel"></div>
  </section>

  <section class="health-section" id="largest-tx-section">
    <div class="section-title">Largest Transactions</div>
    <div class="op-summary" id="largest-tx-cards"></div>
  </section>

  <section class="chart-section">
    <div class="chart-panel" style="margin-bottom:20px">
      <div class="chart-panel-header">
        <div class="chart-panel-title">Volume Over Time</div>
        <div class="chart-toggle" id="vol-chart-toggle">
          <button class="active" onclick="setChartMode('area')">Area</button>
          <button onclick="setChartMode('bar')">Bar</button>
        </div>
      </div>
      <div id="chart-volume-trend"></div>
    </div>
    <div class="chart-grid-2col" style="margin-bottom:20px">
      <div class="chart-panel">
        <div class="chart-panel-header">
          <div class="chart-panel-title">Net Flow (Peg-In &minus; Peg-Out)</div>
        </div>
        <div id="chart-net-flow"></div>
      </div>
      <div class="chart-panel">
        <div class="chart-panel-header">
          <div class="chart-panel-title">Avg Transaction Size</div>
        </div>
        <div id="chart-avg-tx"></div>
      </div>
    </div>
    <div class="chart-grid-2col">
      <div class="chart-panel">
        <div class="chart-panel-header">
          <div class="chart-panel-title">Volume Share</div>
        </div>
        <div id="chart-donut"></div>
      </div>
      <div class="chart-panel">
        <div class="chart-panel-header">
          <div class="chart-panel-title">Transaction Share</div>
        </div>
        <div id="chart-tx-donut"></div>
      </div>
    </div>
  </section>

  <section class="table-section">
    <div class="table-header">
      <div class="section-title">Breakdown</div>
    </div>
    <div class="table-panel">
      <div id="data-table"></div>
      <div id="table-pagination" class="page-controls"></div>
    </div>
  </section>

  <footer>
    Atlas Dashboard
  </footer>
</div>

<script>
let DATA = null;

let currentPeriod = 'month';
let chartMode = 'area';

// ─── Utilities ───

function parseTS(ts) {
  if (!ts) return null;
  // Unix seconds (< 1e12) vs milliseconds or ISO string
  const v = typeof ts === 'number' && ts < 1e12 ? ts * 1000 : ts;
  const d = new Date(v);
  return isNaN(d.getTime()) ? null : d;
}

function periodKey(date, period) {
  if (!date) return 'unknown';
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, '0');
  const d = String(date.getDate()).padStart(2, '0');
  switch (period) {
    case 'quarter': return `${y}-Q${Math.ceil((date.getMonth()+1)/3)}`;
    case 'month': return `${y}-${m}`;
    case 'week':
      const thu = new Date(date);
      thu.setDate(thu.getDate() + 3 - ((thu.getDay() + 6) % 7));
      const jan4 = new Date(thu.getFullYear(), 0, 4);
      const week = 1 + Math.round(((thu - jan4) / 86400000 - 3 + ((jan4.getDay() + 6) % 7)) / 7);
      return `${thu.getFullYear()}-W${String(week).padStart(2,'0')}`;
    case 'day': return `${y}-${m}-${d}`;
    default: return `${y}-${m}`;
  }
}

function groupBy(events, period) {
  const groups = {};
  for (const e of events) {
    const d = parseTS(e.timestamp);
    const key = periodKey(d, period);
    if (!groups[key]) groups[key] = [];
    groups[key].push(e);
  }
  return groups;
}

function sumField(events, field) {
  return events.reduce((s, e) => s + (e[field] || 0), 0);
}

function fmt(n, decimals = 4) {
  if (n >= 1000) return n.toLocaleString(undefined, { maximumFractionDigits: decimals });
  return n.toFixed(decimals);
}

function fmtCompact(n) {
  if (Math.abs(n) >= 1000) return (n / 1000).toFixed(1) + 'k';
  if (Math.abs(n) >= 1) return n.toFixed(2);
  if (Math.abs(n) >= 0.01) return n.toFixed(4);
  return n.toFixed(6);
}

function fmtRBTC(n) {
  if (Math.abs(n) >= 100) return n.toFixed(1);
  if (Math.abs(n) >= 1) return n.toFixed(2);
  return n.toFixed(4);
}

function shortHash(h) {
  if (!h) return '';
  return h.slice(0, 10) + '...' + h.slice(-6);
}

function periodLabel() {
  return { day: 'day', week: 'week', month: 'month', quarter: 'quarter' }[currentPeriod] || 'period';
}

const MONTH_NAMES = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
function fmtPeriodKey(key) {
  // "2025-04" → "Apr 2025", "2025-Q2" → "Q2 2025", "2025-W08" → "W08 2025", "2025-04-10" → "Apr 10, 2025"
  const mMatch = key.match(/^(\d{4})-(\d{2})$/);
  if (mMatch) return MONTH_NAMES[parseInt(mMatch[2], 10) - 1] + ' ' + mMatch[1];
  const qMatch = key.match(/^(\d{4})-(Q\d)$/);
  if (qMatch) return qMatch[2] + ' ' + qMatch[1];
  const wMatch = key.match(/^(\d{4})-(W\d{2})$/);
  if (wMatch) return wMatch[2] + ' ' + wMatch[1];
  const dMatch = key.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (dMatch) return MONTH_NAMES[parseInt(dMatch[2], 10) - 1] + ' ' + parseInt(dMatch[3], 10) + ', ' + dMatch[1];
  return key;
}

function getLatestTwo(events, period) {
  const groups = groupBy(events, period);
  const keys = Object.keys(groups).filter(k => k !== 'unknown').sort();
  if (keys.length === 0) return { current: [], previous: [], currentKey: '', prevKey: '' };
  const currentKey = keys[keys.length - 1];
  const prevKey = keys.length > 1 ? keys[keys.length - 2] : '';
  return {
    current: groups[currentKey] || [],
    previous: prevKey ? (groups[prevKey] || []) : [],
    currentKey,
    prevKey,
  };
}

// ─── BTC Locked ───

function renderBtcLocked() {
  const section = document.getElementById('btc-locked-section');
  const d = DATA.btc_locked;
  if (!d || !d.total_bridged_rbtc) {
    section.style.display = 'none';
    return;
  }
  section.style.display = '';
  const pct = d.pct_locked || 0;
  section.innerHTML = `
    <div class="btc-locked-panel">
      <div class="btc-locked-header">BTC Bridged &amp; Locked</div>
      <div class="btc-locked-stats">
        <div class="btc-locked-stat">
          <div class="btc-locked-stat-label">BTC Bridged</div>
          <div class="btc-locked-stat-value">${fmtRBTC(d.total_bridged_rbtc)}</div>
          <div class="btc-locked-stat-sub">Total in PowPeg</div>
        </div>
        <div class="btc-locked-stat">
          <div class="btc-locked-stat-label">Locked in Contracts</div>
          <div class="btc-locked-stat-value">${fmtRBTC(d.locked_in_contracts_rbtc)}</div>
          <div class="btc-locked-stat-sub">${d.contract_count} contracts</div>
        </div>
        <div class="btc-locked-stat">
          <div class="btc-locked-stat-label">% Bridged &amp; Locked</div>
          <div class="btc-locked-stat-value">${pct.toFixed(1)}%</div>
          <div class="btc-locked-stat-sub">held by smart contracts</div>
        </div>
      </div>
      <div class="btc-locked-bar-wrapper">
        <span class="btc-locked-bar-pct">${pct.toFixed(1)}%</span>
        <div class="btc-locked-bar">
          <div class="btc-locked-bar-fill" style="width:${Math.min(100, pct)}%"></div>
        </div>
        <div class="btc-locked-bar-labels">
          <span>0%</span>
          <span>100%</span>
        </div>
      </div>
    </div>
  `;
}

// ─── Unique Wallets ───

const LP_ADDRESS = '0x82a06ebdb97776a2da4041df8f2b2ea8d3257852';

function getUserAddress(event) {
  return (event.address || '').toLowerCase();
}

function isUserAddress(addr) {
  return addr && addr !== LP_ADDRESS;
}

function computeWalletStats() {
  const periods = ['day', 'week', 'month', 'quarter'];
  const flyoverEvents = [...DATA.flyover_pegins, ...DATA.flyover_pegouts];
  const powpegEvents = [...DATA.powpeg_pegins, ...DATA.powpeg_pegouts];
  const stats = {};

  for (const period of periods) {
    const flyoverGroups = {};
    const powpegGroups = {};
    const combinedGroups = {};

    for (const e of flyoverEvents) {
      const addr = getUserAddress(e);
      if (!isUserAddress(addr)) continue;
      const d = parseTS(e.timestamp);
      const key = periodKey(d, period);
      if (key === 'unknown') continue;
      if (!flyoverGroups[key]) flyoverGroups[key] = new Set();
      if (!combinedGroups[key]) combinedGroups[key] = new Set();
      flyoverGroups[key].add(addr);
      combinedGroups[key].add(addr);
    }

    for (const e of powpegEvents) {
      const addr = getUserAddress(e);
      if (!isUserAddress(addr)) continue;
      const d = parseTS(e.timestamp);
      const key = periodKey(d, period);
      if (key === 'unknown') continue;
      if (!powpegGroups[key]) powpegGroups[key] = new Set();
      if (!combinedGroups[key]) combinedGroups[key] = new Set();
      powpegGroups[key].add(addr);
      combinedGroups[key].add(addr);
    }

    const fKeys = Object.keys(flyoverGroups);
    const pKeys = Object.keys(powpegGroups);
    const cKeys = Object.keys(combinedGroups);

    const avgF = fKeys.length > 0
      ? fKeys.reduce((s, k) => s + flyoverGroups[k].size, 0) / fKeys.length : 0;
    const avgP = pKeys.length > 0
      ? pKeys.reduce((s, k) => s + powpegGroups[k].size, 0) / pKeys.length : 0;

    const allF = new Set();
    const allP = new Set();
    for (const k of fKeys) flyoverGroups[k].forEach(a => allF.add(a));
    for (const k of pKeys) powpegGroups[k].forEach(a => allP.add(a));
    const allC = new Set([...allF, ...allP]);

    stats[period] = {
      avgFlyover: Math.round(avgF),
      avgPowpeg: Math.round(avgP),
      avgCombined: cKeys.length > 0
        ? Math.round(cKeys.reduce((s, k) => s + combinedGroups[k].size, 0) / cKeys.length) : 0,
      totalFlyover: allF.size,
      totalPowpeg: allP.size,
      totalCombined: allC.size,
    };
  }
  return stats;
}

function computeRepeatWallets() {
  const flyoverAddrs = {};
  const powpegAddrs = {};
  const combinedAddrs = {};

  for (const e of [...DATA.flyover_pegins, ...DATA.flyover_pegouts]) {
    const addr = getUserAddress(e);
    if (!isUserAddress(addr)) continue;
    flyoverAddrs[addr] = (flyoverAddrs[addr] || 0) + 1;
    combinedAddrs[addr] = (combinedAddrs[addr] || 0) + 1;
  }

  for (const e of [...DATA.powpeg_pegins, ...DATA.powpeg_pegouts]) {
    const addr = getUserAddress(e);
    if (!isUserAddress(addr)) continue;
    powpegAddrs[addr] = (powpegAddrs[addr] || 0) + 1;
    combinedAddrs[addr] = (combinedAddrs[addr] || 0) + 1;
  }

  function countRepeat(addrMap) {
    const total = Object.keys(addrMap).length;
    const repeat = Object.values(addrMap).filter(c => c > 1).length;
    return { total, repeat, pct: total > 0 ? (repeat / total * 100) : 0 };
  }

  // Cross-protocol: wallets that appear in both flyover and powpeg
  const flyoverSet = new Set(Object.keys(flyoverAddrs));
  const powpegSet = new Set(Object.keys(powpegAddrs));
  let crossProtocol = 0;
  for (const addr of flyoverSet) {
    if (powpegSet.has(addr)) crossProtocol++;
  }

  return {
    flyover: countRepeat(flyoverAddrs),
    powpeg: countRepeat(powpegAddrs),
    combined: countRepeat(combinedAddrs),
    crossProtocol,
  };
}

function renderWallets() {
  const el = document.getElementById('wallets-content');
  const stats = computeWalletStats();
  const repeat = computeRepeatWallets();

  const rows = [
    { label: 'Daily Avg', key: 'day' },
    { label: 'Weekly Avg', key: 'week' },
    { label: 'Monthly Avg', key: 'month' },
    { label: 'Quarterly Avg', key: 'quarter' },
  ];

  let html = `<table class="wallets-table">
    <thead>
      <tr>
        <th>Period</th>
        <th><span class="col-flyover">Flyover</span></th>
        <th><span class="col-powpeg">PowPeg</span></th>
        <th><span class="col-combined">Combined</span></th>
      </tr>
    </thead><tbody>`;

  for (const row of rows) {
    const s = stats[row.key];
    html += '<tr>' +
      '<td>' + row.label + '</td>' +
      '<td class="wallet-num col-flyover">~' + s.avgFlyover + '</td>' +
      '<td class="wallet-num col-powpeg">~' + s.avgPowpeg + '</td>' +
      '<td class="wallet-num col-combined">~' + s.avgCombined + '</td>' +
    '</tr>';
  }

  // Find earliest event date for the "since" label
  let earliest = null;
  const allEvts = [...DATA.flyover_pegins, ...DATA.flyover_pegouts, ...DATA.powpeg_pegins, ...DATA.powpeg_pegouts];
  for (const e of allEvts) {
    const d = parseTS(e.timestamp);
    if (d && (!earliest || d < earliest)) earliest = d;
  }
  const sinceLabel = earliest
    ? 'Since ' + earliest.toLocaleDateString('en-US', { month: 'short', year: 'numeric' })
    : 'Cumulative';

  // Section subheader
  html += '<tr><td colspan="4" style="text-align:center;color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:0.8px;padding:14px 0 6px;border-bottom:none;background:var(--surface)">' + sinceLabel + '</td></tr>';

  // Unique wallets row
  html += '<tr>' +
    '<td>Unique Wallets</td>' +
    '<td class="wallet-num col-flyover">' + repeat.flyover.total + '</td>' +
    '<td class="wallet-num col-powpeg">' + repeat.powpeg.total + '</td>' +
    '<td class="wallet-num col-combined">' + repeat.combined.total + '</td>' +
  '</tr>';

  // Repeat wallets row
  html += '<tr style="background:rgba(158,117,255,0.04)">' +
    '<td>Repeat Wallets<div style="color:var(--muted);font-size:10px;font-weight:400;margin-top:2px">bridged 2+ times</div></td>' +
    '<td class="wallet-num col-flyover">' + repeat.flyover.repeat + ' <span style="font-size:12px;font-weight:600">(' + repeat.flyover.pct.toFixed(0) + '%)</span></td>' +
    '<td class="wallet-num col-powpeg">' + repeat.powpeg.repeat + ' <span style="font-size:12px;font-weight:600">(' + repeat.powpeg.pct.toFixed(0) + '%)</span></td>' +
    '<td class="wallet-num col-combined">' + repeat.combined.repeat + ' <span style="font-size:12px;font-weight:600">(' + repeat.combined.pct.toFixed(0) + '%)</span></td>' +
  '</tr>';

  html += '</tbody></table>';

  if (repeat.crossProtocol > 0) {
    html += '<div style="color:var(--muted);font-size:11px;margin-top:10px;text-align:right">' +
      repeat.crossProtocol + ' wallet' + (repeat.crossProtocol !== 1 ? 's' : '') + ' used both Flyover and PowPeg' +
    '</div>';
  }

  el.innerHTML = html;
}

// ─── Render ───

function deltaText(current, previous) {
  if (previous === 0 && current === 0) return '<span class="op-metric-delta neutral">&ndash;</span>';
  if (previous === 0) return '<span class="op-metric-delta up">&uarr; new</span>';
  const pct = ((current - previous) / Math.abs(previous) * 100).toFixed(0);
  if (current > previous) return `<span class="op-metric-delta up">&uarr; ${Math.abs(pct)}%</span>`;
  if (current < previous) return `<span class="op-metric-delta down">&darr; ${Math.abs(pct)}%</span>`;
  return '<span class="op-metric-delta neutral">&ndash; 0%</span>';
}

function renderSummary() {
  const ops = [
    { name: 'Flyover Peg-In', data: DATA.flyover_pegins, color: '#DEFF19', field: 'value_rbtc' },
    { name: 'Flyover Peg-Out', data: DATA.flyover_pegouts, color: '#F0FF96', field: 'value_rbtc' },
    { name: 'PowPeg Peg-In', data: DATA.powpeg_pegins, color: '#FF9100', field: 'value_rbtc' },
    { name: 'PowPeg Peg-Out', data: DATA.powpeg_pegouts, color: '#FED8A7', field: 'value_rbtc' },
  ];

  const period = currentPeriod;
  let totalTxs = 0, totalVol = 0;

  let cards = '';
  for (const op of ops) {
    const latest = getLatestTwo(op.data, period);
    const curTxs = latest.current.length;
    const prevTxs = latest.previous.length;
    const curVol = sumField(latest.current, op.field);
    const prevVol = sumField(latest.previous, op.field);
    totalTxs += curTxs;
    totalVol += curVol;

    cards += `
      <div class="op-card" style="border-top-color:${op.color}">
        <div class="op-card-name" style="color:${op.color}">${op.name}</div>
        <div class="op-card-metrics">
          <div>
            <div class="op-metric-label">Transactions</div>
            <div class="op-metric-value">${curTxs}</div>
            ${deltaText(curTxs, prevTxs)}
          </div>
          <div>
            <div class="op-metric-label">Volume</div>
            <div class="op-metric-value">${fmtRBTC(curVol)}</div>
            ${deltaText(curVol, prevVol)}
          </div>
        </div>
      </div>`;
  }

  const el = document.getElementById('op-summary');
  el.innerHTML = cards;
}

function renderCharts() {
  const period = currentPeriod;
  const cfg = { displayModeBar: false, responsive: true };
  const hoverLabel = {
    bgcolor: '#1a1a1a', bordercolor: '#2a2a2a',
    font: { family: 'Inter, sans-serif', color: '#FAFAF5', size: 12 }
  };
  const baseLayout = {
    paper_bgcolor: 'transparent',
    plot_bgcolor: 'transparent',
    font: { family: 'Inter, sans-serif', color: '#737373', size: 11 },
    margin: { l: 50, r: 16, t: 8, b: 36 },
    xaxis: { gridcolor: '#1a1a1a', linecolor: '#1e1e1e', zeroline: false },
    yaxis: { gridcolor: '#1a1a1a', linecolor: '#1e1e1e', zeroline: false },
    legend: { orientation: 'h', y: -0.2, font: { size: 10, color: '#737373' } },
    hoverlabel: hoverLabel,
    height: 280,
  };

  const fpG = groupBy(DATA.flyover_pegins, period);
  const foG = groupBy(DATA.flyover_pegouts, period);
  const ppG = groupBy(DATA.powpeg_pegins, period);
  const poG = groupBy(DATA.powpeg_pegouts, period);

  const keys = [...new Set([
    ...Object.keys(fpG), ...Object.keys(foG),
    ...Object.keys(ppG), ...Object.keys(poG),
  ])].filter(k => k !== 'unknown').sort();

  // Volume chart — toggle between area and bar
  const mkHover = (label, rbtcArr) => rbtcArr.map(v =>
    `${label}: ${fmtRBTC(v)}`);

  const fpV = keys.map(k => sumField(fpG[k] || [], 'value_rbtc'));
  const foV = keys.map(k => sumField(foG[k] || [], 'value_rbtc'));
  const ppV = keys.map(k => sumField(ppG[k] || [], 'value_rbtc'));
  const poV = keys.map(k => sumField(poG[k] || [], 'value_rbtc'));

  const volTraces = chartMode === 'area' ? [
    { x: keys, y: fpV, name: 'Flyover In', type: 'scatter', stackgroup: 'vol',
       fillcolor: 'rgba(222,255,25,0.3)', line: { color: '#DEFF19', width: 1.5 },
       text: mkHover('Flyover In', fpV), hoverinfo: 'text' },
    { x: keys, y: foV, name: 'Flyover Out', type: 'scatter', stackgroup: 'vol',
       fillcolor: 'rgba(240,255,150,0.25)', line: { color: '#F0FF96', width: 1.5 },
       text: mkHover('Flyover Out', foV), hoverinfo: 'text' },
    { x: keys, y: ppV, name: 'PowPeg In', type: 'scatter', stackgroup: 'vol',
       fillcolor: 'rgba(255,145,0,0.3)', line: { color: '#FF9100', width: 1.5 },
       text: mkHover('PowPeg In', ppV), hoverinfo: 'text' },
    { x: keys, y: poV, name: 'PowPeg Out', type: 'scatter', stackgroup: 'vol',
       fillcolor: 'rgba(254,216,167,0.25)', line: { color: '#FED8A7', width: 1.5 },
       text: mkHover('PowPeg Out', poV), hoverinfo: 'text' },
  ] : [
    { x: keys, y: fpV, name: 'Flyover In', type: 'bar',
       marker: { color: '#DEFF19', line: { width: 0 } }, textposition: 'none',
       hovertext: mkHover('Flyover In', fpV), hoverinfo: 'text' },
    { x: keys, y: foV, name: 'Flyover Out', type: 'bar',
       marker: { color: '#F0FF96', line: { width: 0 } }, textposition: 'none',
       hovertext: mkHover('Flyover Out', foV), hoverinfo: 'text' },
    { x: keys, y: ppV, name: 'PowPeg In', type: 'bar',
       marker: { color: '#FF9100', line: { width: 0 } }, textposition: 'none',
       hovertext: mkHover('PowPeg In', ppV), hoverinfo: 'text' },
    { x: keys, y: poV, name: 'PowPeg Out', type: 'bar',
       marker: { color: '#FED8A7', line: { width: 0 } }, textposition: 'none',
       hovertext: mkHover('PowPeg Out', poV), hoverinfo: 'text' },
  ];
  const volLayout = {
    ...baseLayout,
    height: 300,
    ...(chartMode === 'bar' ? { barmode: 'stack' } : {}),
    hovermode: 'x unified',
  };
  Plotly.newPlot('chart-volume-trend', volTraces, volLayout, cfg);

  // Volume donut — filtered by period
  const latestFp = getLatestTwo(DATA.flyover_pegins, period);
  const latestFo = getLatestTwo(DATA.flyover_pegouts, period);
  const latestPp = getLatestTwo(DATA.powpeg_pegins, period);
  const latestPo = getLatestTwo(DATA.powpeg_pegouts, period);

  const fpVol = sumField(latestFp.current, 'value_rbtc');
  const foVol = sumField(latestFo.current, 'value_rbtc');
  const ppVol = sumField(latestPp.current, 'value_rbtc');
  const poVol = sumField(latestPo.current, 'value_rbtc');
  const total = fpVol + foVol + ppVol + poVol;

  const donutLayout = {
    ...baseLayout,
    margin: { l: 10, r: 10, t: 10, b: 40 },
    showlegend: true,
    legend: { orientation: 'h', x: 0.5, xanchor: 'center', y: -0.1, font: { size: 10, color: '#737373' } },
    uniformtext: { minsize: 10, mode: 'hide' },
  };

  Plotly.newPlot('chart-donut', [{
    values: [fpVol, foVol, ppVol, poVol],
    labels: ['Flyover In', 'Flyover Out', 'PowPeg In', 'PowPeg Out'],
    type: 'pie',
    hole: 0.6,
    domain: { x: [0.1, 0.9], y: [0.05, 0.95] },
    marker: { colors: ['#DEFF19', '#F0FF96', '#FF9100', '#FED8A7'] },
    textinfo: 'percent',
    textposition: 'inside',
    insidetextorientation: 'horizontal',
    textfont: { color: '#000', size: 11, family: 'Inter, sans-serif' },
    text: [fpVol, foVol, ppVol, poVol].map(v => `${fmtRBTC(v)}`),
    hovertemplate: '%{label}<br>%{text}<br>%{percent}<extra></extra>',
    sort: false,
  }], {
    ...donutLayout,
    annotations: [{
      text: `${fmtCompact(total)}<br><span style="font-size:11px;color:#737373">total</span>`,
      showarrow: false,
      font: { size: 18, color: '#FAFAF5', family: 'Inter, sans-serif' },
      x: 0.5, y: 0.5,
    }],
  }, cfg);

  // Transaction count donut — filtered by period
  const fpTx = latestFp.current.length;
  const foTx = latestFo.current.length;
  const ppTx = latestPp.current.length;
  const poTx = latestPo.current.length;
  const totalTx = fpTx + foTx + ppTx + poTx;

  Plotly.newPlot('chart-tx-donut', [{
    values: [fpTx, foTx, ppTx, poTx],
    labels: ['Flyover In', 'Flyover Out', 'PowPeg In', 'PowPeg Out'],
    type: 'pie',
    hole: 0.6,
    domain: { x: [0.1, 0.9], y: [0.05, 0.95] },
    marker: { colors: ['#DEFF19', '#F0FF96', '#FF9100', '#FED8A7'] },
    textinfo: 'percent',
    textposition: 'inside',
    insidetextorientation: 'horizontal',
    textfont: { color: '#000', size: 11, family: 'Inter, sans-serif' },
    text: [fpTx, foTx, ppTx, poTx].map(v => `${v} txs`),
    hovertemplate: '%{label}<br>%{text}<br>%{percent}<extra></extra>',
    sort: false,
  }], {
    ...donutLayout,
    annotations: [{
      text: `${totalTx}<br><span style="font-size:11px;color:#737373">txs</span>`,
      showarrow: false,
      font: { size: 18, color: '#FAFAF5', family: 'Inter, sans-serif' },
      x: 0.5, y: 0.5,
    }],
  }, cfg);

  // --- Net Flow chart (Peg-In minus Peg-Out) ---
  const flyoverNet = keys.map(k =>
    sumField(fpG[k] || [], 'value_rbtc') - sumField(foG[k] || [], 'value_rbtc'));
  const powpegNet = keys.map(k =>
    sumField(ppG[k] || [], 'value_rbtc') - sumField(poG[k] || [], 'value_rbtc'));

  Plotly.newPlot('chart-net-flow', [
    { x: keys, y: flyoverNet, name: 'Flyover', type: 'bar',
      marker: { color: flyoverNet.map(v => v >= 0 ? '#DEFF19' : 'rgba(222,255,25,0.35)') },
      hovertext: flyoverNet.map(v => 'Flyover: ' + (v >= 0 ? '+' : '') + fmtRBTC(v) + ' RBTC'),
      hoverinfo: 'text' },
    { x: keys, y: powpegNet, name: 'PowPeg', type: 'bar',
      marker: { color: powpegNet.map(v => v >= 0 ? '#FF9100' : 'rgba(255,145,0,0.35)') },
      hovertext: powpegNet.map(v => 'PowPeg: ' + (v >= 0 ? '+' : '') + fmtRBTC(v) + ' RBTC'),
      hoverinfo: 'text' },
  ], {
    ...baseLayout,
    barmode: 'group',
    shapes: [{ type: 'line', x0: 0, x1: 1, xref: 'paper', y0: 0, y1: 0,
      line: { color: '#737373', width: 1, dash: 'dot' } }],
  }, cfg);

  // --- Avg Transaction Size chart ---
  const flyoverAvg = keys.map(k => {
    const evts = [...(fpG[k] || []), ...(foG[k] || [])];
    return evts.length > 0 ? sumField(evts, 'value_rbtc') / evts.length : 0;
  });
  const powpegAvg = keys.map(k => {
    const evts = [...(ppG[k] || []), ...(poG[k] || [])];
    return evts.length > 0 ? sumField(evts, 'value_rbtc') / evts.length : 0;
  });

  Plotly.newPlot('chart-avg-tx', [
    { x: keys, y: flyoverAvg, name: 'Flyover', type: 'scatter', mode: 'lines+markers',
      line: { color: '#DEFF19', width: 2 }, marker: { size: 5, color: '#DEFF19' },
      hovertext: flyoverAvg.map(v => 'Flyover avg: ' + fmtRBTC(v)),
      hoverinfo: 'text' },
    { x: keys, y: powpegAvg, name: 'PowPeg', type: 'scatter', mode: 'lines+markers',
      line: { color: '#FF9100', width: 2 }, marker: { size: 5, color: '#FF9100' },
      hovertext: powpegAvg.map(v => 'PowPeg avg: ' + fmtRBTC(v)),
      hoverinfo: 'text' },
  ], {
    ...baseLayout,
  }, cfg);
}

let tablePage = 0;
const PAGE_SIZE = 15;

function renderTable() {
  const fpG = groupBy(DATA.flyover_pegins, currentPeriod);
  const foG = groupBy(DATA.flyover_pegouts, currentPeriod);
  const ppG = groupBy(DATA.powpeg_pegins, currentPeriod);
  const poG = groupBy(DATA.powpeg_pegouts, currentPeriod);

  const allKeys = [...new Set([
    ...Object.keys(fpG), ...Object.keys(foG),
    ...Object.keys(ppG), ...Object.keys(poG),
  ])].filter(k => k !== 'unknown').sort().reverse();

  const totalPages = Math.max(1, Math.ceil(allKeys.length / PAGE_SIZE));
  tablePage = Math.max(0, Math.min(tablePage, totalPages - 1));
  const pageKeys = allKeys.slice(tablePage * PAGE_SIZE, (tablePage + 1) * PAGE_SIZE);

  // Compute totals across ALL keys (not just current page)
  let totFpTx = 0, totFpVol = 0, totFoTx = 0, totFoVol = 0;
  let totPpTx = 0, totPpVol = 0, totPoTx = 0, totPoVol = 0;
  for (const key of allKeys) {
    const fp = fpG[key] || [], fo = foG[key] || [];
    const pp = ppG[key] || [], po = poG[key] || [];
    totFpTx += fp.length; totFpVol += sumField(fp, 'value_rbtc');
    totFoTx += fo.length; totFoVol += sumField(fo, 'value_rbtc');
    totPpTx += pp.length; totPpVol += sumField(pp, 'value_rbtc');
    totPoTx += po.length; totPoVol += sumField(po, 'value_rbtc');
  }

  let html = `<table>
    <thead>
      <tr>
        <th rowspan="2">Period</th>
        <th colspan="2" class="th-group" style="color:#DEFF19">Flyover Peg-In</th>
        <th colspan="2" class="th-group" style="color:#F0FF96">Flyover Peg-Out</th>
        <th colspan="2" class="th-group" style="color:#FF9100">PowPeg Peg-In</th>
        <th colspan="2" class="th-group" style="color:#FED8A7">PowPeg Peg-Out</th>
      </tr>
      <tr>
        <th>Txs</th><th>Vol</th>
        <th>Txs</th><th>Vol</th>
        <th>Txs</th><th>Vol</th>
        <th>Txs</th><th>Vol</th>
      </tr>
    </thead><tbody>`;

  for (const key of pageKeys) {
    const fp = fpG[key] || [], fo = foG[key] || [];
    const pp = ppG[key] || [], po = poG[key] || [];

    html += `<tr>
      <td><strong>${fmtPeriodKey(key)}</strong></td>
      <td>${fp.length}</td><td>${fmtRBTC(sumField(fp, 'value_rbtc'))}</td>
      <td>${fo.length}</td><td>${fmtRBTC(sumField(fo, 'value_rbtc'))}</td>
      <td>${pp.length}</td><td>${fmtRBTC(sumField(pp, 'value_rbtc'))}</td>
      <td>${po.length}</td><td>${fmtRBTC(sumField(po, 'value_rbtc'))}</td>
    </tr>`;
  }

  // Totals row
  html += `<tr class="totals-row">
    <td>Total</td>
    <td>${totFpTx}</td><td>${fmtRBTC(totFpVol)}</td>
    <td>${totFoTx}</td><td>${fmtRBTC(totFoVol)}</td>
    <td>${totPpTx}</td><td>${fmtRBTC(totPpVol)}</td>
    <td>${totPoTx}</td><td>${fmtRBTC(totPoVol)}</td>
  </tr>`;

  html += '</tbody></table>';
  document.getElementById('data-table').innerHTML = html;

  const pag = document.getElementById('table-pagination');
  if (totalPages <= 1) { pag.innerHTML = ''; return; }
  pag.innerHTML = `
    <button class="page-btn" onclick="tableNav(-1)" ${tablePage === 0 ? 'disabled' : ''}>&larr; Prev</button>
    <span class="page-info">${tablePage + 1} / ${totalPages}</span>
    <button class="page-btn" onclick="tableNav(1)" ${tablePage >= totalPages - 1 ? 'disabled' : ''}>Next &rarr;</button>
  `;
}

function tableNav(dir) { tablePage += dir; renderTable(); }


// ─── Flyover Liquidity Provider (merged Health + LP) ───

const HEALTH_THRESHOLDS = {
  peginBalance:  { warning: 10, critical: 5 },
  pegoutBalance: { warning: 10, critical: 5 },
  btcUtxos:      { warning: 4, critical: 2 },
};
const STALENESS_HOURS = 25;

function assessStatus(value, thresholds) {
  if (value <= thresholds.critical) return 'critical';
  if (value <= thresholds.warning) return 'warning';
  return 'healthy';
}

function renderHealth() {
  const panel = document.getElementById('health-panel');
  const wrapper = document.getElementById('health-section-wrapper');
  const lp = DATA.lp_info || {};
  const refTime = new Date(DATA.generated_at);

  if (!lp.lp_name && DATA.flyover_pegins.length === 0) {
    wrapper.style.display = 'none';
    return;
  }
  wrapper.style.display = '';

  // --- LP performance stats ---
  const lpData = {};
  for (const e of DATA.flyover_pegins) {
    const addr = (e.lp_address || '').toLowerCase();
    if (!addr) continue;
    if (!lpData[addr]) lpData[addr] = { pegins: 0, peginVol: 0, penalties: 0 };
    lpData[addr].pegins++;
    lpData[addr].peginVol += e.value_rbtc || 0;
  }
  for (const e of DATA.penalties) {
    const addr = (e.lp_address || '').toLowerCase();
    if (!addr) continue;
    if (!lpData[addr]) lpData[addr] = { pegins: 0, peginVol: 0, penalties: 0 };
    lpData[addr].penalties++;
  }
  const topLP = Object.entries(lpData).sort((a,b) => b[1].peginVol - a[1].peginVol)[0];
  const lpName = (lp && lp.lp_name) ? lp.lp_name : (topLP ? shortHash(topLP[0]) : 'Unknown');
  const peginDeliveries = topLP ? topLP[1].pegins : 0;
  const pegoutDeliveries = DATA.flyover_pegouts.length;
  const penaltyCount = topLP ? topLP[1].penalties : 0;

  // --- Balances (LPS API = actual available liquidity; on-chain = wallet only) ---
  const peginOnChain = lp.pegin_rbtc != null ? parseFloat(lp.pegin_rbtc) : null;
  const peginBal = lp.lps_pegin_rbtc != null ? parseFloat(lp.lps_pegin_rbtc) : peginOnChain;
  const peginStatus = peginBal != null
    ? assessStatus(peginBal, HEALTH_THRESHOLDS.peginBalance)
    : 'warning';

  const pegoutOnChain = lp.pegout_btc != null ? parseFloat(lp.pegout_btc) : null;
  const pegoutBal = lp.lps_pegout_btc != null ? parseFloat(lp.lps_pegout_btc) : pegoutOnChain;
  const pegoutStatus = pegoutBal != null
    ? assessStatus(pegoutBal, HEALTH_THRESHOLDS.pegoutBalance)
    : 'warning';

  // --- Mempool (pending BTC txs) ---
  const mempoolTxCount = lp.btc_mempool_tx_count != null ? parseInt(lp.btc_mempool_tx_count) : null;

  // --- Last activity ---
  let lastPeginDate = null;
  let lastPeginValue = 0;
  for (const e of DATA.flyover_pegins) {
    const d = parseTS(e.timestamp);
    if (d && (!lastPeginDate || d > lastPeginDate)) {
      lastPeginDate = d;
      lastPeginValue = e.value_rbtc || 0;
    }
  }
  const now = Date.now();
  const peginHoursAgo = lastPeginDate
    ? (now - lastPeginDate.getTime()) / (1000 * 60 * 60)
    : Infinity;

  let lastPegoutDate = null;
  let lastPegoutValue = 0;
  for (const e of DATA.flyover_pegouts) {
    const d = parseTS(e.timestamp);
    if (d && (!lastPegoutDate || d > lastPegoutDate)) {
      lastPegoutDate = d;
      lastPegoutValue = e.value_rbtc || 0;
    }
  }
  const pegoutHoursAgo = lastPegoutDate
    ? (now - lastPegoutDate.getTime()) / (1000 * 60 * 60)
    : Infinity;

  // --- BTC UTXOs ---
  const utxoCount = lp.btc_utxo_count != null ? parseInt(lp.btc_utxo_count) : null;
  const utxoStatus = utxoCount != null
    ? (utxoCount < HEALTH_THRESHOLDS.btcUtxos.critical ? 'critical'
       : utxoCount < HEALTH_THRESHOLDS.btcUtxos.warning ? 'warning' : 'healthy')
    : 'warning';

  // --- Overall status ---
  const statuses = [peginStatus, pegoutStatus, utxoStatus];
  let overall = 'healthy';
  if (statuses.includes('critical')) overall = 'critical';
  else if (statuses.includes('warning')) overall = 'warning';

  const statusColors = { healthy: 'var(--green)', warning: '#EAB308', critical: 'var(--red)' };
  const statusLabels = { healthy: 'Healthy', warning: 'Warning', critical: 'Critical' };

  const dataAgeHours = (Date.now() - refTime.getTime()) / (1000 * 60 * 60);
  const isStale = dataAgeHours > STALENESS_HOURS;

  function hoursLabel(hours) {
    if (!isFinite(hours)) return 'No data';
    if (hours < 1) return 'Just now';
    if (hours < 24) return Math.round(hours) + 'h ago';
    const totalHours = Math.round(hours);
    const days = Math.floor(totalHours / 24);
    const rem = totalHours % 24;
    return days + 'd ' + rem + 'h ago';
  }

  function balanceBar(value, max, status) {
    if (value == null) return '';
    const pct = Math.min(100, Math.max(0, (value / max) * 100));
    return '<div class="health-bar"><div class="health-bar-fill" style="width:' + pct + '%;background:' + statusColors[status] + '"></div></div>';
  }

  const lpLive = lp._live;
  const liveUpdatedAt = lp._updated_at ? new Date(lp._updated_at) : null;
  const liveAgoSec = liveUpdatedAt ? Math.round((Date.now() - liveUpdatedAt.getTime()) / 1000) : null;
  const liveAgoLabel = liveAgoSec != null
    ? (liveAgoSec < 5 ? 'just now'
       : liveAgoSec < 120 ? liveAgoSec + 's ago'
       : Math.round(liveAgoSec / 60) + 'm ago')
    : '';

  let html = '<div class="health-header">' +
    '<h3>' +
      '<span class="health-overall-dot ' + (overall !== 'healthy' ? 'pulse' : '') + '" style="background:' + statusColors[overall] + '"></span>' +
      '<span class="health-overall-label" style="color:' + statusColors[overall] + '">' + statusLabels[overall] + '</span>' +
      '<span class="lp-name" style="margin-left:12px">' + lpName + '</span>' +
      (lpLive ? '<span class="live-badge">LIVE</span>' : '') +
    '</h3>' +
    (lpLive
      ? '<span class="health-updated">Updated ' + liveAgoLabel + '</span>'
      : (isStale ? '<span class="health-staleness">Data is ' + Math.round(dataAgeHours) + 'h old</span>' : '')) +
  '</div>' +
  '<div class="health-grid">' +
    // Row 1, Col 1: Peg-In Balance
    '<div class="health-indicator status-' + peginStatus + '">' +
      '<button class="health-info-btn" onclick="toggleHealthPopover(event)">i</button>' +
      '<div class="health-popover">' +
        '<div class="health-popover-row"><span class="health-popover-dot" style="background:#EAB308"></span> Warning: &lt; 10 RBTC</div>' +
        '<div class="health-popover-row"><span class="health-popover-dot" style="background:var(--red)"></span> Critical: &lt; 5 RBTC</div>' +
      '</div>' +
      '<div class="health-indicator-label">Peg-In Balance</div>' +
      '<div class="health-indicator-value">' + (peginBal != null ? fmtRBTC(peginBal) : 'N/A') + '</div>' +
      '<div class="health-indicator-sub">' + (peginBal != null ? 'RBTC available' : 'No LP data') + '</div>' +
      '<div class="health-indicator-status ' + peginStatus + '">' + statusLabels[peginStatus] + '</div>' +
    '</div>' +
    // Row 1, Col 2: Peg-Out Balance
    '<div class="health-indicator status-' + pegoutStatus + '">' +
      '<button class="health-info-btn" onclick="toggleHealthPopover(event)">i</button>' +
      '<div class="health-popover">' +
        '<div class="health-popover-row"><span class="health-popover-dot" style="background:#EAB308"></span> Warning: &lt; 10 BTC</div>' +
        '<div class="health-popover-row"><span class="health-popover-dot" style="background:var(--red)"></span> Critical: &lt; 5 BTC</div>' +
      '</div>' +
      '<div class="health-indicator-label">Peg-Out Balance</div>' +
      '<div class="health-indicator-value">' + (pegoutBal != null ? fmtRBTC(pegoutBal) : 'N/A') + '</div>' +
      '<div class="health-indicator-sub">' + (pegoutBal != null ? 'BTC available' : 'No LP data') + '</div>' +
      '<div class="health-indicator-status ' + pegoutStatus + '">' + statusLabels[pegoutStatus] + '</div>' +
    '</div>' +
    // Row 1, Col 3: BTC UTXOs
    '<div class="health-indicator status-' + utxoStatus + '">' +
      '<button class="health-info-btn" onclick="toggleHealthPopover(event)">i</button>' +
      '<div class="health-popover">' +
        '<div class="health-popover-row"><span class="health-popover-dot" style="background:#EAB308"></span> Warning: &lt; 4 UTXOs</div>' +
        '<div class="health-popover-row"><span class="health-popover-dot" style="background:var(--red)"></span> Critical: &lt; 2 UTXOs</div>' +
      '</div>' +
      '<div class="health-indicator-label">BTC UTXOs</div>' +
      '<div class="health-indicator-value">' + (utxoCount != null ? utxoCount : 'N/A') + '</div>' +
      '<div class="health-indicator-sub">' + (utxoCount != null ? (utxoCount === 1 ? '1 spendable output' : utxoCount + ' spendable outputs') : 'No data') + '</div>' +
      (mempoolTxCount > 0
        ? '<div class="health-indicator-sub" style="color:#EAB308">' + mempoolTxCount + ' pending tx' + (mempoolTxCount > 1 ? 's' : '') + ' in mempool</div>'
        : '') +
      '<div class="health-indicator-status ' + utxoStatus + '">' + statusLabels[utxoStatus] + '</div>' +
    '</div>' +
    // Row 2, Col 1: Last Peg-In (under Peg-In Balance)
    '<div class="health-indicator">' +
      '<div class="health-indicator-label">Last Peg-In</div>' +
      '<div class="health-indicator-value">' + hoursLabel(peginHoursAgo) + '</div>' +
      '<div class="health-indicator-sub">' + (lastPeginDate ? fmtRBTC(lastPeginValue) : 'Never') + '</div>' +
      '<div class="health-indicator-sub">' + (lastPeginDate ? lastPeginDate.toLocaleDateString('en-US', {month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}) : '') + '</div>' +
    '</div>' +
    // Row 2, Col 2: Last Peg-Out (under Peg-Out Balance)
    '<div class="health-indicator">' +
      '<div class="health-indicator-label">Last Peg-Out</div>' +
      '<div class="health-indicator-value">' + hoursLabel(pegoutHoursAgo) + '</div>' +
      '<div class="health-indicator-sub">' + (lastPegoutDate ? fmtRBTC(lastPegoutValue) : 'Never') + '</div>' +
      '<div class="health-indicator-sub">' + (lastPegoutDate ? lastPegoutDate.toLocaleDateString('en-US', {month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}) : '') + '</div>' +
    '</div>' +
    // Row 2, Col 3: Operations (Deliveries + Penalties)
    '<div class="health-indicator">' +
      '<div class="health-indicator-label">Operations</div>' +
      '<div class="health-indicator-value">' + (peginDeliveries + pegoutDeliveries) + '</div>' +
      '<div class="health-indicator-sub">' + peginDeliveries + ' peg-in · ' + pegoutDeliveries + ' peg-out</div>' +
      '<div class="health-indicator-sub">' + penaltyCount + ' penalt' + (penaltyCount === 1 ? 'y' : 'ies') + '</div>' +
    '</div>' +
  '</div>';

  panel.innerHTML = html;
}

function toggleHealthPopover(e) {
  e.stopPropagation();
  const popover = e.currentTarget.nextElementSibling;
  const wasOpen = popover.classList.contains('open');
  document.querySelectorAll('.health-popover.open').forEach(p => p.classList.remove('open'));
  if (!wasOpen) popover.classList.add('open');
}
document.addEventListener('click', () => {
  document.querySelectorAll('.health-popover.open').forEach(p => p.classList.remove('open'));
});

// ─── Route Health ───

function renderRouteHealth() {
  const section = document.getElementById('route-health-section');
  const panel = document.getElementById('route-health-panel');
  const rh = DATA.route_health;
  if (!rh || !rh.swap_api) {
    section.style.display = 'none';
    return;
  }
  section.style.display = '';

  const apiOk = rh.swap_api.status === 'operational';
  const native = rh.native_routes || {};
  const swapProviders = rh.swap_providers || {};
  const providerIds = rh.swap_provider_ids || [];
  const changes = rh.provider_changes || [];
  const limits = rh.limits_btc_rbtc;
  const history = rh.history || [];
  const MIN_HISTORY_FOR_UPTIME = 12;

  // Known providers that are expected (including upcoming)
  const knownProviders = ['BOLTZ', 'CHANGELLY', 'SYMBIOSIS', 'LIFI'];
  const enabledSet = new Set(providerIds);

  // Overall: API up + count
  const totalEnabled = providerIds.length;
  const overallOk = apiOk && totalEnabled > 0;
  const overallColor = apiOk ? '#08FFD1' : '#FF70E0';
  const overallLabel = !apiOk ? 'Swap API Down' : totalEnabled + ' provider' + (totalEnabled !== 1 ? 's' : '') + ' enabled';

  function computeUptime(key) {
    if (history.length < MIN_HISTORY_FOR_UPTIME) return null;
    let up = 0;
    for (const h of history) { if (h[key] === 'up') up++; }
    return Math.round((up / history.length) * 1000) / 10;
  }

  function row(label, value) {
    return '<div class="route-card-row"><span class="label">' + label + '</span><span class="value">' + value + '</span></div>';
  }

  let html = '';

  html += '<div class="route-health-grid">';

  // --- Native routes: PowPeg ---
  if (native.powpeg) {
    const p = native.powpeg;
    html += '<div class="route-card status-operational">' +
      '<div class="route-card-header">' +
        '<div class="route-card-name"><span class="route-status-dot operational"></span>' + p.name + '</div>' +
        '<span class="route-card-type">Native</span>' +
      '</div>' +
      '<div class="route-card-status operational">ENABLED</div>' +
      '<div class="route-card-details route-card-detail-extra">' +
        row('Speed', p.estimated_speed) +
        row('Fee', p.fee) +
        row('Direction', 'BTC \\u2194 RBTC') +
      '</div>' +
      '<div class="route-card-divider"></div>' +
      '<div class="route-asset-list">' +
        (p.tokens || []).map(function(t) { return '<span class="route-asset-tag">' + t + '</span>'; }).join('') +
      '</div>' +
    '</div>';
  }

  // --- Native routes: Flyover ---
  if (native.flyover) {
    const f = native.flyover;
    html += '<div class="route-card status-operational">' +
      '<div class="route-card-header">' +
        '<div class="route-card-name"><span class="route-status-dot operational"></span>' + f.name + '</div>' +
        '<span class="route-card-type">LP Bridge</span>' +
      '</div>' +
      '<div class="route-card-status operational">ENABLED</div>' +
      '<div class="route-card-details route-card-detail-extra">' +
        row('Speed', f.estimated_speed) +
        row('Fee', f.fee) +
        row('Direction', 'BTC \\u2194 RBTC') +
      '</div>' +
      '<div class="route-card-divider"></div>' +
      '<div class="route-asset-list">' +
        (f.tokens || []).map(function(t) { return '<span class="route-asset-tag">' + t + '</span>'; }).join('') +
      '</div>' +
    '</div>';
  }

  // --- Swap providers (from API) ---
  const netNames = { '30': 'RSK', '31': 'RSK Test', '1': 'Ethereum', '56': 'BSC', 'BTC': 'Bitcoin', 'LN': 'Lightning' };
  function fmtPair(p) {
    const fromParts = (p.from || '').split('(');
    const toParts = (p.to || '').split('(');
    const fromNetRaw = fromParts.length > 1 ? fromParts[1].replace(')', '') : '';
    const toNetRaw = toParts.length > 1 ? toParts[1].replace(')', '') : '';
    const fromNet = netNames[fromNetRaw] || fromNetRaw || '?';
    const toNet = netNames[toNetRaw] || toNetRaw || '?';
    const fromTok = (p.from_token || '?').replace(/</g, '&lt;');
    const toTok = (p.to_token || '?').replace(/</g, '&lt;');
    return fromTok + ' <span style="color:var(--muted)">(' + fromNet + ')</span> \\u2192 ' + toTok + ' <span style="color:var(--muted)">(' + toNet + ')</span>';
  }

  for (const pid of knownProviders) {
    const key = pid.toLowerCase();
    const p = swapProviders[key];
    const enabled = enabledSet.has(pid);

    if (enabled && p) {
      // Provider is live — show pairs in collapsible
      const pairId = 'route-pairs-' + key;
      const pairsHtml = (p.pairs || []).map(function(pair) {
        return '<div style="font-size:11px;line-height:1.8">' + fmtPair(pair) + '</div>';
      }).join('');

      html += '<div class="route-card status-operational">' +
        '<div class="route-card-header">' +
          '<div class="route-card-name"><span class="route-status-dot operational"></span>' + p.name + '</div>' +
          '<span class="route-card-type">Swap</span>' +
        '</div>' +
        '<div class="route-card-status operational">ENABLED</div>' +
        '<div class="route-card-details route-card-detail-extra">' +
          row('Pairs', p.pair_count + ' (' + p.inbound_pairs + ' in, ' + p.outbound_pairs + ' out)') +
        '</div>' +
        '<div class="route-pairs-toggle" onclick="toggleRoutePairs(this, \\'' + pairId + '\\')">' +
          '<span class="arrow">\\u25b6</span> Show pairs' +
        '</div>' +
        '<div class="route-pairs-list" id="' + pairId + '">' + pairsHtml + '</div>' +
      '</div>';
    } else {
      // Provider not enabled — show as pending/upcoming
      html += '<div class="route-card status-degraded">' +
        '<div class="route-card-header">' +
          '<div class="route-card-name"><span class="route-status-dot degraded"></span>' + pid.charAt(0) + pid.slice(1).toLowerCase() + '</div>' +
          '<span class="route-card-type">Swap</span>' +
        '</div>' +
        '<div class="route-card-status degraded">NOT YET ENABLED</div>' +
        '<div class="route-card-details route-card-detail-extra">' +
          row('Status', '<span class="warn">Integration in progress</span>') +
        '</div>' +
      '</div>';
    }
  }

  html += '</div>';

  // --- Provider changes log ---
  if (changes.length > 0) {
    html += '<div class="route-uptime-row" style="flex-direction:column;gap:4px">' +
      '<span style="color:var(--text);font-weight:600;font-size:11px">Recent provider changes</span>';
    const recent = changes.slice(-5);
    for (const c of recent) {
      const icon = c.change === 'added' ? '<span class="up">+</span>' : '<span class="err">\\u2212</span>';
      const date = new Date(c.t).toLocaleDateString('en-US', {month:'short', day:'numeric'});
      html += '<div style="font-size:11px">' + icon + ' ' + c.provider + ' ' + c.change + ' <span style="color:var(--muted)">' + date + '</span></div>';
    }
    html += '</div>';
  }

  // --- Swap API uptime ---
  const apiUptime = computeUptime('swap_api');
  if (apiUptime !== null) {
    html += '<div class="route-uptime-row">Swap API uptime (7d): <span>' + apiUptime + '%</span></div>';
  }

  panel.innerHTML = html;
}

function toggleRoutePairs(toggle, listId) {
  const list = document.getElementById(listId);
  const isOpen = list.classList.contains('open');
  list.classList.toggle('open');
  toggle.classList.toggle('open');
  toggle.querySelector('.arrow').style.transform = isOpen ? '' : 'rotate(90deg)';
  toggle.childNodes[1].textContent = isOpen ? ' Show pairs' : ' Hide pairs';
}

function setChartMode(mode) {
  chartMode = mode;
  document.querySelectorAll('#vol-chart-toggle button').forEach(b => {
    b.classList.toggle('active', b.textContent.toLowerCase() === mode);
  });
  renderCharts();
}

function renderTraffic() {
  const section = document.getElementById('traffic-section');
  const wa = DATA.web_analytics;
  if (!wa || !wa.sessions) {
    section.style.display = 'none';
    return;
  }
  section.style.display = '';

  const funnel = wa.funnel || [];
  const maxSessions = funnel.length > 0 ? funnel[0].sessions : 1;

  let statsHtml = '<div class="traffic-stats">' +
    '<div class="traffic-stat">' +
      '<div class="traffic-stat-label">Sessions</div>' +
      '<div class="traffic-stat-value">' + wa.sessions + '</div>' +
      '<div class="traffic-stat-sub">' + (wa.date_range || '').replace('_', ' ') + '</div>' +
    '</div>' +
    '<div class="traffic-stat">' +
      '<div class="traffic-stat-label">Unique Users</div>' +
      '<div class="traffic-stat-value">' + wa.unique_users + '</div>' +
      '<div class="traffic-stat-sub">' + wa.returning_user_pct + '% returning</div>' +
    '</div>' +
    '<div class="traffic-stat">' +
      '<div class="traffic-stat-label">Pages / Session</div>' +
      '<div class="traffic-stat-value">' + wa.pages_per_session + '</div>' +
      '<div class="traffic-stat-sub">avg depth</div>' +
    '</div>' +
    '<div class="traffic-stat">' +
      '<div class="traffic-stat-label">Active Time</div>' +
      '<div class="traffic-stat-value">' + wa.avg_active_time_min + 'm</div>' +
      '<div class="traffic-stat-sub">avg per session</div>' +
    '</div>' +
  '</div>';

  let funnelHtml = '';
  if (funnel.length > 0) {
    funnelHtml = '<div style="margin-top:4px">';
    for (const step of funnel) {
      const pct = maxSessions > 0 ? (step.sessions / maxSessions * 100) : 0;
      const pctLabel = pct === 100 ? '100%' : pct.toFixed(1) + '%';
      const barWidth = Math.max(pct, 8);
      funnelHtml += '<div class="funnel-step">' +
        '<div class="funnel-step-label">' + step.step + '</div>' +
        '<div class="funnel-step-bar-wrapper">' +
          '<div class="funnel-step-bar">' +
            '<div class="funnel-step-bar-fill" style="width:' + barWidth + '%">' +
              '<span class="funnel-step-bar-text">' + step.sessions + '</span>' +
            '</div>' +
          '</div>' +
        '</div>' +
        '<div class="funnel-step-meta">' + pctLabel + '</div>' +
      '</div>';
    }
    funnelHtml += '</div>';
  }

  const sourceNote = wa.updated_at
    ? '<div class="traffic-source">Source: Microsoft Clarity · Updated ' + wa.updated_at + '</div>'
    : '';

  document.getElementById('traffic-panel').innerHTML = statsHtml + funnelHtml + sourceNote;
}

function renderLargestTx() {
  const ops = [
    { name: 'Flyover Peg-In', data: DATA.flyover_pegins, color: '#DEFF19', field: 'value_rbtc', unit: 'RBTC' },
    { name: 'Flyover Peg-Out', data: DATA.flyover_pegouts, color: '#F0FF96', field: 'value_rbtc', unit: 'RBTC' },
    { name: 'PowPeg Peg-In', data: DATA.powpeg_pegins, color: '#FF9100', field: 'value_rbtc', unit: 'RBTC' },
    { name: 'PowPeg Peg-Out', data: DATA.powpeg_pegouts, color: '#FED8A7', field: 'value_rbtc', unit: 'RBTC' },
  ];

  let html = '';
  for (const op of ops) {
    let largest = null;
    for (const e of op.data) {
      if (!largest || (e[op.field] || 0) > (largest[op.field] || 0)) largest = e;
    }
    const val = largest ? (largest[op.field] || 0) : 0;
    const date = largest ? parseTS(largest.timestamp) : null;
    const hash = largest ? largest.tx_hash : '';
    const explorer = 'https://rootstock.blockscout.com/tx/';

    html += '<div class="op-card" style="border-top-color:' + op.color + '">' +
      '<div class="op-card-name" style="color:' + op.color + '">' + op.name + '</div>' +
      '<div style="margin-top:8px">' +
        '<div class="op-metric-value">' + fmtRBTC(val) + '</div>' +
        '<div style="color:var(--muted);font-size:11px;margin-top:6px">' +
          (date ? date.toLocaleDateString('en-US', {year:'numeric', month:'short', day:'numeric'}) : '') +
        '</div>' +
        (hash ? '<a href="' + explorer + hash + '" target="_blank" rel="noopener" style="color:var(--purple);font-size:11px;text-decoration:none">' + shortHash(hash) + '</a>' : '') +
      '</div>' +
    '</div>';
  }

  document.getElementById('largest-tx-cards').innerHTML = html;
}

function renderAll() {
  renderSummary();
  renderBtcLocked();
  renderWallets();
  renderHealth();
  renderRouteHealth();
  renderLargestTx();
  renderCharts();
  tablePage = 0;
  renderTable();
}

function setPeriod(p) {
  currentPeriod = p;
  document.querySelectorAll('.period-nav button').forEach(b => b.classList.remove('active'));
  document.getElementById('btn-' + p).classList.add('active');
  renderAll();
}

// ─── Live LP Data ───

const LP_BTC_WALLET = '1D2xucTYkxCHvaaZuaKVJTfZQWr4PUjzAy';
const LP_RBTC_WALLET = '0x82A06eBdb97776a2DA4041DF8F2b2Ea8d3257852';
const LPS_URL = 'https://lps.tekscapital.com/providers/liquidity';

async function fetchLiveLPData() {
  if (!DATA || !DATA.lp_info) return;

  const results = await Promise.allSettled([
    fetch(LPS_URL).then(r => r.ok ? r.json() : Promise.reject(r.status)),
    fetch('https://mempool.space/api/address/' + LP_BTC_WALLET + '/utxo').then(r => r.ok ? r.json() : Promise.reject(r.status)),
    fetch('https://mempool.space/api/address/' + LP_BTC_WALLET + '/txs/mempool').then(r => r.ok ? r.json() : Promise.reject(r.status)),
    fetch('https://rootstock.blockscout.com/api/v2/addresses/' + LP_RBTC_WALLET).then(r => r.ok ? r.json() : Promise.reject(r.status)),
  ]);

  const [lpsResult, utxoResult, mempoolResult, blockscoutResult] = results;

  // LPS API — update advertised liquidity
  if (lpsResult.status === 'fulfilled') {
    try {
      const data = lpsResult.value;
      const peginWei = parseInt(data.peginLiquidityAmount || '0');
      const pegoutWei = parseInt(data.pegoutLiquidityAmount || '0');
      if (peginWei > 0) DATA.lp_info.lps_pegin_rbtc = peginWei / 1e18;
      if (pegoutWei > 0) DATA.lp_info.lps_pegout_btc = pegoutWei / 1e18;
    } catch(e) { /* keep static values */ }
  }

  // mempool.space UTXOs — update BTC balance and UTXO count
  if (utxoResult.status === 'fulfilled') {
    try {
      const utxos = utxoResult.value;
      if (Array.isArray(utxos)) {
        DATA.lp_info.btc_utxo_count = utxos.length;
        DATA.lp_info.btc_utxos = utxos.map(u => ({ value_btc: (u.value || 0) / 1e8, confirmed: u.status && u.status.confirmed }));
        const totalSats = utxos.reduce((sum, u) => sum + (u.value || 0), 0);
        DATA.lp_info.pegout_btc = totalSats / 1e8;
      }
    } catch(e) { /* keep static values */ }
  }

  // mempool.space mempool — update pending tx count
  if (mempoolResult.status === 'fulfilled') {
    try {
      const txs = mempoolResult.value;
      DATA.lp_info.btc_mempool_tx_count = Array.isArray(txs) ? txs.length : 0;
    } catch(e) { /* keep static values */ }
  }

  // Blockscout — update RBTC balance
  if (blockscoutResult.status === 'fulfilled') {
    try {
      const addr = blockscoutResult.value;
      if (addr && addr.coin_balance) {
        DATA.lp_info.pegin_rbtc = parseFloat(addr.coin_balance) / 1e18;
      }
    } catch(e) { /* keep static values */ }
  }

  const anySucceeded = results.some(r => r.status === 'fulfilled');
  if (anySucceeded) {
    DATA.lp_info._live = true;
    DATA.lp_info._updated_at = new Date().toISOString();
  }

  // Flash the health panel and re-render
  const panel = document.getElementById('health-panel');
  panel.classList.add('flash');
  setTimeout(() => panel.classList.remove('flash'), 600);

  renderHealth();
}

async function loadData() {
  const overlay = document.getElementById('loading-overlay');
  try {
    const resp = await fetch('./data/dashboard.json?v=' + Date.now());
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    DATA = await resp.json();
    const genDate = new Date(DATA.generated_at);
    document.getElementById('generated-at').textContent = '\u00b7 ' + genDate.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
    overlay.classList.add('hidden');
    renderAll();
    fetchLiveLPData();
    setInterval(fetchLiveLPData, 60000);
  } catch(e) {
    console.error('Failed to load dashboard data:', e);
    overlay.textContent = 'Failed to load dashboard data: ' + e.message;
    overlay.style.color = '#EF4444';
  }
}
loadData();
</script>

</body>
</html>"""


def main():
    print("Loading data files...")
    flyover_pegins = load_json("flyover_pegins.json")
    flyover_pegouts = load_json("flyover_pegouts.json")
    flyover_pegout_refunds = load_json("flyover_pegout_refunds.json")
    flyover_penalties = load_json("flyover_penalties.json")
    flyover_refunds = load_json("flyover_refunds.json")
    powpeg_pegins = load_json("powpeg_pegins.json")
    powpeg_pegouts = load_json("powpeg_pegouts.json")
    lp_info = load_json("flyover_lp_info.json")
    btc_locked_stats = load_json("btc_locked_stats.json")
    web_analytics = load_json("web_analytics.json")
    route_health = load_json("route_health.json")

    print(f"  Flyover peg-ins: {len(flyover_pegins)}")
    print(f"  Flyover peg-outs: {len(flyover_pegouts)}")
    print(f"  Flyover peg-out refunds: {len(flyover_pegout_refunds)}")
    print(f"  Flyover penalties: {len(flyover_penalties)}")
    print(f"  Flyover refunds: {len(flyover_refunds)}")
    print(f"  PowPeg peg-ins: {len(powpeg_pegins)}")
    print(f"  PowPeg peg-outs: {len(powpeg_pegouts)}")
    if lp_info:
        print(f"  LP info: {lp_info.get('lp_name', 'unknown')}")
    if btc_locked_stats:
        print(f"  BTC locked: {btc_locked_stats.get('locked_in_contracts_rbtc', 0)} / {btc_locked_stats.get('total_bridged_rbtc', 0)} RBTC")

    print("\nBuilding dashboard data...")
    data = build_dashboard_data(
        flyover_pegins, flyover_pegouts, flyover_pegout_refunds,
        flyover_penalties, flyover_refunds,
        powpeg_pegins, powpeg_pegouts,
        lp_info=lp_info if isinstance(lp_info, dict) else {},
        btc_locked_stats=btc_locked_stats if isinstance(btc_locked_stats, dict) else {},
        web_analytics=web_analytics if isinstance(web_analytics, dict) else {},
        route_health=route_health if isinstance(route_health, dict) else {},
    )

    print("Writing dashboard JSON...")
    json_dir = os.path.join(PAGES_DIR, "data")
    os.makedirs(json_dir, exist_ok=True)
    json_path = os.path.join(json_dir, "dashboard.json")
    with open(json_path, "w") as f:
        json.dump(data, f)
    print(f"  Data written to {json_path}")

    print("Generating HTML...")
    html = generate_html()

    os.makedirs(PAGES_DIR, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        f.write(html)

    print(f"  HTML written to {OUTPUT_PATH}")
    print(f"\nServe locally: cd {PAGES_DIR} && python3 -m http.server 8000")


if __name__ == "__main__":
    main()
