#!/usr/bin/env python3
"""
Alert system for Atlas Dashboard.

Reads data/*.json files produced by the fetch pipeline, evaluates alert rules
against configurable thresholds, and sends notifications via Telegram bot API.
Tracks sent alerts in data/.alert_state.json to avoid spam (cooldown-based
deduplication).

Environment variables:
    TELEGRAM_BOT_TOKEN        Telegram bot token
    TELEGRAM_CHAT_ID          Telegram chat/group ID
    ALERT_COOLDOWN_WARNING    Minutes between repeat warnings  (default: 240)
    ALERT_COOLDOWN_CRITICAL   Minutes between repeat criticals (default: 60)
"""

import html as html_lib
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"
ALERT_STATE_FILE = DATA_DIR / ".alert_state.json"
CONFIG_FILE = SCRIPT_DIR / "alert_config.json"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Severity levels (ordered)
# ---------------------------------------------------------------------------
HEALTHY = "healthy"
WARNING = "warning"
CRITICAL = "critical"

SEVERITY_ORDER = {HEALTHY: 0, WARNING: 1, CRITICAL: 2}

SEVERITY_EMOJI = {
    HEALTHY: "\u2705",   # ✅
    WARNING: "\u26a0",   # ⚠
    CRITICAL: "\U0001f534",  # 🔴
}

# ---------------------------------------------------------------------------
# Default thresholds & cooldowns
# ---------------------------------------------------------------------------
DEFAULT_THRESHOLDS = {
    "pegin_balance":   {"warning": 10, "critical": 5},
    "pegout_balance":  {"warning": 10, "critical": 5},
    "btc_utxos":       {"warning": 4,  "critical": 2},
    "staleness_hours": {"warning": 25, "critical": 48},
}

DEFAULT_COOLDOWNS = {"warning": 240, "critical": 60}  # minutes

DEFAULT_DASHBOARD_URL = "https://ahmadalkabra.github.io/atlas-dashboard/"

# ---------------------------------------------------------------------------
# Configuration loading
# ---------------------------------------------------------------------------

def load_config():
    """Load alert_config.json if present, falling back to defaults."""
    thresholds = dict(DEFAULT_THRESHOLDS)
    cooldowns = dict(DEFAULT_COOLDOWNS)
    dashboard_url = DEFAULT_DASHBOARD_URL

    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE) as f:
                cfg = json.load(f)
            if "thresholds" in cfg:
                for key, vals in cfg["thresholds"].items():
                    thresholds[key] = vals
            if "cooldown_minutes" in cfg:
                cooldowns.update(cfg["cooldown_minutes"])
            if "dashboard_url" in cfg:
                dashboard_url = cfg["dashboard_url"]
            logger.info("Loaded config from %s", CONFIG_FILE)
        except (json.JSONDecodeError, KeyError) as exc:
            logger.warning("Failed to parse %s, using defaults: %s", CONFIG_FILE, exc)

    # Environment variable overrides for cooldowns
    cooldowns["warning"] = int(os.environ.get(
        "ALERT_COOLDOWN_WARNING", cooldowns["warning"]
    ))
    cooldowns["critical"] = int(os.environ.get(
        "ALERT_COOLDOWN_CRITICAL", cooldowns["critical"]
    ))

    return thresholds, cooldowns, dashboard_url

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_json(path):
    """Load a JSON file, returning None on failure."""
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        logger.warning("Could not load %s: %s", path, exc)
        return None

# ---------------------------------------------------------------------------
# Alert state persistence (deduplication)
# ---------------------------------------------------------------------------

def load_alert_state():
    state = load_json(ALERT_STATE_FILE)
    return state if isinstance(state, dict) else {}


def save_alert_state(state):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(ALERT_STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

# ---------------------------------------------------------------------------
# Alert rule evaluation
# ---------------------------------------------------------------------------

def evaluate_rules(thresholds):
    """
    Evaluate all alert rules and return a list of alert dicts:
        {"rule": str, "severity": str, "message": str}
    """
    alerts = []
    lp = load_json(DATA_DIR / "flyover_lp_info.json")

    # -- LP data rules --
    if lp is not None:
        # Data staleness
        fetched_at = lp.get("fetched_at")
        if fetched_at:
            try:
                fetch_time = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
                age_hours = (datetime.now(timezone.utc) - fetch_time).total_seconds() / 3600
                thresh = thresholds["staleness_hours"]
                if age_hours >= thresh["critical"]:
                    alerts.append({
                        "rule": "data_staleness",
                        "severity": CRITICAL,
                        "message": f"LP data is {age_hours:.1f}h old (threshold: >{thresh['critical']}h)",
                    })
                elif age_hours >= thresh["warning"]:
                    alerts.append({
                        "rule": "data_staleness",
                        "severity": WARNING,
                        "message": f"LP data is {age_hours:.1f}h old (threshold: >{thresh['warning']}h)",
                    })
                else:
                    alerts.append({"rule": "data_staleness", "severity": HEALTHY, "message": ""})
            except (ValueError, TypeError):
                logger.warning("Could not parse fetched_at: %s", fetched_at)

        # Peg-in balance — LPS API value (actual available), fallback to on-chain
        pegin = lp.get("lps_pegin_rbtc") or lp.get("pegin_rbtc")
        if pegin is not None:
            thresh = thresholds["pegin_balance"]
            if pegin < thresh["critical"]:
                alerts.append({
                    "rule": "pegin_balance",
                    "severity": CRITICAL,
                    "message": f"TeksCapital peg-in available: {pegin:.4f} RBTC (threshold: <{thresh['critical']})",
                })
            elif pegin < thresh["warning"]:
                alerts.append({
                    "rule": "pegin_balance",
                    "severity": WARNING,
                    "message": f"TeksCapital peg-in available: {pegin:.4f} RBTC (threshold: <{thresh['warning']})",
                })
            else:
                alerts.append({"rule": "pegin_balance", "severity": HEALTHY, "message": ""})

        # Peg-out balance — LPS API value (actual available), fallback to on-chain
        pegout = lp.get("lps_pegout_btc") or lp.get("pegout_btc")
        if pegout is not None:
            thresh = thresholds["pegout_balance"]
            if pegout < thresh["critical"]:
                alerts.append({
                    "rule": "pegout_balance",
                    "severity": CRITICAL,
                    "message": f"TeksCapital peg-out available: {pegout:.4f} BTC (threshold: <{thresh['critical']})",
                })
            elif pegout < thresh["warning"]:
                alerts.append({
                    "rule": "pegout_balance",
                    "severity": WARNING,
                    "message": f"TeksCapital peg-out available: {pegout:.4f} BTC (threshold: <{thresh['warning']})",
                })
            else:
                alerts.append({"rule": "pegout_balance", "severity": HEALTHY, "message": ""})

        # BTC UTXOs
        utxo_count = lp.get("btc_utxo_count")
        if utxo_count is not None:
            thresh = thresholds["btc_utxos"]
            if utxo_count < thresh["critical"]:
                alerts.append({
                    "rule": "btc_utxos",
                    "severity": CRITICAL,
                    "message": f"BTC UTXO count: {utxo_count} (threshold: <{thresh['critical']})",
                })
            elif utxo_count < thresh["warning"]:
                alerts.append({
                    "rule": "btc_utxos",
                    "severity": WARNING,
                    "message": f"BTC UTXO count: {utxo_count} (threshold: <{thresh['warning']})",
                })
            else:
                alerts.append({"rule": "btc_utxos", "severity": HEALTHY, "message": ""})

        # LPS API unreachable — on-chain data exists but LPS API values missing
        if lp.get("pegin_rbtc") is not None and lp.get("lps_pegin_rbtc") is None:
            alerts.append({
                "rule": "lps_api_unreachable",
                "severity": WARNING,
                "message": "LPS API data missing — dashboard falling back to on-chain values",
            })
        else:
            alerts.append({"rule": "lps_api_unreachable", "severity": HEALTHY, "message": ""})

        # Operational status — peg-in
        op_pegin = lp.get("is_operational_pegin")
        if op_pegin is not None:
            if not op_pegin:
                alerts.append({
                    "rule": "operational_pegin",
                    "severity": CRITICAL,
                    "message": "LP is NOT operational for peg-in",
                })
            else:
                alerts.append({"rule": "operational_pegin", "severity": HEALTHY, "message": ""})

        # Operational status — peg-out (informational only — logged but not alerted)
        op_pegout = lp.get("is_operational_pegout")
        if op_pegout is not None and not op_pegout:
            logger.info("Operational info: LP is NOT operational for peg-out")

        # Collateral below minimum (informational only — logged but not alerted)
        collateral = lp.get("pegout_collateral")
        min_collateral = lp.get("min_collateral")
        if collateral is not None and min_collateral is not None:
            if collateral < min_collateral:
                logger.info(
                    "Collateral info: peg-out collateral (%.5f RBTC) below minimum (%.5f RBTC)",
                    collateral, min_collateral,
                )
    else:
        # If we can't load LP data at all, that's critical
        alerts.append({
            "rule": "data_missing",
            "severity": CRITICAL,
            "message": "flyover_lp_info.json is missing or unreadable",
        })

    # -- Route health rules (RSK Swap API) --
    route_health = load_json(DATA_DIR / "route_health.json")
    if route_health:
        # Route health data staleness
        rh_fetched = route_health.get("fetched_at")
        if rh_fetched:
            try:
                rh_time = datetime.fromisoformat(rh_fetched.replace("Z", "+00:00"))
                rh_age_hours = (datetime.now(timezone.utc) - rh_time).total_seconds() / 3600
                rh_thresh = thresholds.get("route_staleness_hours", {"warning": 4, "critical": 8})
                if rh_age_hours >= rh_thresh["critical"]:
                    alerts.append({
                        "rule": "route_data_staleness",
                        "severity": CRITICAL,
                        "message": f"Route health data is {rh_age_hours:.1f}h old (threshold: >{rh_thresh['critical']}h)",
                    })
                elif rh_age_hours >= rh_thresh["warning"]:
                    alerts.append({
                        "rule": "route_data_staleness",
                        "severity": WARNING,
                        "message": f"Route health data is {rh_age_hours:.1f}h old (threshold: >{rh_thresh['warning']}h)",
                    })
                else:
                    alerts.append({"rule": "route_data_staleness", "severity": HEALTHY, "message": ""})
            except (ValueError, TypeError):
                logger.warning("Could not parse route_health fetched_at: %s", rh_fetched)

        # Swap API itself
        swap_api = route_health.get("swap_api", {})
        api_status = swap_api.get("status", "unknown")
        if api_status == "down":
            alerts.append({
                "rule": "swap_api",
                "severity": CRITICAL,
                "message": "RSK Swap API is DOWN — all swap routes unavailable",
            })
        else:
            alerts.append({"rule": "swap_api", "severity": HEALTHY, "message": ""})

        # Swap API response time
        api_response_ms = swap_api.get("response_ms")
        if api_response_ms is not None and api_status != "down":
            rt_thresh = thresholds.get("route_response_time_ms", {"warning": 5000, "critical": 10000})
            if api_response_ms >= rt_thresh["critical"]:
                alerts.append({
                    "rule": "swap_api_response_time",
                    "severity": CRITICAL,
                    "message": f"RSK Swap API response time: {api_response_ms}ms (threshold: >{rt_thresh['critical']}ms)",
                })
            elif api_response_ms >= rt_thresh["warning"]:
                alerts.append({
                    "rule": "swap_api_response_time",
                    "severity": WARNING,
                    "message": f"RSK Swap API response time: {api_response_ms}ms (threshold: >{rt_thresh['warning']}ms)",
                })
            else:
                alerts.append({"rule": "swap_api_response_time", "severity": HEALTHY, "message": ""})

        # Provider changes (only from current run, not full history)
        for change in route_health.get("new_provider_changes", []):
            if change.get("change") == "removed":
                alerts.append({
                    "rule": f"provider_{change['provider'].lower()}_removed",
                    "severity": WARNING,
                    "message": f"Swap provider {change['provider']} was REMOVED from the API",
                })
            elif change.get("change") == "added":
                # New provider added — informational, not an alert
                logger.info("Provider added: %s", change["provider"])

        # Provider with zero mainnet pairs (enabled but effectively dead)
        for pid, pdata in route_health.get("swap_providers", {}).items():
            pair_count = pdata.get("pair_count", 0)
            provider_name = pdata.get("name", pid.upper())
            if pair_count == 0:
                alerts.append({
                    "rule": f"provider_{pid}_zero_pairs",
                    "severity": WARNING,
                    "message": f"{provider_name} is enabled but has 0 mainnet pairs",
                })
            else:
                alerts.append({"rule": f"provider_{pid}_zero_pairs", "severity": HEALTHY, "message": ""})

        # Flyover peg-in availability (from route health native_routes)
        flyover_rh = route_health.get("native_routes", {}).get("flyover", {})
        pegin_avail_rh = flyover_rh.get("pegin_available")
        if pegin_avail_rh is not None and not pegin_avail_rh:
            alerts.append({
                "rule": "flyover_pegin_down",
                "severity": CRITICAL,
                "message": "Flyover peg-in is NOT available — LP has disabled inbound transfers",
            })
        elif pegin_avail_rh is not None:
            alerts.append({"rule": "flyover_pegin_down", "severity": HEALTHY, "message": ""})

    return alerts

# ---------------------------------------------------------------------------
# Deduplication logic
# ---------------------------------------------------------------------------

def should_send(rule, severity, state, cooldowns, now_ts):
    """
    Decide whether to send a notification for this rule/severity.

    Returns (should_send: bool, is_recovery: bool)
    """
    prev = state.get(rule)
    if prev is None:
        # Never seen — send if not healthy
        return severity != HEALTHY, False

    prev_sev = prev.get("severity", HEALTHY)
    prev_ts = prev.get("timestamp", 0)

    # Recovery: was unhealthy, now healthy
    if severity == HEALTHY and prev_sev != HEALTHY:
        return True, True

    # Healthy → healthy: no notification
    if severity == HEALTHY:
        return False, False

    # Escalation: severity increased
    if SEVERITY_ORDER[severity] > SEVERITY_ORDER[prev_sev]:
        return True, False

    # Same severity: check cooldown
    cooldown_sec = cooldowns.get(severity, 240) * 60
    if now_ts - prev_ts >= cooldown_sec:
        return True, False

    return False, False

# ---------------------------------------------------------------------------
# Notification senders
# ---------------------------------------------------------------------------

def send_telegram(token, chat_id, rule, severity, message, dashboard_url, is_recovery):
    """Send a Telegram notification via Bot API."""
    emoji = SEVERITY_EMOJI[severity]

    if is_recovery:
        title = f"{emoji} RECOVERED: {rule.replace('_', ' ').title()}"
        body = "Status returned to healthy."
    else:
        label = severity.upper()
        title = f"{emoji} {label}: {rule.replace('_', ' ').title()}"
        body = message

    title = html_lib.escape(title)
    body = html_lib.escape(body)
    html = f"<b>{title}</b>\n{body}\nDashboard: {dashboard_url}"
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": html, "parse_mode": "HTML"}

    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        logger.info("Telegram alert sent for %s (%s)", rule, severity)
    except requests.RequestException as exc:
        logger.error("Telegram send failed for %s: %s", rule, exc)

def send_telegram_html(token, chat_id, html):
    """Send a pre-formatted HTML message to Telegram."""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": html, "parse_mode": "HTML"}
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        logger.info("Telegram daily summary sent")
    except requests.RequestException as exc:
        logger.error("Telegram daily summary send failed: %s", exc)


# ---------------------------------------------------------------------------
# Daily summary
# ---------------------------------------------------------------------------

def _count_last_24h(records, ts_key="block_timestamp"):
    """Count transactions and sum RBTC volume from the last 24 hours."""
    cutoff = datetime.now(timezone.utc).timestamp() - 86400
    count = 0
    volume = 0.0
    for rec in records:
        ts_str = rec.get(ts_key)
        if not ts_str:
            continue
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp()
        except (ValueError, TypeError):
            continue
        if ts >= cutoff:
            count += 1
            volume += rec.get("value_rbtc") or rec.get("amount_rbtc") or 0.0
    return count, volume


def maybe_send_daily_summary(state, alerts, dashboard_url, tg_token, tg_chat):
    """
    Send a daily summary message if 24h have elapsed since the last one.
    Returns True if a summary was sent/logged.
    """
    now_ts = time.time()

    # Check 24h cooldown
    summary_state = state.get("_daily_summary", {})
    last_sent = summary_state.get("timestamp", 0)
    if now_ts - last_sent < 86400:
        logger.info("Daily summary: skipped (last sent %.1fh ago)", (now_ts - last_sent) / 3600)
        return False

    # Load transaction data
    flyover_pegins = load_json(DATA_DIR / "flyover_pegins.json") or []
    flyover_pegouts = load_json(DATA_DIR / "flyover_pegouts.json") or []
    powpeg_pegins = load_json(DATA_DIR / "powpeg_pegins.json") or []
    powpeg_pegouts = load_json(DATA_DIR / "powpeg_pegouts.json") or []
    lp = load_json(DATA_DIR / "flyover_lp_info.json")

    fi_count, fi_vol = _count_last_24h(flyover_pegins)
    fo_count, fo_vol = _count_last_24h(flyover_pegouts)
    pi_count, pi_vol = _count_last_24h(powpeg_pegins)
    po_count, po_vol = _count_last_24h(powpeg_pegouts)

    # LP balances
    pegin_avail = ""
    pegout_avail = ""
    utxo_line = ""
    if lp:
        pegin_bal = lp.get("lps_pegin_rbtc") or lp.get("pegin_rbtc")
        if pegin_bal is not None:
            pegin_avail = f"Peg-in: {pegin_bal:.2f} RBTC available"
        pegout_bal = lp.get("lps_pegout_btc") or lp.get("pegout_btc")
        if pegout_bal is not None:
            pegout_avail = f"Peg-out: {pegout_bal:.2f} BTC available"
        utxo = lp.get("btc_utxo_count")
        if utxo is not None:
            utxo_line = f"UTXOs: {utxo}"

    # Active warnings/criticals
    active_warnings = [a for a in alerts if a["severity"] == WARNING]
    active_criticals = [a for a in alerts if a["severity"] == CRITICAL]

    def _tx_label(count):
        return "tx" if count == 1 else "txs"

    today = datetime.now(timezone.utc).strftime("%b %d, %Y")
    lines = [f"\U0001f4ca Daily Summary \u2014 {today}", ""]

    lines.append("Flyover (24h)")
    lines.append(f"Peg-in: {fi_count} {_tx_label(fi_count)}, {fi_vol:.2f} RBTC")
    lines.append(f"Peg-out: {fo_count} {_tx_label(fo_count)}, {fo_vol:.2f} RBTC")
    lines.append("")

    lines.append("PowPeg (24h)")
    lines.append(f"Peg-in: {pi_count} {_tx_label(pi_count)}, {pi_vol:.2f} RBTC")
    lines.append(f"Peg-out: {po_count} {_tx_label(po_count)}, {po_vol:.2f} RBTC")
    lines.append("")

    if pegin_avail or pegout_avail or utxo_line:
        lines.append("LP Balances")
        if pegin_avail:
            lines.append(pegin_avail)
        if pegout_avail:
            lines.append(pegout_avail)
        if utxo_line:
            lines.append(utxo_line)
        lines.append("")

    # Swap route status
    route_health = load_json(DATA_DIR / "route_health.json")
    if route_health:
        swap_api = route_health.get("swap_api", {})
        providers = route_health.get("swap_providers", {})
        native = route_health.get("native_routes", {})

        route_parts = []
        # Native routes
        for rid, rdata in native.items():
            name = rdata.get("name", rid)
            enabled = rdata.get("enabled", False)
            route_parts.append(f"{name}: {'up' if enabled else 'down'}")
        # Swap providers
        for pid, pdata in providers.items():
            name = pdata.get("name", pid)
            pairs = pdata.get("pair_count", 0)
            route_parts.append(f"{name}: {pairs} pairs")

        lines.append("Swap Routes")
        if swap_api.get("status") == "down":
            lines.append("Swap API: DOWN")
        else:
            lines.append(" \u00b7 ".join(route_parts))
        lines.append("")

    if active_criticals:
        lines.append(f"\U0001f534 {len(active_criticals)} active critical{'s' if len(active_criticals) != 1 else ''}")
    if active_warnings:
        lines.append(f"\u26a0 {len(active_warnings)} active warning{'s' if len(active_warnings) != 1 else ''}")
    if not active_criticals and not active_warnings:
        lines.append("\u2705 All systems healthy")

    lines.append(f"Dashboard: {dashboard_url}")

    summary_text = "\n".join(lines)
    logger.info("Daily summary:\n%s", summary_text)

    if tg_token and tg_chat:
        html = html_lib.escape(summary_text)
        send_telegram_html(tg_token, tg_chat, html)

    state["_daily_summary"] = {"timestamp": now_ts}
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    thresholds, cooldowns, dashboard_url = load_config()

    tg_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    tg_chat = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

    has_telegram = bool(tg_token and tg_chat)

    if not has_telegram:
        logger.info("No Telegram credentials configured — alerts will log to stdout only")

    alerts = evaluate_rules(thresholds)
    state = load_alert_state()
    now_ts = time.time()
    notifications_sent = 0

    for alert in alerts:
        rule = alert["rule"]
        severity = alert["severity"]
        message = alert["message"]

        send, is_recovery = should_send(rule, severity, state, cooldowns, now_ts)

        if send:
            # Log to stdout always
            emoji = SEVERITY_EMOJI[severity]
            if is_recovery:
                logger.info("%s RECOVERED: %s", emoji, rule)
            else:
                logger.info("%s %s: %s — %s", emoji, severity.upper(), rule, message)

            # Send to Telegram
            if has_telegram:
                send_telegram(tg_token, tg_chat, rule, severity, message, dashboard_url, is_recovery)

            notifications_sent += 1

        # Update state — only reset timestamp when notification is sent,
        # so cooldown accumulates correctly for repeat alerts.
        prev_ts = state.get(rule, {}).get("timestamp", now_ts)
        state[rule] = {
            "severity": severity,
            "timestamp": now_ts if send else prev_ts,
            "message": message,
        }

    # Daily summary (separate from threshold alerts)
    if maybe_send_daily_summary(state, alerts, dashboard_url, tg_token, tg_chat):
        notifications_sent += 1

    save_alert_state(state)
    logger.info("Alert check complete: %d rules evaluated, %d notifications sent", len(alerts), notifications_sent)


if __name__ == "__main__":
    main()
