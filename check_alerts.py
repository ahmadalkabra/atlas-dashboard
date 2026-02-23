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
log = logging.getLogger("check_alerts")

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
            log.info("Loaded config from %s", CONFIG_FILE)
        except (json.JSONDecodeError, KeyError) as exc:
            log.warning("Failed to parse %s, using defaults: %s", CONFIG_FILE, exc)

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
        log.warning("Could not load %s: %s", path, exc)
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
                log.warning("Could not parse fetched_at: %s", fetched_at)

        # Peg-in balance (RBTC)
        pegin = lp.get("pegin_rbtc")
        if pegin is not None:
            thresh = thresholds["pegin_balance"]
            if pegin < thresh["critical"]:
                alerts.append({
                    "rule": "pegin_balance",
                    "severity": CRITICAL,
                    "message": f"TeksCapital RBTC balance: {pegin:.4f} RBTC (threshold: <{thresh['critical']})",
                })
            elif pegin < thresh["warning"]:
                alerts.append({
                    "rule": "pegin_balance",
                    "severity": WARNING,
                    "message": f"TeksCapital RBTC balance: {pegin:.4f} RBTC (threshold: <{thresh['warning']})",
                })
            else:
                alerts.append({"rule": "pegin_balance", "severity": HEALTHY, "message": ""})

        # Peg-out balance (BTC)
        pegout = lp.get("pegout_btc")
        if pegout is not None:
            thresh = thresholds["pegout_balance"]
            if pegout < thresh["critical"]:
                alerts.append({
                    "rule": "pegout_balance",
                    "severity": CRITICAL,
                    "message": f"TeksCapital BTC balance: {pegout:.4f} BTC (threshold: <{thresh['critical']})",
                })
            elif pegout < thresh["warning"]:
                alerts.append({
                    "rule": "pegout_balance",
                    "severity": WARNING,
                    "message": f"TeksCapital BTC balance: {pegout:.4f} BTC (threshold: <{thresh['warning']})",
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
            log.info("Operational info: LP is NOT operational for peg-out")

        # Collateral below minimum (informational only — logged but not alerted)
        collateral = lp.get("pegout_collateral")
        min_collateral = lp.get("min_collateral")
        if collateral is not None and min_collateral is not None:
            if collateral < min_collateral:
                log.info(
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
        log.info("Telegram alert sent for %s (%s)", rule, severity)
    except requests.RequestException as exc:
        log.error("Telegram send failed for %s: %s", rule, exc)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    thresholds, cooldowns, dashboard_url = load_config()

    tg_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    tg_chat = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

    has_telegram = bool(tg_token and tg_chat)

    if not has_telegram:
        log.info("No Telegram credentials configured — alerts will log to stdout only")

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
                log.info("%s RECOVERED: %s", emoji, rule)
            else:
                log.info("%s %s: %s — %s", emoji, severity.upper(), rule, message)

            # Send to Telegram
            if has_telegram:
                send_telegram(tg_token, tg_chat, rule, severity, message, dashboard_url, is_recovery)

            notifications_sent += 1

        # Update state for this rule
        state[rule] = {
            "severity": severity,
            "timestamp": now_ts,
            "message": message,
        }

    save_alert_state(state)
    log.info("Alert check complete: %d rules evaluated, %d notifications sent", len(alerts), notifications_sent)


if __name__ == "__main__":
    main()
