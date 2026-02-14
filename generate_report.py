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
        return [] if filename != "flyover_lp_info.json" else {}
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
    flyover_penalties: list[dict],
    flyover_refunds: list[dict],
    powpeg_pegins: list[dict],
    powpeg_pegouts: list[dict],
    lp_info: dict | None = None,
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
    fp_pegouts = extract_events(flyover_pegouts, "amount_rbtc", "sender")

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
            "value_rbtc": float(e.get("amount_rbtc") or e.get("value_rbtc") or 0),
            "address": e.get("rsk_address") or e.get("from_address", ""),
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

    return {
        "flyover_pegins": fp_pegins,
        "flyover_pegouts": fp_pegouts,
        "powpeg_pegins": pp_pegins,
        "powpeg_pegouts": pp_pegouts,
        "penalties": penalties,
        "refunds": refunds,
        "lp_info": lp_info or {},
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def generate_html(data: dict) -> str:
    """Generate the full HTML dashboard."""
    data_json = json.dumps(data)

    return f"""<!DOCTYPE html>
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
  :root {{
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
  }}
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background: var(--bg);
    color: var(--text);
    min-height: 100vh;
    -webkit-font-smoothing: antialiased;
  }}
  .dashboard {{
    max-width: 1200px;
    margin: 0 auto;
    padding: 32px 24px;
  }}

  /* --- Header --- */
  header {{
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 32px;
    flex-wrap: wrap;
    gap: 16px;
  }}
  .title-group h1 {{
    font-size: 22px;
    font-weight: 700;
    letter-spacing: -0.5px;
    background: linear-gradient(135deg, #FF9100, #DEFF19);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
  }}
  .title-group .subtitle {{
    color: var(--muted);
    font-size: 12px;
    margin-top: 2px;
  }}
  .period-nav {{
    display: flex;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    overflow: hidden;
  }}
  .period-nav button {{
    background: transparent;
    border: none;
    color: var(--muted);
    padding: 7px 16px;
    font-family: inherit;
    font-size: 13px;
    font-weight: 500;
    cursor: pointer;
    transition: all 0.15s;
  }}
  .period-nav button:hover {{ color: var(--text); background: rgba(255,255,255,0.04); }}
  .period-nav button.active {{ background: var(--text); color: #000; font-weight: 600; }}

  /* --- Operation Summary --- */
  .op-summary {{
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 16px;
    margin-bottom: 28px;
  }}
  .op-card {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 18px;
    transition: border-color 0.15s;
    border-top: 3px solid var(--border);
  }}
  .op-card:hover {{ border-color: var(--border-hover); }}
  .op-card-name {{
    font-size: 12px;
    font-weight: 600;
    margin-bottom: 14px;
    letter-spacing: 0.3px;
  }}
  .op-card-metrics {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 10px;
  }}
  .op-metric-label {{
    color: var(--muted);
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 4px;
  }}
  .op-metric-value {{
    font-size: 20px;
    font-weight: 700;
    letter-spacing: -0.3px;
    line-height: 1.1;
  }}
  .op-metric-delta {{
    font-size: 11px;
    font-weight: 600;
    margin-top: 4px;
  }}
  .op-metric-delta.up {{ color: var(--green); }}
  .op-metric-delta.down {{ color: var(--red); }}
  .op-metric-delta.neutral {{ color: var(--muted); }}
  .op-totals {{
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 16px;
    margin-bottom: 28px;
  }}
  .op-total-card {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 14px 18px;
    display: flex;
    align-items: center;
    justify-content: space-between;
  }}
  .op-total-label {{
    color: var(--muted);
    font-size: 11px;
    font-weight: 500;
  }}
  .op-total-value {{
    font-size: 18px;
    font-weight: 700;
  }}

  /* --- Health Section --- */
  .health-section {{
    margin-bottom: 28px;
  }}
  .health-panel {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 20px;
  }}
  .health-header {{
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 16px;
  }}
  .health-header h3 {{
    font-size: 14px;
    font-weight: 600;
    display: flex;
    align-items: center;
    gap: 10px;
  }}
  .health-overall-dot {{
    width: 10px;
    height: 10px;
    border-radius: 50%;
    display: inline-block;
  }}
  .health-overall-dot.pulse {{
    animation: healthPulse 2s ease-in-out infinite;
  }}
  @keyframes healthPulse {{
    0%, 100% {{ opacity: 1; }}
    50% {{ opacity: 0.4; }}
  }}
  .health-overall-label {{
    font-size: 12px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }}
  .health-staleness {{
    font-size: 11px;
    color: var(--muted);
    background: rgba(239,68,68,0.1);
    border: 1px solid rgba(239,68,68,0.25);
    border-radius: 6px;
    padding: 4px 10px;
  }}
  .health-grid {{
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 12px;
  }}
  .health-indicator {{
    background: var(--bg);
    border-radius: var(--radius-sm);
    padding: 14px;
    border-left: 3px solid var(--border);
  }}
  .health-indicator.status-healthy {{ border-left-color: var(--green); }}
  .health-indicator.status-warning {{ border-left-color: #EAB308; }}
  .health-indicator.status-critical {{ border-left-color: var(--red); }}
  .health-indicator {{ position: relative; }}
  .health-info-btn {{
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
  }}
  .health-info-btn:hover {{ border-color: var(--purple); color: var(--purple); }}
  .health-popover {{
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
  }}
  .health-popover.open {{ display: block; }}
  .health-popover-row {{ display: flex; align-items: center; gap: 6px; }}
  .health-popover-dot {{
    width: 6px;
    height: 6px;
    border-radius: 50%;
    display: inline-block;
    flex-shrink: 0;
  }}
  .health-indicator-label {{
    color: var(--muted);
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 8px;
  }}
  .health-indicator-value {{
    font-size: 18px;
    font-weight: 700;
    margin-bottom: 4px;
    line-height: 1.1;
  }}
  .health-indicator-sub {{
    color: var(--muted);
    font-size: 11px;
    margin-bottom: 8px;
  }}
  .health-indicator-status {{
    font-size: 10px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.3px;
  }}
  .health-indicator-status.healthy {{ color: var(--green); }}
  .health-indicator-status.warning {{ color: #EAB308; }}
  .health-indicator-status.critical {{ color: var(--red); }}
  .health-bar {{
    height: 4px;
    background: var(--border);
    border-radius: 2px;
    margin: 8px 0 4px;
    overflow: hidden;
  }}
  .health-bar-fill {{
    height: 100%;
    border-radius: 2px;
    transition: width 0.3s ease;
  }}

  /* --- Chart Panels --- */
  .chart-section {{ margin-bottom: 20px; }}
  .chart-grid {{
    display: grid;
    grid-template-columns: 3fr 2fr;
    gap: 20px;
    margin-bottom: 20px;
  }}
  .chart-grid-2col {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
    margin-bottom: 20px;
  }}
  .chart-panel {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 20px;
  }}
  .chart-panel-header {{
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 12px;
  }}
  .chart-panel-title {{
    font-size: 13px;
    font-weight: 600;
    color: var(--text);
  }}
  .chart-toggle {{
    display: flex;
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 6px;
    overflow: hidden;
  }}
  .chart-toggle button {{
    background: transparent;
    border: none;
    color: var(--muted);
    padding: 4px 10px;
    font-family: inherit;
    font-size: 11px;
    font-weight: 500;
    cursor: pointer;
    transition: all 0.15s;
  }}
  .chart-toggle button:hover {{ color: var(--text); }}
  .chart-toggle button.active {{ background: var(--surface-2); color: var(--text); }}

  /* --- Table --- */
  .table-section {{ margin-bottom: 28px; }}
  .table-header {{
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 12px;
  }}
  .table-header .section-title {{ margin-bottom: 0; }}
  .section-title {{
    font-size: 13px;
    font-weight: 600;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.8px;
    margin-bottom: 12px;
  }}
  .table-panel {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 16px;
    overflow-x: auto;
  }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  th, td {{ padding: 10px 12px; text-align: right; border-bottom: 1px solid var(--border); }}
  th {{
    color: var(--muted);
    font-weight: 600;
    text-transform: uppercase;
    font-size: 10px;
    letter-spacing: 0.5px;
    position: sticky;
    top: 0;
    background: var(--surface);
    white-space: nowrap;
  }}
  th:first-child, td:first-child {{ text-align: left; }}
  tbody tr:hover td {{ background: rgba(255,255,255,0.02); }}
  .th-group {{
    text-align: center !important;
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0;
    text-transform: none;
    border-bottom: 2px solid var(--border);
  }}
  .totals-row td {{
    font-weight: 700;
    border-top: 2px solid var(--border);
    border-bottom: none;
  }}
  .th-dot {{
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    margin-right: 4px;
    vertical-align: middle;
  }}
  .page-controls {{
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 12px;
    padding: 14px 0 2px;
  }}
  .page-btn {{
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
  }}
  .page-btn:hover:not(:disabled) {{ border-color: var(--purple); color: var(--purple); }}
  .page-btn:disabled {{ opacity: 0.3; cursor: default; }}
  .page-info {{ color: var(--muted); font-size: 12px; }}

  /* --- LP Section --- */
  .lp-section {{ margin-bottom: 28px; }}
  .lp-panel {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 20px;
  }}
  .lp-header {{
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 16px;
  }}
  .lp-header h3 {{ font-size: 14px; font-weight: 600; }}
  .lp-name {{ color: var(--flyover-pegin); font-weight: 600; font-size: 13px; }}
  .lp-stats {{
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 12px;
  }}
  .lp-stat {{
    background: var(--bg);
    border-radius: var(--radius-sm);
    padding: 14px;
    text-align: center;
  }}
  .lp-stat-label {{
    color: var(--muted);
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 6px;
  }}
  .lp-stat-value {{ font-size: 18px; font-weight: 700; }}
  .lp-stat-sub {{ color: var(--muted); font-size: 11px; margin-top: 4px; }}

  /* --- Footer --- */
  footer {{
    text-align: center;
    padding: 20px 0;
    color: var(--muted);
    font-size: 11px;
    border-top: 1px solid var(--border);
  }}
  footer a {{ color: var(--purple); text-decoration: none; }}
  footer a:hover {{ text-decoration: underline; }}

  /* --- Responsive --- */
  @media (max-width: 1024px) {{
    .chart-grid {{ grid-template-columns: 1fr; }}
    .chart-grid-2col {{ grid-template-columns: 1fr; }}
    .op-summary {{ grid-template-columns: repeat(2, 1fr); }}
    .op-totals {{ grid-template-columns: repeat(2, 1fr); }}
    .health-grid {{ grid-template-columns: repeat(2, 1fr); }}
  }}
  @media (max-width: 768px) {{
    .lp-stats {{ grid-template-columns: repeat(2, 1fr); }}
    header {{ flex-direction: column; align-items: flex-start; }}
  }}
  @media (max-width: 480px) {{
    .op-summary {{ grid-template-columns: 1fr; }}
    .op-totals {{ grid-template-columns: 1fr; }}
    .lp-stats {{ grid-template-columns: 1fr; }}
    .health-grid {{ grid-template-columns: 1fr; }}
    .dashboard {{ padding: 16px 12px; }}
  }}
</style>
</head>
<body>

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

  <section class="health-section" id="health-section-wrapper">
    <div class="section-title">Flyover System Health</div>
    <div id="health-panel" class="health-panel"></div>
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

  <section class="lp-section" id="lp-section-wrapper"></section>

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
const DATA = {data_json};

let currentPeriod = 'month';
let chartMode = 'area';

// ─── Utilities ───

function parseTS(ts) {{
  if (!ts) return null;
  const d = new Date(ts);
  return isNaN(d.getTime()) ? null : d;
}}

function periodKey(date, period) {{
  if (!date) return 'unknown';
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, '0');
  const d = String(date.getDate()).padStart(2, '0');
  switch (period) {{
    case 'quarter': return `${{y}}-Q${{Math.ceil((date.getMonth()+1)/3)}}`;
    case 'month': return `${{y}}-${{m}}`;
    case 'week':
      const jan1 = new Date(y, 0, 1);
      const week = Math.ceil(((date - jan1) / 86400000 + jan1.getDay() + 1) / 7);
      return `${{y}}-W${{String(week).padStart(2,'0')}}`;
    case 'day': return `${{y}}-${{m}}-${{d}}`;
    default: return `${{y}}-${{m}}`;
  }}
}}

function groupBy(events, period) {{
  const groups = {{}};
  for (const e of events) {{
    const d = parseTS(e.timestamp);
    const key = periodKey(d, period);
    if (!groups[key]) groups[key] = [];
    groups[key].push(e);
  }}
  return groups;
}}

function sumField(events, field) {{
  return events.reduce((s, e) => s + (e[field] || 0), 0);
}}

function fmt(n, decimals = 4) {{
  if (n >= 1000) return n.toLocaleString(undefined, {{ maximumFractionDigits: decimals }});
  return n.toFixed(decimals);
}}

function fmtCompact(n) {{
  if (Math.abs(n) >= 1000) return (n / 1000).toFixed(1) + 'k';
  if (Math.abs(n) >= 1) return n.toFixed(2);
  if (Math.abs(n) >= 0.01) return n.toFixed(4);
  return n.toFixed(6);
}}

function fmtRBTC(n) {{
  if (Math.abs(n) >= 100) return n.toFixed(1);
  if (Math.abs(n) >= 1) return n.toFixed(2);
  return n.toFixed(4);
}}

function shortHash(h) {{
  if (!h) return '';
  return h.slice(0, 10) + '...' + h.slice(-6);
}}

function periodLabel() {{
  return {{ day: 'day', week: 'week', month: 'month', quarter: 'quarter' }}[currentPeriod] || 'period';
}}

function getLatestTwo(events, period) {{
  const groups = groupBy(events, period);
  const keys = Object.keys(groups).filter(k => k !== 'unknown').sort();
  if (keys.length === 0) return {{ current: [], previous: [], currentKey: '', prevKey: '' }};
  const currentKey = keys[keys.length - 1];
  const prevKey = keys.length > 1 ? keys[keys.length - 2] : '';
  return {{
    current: groups[currentKey] || [],
    previous: prevKey ? (groups[prevKey] || []) : [],
    currentKey,
    prevKey,
  }};
}}

// ─── Render ───

function deltaText(current, previous) {{
  if (previous === 0 && current === 0) return '<span class="op-metric-delta neutral">&ndash;</span>';
  if (previous === 0) return '<span class="op-metric-delta up">&uarr; new</span>';
  const pct = ((current - previous) / Math.abs(previous) * 100).toFixed(0);
  if (current > previous) return `<span class="op-metric-delta up">&uarr; ${{Math.abs(pct)}}%</span>`;
  if (current < previous) return `<span class="op-metric-delta down">&darr; ${{Math.abs(pct)}}%</span>`;
  return '<span class="op-metric-delta neutral">&ndash; 0%</span>';
}}

function renderSummary() {{
  const ops = [
    {{ name: 'Flyover Peg-In', data: DATA.flyover_pegins, color: '#DEFF19', field: 'value_rbtc' }},
    {{ name: 'Flyover Peg-Out', data: DATA.flyover_pegouts, color: '#F0FF96', field: 'value_rbtc' }},
    {{ name: 'PowPeg Peg-In', data: DATA.powpeg_pegins, color: '#FF9100', field: 'value_rbtc' }},
    {{ name: 'PowPeg Peg-Out', data: DATA.powpeg_pegouts, color: '#FED8A7', field: 'value_rbtc' }},
  ];

  const period = currentPeriod;
  let totalTxs = 0, totalVol = 0;

  let cards = '';
  for (const op of ops) {{
    const latest = getLatestTwo(op.data, period);
    const curTxs = latest.current.length;
    const prevTxs = latest.previous.length;
    const curVol = sumField(latest.current, op.field);
    const prevVol = sumField(latest.previous, op.field);
    const darkText = ['#F0FF96', '#FED8A7', '#DEFF19'].includes(op.color);
    totalTxs += curTxs;
    totalVol += curVol;

    cards += `
      <div class="op-card" style="border-top-color:${{op.color}}">
        <div class="op-card-name" style="color:${{op.color}}">${{op.name}}</div>
        <div class="op-card-metrics">
          <div>
            <div class="op-metric-label">Transactions</div>
            <div class="op-metric-value">${{curTxs}}</div>
            ${{deltaText(curTxs, prevTxs)}}
          </div>
          <div>
            <div class="op-metric-label">Volume</div>
            <div class="op-metric-value">${{fmtRBTC(curVol)}}</div>
            ${{deltaText(curVol, prevVol)}}
          </div>
        </div>
      </div>`;
  }}

  const el = document.getElementById('op-summary');
  el.innerHTML = cards;
}}

function renderCharts() {{
  const period = currentPeriod;
  const cfg = {{ displayModeBar: false, responsive: true }};
  const hoverLabel = {{
    bgcolor: '#1a1a1a', bordercolor: '#2a2a2a',
    font: {{ family: 'Inter, sans-serif', color: '#FAFAF5', size: 12 }}
  }};
  const baseLayout = {{
    paper_bgcolor: 'transparent',
    plot_bgcolor: 'transparent',
    font: {{ family: 'Inter, sans-serif', color: '#737373', size: 11 }},
    margin: {{ l: 50, r: 16, t: 8, b: 36 }},
    xaxis: {{ gridcolor: '#1a1a1a', linecolor: '#1e1e1e', zeroline: false }},
    yaxis: {{ gridcolor: '#1a1a1a', linecolor: '#1e1e1e', zeroline: false }},
    legend: {{ orientation: 'h', y: -0.2, font: {{ size: 10, color: '#737373' }} }},
    hoverlabel: hoverLabel,
    height: 280,
  }};

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
    `${{label}}: ${{fmtRBTC(v)}}`);

  const fpV = keys.map(k => sumField(fpG[k] || [], 'value_rbtc'));
  const foV = keys.map(k => sumField(foG[k] || [], 'value_rbtc'));
  const ppV = keys.map(k => sumField(ppG[k] || [], 'value_rbtc'));
  const poV = keys.map(k => sumField(poG[k] || [], 'value_rbtc'));

  const volTraces = chartMode === 'area' ? [
    {{ x: keys, y: fpV, name: 'Flyover In', type: 'scatter', stackgroup: 'vol',
       fillcolor: 'rgba(222,255,25,0.3)', line: {{ color: '#DEFF19', width: 1.5 }},
       text: mkHover('Flyover In', fpV), hoverinfo: 'text' }},
    {{ x: keys, y: foV, name: 'Flyover Out', type: 'scatter', stackgroup: 'vol',
       fillcolor: 'rgba(240,255,150,0.25)', line: {{ color: '#F0FF96', width: 1.5 }},
       text: mkHover('Flyover Out', foV), hoverinfo: 'text' }},
    {{ x: keys, y: ppV, name: 'PowPeg In', type: 'scatter', stackgroup: 'vol',
       fillcolor: 'rgba(255,145,0,0.3)', line: {{ color: '#FF9100', width: 1.5 }},
       text: mkHover('PowPeg In', ppV), hoverinfo: 'text' }},
    {{ x: keys, y: poV, name: 'PowPeg Out', type: 'scatter', stackgroup: 'vol',
       fillcolor: 'rgba(254,216,167,0.25)', line: {{ color: '#FED8A7', width: 1.5 }},
       text: mkHover('PowPeg Out', poV), hoverinfo: 'text' }},
  ] : [
    {{ x: keys, y: fpV, name: 'Flyover In', type: 'bar',
       marker: {{ color: '#DEFF19', line: {{ width: 0 }} }}, textposition: 'none',
       hovertext: mkHover('Flyover In', fpV), hoverinfo: 'text' }},
    {{ x: keys, y: foV, name: 'Flyover Out', type: 'bar',
       marker: {{ color: '#F0FF96', line: {{ width: 0 }} }}, textposition: 'none',
       hovertext: mkHover('Flyover Out', foV), hoverinfo: 'text' }},
    {{ x: keys, y: ppV, name: 'PowPeg In', type: 'bar',
       marker: {{ color: '#FF9100', line: {{ width: 0 }} }}, textposition: 'none',
       hovertext: mkHover('PowPeg In', ppV), hoverinfo: 'text' }},
    {{ x: keys, y: poV, name: 'PowPeg Out', type: 'bar',
       marker: {{ color: '#FED8A7', line: {{ width: 0 }} }}, textposition: 'none',
       hovertext: mkHover('PowPeg Out', poV), hoverinfo: 'text' }},
  ];
  const volLayout = {{
    ...baseLayout,
    height: 300,
    ...(chartMode === 'bar' ? {{ barmode: 'stack' }} : {{}}),
    hovermode: 'x unified',
  }};
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

  Plotly.newPlot('chart-donut', [{{
    values: [fpVol, foVol, ppVol, poVol],
    labels: ['Flyover In', 'Flyover Out', 'PowPeg In', 'PowPeg Out'],
    type: 'pie',
    hole: 0.6,
    marker: {{ colors: ['#DEFF19', '#F0FF96', '#FF9100', '#FED8A7'] }},
    textinfo: 'percent',
    textfont: {{ color: '#000', size: 11, family: 'Inter, sans-serif' }},
    text: [fpVol, foVol, ppVol, poVol].map(v => `${{fmtRBTC(v)}}`),
    hovertemplate: '%{{label}}<br>%{{text}}<br>%{{percent}}<extra></extra>',
    sort: false,
  }}], {{
    ...baseLayout,
    margin: {{ l: 10, r: 10, t: 10, b: 10 }},
    showlegend: true,
    legend: {{ orientation: 'h', y: -0.05, font: {{ size: 10, color: '#737373' }} }},
    annotations: [{{
      text: `${{fmtCompact(total)}}<br><span style="font-size:11px;color:#737373">total</span>`,
      showarrow: false,
      font: {{ size: 18, color: '#FAFAF5', family: 'Inter, sans-serif' }},
      x: 0.5, y: 0.5,
    }}],
  }}, cfg);

  // Transaction count donut — filtered by period
  const fpTx = latestFp.current.length;
  const foTx = latestFo.current.length;
  const ppTx = latestPp.current.length;
  const poTx = latestPo.current.length;
  const totalTx = fpTx + foTx + ppTx + poTx;

  Plotly.newPlot('chart-tx-donut', [{{
    values: [fpTx, foTx, ppTx, poTx],
    labels: ['Flyover In', 'Flyover Out', 'PowPeg In', 'PowPeg Out'],
    type: 'pie',
    hole: 0.6,
    marker: {{ colors: ['#DEFF19', '#F0FF96', '#FF9100', '#FED8A7'] }},
    textinfo: 'percent',
    textfont: {{ color: '#000', size: 11, family: 'Inter, sans-serif' }},
    text: [fpTx, foTx, ppTx, poTx].map(v => `${{v}} txs`),
    hovertemplate: '%{{label}}<br>%{{text}}<br>%{{percent}}<extra></extra>',
    sort: false,
  }}], {{
    ...baseLayout,
    margin: {{ l: 10, r: 10, t: 10, b: 10 }},
    showlegend: true,
    legend: {{ orientation: 'h', y: -0.05, font: {{ size: 10, color: '#737373' }} }},
    annotations: [{{
      text: `${{totalTx}}<br><span style="font-size:11px;color:#737373">txs</span>`,
      showarrow: false,
      font: {{ size: 18, color: '#FAFAF5', family: 'Inter, sans-serif' }},
      x: 0.5, y: 0.5,
    }}],
  }}, cfg);
}}

let tablePage = 0;
const PAGE_SIZE = 15;

function renderTable() {{
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
  for (const key of allKeys) {{
    const fp = fpG[key] || [], fo = foG[key] || [];
    const pp = ppG[key] || [], po = poG[key] || [];
    totFpTx += fp.length; totFpVol += sumField(fp, 'value_rbtc');
    totFoTx += fo.length; totFoVol += sumField(fo, 'value_rbtc');
    totPpTx += pp.length; totPpVol += sumField(pp, 'value_rbtc');
    totPoTx += po.length; totPoVol += sumField(po, 'value_rbtc');
  }}

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

  for (const key of pageKeys) {{
    const fp = fpG[key] || [], fo = foG[key] || [];
    const pp = ppG[key] || [], po = poG[key] || [];

    html += `<tr>
      <td><strong>${{key}}</strong></td>
      <td>${{fp.length}}</td><td>${{fmtRBTC(sumField(fp, 'value_rbtc'))}}</td>
      <td>${{fo.length}}</td><td>${{fmtRBTC(sumField(fo, 'value_rbtc'))}}</td>
      <td>${{pp.length}}</td><td>${{fmtRBTC(sumField(pp, 'value_rbtc'))}}</td>
      <td>${{po.length}}</td><td>${{fmtRBTC(sumField(po, 'value_rbtc'))}}</td>
    </tr>`;
  }}

  // Totals row
  html += `<tr class="totals-row">
    <td>Total</td>
    <td>${{totFpTx}}</td><td>${{fmtRBTC(totFpVol)}}</td>
    <td>${{totFoTx}}</td><td>${{fmtRBTC(totFoVol)}}</td>
    <td>${{totPpTx}}</td><td>${{fmtRBTC(totPpVol)}}</td>
    <td>${{totPoTx}}</td><td>${{fmtRBTC(totPoVol)}}</td>
  </tr>`;

  html += '</tbody></table>';
  document.getElementById('data-table').innerHTML = html;

  const pag = document.getElementById('table-pagination');
  if (totalPages <= 1) {{ pag.innerHTML = ''; return; }}
  pag.innerHTML = `
    <button class="page-btn" onclick="tableNav(-1)" ${{tablePage === 0 ? 'disabled' : ''}}>&larr; Prev</button>
    <span class="page-info">${{tablePage + 1}} / ${{totalPages}}</span>
    <button class="page-btn" onclick="tableNav(1)" ${{tablePage >= totalPages - 1 ? 'disabled' : ''}}>Next &rarr;</button>
  `;
}}

function tableNav(dir) {{ tablePage += dir; renderTable(); }}


function renderLPSection() {{
  const lp = DATA.lp_info;
  const wrapper = document.getElementById('lp-section-wrapper');

  const lpData = {{}};
  for (const e of DATA.flyover_pegins) {{
    const addr = (e.lp_address || '').toLowerCase();
    if (!addr) continue;
    if (!lpData[addr]) lpData[addr] = {{ pegins: 0, peginVol: 0, penalties: 0 }};
    lpData[addr].pegins++;
    lpData[addr].peginVol += e.value_rbtc || 0;
  }}
  for (const e of DATA.penalties) {{
    const addr = (e.lp_address || '').toLowerCase();
    if (!addr) continue;
    if (!lpData[addr]) lpData[addr] = {{ pegins: 0, peginVol: 0, penalties: 0 }};
    lpData[addr].penalties++;
  }}
  const topLP = Object.entries(lpData).sort((a,b) => b[1].peginVol - a[1].peginVol)[0];

  if (!lp || !lp.lp_name) {{
    if (!topLP) {{ wrapper.innerHTML = ''; return; }}
  }}

  const lpName = (lp && lp.lp_name) ? lp.lp_name : (topLP ? shortHash(topLP[0]) : 'Unknown');
  const peginLiq = (lp && lp.pegin_rbtc) ? fmt(lp.pegin_rbtc) : 'N/A';
  const pegoutLiq = (lp && lp.pegout_btc) ? fmt(lp.pegout_btc) : 'N/A';
  const deliveries = topLP ? topLP[1].pegins : 0;
  const penaltyCount = topLP ? topLP[1].penalties : 0;

  wrapper.innerHTML = `
    <div class="section-title">Liquidity Provider</div>
    <div class="lp-panel">
      <div class="lp-header">
        <h3>LP Performance</h3>
        <span class="lp-name">${{lpName}}</span>
      </div>
      <div class="lp-stats">
        <div class="lp-stat">
          <div class="lp-stat-label">Peg-In Liquidity</div>
          <div class="lp-stat-value" style="color:#DEFF19">${{peginLiq}}</div>
          <div class="lp-stat-sub">available</div>
        </div>
        <div class="lp-stat">
          <div class="lp-stat-label">Peg-Out Liquidity</div>
          <div class="lp-stat-value" style="color:#F0FF96">${{pegoutLiq}}</div>
          <div class="lp-stat-sub">BTC available</div>
        </div>
        <div class="lp-stat">
          <div class="lp-stat-label">Deliveries</div>
          <div class="lp-stat-value">${{deliveries}}</div>
          <div class="lp-stat-sub">transfers</div>
        </div>
        <div class="lp-stat">
          <div class="lp-stat-label">Penalties</div>
          <div class="lp-stat-value" style="color:${{penaltyCount > 0 ? 'var(--red)' : 'var(--green)'}}">${{penaltyCount}}</div>
          <div class="lp-stat-sub">${{penaltyCount === 0 ? 'clean' : 'incurred'}}</div>
        </div>
      </div>
    </div>
  `;
}}

// ─── Health Section ───

const HEALTH_THRESHOLDS = {{
  peginBalance:  {{ warning: 10, critical: 5 }},
  pegoutBalance: {{ warning: 10, critical: 5 }},
}};
const STALENESS_HOURS = 25;

function assessStatus(value, thresholds) {{
  if (value <= thresholds.critical) return 'critical';
  if (value <= thresholds.warning) return 'warning';
  return 'healthy';
}}

function renderHealth() {{
  const panel = document.getElementById('health-panel');
  const wrapper = document.getElementById('health-section-wrapper');
  const lp = DATA.lp_info || {{}};
  const refTime = new Date(DATA.generated_at);

  // If no LP info at all, hide the section
  if (!lp.lp_name && DATA.flyover_pegins.length === 0) {{
    wrapper.style.display = 'none';
    return;
  }}
  wrapper.style.display = '';

  // --- 1. Peg-In LP Balance ---
  const peginBal = lp.pegin_rbtc != null ? parseFloat(lp.pegin_rbtc) : null;
  const peginStatus = peginBal != null
    ? assessStatus(peginBal, HEALTH_THRESHOLDS.peginBalance)
    : 'warning';
  const peginMax = 50;

  // --- 2. Peg-Out LP Balance ---
  const pegoutBal = lp.pegout_btc != null ? parseFloat(lp.pegout_btc) : null;
  const pegoutStatus = pegoutBal != null
    ? assessStatus(pegoutBal, HEALTH_THRESHOLDS.pegoutBalance)
    : 'warning';
  const pegoutMax = 50;

  // --- Last activity timestamps (info only, no health status) ---
  let lastPeginDate = null;
  let lastPeginValue = 0;
  for (const e of DATA.flyover_pegins) {{
    const d = parseTS(e.timestamp);
    if (d && (!lastPeginDate || d > lastPeginDate)) {{
      lastPeginDate = d;
      lastPeginValue = e.value_rbtc || 0;
    }}
  }}
  const peginHoursAgo = lastPeginDate
    ? (refTime - lastPeginDate) / (1000 * 60 * 60)
    : Infinity;

  let lastPegoutDate = null;
  let lastPegoutValue = 0;
  for (const e of DATA.flyover_pegouts) {{
    const d = parseTS(e.timestamp);
    if (d && (!lastPegoutDate || d > lastPegoutDate)) {{
      lastPegoutDate = d;
      lastPegoutValue = e.value_rbtc || 0;
    }}
  }}
  const pegoutHoursAgo = lastPegoutDate
    ? (refTime - lastPegoutDate) / (1000 * 60 * 60)
    : Infinity;

  // --- Overall status (worst of all indicators) ---
  const statuses = [peginStatus, pegoutStatus];
  let overall = 'healthy';
  if (statuses.includes('critical')) overall = 'critical';
  else if (statuses.includes('warning')) overall = 'warning';

  const statusColors = {{ healthy: 'var(--green)', warning: '#EAB308', critical: 'var(--red)' }};
  const statusLabels = {{ healthy: 'Healthy', warning: 'Warning', critical: 'Critical' }};

  // --- Staleness check ---
  const dataAgeHours = (Date.now() - refTime.getTime()) / (1000 * 60 * 60);
  const isStale = dataAgeHours > STALENESS_HOURS;

  // --- Build HTML ---
  function hoursLabel(hours) {{
    if (!isFinite(hours)) return 'No data';
    if (hours < 1) return 'Just now';
    if (hours < 24) return Math.round(hours) + 'h ago';
    const days = Math.floor(hours / 24);
    const rem = Math.round(hours % 24);
    return days + 'd ' + rem + 'h ago';
  }}

  function balanceBar(value, max, status) {{
    if (value == null) return '';
    const pct = Math.min(100, Math.max(0, (value / max) * 100));
    return `<div class="health-bar"><div class="health-bar-fill" style="width:${{pct}}%;background:${{statusColors[status]}}"></div></div>`;
  }}

  let html = `<div class="health-header">
    <h3>
      <span class="health-overall-dot ${{overall !== 'healthy' ? 'pulse' : ''}}" style="background:${{statusColors[overall]}}"></span>
      <span class="health-overall-label" style="color:${{statusColors[overall]}}">${{statusLabels[overall]}}</span>
    </h3>
    ${{isStale ? `<span class="health-staleness">Data is ${{Math.round(dataAgeHours)}}h old</span>` : ''}}
  </div>
  <div class="health-grid">
    <div class="health-indicator status-${{peginStatus}}">
      <button class="health-info-btn" onclick="toggleHealthPopover(event)">i</button>
      <div class="health-popover">
        <div class="health-popover-row"><span class="health-popover-dot" style="background:#EAB308"></span> Warning: &lt; 10 RBTC</div>
        <div class="health-popover-row"><span class="health-popover-dot" style="background:var(--red)"></span> Critical: &lt; 5 RBTC</div>
      </div>
      <div class="health-indicator-label">Peg-In LP Balance</div>
      <div class="health-indicator-value">${{peginBal != null ? fmtRBTC(peginBal) : 'N/A'}}</div>
      <div class="health-indicator-sub">${{peginBal != null ? 'RBTC available' : 'No LP data'}}</div>
      ${{balanceBar(peginBal, peginMax, peginStatus)}}
      <div class="health-indicator-status ${{peginStatus}}">${{statusLabels[peginStatus]}}</div>
    </div>
    <div class="health-indicator status-${{pegoutStatus}}">
      <button class="health-info-btn" onclick="toggleHealthPopover(event)">i</button>
      <div class="health-popover">
        <div class="health-popover-row"><span class="health-popover-dot" style="background:#EAB308"></span> Warning: &lt; 10 BTC</div>
        <div class="health-popover-row"><span class="health-popover-dot" style="background:var(--red)"></span> Critical: &lt; 5 BTC</div>
      </div>
      <div class="health-indicator-label">Peg-Out LP Balance</div>
      <div class="health-indicator-value">${{pegoutBal != null ? fmtRBTC(pegoutBal) : 'N/A'}}</div>
      <div class="health-indicator-sub">${{pegoutBal != null ? 'BTC available' : 'No LP data'}}</div>
      ${{balanceBar(pegoutBal, pegoutMax, pegoutStatus)}}
      <div class="health-indicator-status ${{pegoutStatus}}">${{statusLabels[pegoutStatus]}}</div>
    </div>
    <div class="health-indicator">
      <div class="health-indicator-label">Last Peg-In</div>
      <div class="health-indicator-value">${{hoursLabel(peginHoursAgo)}}</div>
      <div class="health-indicator-sub">${{lastPeginDate ? fmtRBTC(lastPeginValue) : 'Never'}}</div>
      <div class="health-indicator-sub">${{lastPeginDate ? lastPeginDate.toLocaleDateString('en-US', {{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}}) : ''}}</div>
    </div>
    <div class="health-indicator">
      <div class="health-indicator-label">Last Peg-Out</div>
      <div class="health-indicator-value">${{hoursLabel(pegoutHoursAgo)}}</div>
      <div class="health-indicator-sub">${{lastPegoutDate ? fmtRBTC(lastPegoutValue) : 'Never'}}</div>
      <div class="health-indicator-sub">${{lastPegoutDate ? lastPegoutDate.toLocaleDateString('en-US', {{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}}) : ''}}</div>
    </div>
  </div>`;

  panel.innerHTML = html;
}}

function toggleHealthPopover(e) {{
  e.stopPropagation();
  const popover = e.currentTarget.nextElementSibling;
  const wasOpen = popover.classList.contains('open');
  document.querySelectorAll('.health-popover.open').forEach(p => p.classList.remove('open'));
  if (!wasOpen) popover.classList.add('open');
}}
document.addEventListener('click', () => {{
  document.querySelectorAll('.health-popover.open').forEach(p => p.classList.remove('open'));
}});

function setChartMode(mode) {{
  chartMode = mode;
  document.querySelectorAll('#vol-chart-toggle button').forEach(b => b.classList.remove('active'));
  event.target.classList.add('active');
  renderCharts();
}}

function renderAll() {{
  renderSummary();
  renderHealth();
  renderCharts();
  renderLPSection();
  tablePage = 0;
  renderTable();
}}

function setPeriod(p) {{
  currentPeriod = p;
  document.querySelectorAll('.period-nav button').forEach(b => b.classList.remove('active'));
  document.getElementById('btn-' + p).classList.add('active');
  renderAll();
}}

const genDate = new Date(DATA.generated_at);
document.getElementById('generated-at').textContent = '\\u00b7 ' + genDate.toLocaleDateString('en-US', {{ year: 'numeric', month: 'short', day: 'numeric' }});
try {{ renderAll(); }} catch(e) {{ console.error('renderAll failed:', e); }}
</script>

</body>
</html>"""


def main():
    print("Loading data files...")
    flyover_pegins = load_json("flyover_pegins.json")
    flyover_pegouts = load_json("flyover_pegouts.json")
    flyover_penalties = load_json("flyover_penalties.json")
    flyover_refunds = load_json("flyover_refunds.json")
    powpeg_pegins = load_json("powpeg_pegins.json")
    powpeg_pegouts = load_json("powpeg_pegouts.json")
    lp_info = load_json("flyover_lp_info.json")

    print(f"  Flyover peg-ins: {len(flyover_pegins)}")
    print(f"  Flyover peg-outs: {len(flyover_pegouts)}")
    print(f"  Flyover penalties: {len(flyover_penalties)}")
    print(f"  Flyover refunds: {len(flyover_refunds)}")
    print(f"  PowPeg peg-ins: {len(powpeg_pegins)}")
    print(f"  PowPeg peg-outs: {len(powpeg_pegouts)}")
    if lp_info:
        print(f"  LP info: {lp_info.get('lp_name', 'unknown')}")

    print("\nBuilding dashboard data...")
    data = build_dashboard_data(
        flyover_pegins, flyover_pegouts,
        flyover_penalties, flyover_refunds,
        powpeg_pegins, powpeg_pegouts,
        lp_info=lp_info if isinstance(lp_info, dict) else {},
    )

    print("Generating HTML...")
    html = generate_html(data)

    os.makedirs(PAGES_DIR, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        f.write(html)

    print(f"\nDashboard written to {OUTPUT_PATH}")
    print(f"Open in browser: file://{OUTPUT_PATH}")


if __name__ == "__main__":
    main()
