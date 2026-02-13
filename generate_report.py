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

  /* --- KPI Row --- */
  .kpi-row {{
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 16px;
    margin-bottom: 28px;
  }}
  .kpi-card {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 20px;
    transition: border-color 0.15s;
  }}
  .kpi-card:hover {{ border-color: var(--border-hover); }}
  .kpi-label {{
    color: var(--muted);
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.6px;
    margin-bottom: 8px;
    font-weight: 500;
  }}
  .kpi-value {{
    font-size: 28px;
    font-weight: 700;
    letter-spacing: -0.5px;
    line-height: 1.1;
  }}
  .kpi-sub {{
    color: var(--muted);
    font-size: 12px;
    margin-top: 8px;
    display: flex;
    align-items: center;
    gap: 6px;
  }}
  .kpi-delta {{
    font-size: 12px;
    font-weight: 600;
    padding: 2px 6px;
    border-radius: 4px;
  }}
  .kpi-delta.up {{ color: var(--green); background: rgba(34,197,94,0.12); }}
  .kpi-delta.down {{ color: var(--red); background: rgba(239,68,68,0.12); }}
  .kpi-delta.neutral {{ color: var(--muted); background: rgba(115,115,115,0.12); }}
  .kpi-card.net-positive .kpi-value {{ color: var(--green); }}
  .kpi-card.net-negative .kpi-value {{ color: var(--red); }}

  /* --- Chart Panels --- */
  .chart-section {{ margin-bottom: 20px; }}
  .chart-grid {{
    display: grid;
    grid-template-columns: 3fr 2fr;
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
  .th-dot {{
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    margin-right: 4px;
    vertical-align: middle;
  }}
  .badge {{
    display: inline-block;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 11px;
    font-weight: 600;
    white-space: nowrap;
  }}
  .badge.fo-pegin {{ background: var(--flyover-pegin); color: #000; }}
  .badge.fo-pegout {{ background: var(--flyover-pegout); color: #000; }}
  .badge.pp-pegin {{ background: var(--powpeg-pegin); color: #fff; }}
  .badge.pp-pegout {{ background: var(--powpeg-pegout); color: #000; }}
  .tx-link {{ color: var(--purple); text-decoration: none; font-weight: 500; }}
  .tx-link:hover {{ text-decoration: underline; }}
  .expand-btn {{
    background: none;
    border: 1px solid var(--border);
    color: var(--muted);
    cursor: pointer;
    font-size: 12px;
    font-family: inherit;
    padding: 3px 10px;
    border-radius: 4px;
    transition: all 0.15s;
  }}
  .expand-btn:hover {{ border-color: var(--purple); color: var(--purple); }}
  .detail-row {{ display: none; }}
  .detail-row.open {{ display: table-row; }}
  .detail-row td {{ background: rgba(0,0,0,0.25) !important; padding: 6px 12px; font-size: 12px; }}
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
  }}
  @media (max-width: 768px) {{
    .kpi-row {{ grid-template-columns: 1fr; }}
    .lp-stats {{ grid-template-columns: repeat(2, 1fr); }}
    header {{ flex-direction: column; align-items: flex-start; }}
  }}
  @media (max-width: 480px) {{
    .lp-stats {{ grid-template-columns: 1fr; }}
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

  <section class="kpi-row" id="hero-kpis"></section>

  <section class="chart-section">
    <div class="chart-panel">
      <div class="chart-panel-header">
        <div class="chart-panel-title">Volume Over Time</div>
        <div class="chart-toggle" id="vol-chart-toggle">
          <button class="active" onclick="setChartMode('area')">Area</button>
          <button onclick="setChartMode('bar')">Bar</button>
        </div>
      </div>
      <div id="chart-volume-trend"></div>
    </div>
  </section>

  <section class="chart-grid">
    <div class="chart-panel">
      <div class="chart-panel-header">
        <div class="chart-panel-title">Transactions</div>
      </div>
      <div id="chart-count"></div>
    </div>
    <div class="chart-panel">
      <div class="chart-panel-header">
        <div class="chart-panel-title">Volume Share</div>
      </div>
      <div id="chart-donut"></div>
    </div>
  </section>

  <section class="lp-section" id="lp-section-wrapper"></section>

  <section class="table-section">
    <div class="section-title">Breakdown</div>
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

function deltaHTML(current, previous) {{
  if (previous === 0 && current === 0) return '<span class="kpi-delta neutral">-</span>';
  if (previous === 0) return '<span class="kpi-delta up">&uarr; new</span>';
  const pct = ((current - previous) / Math.abs(previous) * 100).toFixed(0);
  if (current > previous) return `<span class="kpi-delta up">&uarr; ${{Math.abs(pct)}}%</span>`;
  if (current < previous) return `<span class="kpi-delta down">&darr; ${{Math.abs(pct)}}%</span>`;
  return '<span class="kpi-delta neutral">&ndash; 0%</span>';
}}

// ─── Render ───

function renderHeroKPIs() {{
  const fp = DATA.flyover_pegins, fo = DATA.flyover_pegouts;
  const pp = DATA.powpeg_pegins, po = DATA.powpeg_pegouts;
  const allEvents = [...fp, ...fo, ...pp, ...po];
  const latest = getLatestTwo(allEvents, currentPeriod);

  const curVol = sumField(latest.current, 'value_rbtc');
  const prevVol = sumField(latest.previous, 'value_rbtc');
  const curTxs = latest.current.length;
  const prevTxs = latest.previous.length;

  const latestIn = getLatestTwo([...fp, ...pp], currentPeriod);
  const latestOut = getLatestTwo([...fo, ...po], currentPeriod);
  const curNet = sumField(latestIn.current, 'value_rbtc') - sumField(latestOut.current, 'value_rbtc');
  const prevNet = sumField(latestIn.previous, 'value_rbtc') - sumField(latestOut.previous, 'value_rbtc');
  const netClass = curNet >= 0 ? 'net-positive' : 'net-negative';
  const pKey = latest.currentKey;

  document.getElementById('hero-kpis').innerHTML = `
    <div class="kpi-card">
      <div class="kpi-label">Volume &middot; ${{pKey}}</div>
      <div class="kpi-value">${{fmtCompact(curVol)}}</div>
      <div class="kpi-sub">${{deltaHTML(curVol, prevVol)}} vs prev ${{periodLabel()}}</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Transactions &middot; ${{pKey}}</div>
      <div class="kpi-value">${{curTxs.toLocaleString()}}</div>
      <div class="kpi-sub">${{deltaHTML(curTxs, prevTxs)}} vs prev ${{periodLabel()}}</div>
    </div>
    <div class="kpi-card ${{netClass}}">
      <div class="kpi-label">Net Flow &middot; ${{pKey}}</div>
      <div class="kpi-value">${{curNet >= 0 ? '+' : ''}}${{fmtCompact(curNet)}}</div>
      <div class="kpi-sub">${{deltaHTML(curNet, prevNet)}} peg-in minus peg-out</div>
    </div>
  `;
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
  const volTraces = chartMode === 'area' ? [
    {{ x: keys, y: keys.map(k => sumField(fpG[k] || [], 'value_rbtc')),
       name: 'Flyover In', type: 'scatter', stackgroup: 'vol',
       fillcolor: 'rgba(222,255,25,0.3)', line: {{ color: '#DEFF19', width: 1.5 }},
       hovertemplate: '%{{x}}<br>Flyover In: %{{y:.4f}}<extra></extra>' }},
    {{ x: keys, y: keys.map(k => sumField(foG[k] || [], 'value_rbtc')),
       name: 'Flyover Out', type: 'scatter', stackgroup: 'vol',
       fillcolor: 'rgba(240,255,150,0.25)', line: {{ color: '#F0FF96', width: 1.5 }},
       hovertemplate: '%{{x}}<br>Flyover Out: %{{y:.4f}}<extra></extra>' }},
    {{ x: keys, y: keys.map(k => sumField(ppG[k] || [], 'value_rbtc')),
       name: 'PowPeg In', type: 'scatter', stackgroup: 'vol',
       fillcolor: 'rgba(255,145,0,0.3)', line: {{ color: '#FF9100', width: 1.5 }},
       hovertemplate: '%{{x}}<br>PowPeg In: %{{y:.4f}}<extra></extra>' }},
    {{ x: keys, y: keys.map(k => sumField(poG[k] || [], 'value_rbtc')),
       name: 'PowPeg Out', type: 'scatter', stackgroup: 'vol',
       fillcolor: 'rgba(254,216,167,0.25)', line: {{ color: '#FED8A7', width: 1.5 }},
       hovertemplate: '%{{x}}<br>PowPeg Out: %{{y:.4f}}<extra></extra>' }},
  ] : [
    {{ x: keys, y: keys.map(k => sumField(fpG[k] || [], 'value_rbtc')),
       name: 'Flyover In', type: 'bar', marker: {{ color: '#DEFF19', line: {{ width: 0 }} }},
       hovertemplate: '%{{x}}<br>Flyover In: %{{y:.4f}}<extra></extra>' }},
    {{ x: keys, y: keys.map(k => sumField(foG[k] || [], 'value_rbtc')),
       name: 'Flyover Out', type: 'bar', marker: {{ color: '#F0FF96', line: {{ width: 0 }} }},
       hovertemplate: '%{{x}}<br>Flyover Out: %{{y:.4f}}<extra></extra>' }},
    {{ x: keys, y: keys.map(k => sumField(ppG[k] || [], 'value_rbtc')),
       name: 'PowPeg In', type: 'bar', marker: {{ color: '#FF9100', line: {{ width: 0 }} }},
       hovertemplate: '%{{x}}<br>PowPeg In: %{{y:.4f}}<extra></extra>' }},
    {{ x: keys, y: keys.map(k => sumField(poG[k] || [], 'value_rbtc')),
       name: 'PowPeg Out', type: 'bar', marker: {{ color: '#FED8A7', line: {{ width: 0 }} }},
       hovertemplate: '%{{x}}<br>PowPeg Out: %{{y:.4f}}<extra></extra>' }},
  ];
  const volLayout = chartMode === 'bar'
    ? {{ ...baseLayout, height: 300, barmode: 'stack' }}
    : {{ ...baseLayout, height: 300 }};
  Plotly.newPlot('chart-volume-trend', volTraces, volLayout, cfg);

  // Tx count — stacked bar
  Plotly.newPlot('chart-count', [
    {{ x: keys, y: keys.map(k => (fpG[k] || []).length), name: 'Flyover In', type: 'bar',
       marker: {{ color: '#DEFF19', line: {{ width: 0 }} }}, hovertemplate: '%{{x}}<br>Flyover In: %{{y}}<extra></extra>' }},
    {{ x: keys, y: keys.map(k => (foG[k] || []).length), name: 'Flyover Out', type: 'bar',
       marker: {{ color: '#F0FF96', line: {{ width: 0 }} }}, hovertemplate: '%{{x}}<br>Flyover Out: %{{y}}<extra></extra>' }},
    {{ x: keys, y: keys.map(k => (ppG[k] || []).length), name: 'PowPeg In', type: 'bar',
       marker: {{ color: '#FF9100', line: {{ width: 0 }} }}, hovertemplate: '%{{x}}<br>PowPeg In: %{{y}}<extra></extra>' }},
    {{ x: keys, y: keys.map(k => (poG[k] || []).length), name: 'PowPeg Out', type: 'bar',
       marker: {{ color: '#FED8A7', line: {{ width: 0 }} }}, hovertemplate: '%{{x}}<br>PowPeg Out: %{{y}}<extra></extra>' }},
  ], {{ ...baseLayout, barmode: 'stack' }}, cfg);

  // Volume donut
  const fpVol = sumField(DATA.flyover_pegins, 'value_rbtc');
  const foVol = sumField(DATA.flyover_pegouts, 'value_rbtc');
  const ppVol = sumField(DATA.powpeg_pegins, 'value_rbtc');
  const poVol = sumField(DATA.powpeg_pegouts, 'value_rbtc');
  const total = fpVol + foVol + ppVol + poVol;

  Plotly.newPlot('chart-donut', [{{
    values: [fpVol, foVol, ppVol, poVol],
    labels: ['Flyover In', 'Flyover Out', 'PowPeg In', 'PowPeg Out'],
    type: 'pie',
    hole: 0.6,
    marker: {{ colors: ['#DEFF19', '#F0FF96', '#FF9100', '#FED8A7'] }},
    textinfo: 'percent',
    textfont: {{ color: '#000', size: 11, family: 'Inter, sans-serif' }},
    hovertemplate: '%{{label}}<br>%{{value:.4f}}<br>%{{percent}}<extra></extra>',
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
}}

let tablePage = 0;
const PAGE_SIZE = 10;

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

  let html = `<table>
    <thead><tr>
      <th>Period</th>
      <th><span class="th-dot" style="background:#DEFF19"></span>FO In</th>
      <th>Vol</th>
      <th><span class="th-dot" style="background:#F0FF96"></span>FO Out</th>
      <th>Vol</th>
      <th><span class="th-dot" style="background:#FF9100"></span>PP In</th>
      <th>Vol</th>
      <th><span class="th-dot" style="background:#FED8A7"></span>PP Out</th>
      <th>Vol</th>
      <th></th>
    </tr></thead><tbody>`;

  for (const key of pageKeys) {{
    const fp = fpG[key] || [], fo = foG[key] || [];
    const pp = ppG[key] || [], po = poG[key] || [];
    const rowId = 'row-' + key.replace(/[^a-zA-Z0-9]/g, '');

    html += `<tr>
      <td><strong>${{key}}</strong></td>
      <td>${{fp.length}}</td><td>${{fmt(sumField(fp, 'value_rbtc'))}}</td>
      <td>${{fo.length}}</td><td>${{fmt(sumField(fo, 'value_rbtc'))}}</td>
      <td>${{pp.length}}</td><td>${{fmt(sumField(pp, 'value_rbtc'))}}</td>
      <td>${{po.length}}</td><td>${{fmt(sumField(po, 'value_rbtc'))}}</td>
      <td><button class="expand-btn" onclick="toggleDetails('${{rowId}}', this)">&darr;</button></td>
    </tr>`;

    const txs = [
      ...fp.map(e => ({{ ...e, cat: 'FO In', bc: 'fo-pegin' }})),
      ...fo.map(e => ({{ ...e, cat: 'FO Out', bc: 'fo-pegout' }})),
      ...pp.map(e => ({{ ...e, cat: 'PP In', bc: 'pp-pegin' }})),
      ...po.map(e => ({{ ...e, cat: 'PP Out', bc: 'pp-pegout' }})),
    ].sort((a, b) => (b.timestamp || '').localeCompare(a.timestamp || ''));

    for (const tx of txs.slice(0, 20)) {{
      html += `<tr class="detail-row ${{rowId}}">
        <td colspan="2"><span class="badge ${{tx.bc}}">${{tx.cat}}</span></td>
        <td colspan="2"><a class="tx-link" href="https://rootstock.blockscout.com/tx/${{tx.tx_hash}}" target="_blank">${{shortHash(tx.tx_hash)}}</a></td>
        <td colspan="2">${{fmt(tx.value_rbtc, 6)}}</td>
        <td colspan="2">${{tx.timestamp ? new Date(tx.timestamp).toLocaleDateString() : 'N/A'}}</td>
        <td colspan="2">${{shortHash(tx.address)}}</td>
      </tr>`;
    }}
    if (txs.length > 20) {{
      html += `<tr class="detail-row ${{rowId}}"><td colspan="10" style="text-align:center;color:var(--muted)">... ${{txs.length - 20}} more</td></tr>`;
    }}
  }}

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
          <div class="lp-stat-sub">available</div>
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

function toggleDetails(rowId, btn) {{
  const rows = document.querySelectorAll('.' + rowId);
  const isOpen = rows.length > 0 && rows[0].classList.contains('open');
  rows.forEach(el => el.classList.toggle('open'));
  if (btn) btn.innerHTML = isOpen ? '&darr;' : '&uarr;';
}}

function setChartMode(mode) {{
  chartMode = mode;
  document.querySelectorAll('#vol-chart-toggle button').forEach(b => b.classList.remove('active'));
  event.target.classList.add('active');
  renderCharts();
}}

function renderAll() {{
  renderHeroKPIs();
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
