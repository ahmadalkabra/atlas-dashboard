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

    # Flyover peg-ins (CallForUser events) — include LP address (from_address)
    fp_pegins = []
    for e in flyover_pegins:
        if e.get("event") != "CallForUser":
            continue
        ts = parse_timestamp(e.get("block_timestamp", ""))
        fp_pegins.append({
            "tx_hash": e.get("tx_hash", ""),
            "block": e.get("block_number", 0),
            "timestamp": ts.isoformat() if ts else "",
            "value_rbtc": float(e.get("value_rbtc", 0)),
            "address": e.get("dest_address", ""),
            "lp_address": e.get("from_address", ""),
        })

    # Flyover peg-outs (PegOutDeposit events)
    fp_pegouts = extract_events(flyover_pegouts, "amount_rbtc", "sender", "PegOutDeposit")

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
<title>Rootstock Peg Dashboard — Flyover & PowPeg</title>
<script src="https://cdn.plot.ly/plotly-2.35.0.min.js"></script>
<style>
  :root {{
    --bg: #0f1117;
    --card: #1a1d27;
    --border: #2a2d3a;
    --text: #e1e4ea;
    --muted: #8b8fa3;
    --accent: #ff6b35;
    --accent2: #4ecdc4;
    --accent3: #45b7d1;
    --accent4: #f7dc6f;
    --green: #2ecc71;
    --red: #e74c3c;
  }}
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: var(--bg);
    color: var(--text);
    padding: 20px;
    min-height: 100vh;
  }}
  .header {{
    text-align: center;
    margin-bottom: 30px;
  }}
  .header h1 {{
    font-size: 28px;
    margin-bottom: 8px;
  }}
  .header .subtitle {{
    color: var(--muted);
    font-size: 14px;
  }}
  .controls {{
    display: flex;
    justify-content: center;
    gap: 10px;
    margin-bottom: 30px;
    flex-wrap: wrap;
  }}
  .controls button {{
    background: var(--card);
    border: 1px solid var(--border);
    color: var(--text);
    padding: 8px 20px;
    border-radius: 6px;
    cursor: pointer;
    font-size: 14px;
    transition: all 0.2s;
  }}
  .controls button:hover {{ border-color: var(--accent); }}
  .controls button.active {{
    background: var(--accent);
    border-color: var(--accent);
    color: #fff;
  }}
  .cards {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 16px;
    margin-bottom: 30px;
  }}
  .card {{
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 20px;
  }}
  .card .label {{
    color: var(--muted);
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 8px;
  }}
  .card .value {{
    font-size: 24px;
    font-weight: 700;
  }}
  .card .sub {{
    color: var(--muted);
    font-size: 13px;
    margin-top: 4px;
  }}
  .card.flyover .value {{ color: var(--accent); }}
  .card.powpeg .value {{ color: var(--accent2); }}
  .card.split .value {{ color: var(--accent3); }}
  .card.failure .value {{ color: var(--red); }}
  .charts {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
    margin-bottom: 30px;
  }}
  .chart-container {{
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 16px;
  }}
  .chart-container.full-width {{
    grid-column: 1 / -1;
  }}
  .chart-title {{
    font-size: 16px;
    font-weight: 600;
    margin-bottom: 12px;
  }}
  table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
  }}
  th, td {{
    padding: 10px 12px;
    text-align: right;
    border-bottom: 1px solid var(--border);
  }}
  th {{
    color: var(--muted);
    font-weight: 600;
    text-transform: uppercase;
    font-size: 11px;
    letter-spacing: 0.5px;
    position: sticky;
    top: 0;
    background: var(--card);
  }}
  th:first-child, td:first-child {{ text-align: left; }}
  tr:hover td {{ background: rgba(255,255,255,0.03); }}
  .table-container {{
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 16px;
    overflow-x: auto;
    margin-bottom: 30px;
  }}
  .table-container .chart-title {{ margin-bottom: 12px; }}
  .tx-link {{
    color: var(--accent3);
    text-decoration: none;
  }}
  .tx-link:hover {{ text-decoration: underline; }}
  .expand-btn {{
    background: none;
    border: none;
    color: var(--accent3);
    cursor: pointer;
    font-size: 12px;
    padding: 2px 8px;
  }}
  .expand-btn:hover {{ text-decoration: underline; }}
  .detail-row {{ display: none; }}
  .detail-row.open {{ display: table-row; }}
  .detail-row td {{
    background: rgba(0,0,0,0.2);
    padding: 4px 12px;
    font-size: 12px;
  }}
  @media (max-width: 768px) {{
    .charts {{ grid-template-columns: 1fr; }}
    .cards {{ grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); }}
  }}
</style>
</head>
<body>

<div class="header">
  <h1>Rootstock Peg Dashboard</h1>
  <p class="subtitle">Flyover & PowPeg — Transaction counts and volumes</p>
  <p class="subtitle" id="generated-at"></p>
</div>

<div class="controls">
  <button onclick="setPeriod('year')" id="btn-year">Year</button>
  <button onclick="setPeriod('quarter')" id="btn-quarter">Quarter</button>
  <button onclick="setPeriod('month')" id="btn-month" class="active">Month</button>
  <button onclick="setPeriod('week')" id="btn-week">Week</button>
  <button onclick="setPeriod('day')" id="btn-day">Day</button>
</div>

<div class="cards" id="summary-cards"></div>

<div class="charts">
  <div class="chart-container">
    <div class="chart-title">Transaction Count by Period</div>
    <div id="chart-count"></div>
  </div>
  <div class="chart-container">
    <div class="chart-title">Volume by Period (RBTC)</div>
    <div id="chart-volume"></div>
  </div>
  <div class="chart-container">
    <div class="chart-title">Cumulative Volume Over Time (RBTC)</div>
    <div id="chart-cumulative"></div>
  </div>
  <div class="chart-container">
    <div class="chart-title">Flyover vs PowPeg Share</div>
    <div id="chart-pie"></div>
  </div>
</div>

<div class="table-container">
  <div class="chart-title">Aggregated Data by Period</div>
  <div id="data-table"></div>
</div>

<div class="table-container">
  <div class="chart-title">LP Performance (Flyover)</div>
  <div id="lp-table"></div>
</div>

<script>
const DATA = {data_json};

let currentPeriod = 'month';

// --- Utility functions ---

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
    case 'year': return `${{y}}`;
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

function uniqueAddresses(events, field) {{
  const addrs = new Set();
  for (const e of events) {{
    if (e[field]) addrs.add(e[field].toLowerCase());
  }}
  return addrs;
}}

function fmt(n, decimals = 4) {{
  if (n >= 1000) return n.toLocaleString(undefined, {{ maximumFractionDigits: decimals }});
  return n.toFixed(decimals);
}}

function shortHash(h) {{
  if (!h) return '';
  return h.slice(0, 10) + '...' + h.slice(-6);
}}

// --- Rendering ---

function renderCards() {{
  const fp = DATA.flyover_pegins;
  const fo = DATA.flyover_pegouts;
  const pp = DATA.powpeg_pegins;
  const po = DATA.powpeg_pegouts;

  const fpCount = fp.length;
  const fpVol = sumField(fp, 'value_rbtc');
  const foCount = fo.length;
  const foVol = sumField(fo, 'value_rbtc');
  const ppCount = pp.length;
  const ppVol = sumField(pp, 'value_rbtc');
  const poCount = po.length;
  const poVol = sumField(po, 'value_rbtc');

  const totalFlyover = fpVol + foVol;
  const totalPowpeg = ppVol + poVol;
  const total = totalFlyover + totalPowpeg;
  const flyoverPct = total > 0 ? (totalFlyover / total * 100).toFixed(1) : '0';
  const powpegPct = total > 0 ? (totalPowpeg / total * 100).toFixed(1) : '0';

  const penalties = DATA.penalties.length;
  const refunds = DATA.refunds.length;
  const totalFlyoverTxs = fpCount + foCount;
  const failureRate = totalFlyoverTxs > 0 ? ((penalties + refunds) / totalFlyoverTxs * 100).toFixed(1) : '0';

  const allAddrs = new Set();
  fp.forEach(e => {{ if (e.address) allAddrs.add(e.address.toLowerCase()); }});
  fo.forEach(e => {{ if (e.address) allAddrs.add(e.address.toLowerCase()); }});
  pp.forEach(e => {{ if (e.address) allAddrs.add(e.address.toLowerCase()); }});
  po.forEach(e => {{ if (e.address) allAddrs.add(e.address.toLowerCase()); }});

  const html = `
    <div class="card flyover">
      <div class="label">Flyover Peg-In</div>
      <div class="value">${{fpCount}} txs</div>
      <div class="sub">${{fmt(fpVol)}} RBTC</div>
    </div>
    <div class="card flyover">
      <div class="label">Flyover Peg-Out</div>
      <div class="value">${{foCount}} txs</div>
      <div class="sub">${{fmt(foVol)}} RBTC</div>
    </div>
    <div class="card powpeg">
      <div class="label">PowPeg Peg-In</div>
      <div class="value">${{ppCount}} txs</div>
      <div class="sub">${{fmt(ppVol)}} RBTC</div>
    </div>
    <div class="card powpeg">
      <div class="label">PowPeg Peg-Out</div>
      <div class="value">${{poCount}} txs</div>
      <div class="sub">${{fmt(poVol)}} RBTC</div>
    </div>
    <div class="card split">
      <div class="label">Flyover vs PowPeg</div>
      <div class="value">${{flyoverPct}}% / ${{powpegPct}}%</div>
      <div class="sub">by volume</div>
    </div>
    <div class="card failure">
      <div class="label">Flyover Failure Rate</div>
      <div class="value">${{failureRate}}%</div>
      <div class="sub">${{penalties}} penalties, ${{refunds}} refunds</div>
    </div>
    <div class="card">
      <div class="label">Unique Addresses</div>
      <div class="value">${{allAddrs.size}}</div>
      <div class="sub">across all events</div>
    </div>
    <div class="card">
      <div class="label">Total Volume</div>
      <div class="value">${{fmt(total)}} RBTC</div>
      <div class="sub">all peg-in + peg-out</div>
    </div>
  `;

  // Add LP liquidity cards if available
  const lp = DATA.lp_info;
  if (lp && lp.lp_name) {{
    html += `
    <div class="card flyover">
      <div class="label">LP Peg-In Liquidity</div>
      <div class="value">${{fmt(lp.pegin_rbtc || 0)}} RBTC</div>
      <div class="sub">${{lp.lp_name}} — <a class="tx-link" href="https://rootstock.blockscout.com/address/${{lp.rbtc_wallet}}" target="_blank">${{lp.rbtc_wallet ? lp.rbtc_wallet.slice(0,10)+'...' : ''}}</a></div>
    </div>
    <div class="card flyover">
      <div class="label">LP Peg-Out Liquidity</div>
      <div class="value">${{fmt(lp.pegout_btc || 0)}} BTC</div>
      <div class="sub">${{lp.lp_name}} — <a class="tx-link" href="https://mempool.space/address/${{lp.btc_wallet}}" target="_blank">${{lp.btc_wallet ? lp.btc_wallet.slice(0,12)+'...' : ''}}</a></div>
    </div>`;
  }}

  document.getElementById('summary-cards').innerHTML = html;
}}

function renderCharts() {{
  const period = currentPeriod;
  const plotlyConfig = {{ displayModeBar: false, responsive: true }};
  const plotlyLayout = {{
    paper_bgcolor: 'transparent',
    plot_bgcolor: 'transparent',
    font: {{ color: '#8b8fa3', size: 12 }},
    margin: {{ l: 50, r: 20, t: 10, b: 40 }},
    xaxis: {{ gridcolor: '#2a2d3a' }},
    yaxis: {{ gridcolor: '#2a2d3a' }},
    legend: {{ orientation: 'h', y: -0.2 }},
    barmode: 'group',
  }};

  // Group data by period
  const fpGroups = groupBy(DATA.flyover_pegins, period);
  const foGroups = groupBy(DATA.flyover_pegouts, period);
  const ppGroups = groupBy(DATA.powpeg_pegins, period);
  const poGroups = groupBy(DATA.powpeg_pegouts, period);

  // Get all unique period keys, sorted
  const allKeys = [...new Set([
    ...Object.keys(fpGroups),
    ...Object.keys(foGroups),
    ...Object.keys(ppGroups),
    ...Object.keys(poGroups),
  ])].filter(k => k !== 'unknown').sort();

  // Count chart
  Plotly.newPlot('chart-count', [
    {{ x: allKeys, y: allKeys.map(k => (fpGroups[k] || []).length), name: 'Flyover Peg-In', type: 'bar', marker: {{ color: '#ff6b35' }} }},
    {{ x: allKeys, y: allKeys.map(k => (foGroups[k] || []).length), name: 'Flyover Peg-Out', type: 'bar', marker: {{ color: '#ff9a76' }} }},
    {{ x: allKeys, y: allKeys.map(k => (ppGroups[k] || []).length), name: 'PowPeg Peg-In', type: 'bar', marker: {{ color: '#4ecdc4' }} }},
    {{ x: allKeys, y: allKeys.map(k => (poGroups[k] || []).length), name: 'PowPeg Peg-Out', type: 'bar', marker: {{ color: '#7eddd6' }} }},
  ], plotlyLayout, plotlyConfig);

  // Volume chart
  Plotly.newPlot('chart-volume', [
    {{ x: allKeys, y: allKeys.map(k => sumField(fpGroups[k] || [], 'value_rbtc')), name: 'Flyover Peg-In', type: 'bar', marker: {{ color: '#ff6b35' }} }},
    {{ x: allKeys, y: allKeys.map(k => sumField(foGroups[k] || [], 'value_rbtc')), name: 'Flyover Peg-Out', type: 'bar', marker: {{ color: '#ff9a76' }} }},
    {{ x: allKeys, y: allKeys.map(k => sumField(ppGroups[k] || [], 'value_rbtc')), name: 'PowPeg Peg-In', type: 'bar', marker: {{ color: '#4ecdc4' }} }},
    {{ x: allKeys, y: allKeys.map(k => sumField(poGroups[k] || [], 'value_rbtc')), name: 'PowPeg Peg-Out', type: 'bar', marker: {{ color: '#7eddd6' }} }},
  ], plotlyLayout, plotlyConfig);

  // Cumulative chart
  let cumFP = 0, cumFO = 0, cumPP = 0, cumPO = 0;
  const cumFPData = [], cumFOData = [], cumPPData = [], cumPOData = [];
  for (const k of allKeys) {{
    cumFP += sumField(fpGroups[k] || [], 'value_rbtc');
    cumFO += sumField(foGroups[k] || [], 'value_rbtc');
    cumPP += sumField(ppGroups[k] || [], 'value_rbtc');
    cumPO += sumField(poGroups[k] || [], 'value_rbtc');
    cumFPData.push(cumFP);
    cumFOData.push(cumFO);
    cumPPData.push(cumPP);
    cumPOData.push(cumPO);
  }}
  Plotly.newPlot('chart-cumulative', [
    {{ x: allKeys, y: cumFPData, name: 'Flyover Peg-In', type: 'scatter', mode: 'lines', line: {{ color: '#ff6b35', width: 2 }} }},
    {{ x: allKeys, y: cumFOData, name: 'Flyover Peg-Out', type: 'scatter', mode: 'lines', line: {{ color: '#ff9a76', width: 2 }} }},
    {{ x: allKeys, y: cumPPData, name: 'PowPeg Peg-In', type: 'scatter', mode: 'lines', line: {{ color: '#4ecdc4', width: 2 }} }},
    {{ x: allKeys, y: cumPOData, name: 'PowPeg Peg-Out', type: 'scatter', mode: 'lines', line: {{ color: '#7eddd6', width: 2 }} }},
  ], {{...plotlyLayout, yaxis: {{ ...plotlyLayout.yaxis, title: 'RBTC' }} }}, plotlyConfig);

  // Pie chart
  const flyoverTotal = sumField(DATA.flyover_pegins, 'value_rbtc') + sumField(DATA.flyover_pegouts, 'value_rbtc');
  const powpegTotal = sumField(DATA.powpeg_pegins, 'value_rbtc') + sumField(DATA.powpeg_pegouts, 'value_rbtc');
  Plotly.newPlot('chart-pie', [{{
    values: [flyoverTotal, powpegTotal],
    labels: ['Flyover', 'PowPeg'],
    type: 'pie',
    marker: {{ colors: ['#ff6b35', '#4ecdc4'] }},
    textinfo: 'label+percent',
    textfont: {{ color: '#fff' }},
  }}], {{
    ...plotlyLayout,
    margin: {{ l: 20, r: 20, t: 10, b: 10 }},
    showlegend: false,
  }}, plotlyConfig);
}}

function renderTable() {{
  const period = currentPeriod;
  const fpGroups = groupBy(DATA.flyover_pegins, period);
  const foGroups = groupBy(DATA.flyover_pegouts, period);
  const ppGroups = groupBy(DATA.powpeg_pegins, period);
  const poGroups = groupBy(DATA.powpeg_pegouts, period);

  const allKeys = [...new Set([
    ...Object.keys(fpGroups),
    ...Object.keys(foGroups),
    ...Object.keys(ppGroups),
    ...Object.keys(poGroups),
  ])].filter(k => k !== 'unknown').sort().reverse();

  let html = `<table>
    <thead>
      <tr>
        <th>Period</th>
        <th>FO Peg-In #</th>
        <th>FO Peg-In Vol</th>
        <th>FO Peg-Out #</th>
        <th>FO Peg-Out Vol</th>
        <th>PP Peg-In #</th>
        <th>PP Peg-In Vol</th>
        <th>PP Peg-Out #</th>
        <th>PP Peg-Out Vol</th>
        <th></th>
      </tr>
    </thead>
    <tbody>`;

  for (const key of allKeys) {{
    const fp = fpGroups[key] || [];
    const fo = foGroups[key] || [];
    const pp = ppGroups[key] || [];
    const po = poGroups[key] || [];
    const rowId = 'row-' + key.replace(/[^a-zA-Z0-9]/g, '');

    html += `
      <tr>
        <td>${{key}}</td>
        <td>${{fp.length}}</td>
        <td>${{fmt(sumField(fp, 'value_rbtc'))}}</td>
        <td>${{fo.length}}</td>
        <td>${{fmt(sumField(fo, 'value_rbtc'))}}</td>
        <td>${{pp.length}}</td>
        <td>${{fmt(sumField(pp, 'value_rbtc'))}}</td>
        <td>${{po.length}}</td>
        <td>${{fmt(sumField(po, 'value_rbtc'))}}</td>
        <td><button class="expand-btn" onclick="toggleDetails('${{rowId}}')">details</button></td>
      </tr>`;

    // Detail rows for individual transactions
    const allTxs = [
      ...fp.map(e => ({{ ...e, category: 'FO Peg-In' }})),
      ...fo.map(e => ({{ ...e, category: 'FO Peg-Out' }})),
      ...pp.map(e => ({{ ...e, category: 'PP Peg-In' }})),
      ...po.map(e => ({{ ...e, category: 'PP Peg-Out' }})),
    ].sort((a, b) => (b.timestamp || '').localeCompare(a.timestamp || ''));

    for (const tx of allTxs.slice(0, 20)) {{
      html += `
        <tr class="detail-row ${{rowId}}">
          <td colspan="2">${{tx.category}}</td>
          <td colspan="2"><a class="tx-link" href="https://rootstock.blockscout.com/tx/${{tx.tx_hash}}" target="_blank">${{shortHash(tx.tx_hash)}}</a></td>
          <td colspan="2">${{fmt(tx.value_rbtc, 6)}} RBTC</td>
          <td colspan="2">${{tx.timestamp ? new Date(tx.timestamp).toLocaleDateString() : 'N/A'}}</td>
          <td colspan="2">${{shortHash(tx.address)}}</td>
        </tr>`;
    }}
    if (allTxs.length > 20) {{
      html += `<tr class="detail-row ${{rowId}}"><td colspan="10" style="text-align:center;color:var(--muted)">... and ${{allTxs.length - 20}} more</td></tr>`;
    }}
  }}

  html += '</tbody></table>';
  document.getElementById('data-table').innerHTML = html;
}}

function renderLPTable() {{
  // Group deliveries and penalties by LP address
  const lpData = {{}};

  for (const e of DATA.flyover_pegins) {{
    const addr = (e.lp_address || '').toLowerCase();
    if (!addr) continue;
    if (!lpData[addr]) lpData[addr] = {{ pegins: 0, peginVol: 0, penalties: 0, penaltyVol: 0 }};
    lpData[addr].pegins++;
    lpData[addr].peginVol += e.value_rbtc || 0;
  }}

  for (const e of DATA.penalties) {{
    const addr = (e.lp_address || '').toLowerCase();
    if (!addr) continue;
    if (!lpData[addr]) lpData[addr] = {{ pegins: 0, peginVol: 0, penalties: 0, penaltyVol: 0 }};
    lpData[addr].penalties++;
    lpData[addr].penaltyVol += e.penalty_rbtc || 0;
  }}

  const entries = Object.entries(lpData).sort((a, b) => b[1].peginVol - a[1].peginVol);

  if (entries.length === 0) {{
    document.getElementById('lp-table').innerHTML = '<p style="color:var(--muted)">No LP data available</p>';
    return;
  }}

  let html = `<table>
    <thead>
      <tr>
        <th>LP Address</th>
        <th>Deliveries</th>
        <th>Volume (RBTC)</th>
        <th>Penalties</th>
        <th>Penalty Amount (RBTC)</th>
      </tr>
    </thead>
    <tbody>`;

  for (const [addr, d] of entries) {{
    html += `
      <tr>
        <td><a class="tx-link" href="https://rootstock.blockscout.com/address/${{addr}}" target="_blank">${{shortHash(addr)}}</a></td>
        <td>${{d.pegins}}</td>
        <td>${{fmt(d.peginVol)}}</td>
        <td>${{d.penalties}}</td>
        <td>${{fmt(d.penaltyVol)}}</td>
      </tr>`;
  }}

  html += '</tbody></table>';
  document.getElementById('lp-table').innerHTML = html;
}}

function toggleDetails(rowId) {{
  document.querySelectorAll('.' + rowId).forEach(el => {{
    el.classList.toggle('open');
  }});
}}

function setPeriod(p) {{
  currentPeriod = p;
  document.querySelectorAll('.controls button').forEach(b => b.classList.remove('active'));
  document.getElementById('btn-' + p).classList.add('active');
  renderCharts();
  renderTable();
}}

// Initialize
document.getElementById('generated-at').textContent = 'Generated: ' + new Date(DATA.generated_at).toLocaleString();
try {{ renderCards(); }} catch(e) {{ console.error('renderCards failed:', e); }}
try {{ renderCharts(); }} catch(e) {{ console.error('renderCharts failed:', e); }}
try {{ renderTable(); }} catch(e) {{ console.error('renderTable failed:', e); }}
try {{ renderLPTable(); }} catch(e) {{ console.error('renderLPTable failed:', e); }}
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
