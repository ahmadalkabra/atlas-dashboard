"""
Flask application server for Atlas Dashboard.

Serves the dashboard dynamically, reloading JSON data from /data
on each request so changes are reflected immediately.
"""

from flask import Flask, Response, jsonify
from generate_report import (
    load_json,
    build_dashboard_data,
    generate_html,
)

app = Flask(__name__)


def load_all_data() -> dict:
    """Load all JSON data files and build dashboard data."""
    flyover_pegins = load_json("flyover_pegins.json")
    flyover_pegouts = load_json("flyover_pegouts.json")
    flyover_penalties = load_json("flyover_penalties.json")
    flyover_refunds = load_json("flyover_refunds.json")
    powpeg_pegins = load_json("powpeg_pegins.json")
    powpeg_pegouts = load_json("powpeg_pegouts.json")
    lp_info = load_json("flyover_lp_info.json")
    btc_locked_stats = load_json("btc_locked_stats.json")

    return build_dashboard_data(
        flyover_pegins,
        flyover_pegouts,
        flyover_penalties,
        flyover_refunds,
        powpeg_pegins,
        powpeg_pegouts,
        lp_info=lp_info if isinstance(lp_info, dict) else {},
        btc_locked_stats=btc_locked_stats if isinstance(btc_locked_stats, dict) else {},
    )


@app.route("/")
def dashboard():
    """Serve the dashboard HTML, regenerated on each request."""
    data = load_all_data()
    html = generate_html(data)
    return Response(html, mimetype="text/html")


@app.route("/api/data")
def api_data():
    """Return the raw dashboard data as JSON."""
    data = load_all_data()
    return jsonify(data)


@app.route("/health")
def health():
    """Health check endpoint."""
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
