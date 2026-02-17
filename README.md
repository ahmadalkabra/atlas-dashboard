# Atlas Dashboard

A Flask-based web dashboard for visualizing Flyover and PowPeg event data. The dashboard displays metrics and interactive charts using Plotly.js.

## Prerequisites

- Python 3.14
- pipenv

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/ahmadalkabra/atlas-dashboard.git
   cd atlas-dashboard
   ```

2. Install dependencies using pipenv:
   ```bash
   pipenv install
   ```

## Running the Application

Start the Flask development server:
```bash
pipenv run python app.py
```

The dashboard will be available at `http://localhost:5000`.

### Available Endpoints

- `/` - Main dashboard with interactive charts
- `/api/data` - Raw dashboard data as JSON
- `/health` - Health check endpoint

## Data Files

The application loads JSON data from the `data/` directory:
- `flyover_pegins.json`
- `flyover_pegouts.json`
- `flyover_penalties.json`
- `flyover_refunds.json`
- `powpeg_pegins.json`
- `powpeg_pegouts.json`
- `flyover_lp_info.json`
- `btc_locked_stats.json`

Data is reloaded on each request, so changes to these files are reflected immediately.
