# Atlas Bridge — February 2026 Report

**Period:** February 1–28, 2026
**Data source:** On-chain (Blockscout), Flyover LPS API
**Coverage:** PowPeg + Flyover only (swap routes not yet tracked)

---

## Key Metrics

| Metric | February | January | MoM Change |
|---|---|---|---|
| Total volume | 153.76 rBTC | 337.16 rBTC | -54% |
| Total transactions | 110 | 148 | -26% |
| Avg transaction size | 1.40 rBTC | 2.28 rBTC | -39% |
| Unique wallets | 71 | — | — |
| Net flow | -85.66 rBTC | -304.59 rBTC | Improved |

> January's volume was inflated by 3 whale PowPeg peg-outs (100 + 82 + 70 rBTC). Excluding those, February's activity was comparable.

---

## Volume by Operation

| Operation | Volume (rBTC) | Share | MoM Change |
|---|---|---|---|
| PowPeg Peg-Out | 61.95 | 40.3% | -79% |
| Flyover Peg-Out | 57.76 | 37.6% | +171% |
| PowPeg Peg-In | 27.75 | 18.0% | +106% |
| Flyover Peg-In | 6.30 | 4.1% | +125% |

**Transaction share:** Flyover Out dominates at 44.5% of transactions (49 txs), followed by PowPeg In at 22.7% (25 txs), Flyover In at 20.9% (23 txs), and PowPeg Out at 11.8% (13 txs).

---

## Flow Analysis

**3.5x more volume going out than coming in.** Flyover is 9:1 outbound, PowPeg is 2.2:1.

| Bridge | In (rBTC) | Out (rBTC) | Net (rBTC) |
|---|---|---|---|
| Flyover | 6.30 | 57.76 | -51.46 |
| PowPeg | 27.75 | 61.95 | -34.20 |
| **Total** | **34.05** | **119.71** | **-85.66** |

February's outflow was driven by 5 large transactions:

| Date | Operation | Amount (rBTC) |
|---|---|---|
| Feb 4 | PowPeg Out | 18.48 |
| Feb 21 | PowPeg Out | 17.32 |
| Feb 6 | PowPeg Out | 17.18 |
| Feb 6 | Flyover Out | 15.02 |
| Feb 4 | Flyover Out | 15.02 |

The two Flyover peg-outs hit the 15 rBTC maximum — users may be constrained by LP limits.

---

## 3-Month Trend

| Metric | December | January | February |
|---|---|---|---|
| Flyover In | 3.30 | 2.80 | 6.30 |
| Flyover Out | 20.47 | 21.29 | 57.76 |
| PowPeg In | 14.98 | 13.48 | 27.75 |
| PowPeg Out | 51.29 | 299.58 | 61.95 |
| **Net Flow** | **-53.48** | **-304.59** | **-85.66** |

Persistent net outflow across all three months. January was extreme due to whale peg-outs. February normalized but remained heavily outbound.

---

## Highlights

- **Peg-in volume doubled** — PowPeg In +106%, Flyover In +125% (off a low base). First sustained increase in inflows.
- **Flyover peg-out surged** — 57.76 rBTC (+171%), indicating strong demand for fast exits from Rootstock.
- **PowPeg peg-out normalized** — 61.95 rBTC vs January's anomalous 299.58 rBTC (whale-driven).

---

## Focus Areas

### 1. Peg-in imbalance

Inflows are a fraction of outflows. Flyover In is just 4% of total volume. What's driving users to other peg-in routes (exchanges, Boltz, Changelly)? This data is not yet visible — swap route coverage is the top priority for dashboard expansion.

### 2. Flyover max-limit transactions

Two February peg-outs hit the 15 rBTC ceiling exactly. Users may need higher limits. Worth assessing whether LP limit increases are feasible.

### 3. March early signal

First 12 days of March show 0 PowPeg peg-ins and only 0.04 rBTC via Flyover peg-in. Peg-out activity continues (26.83 rBTC out). If this persists, it signals either seasonal slowdown or a shift to swap routes the dashboard doesn't track.

### 4. Swap route blindspot

This report covers PowPeg and Flyover only. Boltz and Changelly handle an estimated ~7x the daily volume but are not yet instrumented. Adding all-route coverage would fundamentally change the picture. This is blocked on dev team (swap route transaction persistence).

---

*Generated from Atlas Dashboard on-chain data. March 12, 2026.*
