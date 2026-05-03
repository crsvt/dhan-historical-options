# Dhan Options Data Fetcher

A robust, standalone utility for downloading historical options premiums from the DhanHQ API, specifically tailored for strategy backtesting.

## Setup

1. **Credentials**: Copy `keys.example.toml` to `keys.toml`.
   ```bash
   cp keys.example.toml keys.toml
   ```
   Fill in your `client_id` and `access_token`.
2. **Dependencies**: Ensure you have the required Python libraries.
   ```bash
   pip install requests pandas tomli
   ```

## Usage

Run the fetcher interactively:
```bash
python3 dhan_data_fetcher.py
```

### Input Requirements
- **Index**: Select Nifty 50, Sensex, or Bank Nifty.
- **Interval**: 1m, 5m, 15m, 25m, or 60m.
- **Date Format**: Enter dates in `DD/MM/YYYY` format (e.g., `30/04/2026`).

## Data Storage Structure

Data is organized into individual daily CSV files to ensure high performance and easy debugging.

**Folder Tree Example:**
```text
.
├── dhan_data_fetcher.py
├── keys.toml
├── keys.example.toml
├── .gitignore
└── data/
    ├── holidays.json        # Auto-generated list of non-trading dates
    ├── NIFTY/
    │   └── 1min/
    │       └── 2026/
    │           └── 04/
    │               ├── 2026-04-28.csv
    │               ├── 2026-04-29.csv
    │               └── 2026-04-30.csv
    ├── SENSEX/
    │   └── ...
    └── BANKNIFTY/
        └── ...
```

### CSV Schema
- `timestamp`: IST candle time (YYYY-MM-DD HH:MM:SS).
- `open`, `high`, `low`, `close`: Option premium prices.
- `volume`: Trading volume.
- `strike_price`: The actual strike price (e.g., 24050.0).
- `spot_price`: Underlying index price at that candle.
- `index`, `option_type`, `strike_label`: Metadata for filtering and strategy logic.

## Verification & Quality Reports

### Manual Verification Checkpoint
At the end of each download, the script prints a snapshot of the last candle. You should cross-check these values against your terminal or TradingView charts to ensure data accuracy:
```text
[MANUAL VERIFICATION - LAST CANDLE]
Timestamp : 2026-04-30 15:15:00
Spot Price : 24043.70
ATM Strike : 24050
ATM CALL   : 24050CE  Close: 194.70
ATM PUT    : 24050PE  Close: 163.75
```

### Data Quality Report
The script runs a post-download integrity check covering:
- **Trading Days**: Total days processed.
- **Missing Candles (%)**: Percentage of candles missing relative to expected market hours (9:15 to 15:30).
- **Duplicates**: Identification of overlapping data points.
- **Null/Zero Prices**: Flags missing or invalid price data.
- **Intraday Gaps**: Detects non-sequential timestamps *within* a trading day.

### Holiday Tracking (`holidays.json`)
To prevent "hallucination" during backtesting, the script automatically identifies dates that were requested but returned **no data** from the Dhan API.
- These dates are logged in `data/holidays.json`.
- When your backtest loader iterates through a range, it should check this file to distinguish between missing downloads and actual trading holidays.

## Dhan API Limitations & Notes
- **Rate Limiting**: Strictly enforced at 1 request per 3 seconds (3.1s throttle).
- **Holiday Gaps**: If the API returns no data for a requested date, it is marked in `holidays.json` and no CSV is created.
- **Data Retention**: Rolling options data for expired contracts is subject to Dhan's data retention policies.
