# Dhan Data Tools

This repository contains robust tools for fetching historical data from the DhanHQ API, optimized for algorithmic trading backtesting.

## 1. Dhan Data Downloader (`dhan_data_downloader.py`)
The **Primary All-in-One Downloader**. It is designed to download both index spot data and options premium data in a single automated flow.

### Key Features:
- **Combined Download**: Downloads **Spot Index OHLC** and **Options Premiums** (with OI) sequentially in one run.
- **Accurate Index Tracking**: Uses the `IDX_I` segment to ensure 100% accurate spot prices for Nifty 50, Sensex, and Bank Nifty.
- **Auto-Resume**: Scans your `data/` folder and automatically skips already-downloaded dates.
- **Expiry Selection**: Support for Weekly, Monthly, or Both (Separate folders).
- **Quality Reports**: Generates integrity reports for both Spot and Options data (Missing candles, OI coverage, Intraday gaps).
- **Holiday & Weekend Logic**: Automatically skips Saturdays/Sundays and identifies trading holidays.

## 2. Dhan Data Fetcher (`dhan_data_fetcher.py`)
The **Legacy/Simple Fetcher**. This is the original, minimal version of the script. It focuses only on downloading options data for a specified range without the auto-resume, spot data, or advanced folder management features.

---

## Setup

1. **Credentials**: Copy `keys.example.toml` to `keys.toml`.
   ```bash
   cp keys.example.toml keys.toml
   ```
   Fill in your `client_id` and `access_token`.
2. **Dependencies**: 
   ```bash
   pip install requests pandas tomli
   ```

## Usage (Dhan Data Downloader)

```bash
python3 dhan_data_downloader.py
```

### User Input Flow:
1. **[1] Index**: Select Nifty 50, Sensex, or Bank Nifty.
2. **[2] Interval**: 1m, 5m, 15m, 25m, or 60m.
3. **[3] Expiry Type**: Select Weekly, Monthly, or Both.
4. **[4] Date Range**: Enter start and end dates in `DD/MM/YYYY`.
5. **[5] Automated Execution**: The script will then sequentially fetch Spot data and Options data.

## Data Storage Structure

```text
data/
├── holidays.json        # Log of non-trading dates and gaps
├── NIFTY/
│   ├── spot/            # Index Spot Data (Nifty 50)
│   │   └── 1min/
│   └── 1min/
│       ├── weekly/      # Weekly Options
│       └── monthly/     # Monthly Options
├── SENSEX/
│   └── ...
└── BANKNIFTY/
    └── ...
```

### CSV Schema Highlights
- **Options**: `timestamp, open, high, low, close, volume, open_interest, strike_price, spot_price, index, option_type, strike_label`
- **Spot**: `timestamp, open, high, low, close, volume`

## Dhan API Limitations & Notes
- **Rate Limiting**: Throttled at 3.1s per request to stay within Dhan's data API limits.
- **Data Chunking**: Options are fetched in 15-day chunks; Spot is fetched in 90-day chunks for efficiency.
- **Weekends**: Saturdays and Sundays are automatically excluded from the fetch queue.
