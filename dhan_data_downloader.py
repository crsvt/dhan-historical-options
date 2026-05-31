import os
import sys
import time
import json
import argparse
import requests
import pandas as pd
import warnings
from datetime import datetime, timedelta

# Suppress pandas warnings
warnings.filterwarnings("ignore")

# Try to import tomllib (Python 3.11+) or tomli/toml
try:
    import tomllib
except ImportError:
    try:
        import tomli as tomllib
    except ImportError:
        import toml as tomllib

# ── Configuration ─────────────────────────────────────────────────────────────

BASE_URL = "https://api.dhan.co/v2"

# ── Dhan Client ───────────────────────────────────────────────────────────────

class DhanClient:
    def __init__(self, access_token, client_id=None):
        self.access_token = access_token
        self.client_id = client_id
        self.headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'access-token': self.access_token
        }
        if self.client_id:
            self.headers['client-id'] = self.client_id
            
        self.last_call_time = 0
        # Complies with 5 requests/sec limit for Data APIs (0.25s delay)
        self.rate_limit_delay = 0.25 

    def _throttle(self):
        elapsed = time.time() - self.last_call_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self.last_call_time = time.time()

    def _make_post_request(self, url, payload, max_retries=5):
        for attempt in range(max_retries):
            self._throttle()
            try:
                response = requests.post(url, headers=self.headers, json=payload, timeout=30)
                if response.status_code == 200:
                    return response.json()
                
                # Check for rate limit error (HTTP 429 or DH-904)
                is_rate_limit = (
                    response.status_code == 429 or
                    "Too many requests" in response.text or
                    "DH-904" in response.text
                )
                
                if is_rate_limit:
                    backoff = (2 ** attempt) * 2.0  # 2s, 4s, 8s, 16s, 32s
                    sys.stdout.write(f"\n  [!] Rate limit reached (429/DH-904). Retrying in {backoff:.1f}s (Attempt {attempt+1}/{max_retries})...\n")
                    sys.stdout.flush()
                    time.sleep(backoff)
                    continue
                
                return {"error": response.text, "status_code": response.status_code}
            except Exception as e:
                if attempt == max_retries - 1:
                    return {"error": str(e), "status_code": 500}
                time.sleep(2.0)
        return {"error": "Max retries exceeded due to rate limits", "status_code": 429}

    def get_spot_data(self, payload):
        url = f"{BASE_URL}/charts/intraday"
        return self._make_post_request(url, payload)

    def get_spot_daily_data(self, payload):
        url = f"{BASE_URL}/charts/historical"
        return self._make_post_request(url, payload)

    def get_rolling_options(self, payload):
        url = f"{BASE_URL}/charts/rollingoption"
        return self._make_post_request(url, payload)


# ── Progress Tracker ─────────────────────────────────────────────────────────

class ProgressTracker:
    def __init__(self, total_tasks):
        self.total_tasks = total_tasks
        self.completed_tasks = 0
        self.skipped_tasks = 0
        self.fetch_only_completed = 0
        self.fetch_time_total = 0
        self.start_time = time.time()
        self.last_fetch_start = 0

    def start_fetch(self):
        self.last_fetch_start = time.time()

    def update(self, count=1):
        if self.last_fetch_start > 0:
            self.fetch_time_total += (time.time() - self.last_fetch_start)
            self.fetch_only_completed += 1
            self.last_fetch_start = 0
        self.completed_tasks += count
        self._render()

    def skip(self, count=1):
        self.skipped_tasks += count
        self.completed_tasks += count
        self._render()

    def _render(self):
        avg_time = self.fetch_time_total / self.fetch_only_completed if self.fetch_only_completed > 0 else 0
        remaining = avg_time * (self.total_tasks - self.completed_tasks)
        
        percent = (self.completed_tasks / self.total_tasks) * 100 if self.total_tasks > 0 else 100
        mins, secs = divmod(int(remaining), 60)
        
        sys.stdout.write(f"\r    Progress: {percent:5.1f}% | Est. Remaining: {mins:02d}m {secs:02d}s | Done: {self.completed_tasks}/{self.total_tasks} | Skipped: {self.skipped_tasks} ")
        sys.stdout.flush()


# ── Data Quality Report ───────────────────────────────────────────────────────

def run_data_quality_report(df, interval, label="DATA"):
    if df.empty:
        return
    
    print("\n" + "="*45)
    print(f"      {label} QUALITY REPORT ({interval} timeframe)")
    print("="*45)
    
    expected_per_day = {"1": 375, "5": 75, "15": 25, "25": 15, "60": 7, "daily": 1}
    unique_days = df['timestamp'].dt.date.unique()
    num_days = len(unique_days)
    expected_total = expected_per_day.get(str(interval), 0) * num_days
    
    if 'strike_label' in df.columns:
        counts = df.groupby(['strike_label', 'option_type']).size()
        avg_candles = counts.mean()
        duplicates = df.duplicated(subset=['timestamp', 'option_type', 'strike_label']).sum()
    else:
        avg_candles = len(df)
        duplicates = df.duplicated(subset=['timestamp']).sum()
    
    missing_pct = max(0, (expected_total - avg_candles) / expected_total * 100) if expected_total > 0 else 0
    null_premiums = df['close'].isna().sum()
    zero_premiums = (df['close'] == 0).sum()
    
    gap_found = False
    if str(interval) != "daily":
        group_cols = ['strike_label', 'option_type', df['timestamp'].dt.date] if 'strike_label' in df.columns else [df['timestamp'].dt.date]
        for _, group in df.groupby(group_cols):
            group = group.sort_values('timestamp')
            if len(group) > 1:
                diffs = group['timestamp'].diff().dropna()
                threshold = pd.Timedelta(minutes=int(interval) * 2.5)
                if (diffs > threshold).any():
                    gap_found = True
                    break

    print(f"  Trading Days      : {num_days}")
    print(f"  Avg Candles/Day   : {avg_candles/num_days if num_days > 0 else 0:.1f} (Expected: {expected_per_day.get(str(interval), 0)})")
    print(f"  Missing Candles   : {missing_pct:.1f}%")
    print(f"  Duplicates        : {duplicates}")
    print(f"  Null/Zero Prices  : {null_premiums + zero_premiums}")
    print(f"  Intraday Gaps     : {'YES' if gap_found else 'NO'}")
    
    if 'open_interest' in df.columns:
        oi_coverage = (df['open_interest'] > 0).sum() / len(df) * 100 if len(df) > 0 else 0
        print(f"  OI Coverage       : {oi_coverage:.1f}%")
        if oi_coverage < 50:
            print("\n  [!] Low OI coverage — OI-based filters unreliable for this period.")

    if missing_pct > 15 or duplicates > 0 or (null_premiums + zero_premiums) > 0:
        print("\n  [!] Issues detected. Verify data integrity manually.")
    else:
        print("\n  [OK] Data quality is good.")
    print("="*45)


# ── Option Aggregation Engine (Intraday -> Multi-timeframe) ──────────────────

def aggregate_options_to_timeframe(input_dir, output_dir, interval, year, month):
    """
    Scans the downloaded 1-minute level options files, aggregates them to the target interval
    (5, 15, 25, 60 min, or daily), and writes the resampled files to output_dir.
    """
    src_path = os.path.join(input_dir, year, month)
    if not os.path.exists(src_path):
        return
        
    for fname in sorted(os.listdir(src_path)):
        if not fname.endswith(".csv"):
            continue
        
        # Performance Optimization: Skip local aggregation if the file exists and is not corrupted (>1KB)
        dest_path = os.path.join(output_dir, year, month)
        dest_file = os.path.join(dest_path, fname)
        if os.path.exists(dest_file) and os.path.getsize(dest_file) > 1000:
            continue
            
        file_path = os.path.join(src_path, fname)
        try:
            df = pd.read_csv(file_path)
            if df.empty:
                continue
            
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            if str(interval) == "daily":
                # Daily OHLC + OI + Spot strike-wise
                aggregated = []
                for (slabel, otype), group in df.groupby(['strike_label', 'option_type']):
                    group = group.sort_values('timestamp')
                    daily_row = {
                        "timestamp": pd.Timestamp(group['timestamp'].dt.date.iloc[0]),
                        "open": group['open'].iloc[0],
                        "high": group['high'].max(),
                        "low": group['low'].min(),
                        "close": group['close'].iloc[-1],
                        "volume": int(group['volume'].sum()),
                        "open_interest": int(group['open_interest'].iloc[-1]), # EOD OI
                        "strike_price": group['strike_price'].iloc[0],
                        "spot_price": group['spot_price'].iloc[-1], # EOD Spot
                        "index": group['index'].iloc[0],
                        "option_type": otype,
                        "strike_label": slabel
                    }
                    if "implied_volatility" in group.columns:
                        daily_row["implied_volatility"] = group["implied_volatility"].iloc[-1]
                    aggregated.append(daily_row)
                
                if aggregated:
                    out_df = pd.DataFrame(aggregated).sort_values(['strike_label', 'option_type'])
                    dest_path = os.path.join(output_dir, year, month)
                    os.makedirs(dest_path, exist_ok=True)
                    out_df.to_csv(os.path.join(dest_path, fname), index=False)
            else:
                # Intraday resampling (5, 15, 25, 60 minutes)
                aggregated = []
                for (slabel, otype), group in df.groupby(['strike_label', 'option_type']):
                    group = group.sort_values('timestamp')
                    group.set_index('timestamp', inplace=True)
                    
                    resample_rule = f"{interval}Min" if str(interval) != "60" else "60Min"
                    # Resample rules: label='left' is standard in financial charts
                    resampled = group.resample(resample_rule, label='left').agg({
                        "open": "first",
                        "high": "max",
                        "low": "min",
                        "close": "last",
                        "volume": "sum",
                        "open_interest": "last",
                        "strike_price": "first",
                        "spot_price": "last",
                        "index": "first"
                    })
                    
                    if "implied_volatility" in group.columns:
                        resampled["implied_volatility"] = group["implied_volatility"].resample(resample_rule, label='left').last()
                        
                    resampled.dropna(subset=['close'], inplace=True)
                    resampled.reset_index(inplace=True)
                    resampled['option_type'] = otype
                    resampled['strike_label'] = slabel
                    aggregated.append(resampled)
                    
                if aggregated:
                    out_df = pd.concat(aggregated, ignore_index=True).sort_values(['timestamp', 'strike_label', 'option_type'])
                    dest_path = os.path.join(output_dir, year, month)
                    os.makedirs(dest_path, exist_ok=True)
                    out_df.to_csv(os.path.join(dest_path, fname), index=False)
                    
        except Exception as e:
            print(f"  [ERROR] Failed to aggregate to {interval} timeframe for {file_path}: {e}")


# ── Credentials Loader ─────────────────────────────────────────────────────────

def load_credentials():
    path = os.path.join(os.path.dirname(__file__), "keys.toml")
    if not os.path.exists(path):
        return None, None
        
    # Check Python version to use standard tomllib or toml fallback
    try:
        if sys.version_info >= (3, 11):
            with open(path, "rb") as f:
                config = tomllib.load(f)
        else:
            with open(path, "r") as f:
                config = tomllib.load(f)
        dhan = config.get("broker", {}).get("dhan", {})
        return dhan.get("client_id"), dhan.get("access_token")
    except Exception:
        # Robust regex fallback
        try:
            with open(path, "r") as f:
                content = f.read()
            import re
            client_match = re.search(r'client_id\s*=\s*"([^"]+)"', content)
            token_match = re.search(r'access_token\s*=\s*"([^"]+)"', content)
            client_id = client_match.group(1) if client_match else None
            access_token = token_match.group(1) if token_match else None
            return client_id, access_token
        except Exception:
            return None, None


# ── Core Download Routines ────────────────────────────────────────────────────

def load_holidays_cache():
    h_path = os.path.join("data", "holidays.json")
    if os.path.exists(h_path):
        try:
            with open(h_path, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def download_spot_data(client, index_meta_item, interval, start_date, end_date, tracker):
    info = index_meta_item
    print(f"\n[SPOT] Fetching {info['dir']} Spot Data ({interval} timeframe)...")
    
    # 90-day chunking
    spot_chunks = []
    curr = start_date
    while curr <= end_date:
        chunk_end = curr + timedelta(days=89)
        if chunk_end > end_date: 
            chunk_end = end_date
        spot_chunks.append((curr, chunk_end))
        curr = chunk_end + timedelta(days=1)
        
    for chunk_start, chunk_end in spot_chunks:
        chunk_range = [(chunk_start + timedelta(days=x)) for x in range((chunk_end - chunk_start).days + 1)]
        chunk_trading_days = [d for d in chunk_range if d.weekday() < 5]
        
        if not chunk_trading_days:
            tracker.skip(1)
            continue
            
        # For daily spot data, we consolidate everything in one file, so we fetch if any part is needed
        # For intraday spot data, we check daily files
        if interval == "daily":
            all_exist = False
        else:
            all_exist = True
            holidays_cache = load_holidays_cache()
            for d in chunk_trading_days:
                y, m, ds = d.strftime("%Y"), d.strftime("%m"), d.strftime("%Y-%m-%d")
                fpath = os.path.join("data", info['dir'], "spot", f"{interval}", y, m, f"{ds}.csv")
                
                is_holiday = False
                if info['dir'] in holidays_cache:
                    for h_str in holidays_cache[info['dir']]:
                        if ds in h_str:
                            is_holiday = True
                            break
                            
                # Ensure file exists, is not a holiday, and is not corrupted (at least 1KB in size)
                is_corrupted = os.path.exists(fpath) and os.path.getsize(fpath) < 1000
                if (not os.path.exists(fpath) or is_corrupted) and not is_holiday:
                    all_exist = False
                    break
        
        if all_exist:
            tracker.skip(1)
            continue

        tracker.start_fetch()
        
        if interval == "daily":
            payload = {
                "securityId": str(info['id']),
                "exchangeSegment": info['spot_seg'],
                "instrument": "INDEX",
                "fromDate": chunk_start.strftime("%Y-%m-%d"),
                "toDate": chunk_end.strftime("%Y-%m-%d")
            }
            resp = client.get_spot_daily_data(payload)
        else:
            payload = {
                "securityId": str(info['id']),
                "exchangeSegment": info['spot_seg'],
                "instrument": "INDEX",
                "interval": str(interval),
                "fromDate": chunk_start.strftime("%Y-%m-%d"),
                "toDate": chunk_end.strftime("%Y-%m-%d")
            }
            resp = client.get_spot_data(payload)
            
        if resp and "timestamp" in resp and len(resp["timestamp"]) > 0:
            df_chunk = pd.DataFrame({
                "timestamp": pd.to_datetime(resp["timestamp"], unit='s'),
                "open": resp.get("open", []),
                "high": resp.get("high", []),
                "low": resp.get("low", []),
                "close": resp.get("close", []),
                "volume": resp.get("volume", [])
            })
            
            # Localize and convert timezone
            df_chunk['timestamp'] = df_chunk['timestamp'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)
            df_chunk = df_chunk.drop_duplicates(subset=['timestamp']).sort_values('timestamp')
            
            # Save Immediately
            if interval == "daily":
                f_dir = os.path.join("data", info['dir'], "spot", "daily")
                os.makedirs(f_dir, exist_ok=True)
                f_path = os.path.join(f_dir, "daily.csv")
                if os.path.exists(f_path):
                    try:
                        existing = pd.read_csv(f_path)
                        existing['timestamp'] = pd.to_datetime(existing['timestamp'])
                        combined = pd.concat([existing, df_chunk], ignore_index=True)
                        combined = combined.drop_duplicates(subset=['timestamp']).sort_values('timestamp')
                        combined.to_csv(f_path, index=False)
                    except Exception:
                        df_chunk.to_csv(f_path, index=False)
                else:
                    df_chunk.to_csv(f_path, index=False)
            else:
                for day, day_df in df_chunk.groupby(df_chunk['timestamp'].dt.date):
                    year, month, d_str = day.strftime("%Y"), day.strftime("%m"), day.strftime("%Y-%m-%d")
                    f_dir = os.path.join("data", info['dir'], "spot", f"{interval}", year, month)
                    os.makedirs(f_dir, exist_ok=True)
                    day_df.to_csv(os.path.join(f_dir, f"{d_str}.csv"), index=False)
                    
        tracker.update(1)


def download_options_data(client, index_meta_item, interval, start_date, end_date, expiry_configs, tracker):
    info = index_meta_item
    print(f"\n[OPTIONS] Fetching {info['dir']} Options Data ({interval} timeframe)...")
    
    # 30-day chunking
    opt_chunks = []
    curr = start_date
    while curr <= end_date:
        chunk_end = curr + timedelta(days=29)
        if chunk_end > end_date: 
            chunk_end = end_date
        opt_chunks.append((curr, chunk_end))
        curr = chunk_end + timedelta(days=1)

    # Extended options strike list (+/- 10 strikes ATM)
    strikes_labels = ["ATM"] + [f"ATM+{i}" for i in range(1, 11)] + [f"ATM-{i}" for i in range(1, 11)]
    types = ["CALL", "PUT"]
    
    for config in expiry_configs:
        for chunk_start, chunk_end in opt_chunks:
            chunk_range = [(chunk_start + timedelta(days=x)) for x in range((chunk_end - chunk_start).days + 1)]
            chunk_trading_days = [d for d in chunk_range if d.weekday() < 5]
            
            if not chunk_trading_days:
                tracker.skip(len(strikes_labels) * len(types))
                continue
                
            # For daily options, we download 1min data first, then aggregate, so check daily files
            check_interval = "1" if interval == "daily" else interval
            
            all_exist = True
            holidays_cache = load_holidays_cache()
            for d in chunk_trading_days:
                y, m, ds = d.strftime("%Y"), d.strftime("%m"), d.strftime("%Y-%m-%d")
                fpath = os.path.join("data", info['dir'], f"{check_interval}min", config['subfolder'], y, m, f"{ds}.csv")
                
                is_holiday = False
                if info['dir'] in holidays_cache:
                    for h_str in holidays_cache[info['dir']]:
                        if ds in h_str:
                            is_holiday = True
                            break
                            
                # Ensure file exists, is not a holiday, and is not corrupted (at least 10KB in size)
                is_corrupted = os.path.exists(fpath) and os.path.getsize(fpath) < 10000
                if (not os.path.exists(fpath) or is_corrupted) and not is_holiday:
                    all_exist = False
                    break
                    
            if all_exist:
                tracker.skip(len(strikes_labels) * len(types))
                continue

            chunk_rows = []
            
            for slabel in strikes_labels:
                for otype in types:
                    payload = {
                        "exchangeSegment": info['segment'],
                        "interval": str(check_interval),
                        "securityId": info['id'],
                        "instrument": "OPTIDX",
                        "expiryFlag": config['flag'], 
                        "expiryCode": config['code'],
                        "strike": slabel, 
                        "drvOptionType": otype,
                        "requiredData": ["open", "high", "low", "close", "volume", "strike", "spot", "oi", "iv"],
                        "fromDate": chunk_start.strftime("%Y-%m-%d"),
                        "toDate": chunk_end.strftime("%Y-%m-%d")
                    }
                    
                    tracker.start_fetch()
                    resp = client.get_rolling_options(payload)
                    
                    if resp and "data" in resp:
                        key = 'ce' if otype == 'CALL' else 'pe'
                        o_data = resp['data'].get(key, {})
                        if o_data and "timestamp" in o_data and len(o_data["timestamp"]) > 0:
                            df_c = pd.DataFrame({
                                "timestamp": pd.to_datetime(o_data["timestamp"], unit='s'),
                                "open": o_data.get("open", []),
                                "high": o_data.get("high", []),
                                "low": o_data.get("low", []),
                                "close": o_data.get("close", []),
                                "volume": o_data.get("volume", []),
                                "open_interest": o_data.get("oi", []),
                                "strike_price": o_data.get("strike", []),
                                "spot_price": o_data.get("spot", []),
                                "implied_volatility": o_data.get("iv", [])
                            })
                            df_c['open_interest'] = df_c['open_interest'].fillna(0).astype(int)
                            df_c['index'] = info['dir']
                            df_c['option_type'] = "CE" if otype == "CALL" else "PE"
                            df_c['strike_label'] = slabel
                            chunk_rows.append(df_c)
                            
                    tracker.update(1)
            
            # Save the chunk immediately once all strikes/types are done for safety
            if chunk_rows:
                opt_df = pd.concat(chunk_rows, ignore_index=True)
                opt_df['timestamp'] = opt_df['timestamp'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)
                opt_df = opt_df.drop_duplicates(subset=['timestamp', 'option_type', 'strike_label']).sort_values(['timestamp', 'strike_label', 'option_type'])
                
                for day, day_df in opt_df.groupby(opt_df['timestamp'].dt.date):
                    year, month, d_str = day.strftime("%Y"), day.strftime("%m"), day.strftime("%Y-%m-%d")
                    f_path = os.path.join("data", info['dir'], f"{check_interval}min", config['subfolder'], year, month)
                    os.makedirs(f_path, exist_ok=True)
                    day_df.to_csv(os.path.join(f_path, f"{d_str}.csv"), index=False)
                    



# ── Main Controller ───────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="Dhan Historical Data Downloader (Spot & Options)")
    parser.add_argument("--batch", action="store_true", help="Run in completely automated batch mode")
    parser.add_argument("--indices", type=str, default="NIFTY,SENSEX,BANKNIFTY,FINNIFTY,INDIAVIX", help="Comma-separated indices (NIFTY, SENSEX, BANKNIFTY, FINNIFTY, INDIAVIX)")
    parser.add_argument("--intervals", type=str, default="1,5,15,25,60,daily", help="Comma-separated intervals (1, 5, 15, 25, 60, daily)")
    parser.add_argument("--expiry", type=str, default="both", choices=["weekly", "monthly", "both"], help="Expiry types to fetch")
    parser.add_argument("--start", type=str, help="Start date in DD/MM/YYYY format")
    parser.add_argument("--end", type=str, help="End date in DD/MM/YYYY format")
    return parser.parse_args()


def main():
    try:
        print("═"*80)
        print("    DHAN ULTIMATE HISTORICAL DATA DOWNLOADER (SPOT + OPTIONS)")
        print("═"*80)

        # 1. Load Credentials
        client_id, access_token = load_credentials()
        if not access_token:
            print("[!] Error: Dhan access_token not found in keys.toml under [broker.dhan]")
            sys.exit(1)

        client = DhanClient(access_token, client_id)
        args = parse_args()
        
        index_meta = {
            "NIFTY":      {"id": 13, "lot": 65, "segment": "NSE_FNO", "dir": "NIFTY", "spot_seg": "IDX_I"},
            "SENSEX":     {"id": 51, "lot": 20, "segment": "BSE_FNO", "dir": "SENSEX", "spot_seg": "IDX_I"},
            "BANKNIFTY":  {"id": 25, "lot": 15, "segment": "NSE_FNO", "dir": "BANKNIFTY", "spot_seg": "IDX_I"},
            "FINNIFTY":   {"id": 27, "lot": 40, "segment": "NSE_FNO", "dir": "FINNIFTY", "spot_seg": "IDX_I"},
            "INDIAVIX":   {"id": 21, "lot": 0,  "segment": "",        "dir": "INDIAVIX",  "spot_seg": "IDX_I"}
        }

        # Setup selections
        if args.batch:
            print("\n  [!] Running in Automated Batch Mode...")
            selected_indices = [idx.strip().upper() for idx in args.indices.split(",") if idx.strip().upper() in index_meta]
            selected_intervals = [tf.strip() for tf in args.intervals.split(",")]
            
            # Setup Expiries
            if args.expiry == "weekly":
                expiry_configs = [{"flag": "WEEK", "code": 1, "subfolder": "weekly"}]
            elif args.expiry == "monthly":
                expiry_configs = [{"flag": "MONTH", "code": 1, "subfolder": "monthly"}]
            else:
                expiry_configs = [
                    {"flag": "WEEK", "code": 1, "subfolder": "weekly"},
                    {"flag": "MONTH", "code": 1, "subfolder": "monthly"}
                ]
                
            # Default Start/End (5 years ago to today, standard maximum range for Dhan)
            today = datetime.now()
            default_start = today - timedelta(days=5*365) # ~5 years
            start_date = datetime.strptime(args.start, "%d/%m/%Y") if args.start else default_start
            end_date = datetime.strptime(args.end, "%d/%m/%Y") if args.end else today
            
        else:
            # Interactive Mode
            indices_list = ["NIFTY", "SENSEX", "BANKNIFTY", "FINNIFTY", "INDIAVIX", "ALL"]
            print("\n[1] Select Index:")
            for i, idx in enumerate(indices_list, 1): 
                print(f"  {i}. {idx}")
            idx_choice = input("\n  Choice (1-6) [1]: ") or "1"
            choice_str = indices_list[int(idx_choice)-1]
            selected_indices = ["NIFTY", "SENSEX", "BANKNIFTY", "FINNIFTY", "INDIAVIX"] if choice_str == "ALL" else [choice_str]

            intervals_list = ["1", "5", "15", "25", "60", "daily", "ALL"]
            print(f"\n[2] Select Interval:")
            for i, tf in enumerate(intervals_list, 1): 
                print(f"  {i}. {tf}{'m' if tf != 'daily' and tf != 'ALL' else ''}")
            tf_choice = input("\n  Choice (1-7) [1]: ") or "1"
            choice_tf = intervals_list[int(tf_choice)-1]
            selected_intervals = ["1", "5", "15", "25", "60", "daily"] if choice_tf == "ALL" else [choice_tf]

            print("\n[3] Expiry Type (for Options):")
            print("  1. Weekly")
            print("  2. Monthly")
            print("  3. Both")
            exp_choice = input("\n  Choice (1-3) [1]: ") or "1"
            if exp_choice == "1":
                expiry_configs = [{"flag": "WEEK", "code": 1, "subfolder": "weekly"}]
            elif exp_choice == "2":
                expiry_configs = [{"flag": "MONTH", "code": 1, "subfolder": "monthly"}]
            else:
                expiry_configs = [
                    {"flag": "WEEK", "code": 1, "subfolder": "weekly"},
                    {"flag": "MONTH", "code": 1, "subfolder": "monthly"}
                ]

            print(f"\n[4] Select Date Range (DD/MM/YYYY) [Default = Last 5 Years]:")
            while True:
                try:
                    start_str = input("  From Date: ")
                    end_str   = input("  To Date:   ")
                    
                    if not start_str.strip():
                        start_date = datetime.now() - timedelta(days=5*365)
                    else:
                        start_date = datetime.strptime(start_str.strip(), "%d/%m/%Y")
                        
                    if not end_str.strip():
                        end_date = datetime.now()
                    else:
                        end_date = datetime.strptime(end_str.strip(), "%d/%m/%Y")
                        
                    if end_date < start_date:
                        print("    [!] Error: End date must be after start date.")
                        continue
                    break
                except ValueError:
                    print("    [!] Error: Use DD/MM/YYYY.")

        # Print target config summary
        print(f"\n  Indices    : {selected_indices}")
        print(f"  Intervals  : {selected_intervals}")
        print(f"  Expiries   : {[c['subfolder'] for c in expiry_configs]}")
        print(f"  Date Range : {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print("═"*80)

        # Calculate exact number of tasks for scheduling progress
        spot_chunks_count = 0
        curr = start_date
        while curr <= end_date:
            spot_chunks_count += 1
            curr += timedelta(days=90)
            
        opt_chunks_count = 0
        curr = start_date
        while curr <= end_date:
            opt_chunks_count += 1
            curr += timedelta(days=30)

        # Spot Tasks Count
        total_spot_tasks = len(selected_indices) * len(selected_intervals) * spot_chunks_count
        
        # Options Tasks Count (Options uses ATM +/- 10 strikes = 21, and 2 types = CALL/PUT)
        strikes_labels = ["ATM"] + [f"ATM+{i}" for i in range(1, 11)] + [f"ATM-{i}" for i in range(1, 11)]
        opt_api_calls_per_chunk = len(strikes_labels) * 2 # 21 strikes * 2 types (CALL/PUT)
        
        # Performance Optimization: We only download options data at the '1' minute interval
        # from the Dhan API, and locally resample all other requested timeframes.
        # Skip options task counting for indices without derivatives segment (like INDIAVIX).
        total_options_tasks = 0
        for idx in selected_indices:
            if index_meta[idx]['segment']:
                total_options_tasks += 1 * len(expiry_configs) * opt_chunks_count * opt_api_calls_per_chunk
        
        total_scheduled_tasks = total_spot_tasks + total_options_tasks
        tracker = ProgressTracker(total_scheduled_tasks)

        # ── Sequential Execution ──────────────────────────────────────────────────
        
        for index_name in selected_indices:
            info = index_meta[index_name]
            
            # Fetch Spot
            for tf in selected_intervals:
                download_spot_data(client, info, tf, start_date, end_date, tracker)
                
            # Fetch Options: Only fetch if index has an active derivatives segment (like Nifty/BankNifty/FinNifty)
            if info['segment']:
                download_options_data(client, info, "1", start_date, end_date, expiry_configs, tracker)

                # Generate all other requested options timeframes locally via aggregation
                other_intervals = [tf for tf in selected_intervals if tf != "1"]
                if other_intervals:
                    # Calculate all years/months to aggregate
                    months_to_aggregate = set()
                    curr = start_date
                    while curr <= end_date:
                        months_to_aggregate.add((curr.strftime("%Y"), curr.strftime("%m")))
                        curr += timedelta(days=1)
                        
                    for tf in other_intervals:
                        print(f"\n  [AGGREGATION] Generating {tf}{'' if tf == 'daily' else 'min'} options for {index_name}...")
                        for config in expiry_configs:
                            src_dir = os.path.join("data", info['dir'], "1min", config['subfolder'])
                            dest_dir = os.path.join("data", info['dir'], f"{tf}{'' if tf == 'daily' else 'min'}", config['subfolder'])
                            for year, month in sorted(months_to_aggregate):
                                aggregate_options_to_timeframe(src_dir, dest_dir, tf, year, month)
            else:
                tracker.skip(0) # Align tracking count

        # ── Data Quality Reporting & Verification ─────────────────────────────────
        print("\n\n" + "═"*80)
        print("    DATA QUALITY & VERIFICATION")
        print("═"*80)
        
        all_requested_dates = set((start_date + timedelta(days=x)).date() for x in range((end_date - start_date).days + 1) if (start_date + timedelta(days=x)).weekday() < 5)
        holidays = []

        for index_name in selected_indices:
            info = index_meta[index_name]
            
            # Spot Quality and Holiday checks
            for tf in selected_intervals:
                # Load all downloaded files to run consolidated report
                spot_rows = []
                if tf == "daily":
                    fpath = os.path.join("data", info['dir'], "spot", "daily", "daily.csv")
                    if os.path.exists(fpath):
                        spot_rows.append(pd.read_csv(fpath))
                else:
                    for d in all_requested_dates:
                        y, m, ds = d.strftime("%Y"), d.strftime("%m"), d.strftime("%Y-%m-%d")
                        fpath = os.path.join("data", info['dir'], "spot", f"{tf}", y, m, f"{ds}.csv")
                        if os.path.exists(fpath):
                            spot_rows.append(pd.read_csv(fpath))
                        else:
                            holidays.append((d, f"{index_name} spot ({tf})"))
                            
                if spot_rows:
                    spot_df = pd.concat(spot_rows, ignore_index=True)
                    spot_df['timestamp'] = pd.to_datetime(spot_df['timestamp'])
                    run_data_quality_report(spot_df, tf, f"{index_name} SPOT")

            # Options Quality and Holiday checks (Only if the index has options)
            if info['segment']:
                for tf in selected_intervals:
                    for config in expiry_configs:
                        opt_rows = []
                        for d in all_requested_dates:
                            y, m, ds = d.strftime("%Y"), d.strftime("%m"), d.strftime("%Y-%m-%d")
                            
                            fpath = os.path.join("data", info['dir'], f"{tf}{'' if tf == 'daily' else 'min'}", config['subfolder'], y, m, f"{ds}.csv")
                            if os.path.exists(fpath):
                                opt_rows.append(pd.read_csv(fpath))
                            else:
                                holidays.append((d, f"{index_name} options ({config['subfolder']} - {tf})"))
                                
                        if opt_rows:
                            opt_df = pd.concat(opt_rows, ignore_index=True)
                            opt_df['timestamp'] = pd.to_datetime(opt_df['timestamp'])
                            run_data_quality_report(opt_df, tf, f"{index_name} OPTIONS ({config['subfolder'].upper()})")

        # ── Holidays Logging ──────────────────────────────────────────────────────
        if holidays:
            h_path = os.path.join("data", "holidays.json")
            os.makedirs("data", exist_ok=True)
            h_data = {}
            if os.path.exists(h_path):
                try:
                    with open(h_path, "r") as f:
                        h_data = json.load(f)
                except Exception:
                    h_data = {}
                    
            for index_name in selected_indices:
                info = index_meta[index_name]
                if info['dir'] not in h_data:
                    h_data[info['dir']] = []
                    
                for h_date, h_type in holidays:
                    if index_name in h_type:
                        h_str = f"{h_date.strftime('%Y-%m-%d')} ({h_type})"
                        if h_str not in h_data[info['dir']]:
                            h_data[info['dir']].append(h_str)
                            
            with open(h_path, "w") as f:
                json.dump(h_data, f, indent=4)
            print(f"\n  [!] Missing/holiday dates detected. Logged to data/holidays.json")

        print("\n" + "═"*80)
        print("    DOWNLOAD PROCESS SUCCESSFULLY COMPLETED!")
        print("═"*80)

    except KeyboardInterrupt:
        print("\n\n[!] Download process aborted by user.")
        sys.exit(0)
    except Exception as e:
        import traceback
        print(f"\n\n[FATAL ERROR] {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
