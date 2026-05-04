import os
import sys
import time
import json
import requests
import pandas as pd
import warnings
from datetime import datetime, timedelta

# Suppress pandas warnings
warnings.filterwarnings("ignore")

# Try to import tomllib (3.11+) or tomli/toml
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
    def __init__(self, access_token):
        self.access_token = access_token
        self.headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'access-token': self.access_token
        }
        self.last_call_time = 0
        self.rate_limit_delay = 3.1 

    def _throttle(self):
        elapsed = time.time() - self.last_call_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self.last_call_time = time.time()

    def get_rolling_options(self, payload):
        self._throttle()
        url = f"{BASE_URL}/charts/rollingoption"
        try:
            response = requests.post(url, headers=self.headers, json=payload, timeout=30)
            if response.status_code == 200:
                return response.json()
            else:
                return {"error": response.text, "status_code": response.status_code}
        except Exception as e:
            return {"error": str(e), "status_code": 500}

    def get_spot_data(self, payload):
        self._throttle()
        url = f"{BASE_URL}/charts/intraday"
        try:
            response = requests.post(url, headers=self.headers, json=payload, timeout=30)
            if response.status_code == 200:
                return response.json()
            else:
                return {"error": response.text, "status_code": response.status_code}
        except Exception as e:
            return {"error": str(e), "status_code": 500}

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

    def update(self):
        if self.last_fetch_start > 0:
            self.fetch_time_total += (time.time() - self.last_fetch_start)
            self.fetch_only_completed += 1
            self.last_fetch_start = 0
        self.completed_tasks += 1
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
    
    print("\n" + "="*40)
    print(f"      {label} QUALITY REPORT")
    print("="*40)
    
    expected_per_day = {"1": 375, "5": 75, "15": 25, "25": 15, "60": 7}
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
    print(f"  Missing Candles   : {missing_pct:.1f}%")
    print(f"  Duplicates        : {duplicates}")
    print(f"  Null/Zero Prices  : {null_premiums + zero_premiums}")
    print(f"  Intraday Gaps     : {'YES' if gap_found else 'NO'}")
    
    if 'open_interest' in df.columns:
        oi_coverage = (df['open_interest'] > 0).sum() / len(df) * 100
        print(f"  OI Coverage       : {oi_coverage:.1f}%")
        if oi_coverage < 50:
            print("\n  [!] Low OI coverage — OI-based filters unreliable for this period.")

    if missing_pct > 15 or duplicates > 0 or (null_premiums + zero_premiums) > 0:
        print("\n  [!] Issues detected. Verify data manually.")
    else:
        print("\n  [OK] Data quality is good.")
    print("="*40)

# ── Main Application ──────────────────────────────────────────────────────────

def load_credentials():
    path = os.path.join(os.path.dirname(__file__), "keys.toml")
    if os.path.exists(path):
        with open(path, "r") as f:
            content = f.read()
            import re
            match = re.search(r'access_token\s*=\s*"([^"]+)"', content)
            if match: return match.group(1)
    return None

def main():
    try:
        print("═"*80)
        print("  DHAN DATA DOWNLOADER (SPOT + OPTIONS AUTOMATION)")
        print("═"*80)

        access_token = load_credentials()
        if not access_token:
            print("[!] Error: Dhan access_token not found in keys.toml")
            sys.exit(1)

        client = DhanClient(access_token)
        
        # 1. Index Selection
        indices = ["NIFTY 50", "SENSEX", "BANK NIFTY"]
        print("\n[1] Select Index:")
        for i, idx in enumerate(indices, 1): print(f"  {i}. {idx}")
        idx_choice = input("\n  Choice (1-3) [1]: ") or "1"
        selected_index = indices[int(idx_choice)-1]

        # 2. Interval Selection
        timeframes = ["1", "5", "15", "25", "60"]
        print(f"\n[2] Select Interval (m):")
        for i, tf in enumerate(timeframes, 1): print(f"  {i}. {tf}m")
        tf_choice = input("\n  Choice (1-5) [1]: ") or "1"
        selected_interval = timeframes[int(tf_choice)-1]

        # 3. Expiry Type Selection
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

        # 4. Date Range
        print(f"\n[4] Select Date Range (DD/MM/YYYY):")
        while True:
            try:
                start_str = input("  From Date: ")
                end_str   = input("  To Date:   ")
                start_date = datetime.strptime(start_str.strip(), "%d/%m/%Y")
                end_date   = datetime.strptime(end_str.strip(), "%d/%m/%Y")
                if end_date < start_date:
                    print("    [!] Error: End date must be after start date.")
                    continue
                break
            except ValueError:
                print("    [!] Error: Use DD/MM/YYYY.")

        index_meta = {
            "NIFTY 50":   {"id": 13, "lot": 65, "segment": "NSE_FNO", "dir": "NIFTY", "spot_seg": "IDX_I"},
            "SENSEX":     {"id": 51, "lot": 20, "segment": "BSE_FNO", "dir": "SENSEX", "spot_seg": "IDX_I"},
            "BANK NIFTY": {"id": 25, "lot": 15, "segment": "NSE_FNO", "dir": "BANKNIFTY", "spot_seg": "IDX_I"}
        }
        info = index_meta[selected_index]

        # ── Preparation ───────────────────────────────────────────────────────
        
        # Chunking for Spot (90 days)
        spot_chunks = []
        curr = start_date
        while curr <= end_date:
            chunk_end = curr + timedelta(days=89)
            if chunk_end > end_date: chunk_end = end_date
            spot_chunks.append((curr, chunk_end))
            curr = chunk_end + timedelta(days=1)
            
        # Chunking for Options (15 days)
        opt_chunks = []
        curr = start_date
        while curr <= end_date:
            chunk_end = curr + timedelta(days=14)
            if chunk_end > end_date: chunk_end = end_date
            opt_chunks.append((curr, chunk_end))
            curr = chunk_end + timedelta(days=1)

        strikes_labels = ["ATM"] + [f"ATM+{i}" for i in range(1, 6)] + [f"ATM-{i}" for i in range(1, 6)]
        types = ["CALL", "PUT"]
        
        spot_tasks = len(spot_chunks)
        opt_tasks = len(expiry_configs) * len(opt_chunks) * len(strikes_labels) * len(types)
        total_tasks = spot_tasks + opt_tasks
        
        tracker = ProgressTracker(total_tasks)
        
        # ── 1. SPOT DATA FETCH ────────────────────────────────────────────────
        print(f"\n[5] Fetching {selected_index} Spot Data...")
        spot_rows = []
        for chunk_start, chunk_end in spot_chunks:
            chunk_range = [(chunk_start + timedelta(days=x)) for x in range((chunk_end - chunk_start).days + 1)]
            chunk_trading_days = [d for d in chunk_range if d.weekday() < 5]
            
            all_exist = True
            if not chunk_trading_days: all_exist = False
            for d in chunk_trading_days:
                y, m, ds = d.strftime("%Y"), d.strftime("%m"), d.strftime("%Y-%m-%d")
                fpath = os.path.join("data", info['dir'], "spot", f"{selected_interval}min", y, m, f"{ds}.csv")
                if not os.path.exists(fpath):
                    all_exist = False
                    break
            
            if all_exist:
                for d in chunk_trading_days:
                    print(f"  [SKIP] Spot: {d.strftime('%Y-%m-%d')} already downloaded")
                tracker.skip(1)
                continue

            payload = {
                "securityId": str(info['id']),
                "exchangeSegment": info['spot_seg'],
                "instrument": "INDEX",
                "interval": selected_interval,
                "fromDate": chunk_start.strftime("%Y-%m-%d"),
                "toDate": chunk_end.strftime("%Y-%m-%d")
            }
            tracker.start_fetch()
            resp = client.get_spot_data(payload)
            if resp and "timestamp" in resp:
                df_chunk = pd.DataFrame({
                    "timestamp": pd.to_datetime(resp["timestamp"], unit='s'),
                    "open": resp.get("open", []),
                    "high": resp.get("high", []),
                    "low": resp.get("low", []),
                    "close": resp.get("close", []),
                    "volume": resp.get("volume", [])
                })
                spot_rows.append(df_chunk)
            tracker.update()

        # ── 2. OPTIONS DATA FETCH ─────────────────────────────────────────────
        print(f"\n[6] Fetching {selected_index} Options Data...")
        opt_rows = []
        for config in expiry_configs:
            for chunk_start, chunk_end in opt_chunks:
                chunk_range = [(chunk_start + timedelta(days=x)) for x in range((chunk_end - chunk_start).days + 1)]
                chunk_trading_days = [d for d in chunk_range if d.weekday() < 5]
                
                all_exist = True
                if not chunk_trading_days: all_exist = False
                for d in chunk_trading_days:
                    y, m, ds = d.strftime("%Y"), d.strftime("%m"), d.strftime("%Y-%m-%d")
                    fpath = os.path.join("data", info['dir'], f"{selected_interval}min", config['subfolder'], y, m, f"{ds}.csv")
                    if not os.path.exists(fpath):
                        all_exist = False
                        break
                
                if all_exist:
                    for d in chunk_trading_days:
                        print(f"  [SKIP] Options ({config['subfolder']}): {d.strftime('%Y-%m-%d')} already downloaded")
                    tracker.skip(len(strikes_labels) * len(types))
                    continue

                for slabel in strikes_labels:
                    for otype in types:
                        payload = {
                            "exchangeSegment": info['segment'],
                            "interval": selected_interval,
                            "securityId": info['id'],
                            "instrument": "OPTIDX",
                            "expiryFlag": config['flag'], "expiryCode": config['code'],
                            "strike": slabel, "drvOptionType": otype,
                            "requiredData": ["open", "high", "low", "close", "volume", "strike", "spot", "oi"],
                            "fromDate": chunk_start.strftime("%Y-%m-%d"),
                            "toDate": chunk_end.strftime("%Y-%m-%d")
                        }
                        tracker.start_fetch()
                        resp = client.get_rolling_options(payload)
                        if resp and "data" in resp:
                            key = 'ce' if otype == 'CALL' else 'pe'
                            o_data = resp['data'].get(key, {})
                            if o_data and "timestamp" in o_data:
                                df_c = pd.DataFrame({
                                    "timestamp": pd.to_datetime(o_data["timestamp"], unit='s'),
                                    "open": o_data.get("open", []),
                                    "high": o_data.get("high", []),
                                    "low": o_data.get("low", []),
                                    "close": o_data.get("close", []),
                                    "volume": o_data.get("volume", []),
                                    "open_interest": o_data.get("oi", []),
                                    "strike_price": o_data.get("strike", []),
                                    "spot_price": o_data.get("spot", [])
                                })
                                df_c['open_interest'] = df_c['open_interest'].fillna(0).astype(int)
                                df_c['index'] = selected_index
                                df_c['option_type'] = "CE" if otype == "CALL" else "PE"
                                df_c['strike_label'] = slabel
                                df_c['expiry_type'] = config['subfolder']
                                opt_rows.append(df_c)
                        tracker.update()

        # ── 3. SAVE AND REPORT ────────────────────────────────────────────────
        print(f"\n\n[7] Processing and Saving...")
        
        all_requested_dates = set((start_date + timedelta(days=x)).date() for x in range((end_date - start_date).days + 1) if (start_date + timedelta(days=x)).weekday() < 5)

        # Save Spot
        if spot_rows:
            spot_df = pd.concat(spot_rows, ignore_index=True).drop_duplicates(subset=['timestamp']).sort_values('timestamp')
            spot_df['timestamp'] = spot_df['timestamp'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)
            
            for day, day_df in spot_df.groupby(spot_df['timestamp'].dt.date):
                year, month, d_str = day.strftime("%Y"), day.strftime("%m"), day.strftime("%Y-%m-%d")
                f_path = os.path.join("data", info['dir'], "spot", f"{selected_interval}min", year, month)
                os.makedirs(f_path, exist_ok=True)
                day_df.to_csv(os.path.join(f_path, f"{d_str}.csv"), index=False)
            run_data_quality_report(spot_df, selected_interval, "SPOT")

        # Save Options
        if opt_rows:
            opt_df = pd.concat(opt_rows, ignore_index=True).drop_duplicates(subset=['timestamp', 'option_type', 'strike_label', 'expiry_type']).sort_values(['timestamp', 'strike_label', 'option_type'])
            opt_df['timestamp'] = opt_df['timestamp'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)
            
            for day, day_df in opt_df.groupby(opt_df['timestamp'].dt.date):
                year, month, d_str = day.strftime("%Y"), day.strftime("%m"), day.strftime("%Y-%m-%d")
                for exp_type, exp_df in day_df.groupby('expiry_type'):
                    f_path = os.path.join("data", info['dir'], f"{selected_interval}min", exp_type, year, month)
                    os.makedirs(f_path, exist_ok=True)
                    exp_df.drop(columns=['expiry_type']).to_csv(os.path.join(f_path, f"{d_str}.csv"), index=False)
            run_data_quality_report(opt_df, selected_interval, "OPTIONS")

        # ── 4. HOLIDAYS AND VERIFICATION ──────────────────────────────────────
        holidays = []
        for d in all_requested_dates:
            y, m, ds = d.strftime("%Y"), d.strftime("%m"), d.strftime("%Y-%m-%d")
            # Check Spot
            if not os.path.exists(os.path.join("data", info['dir'], "spot", f"{selected_interval}min", y, m, f"{ds}.csv")):
                holidays.append((d, "spot"))
            # Check Options
            for config in expiry_configs:
                if not os.path.exists(os.path.join("data", info['dir'], f"{selected_interval}min", config['subfolder'], y, m, f"{ds}.csv")):
                    holidays.append((d, config['subfolder']))

        if holidays:
            h_path = os.path.join("data", "holidays.json")
            os.makedirs("data", exist_ok=True)
            h_data = {}
            if os.path.exists(h_path):
                with open(h_path, "r") as f: h_data = json.load(f)
            if info['dir'] not in h_data: h_data[info['dir']] = []
            for h_date, h_type in holidays:
                h_str = f"{h_date.strftime('%Y-%m-%d')} ({h_type})"
                if h_str not in h_data[info['dir']]: h_data[info['dir']].append(h_str)
            with open(h_path, "w") as f: json.dump(h_data, f, indent=4)
            print(f"\n  (!) Found {len(holidays)} missing trading dates. Logged to holidays.json")

        print("\n" + "═"*40)
        print("  DOWNLOAD COMPLETE")
        print("═"*40)

    except KeyboardInterrupt:
        print("\n\n[!] Aborted.")
        sys.exit(0)
    except Exception as e:
        print(f"\n\n[ERROR] {e}")

if __name__ == "__main__":
    main()
