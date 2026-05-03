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
MASTER_CSV_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"

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

# ── Progress Tracker ─────────────────────────────────────────────────────────

class ProgressTracker:
    def __init__(self, total_tasks):
        self.total_tasks = total_tasks
        self.completed_tasks = 0
        self.start_time = time.time()

    def update(self):
        self.completed_tasks += 1
        elapsed = time.time() - self.start_time
        avg_time = elapsed / self.completed_tasks if self.completed_tasks > 0 else 0
        remaining = avg_time * (self.total_tasks - self.completed_tasks)
        
        percent = (self.completed_tasks / self.total_tasks) * 100
        mins, secs = divmod(int(remaining), 60)
        
        sys.stdout.write(f"\r    Progress: {percent:5.1f}% | Est. Remaining: {mins:02d}m {secs:02d}s | Done: {self.completed_tasks}/{self.total_tasks} ")
        sys.stdout.flush()

# ── Data Quality Report ───────────────────────────────────────────────────────

def run_data_quality_report(df, interval):
    if df.empty:
        return
    
    print("\n" + "="*40)
    print("      DATA QUALITY REPORT")
    print("="*40)
    
    expected_per_day = {"1": 375, "5": 75, "15": 25, "25": 15, "60": 7}
    unique_days = df['timestamp'].dt.date.unique()
    num_days = len(unique_days)
    expected_total = expected_per_day.get(str(interval), 0) * num_days
    
    counts = df.groupby(['strike_label', 'option_type']).size()
    avg_candles = counts.mean()
    
    missing_pct = max(0, (expected_total - avg_candles) / expected_total * 100) if expected_total > 0 else 0
    duplicates = df.duplicated(subset=['timestamp', 'option_type', 'strike_label']).sum()
    null_premiums = df['close'].isna().sum()
    zero_premiums = (df['close'] == 0).sum()
    
    gap_found = False
    for _, group in df.groupby(['strike_label', 'option_type', df['timestamp'].dt.date]):
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
        print("  DHAN OPTIONS DATA FETCHER")
        print("═"*80)

        access_token = load_credentials()
        if not access_token:
            print("[!] Error: Dhan access_token not found in keys.toml")
            sys.exit(1)

        client = DhanClient(access_token)
        
        # 1. Inputs
        indices = ["NIFTY 50", "SENSEX", "BANK NIFTY"]
        print("\n[1] Select Index:")
        for i, idx in enumerate(indices, 1): print(f"  {i}. {idx}")
        idx_choice = input("\n  Choice (1-3) [1]: ") or "1"
        selected_index = indices[int(idx_choice)-1]

        timeframes = ["1", "5", "15", "25", "60"]
        print(f"\n[2] Select Interval (m):")
        for i, tf in enumerate(timeframes, 1): print(f"  {i}. {tf}m")
        tf_choice = input("\n  Choice (1-5) [1]: ") or "1"
        selected_interval = timeframes[int(tf_choice)-1]

        print(f"\n[3] Select Date Range (DD/MM/YYYY):")
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
            "NIFTY 50":   {"id": 13, "lot": 65, "segment": "NSE_FNO", "dir": "NIFTY"},
            "SENSEX":     {"id": 51, "lot": 20, "segment": "BSE_FNO", "dir": "SENSEX"},
            "BANK NIFTY": {"id": 25, "lot": 15, "segment": "NSE_FNO", "dir": "BANKNIFTY"}
        }
        info = index_meta[selected_index]

        # 2. Chunk Dates (Max 15 days per Dhan API)
        date_chunks = []
        curr = start_date
        while curr <= end_date:
            chunk_end = curr + timedelta(days=14)
            if chunk_end > end_date: chunk_end = end_date
            date_chunks.append((curr, chunk_end))
            curr = chunk_end + timedelta(days=1)

        # 3. Fetch
        strikes_labels = ["ATM"] + [f"ATM+{i}" for i in range(1, 6)] + [f"ATM-{i}" for i in range(1, 6)]
        types = ["CALL", "PUT"]
        total_tasks = len(strikes_labels) * len(types) * len(date_chunks)
        
        tracker = ProgressTracker(total_tasks)
        all_rows = []

        print(f"\n[4] Downloading {selected_index} Options Data...")
        
        for chunk_start, chunk_end in date_chunks:
            for slabel in strikes_labels:
                for otype in types:
                    payload = {
                        "exchangeSegment": info['segment'],
                        "interval": selected_interval,
                        "securityId": info['id'],
                        "instrument": "OPTIDX",
                        "expiryFlag": "WEEK", "expiryCode": 1,
                        "strike": slabel, "drvOptionType": otype,
                        "requiredData": ["open", "high", "low", "close", "volume", "strike", "spot"],
                        "fromDate": chunk_start.strftime("%Y-%m-%d"),
                        "toDate": chunk_end.strftime("%Y-%m-%d")
                    }
                    
                    resp = client.get_rolling_options(payload)
                    if resp and "data" in resp:
                        key = 'ce' if otype == 'CALL' else 'pe'
                        opt_data = resp['data'].get(key, {})
                        if opt_data and "timestamp" in opt_data:
                            df_chunk = pd.DataFrame({
                                "timestamp": pd.to_datetime(opt_data["timestamp"], unit='s'),
                                "open": opt_data.get("open", []),
                                "high": opt_data.get("high", []),
                                "low": opt_data.get("low", []),
                                "close": opt_data.get("close", []),
                                "volume": opt_data.get("volume", []),
                                "strike_price": opt_data.get("strike", []),
                                "spot_price": opt_data.get("spot", [])
                            })
                            df_chunk['index'] = selected_index
                            df_chunk['option_type'] = "CE" if otype == "CALL" else "PE"
                            df_chunk['strike_label'] = slabel
                            all_rows.append(df_chunk)
                    
                    tracker.update()

        # 4. Process
        if all_rows:
            final_df = pd.concat(all_rows, ignore_index=True)
            final_df = final_df.drop_duplicates(subset=['timestamp', 'option_type', 'strike_label'])
            final_df = final_df.sort_values(['timestamp', 'strike_label', 'option_type'])
            final_df['timestamp'] = final_df['timestamp'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)
            
            downloaded_dates = set(final_df['timestamp'].dt.date)
            all_requested_dates = set((start_date + timedelta(days=x)).date() for x in range((end_date - start_date).days + 1))
            
            holidays = [d for d in all_requested_dates if d not in downloaded_dates]
            
            # Storage
            print(f"\n\n[5] Saving Data...")
            for day, day_df in final_df.groupby(final_df['timestamp'].dt.date):
                year, month, date_str = day.strftime("%Y"), day.strftime("%m"), day.strftime("%Y-%m-%d")
                folder_path = os.path.join("data", info['dir'], f"{selected_interval}min", year, month)
                os.makedirs(folder_path, exist_ok=True)
                day_df.to_csv(os.path.join(folder_path, f"{date_str}.csv"), index=False)
            
            # Update Holiday Log
            if holidays:
                h_log_path = os.path.join("data", "holidays.json")
                os.makedirs("data", exist_ok=True)
                h_data = {}
                if os.path.exists(h_log_path):
                    with open(h_log_path, "r") as f: h_data = json.load(f)
                
                index_key = info['dir']
                if index_key not in h_data: h_data[index_key] = []
                for h in holidays:
                    h_str = h.strftime("%Y-%m-%d")
                    if h_str not in h_data[index_key]: h_data[index_key].append(h_str)
                
                with open(h_log_path, "w") as f: json.dump(h_data, f, indent=4)
                print(f"  (!) Found {len(holidays)} holiday/gap dates. Logged to holidays.json")

            # Report
            run_data_quality_report(final_df, selected_interval)
            
            # Verification Block
            last_ts = final_df['timestamp'].max()
            last_snap = final_df[final_df['timestamp'] == last_ts]
            atm_ce = last_snap[(last_snap['strike_label'] == 'ATM') & (last_snap['option_type'] == 'CE')]
            atm_pe = last_snap[(last_snap['strike_label'] == 'ATM') & (last_snap['option_type'] == 'PE')]
            
            print(f"\n[MANUAL VERIFICATION - LAST CANDLE]")
            print(f"Timestamp : {last_ts.strftime('%Y-%m-%d %H:%M:%S')}")
            if not atm_ce.empty:
                sval = int(atm_ce.iloc[0]['strike_price'])
                print(f"Spot Price : {atm_ce.iloc[0]['spot_price']:.2f}")
                print(f"ATM Strike : {sval}")
                print(f"ATM CALL   : {sval}CE  Close: {atm_ce.iloc[0]['close']:.2f}")
            if not atm_pe.empty:
                sval = int(atm_pe.iloc[0]['strike_price'])
                print(f"ATM PUT    : {sval}PE  Close: {atm_pe.iloc[0]['close']:.2f}")
            
            print("\n" + "═"*40)
            print("  DOWNLOAD COMPLETE")
            print("═"*40)
        else:
            print("\n\n[!] No data downloaded. These dates may be trading holidays or outside the 15-day API limit per request.")

    except KeyboardInterrupt:
        print("\n\n[!] Aborted.")
        sys.exit(0)
    except Exception as e:
        print(f"\n\n[ERROR] {e}")

if __name__ == "__main__":
    main()
