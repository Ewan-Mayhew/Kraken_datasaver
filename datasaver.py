import requests
import pandas as pd
import time
import datetime
import os

def fetch_and_save_trades_since(
    start_dt_utc: datetime.datetime,
    pair: str,
    csv_filename: str,
    sleep_sec: float = 1.0
):
    """
    Fetch all trades for `pair` on Kraken from `start_dt_utc` up to the moment
    this function starts (cutoff). After each batch (up to ~1000 trades),
    append any new trades to `csv_filename` immediately, and print progress
    with batch number, how many were saved, date range, and running total.
    """
    URL = "https://api.kraken.com/0/public/Trades"

    # 1) Compute 'since' cursor in nanoseconds from the chosen start date
    start_ts_s   = start_dt_utc.timestamp()             # e.g. 1672531200.0
    start_ns     = int(start_ts_s * 1e9)                 # nanoseconds
    since_cursor = start_ns

    # 2) Freeze the current time in milliseconds as our cutoff
    cutoff_ms    = int(time.time() * 1000)
    cutoff_dt    = datetime.datetime.utcfromtimestamp(cutoff_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")

    print(f"→ start_dt_utc   = {start_dt_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"→ since_cursor   = {since_cursor}  (ns)")
    print(f"→ cutoff (now)   = {cutoff_dt} UTC   (ms = {cutoff_ms})\n")
    print("Beginning to page forward…\n")

    # 3) Prepare CSV file: write header (overwrite if file exists)
    header_df = pd.DataFrame(columns=["timestamp", "price", "volume", "side"])
    header_df.to_csv(csv_filename, index=False)

    total_saved = 0
    batch_num   = 0

    # 4) Page forward
    while True:
        batch_num += 1
        params = {"pair": pair, "since": since_cursor}
        resp = requests.get(URL, params=params)
        resp.raise_for_status()
        data = resp.json()
        if data.get("error"):
            raise RuntimeError(f"Kraken API returned error: {data['error']}")

        trades = data["result"][pair]              # list of ~1000 trades (oldest→newest)
        last_cursor = int(data["result"]["last"])  # next "since" cursor (nanoseconds)

        if not trades:
            print(f"Batch {batch_num}: no trades returned. Stopping.\n")
            break

        # 5) Filter and collect only trades in [start_ms .. cutoff_ms]
        batch_rows = []
        for t in trades:
            price   = float(t[0])
            volume  = float(t[1])
            time_s  = float(t[2])
            ts_ms   = int(time_s * 1000)
            side    = "buy" if t[3] == "b" else "sell"

            # Skip any trade before our start date
            if ts_ms < int(start_ts_s * 1000):
                continue
            # Skip any trade beyond our cutoff
            if ts_ms > cutoff_ms:
                continue

            batch_rows.append((ts_ms, price, volume, side))

        # 6) If we saved any in this batch, append them to CSV
        num_saved = len(batch_rows)
        if num_saved > 0:
            df_batch = pd.DataFrame(
                batch_rows, columns=["timestamp", "price", "volume", "side"]
            )
            # append to CSV (no header, since we've already written it once)
            df_batch.to_csv(csv_filename, mode="a", index=False, header=False)

            # Determine first/last timestamp of this batch for printing
            first_ts_ms = min(r[0] for r in batch_rows)
            last_ts_ms  = max(r[0] for r in batch_rows)
            first_dt    = datetime.datetime.utcfromtimestamp(first_ts_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
            last_dt     = datetime.datetime.utcfromtimestamp(last_ts_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
        else:
            first_dt = last_dt = "-"

        total_saved += num_saved

        print(
            f"Batch {batch_num}: received {len(trades):,} trades → saved {num_saved:,} "
            f"({first_dt} → {last_dt} UTC). Total saved so far: {total_saved:,}"
        )

        # 7) If the newest trade in 'trades' is beyond cutoff, stop
        last_trade_ts_s = float(trades[-1][2])
        last_trade_ms   = int(last_trade_ts_s * 1000)
        if last_trade_ms > cutoff_ms:
            print(f">>> Reached cutoff at {cutoff_dt} UTC. Stopping.\n")
            break

        # 8) Advance cursor and sleep
        since_cursor = last_cursor
        time.sleep(sleep_sec)

    print(f"✔ Done fetching. Final number of trades saved: {total_saved:,} → {csv_filename}\n")


# ────────────────────────────────────────────────────────────────────────────────
# Usage on your local device:

if __name__ == "__main__":
    # Change this date if you want a different starting point
    start_date = datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    pair       = "XXBTZUSD"                  # Kraken’s BTC/USD symbol
    csv_out    = "kraken_btc_trades_since_2023.csv"
    pause_sec  = 1.0                          # seconds to sleep between API calls

    # Remove any preexisting file (so we start fresh each run)
    if os.path.exists(csv_out):
        os.remove(csv_out)

    print(f"Requesting all {pair} trades since {start_date.strftime('%Y-%m-%d %H:%M:%S')} UTC …\n")
    fetch_and_save_trades_since(start_date, pair, csv_out, sleep_sec=pause_sec)
