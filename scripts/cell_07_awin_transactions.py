# cell_07_awin_transactions.py — Awin transaction fetch & processing
# Uses session dates and advertiser ID. Produces: df_awin_tx

import os
import time
import __main__
from datetime import datetime, timedelta
from urllib.parse import urlparse

import requests
import pandas as pd
import ipywidgets as widgets
from IPython.display import display, HTML

# ── Prerequisites ────────────────────────────────────────────────
for _r in ["ADVERTISER_ID", "SESSION_START_DATE", "SESSION_END_DATE",
           "_scroll_table", "download_file", "PATHS"]:
    if not hasattr(__main__, _r) or getattr(__main__, _r) is None:
        raise RuntimeError(f"Missing '{_r}'. Run earlier cells first.")

if not os.environ.get("AWAPI"):
    raise RuntimeError("AWAPI not set in environment. Run the Session Config cell first.")

ADVERTISER_ID = __main__.ADVERTISER_ID
SESSION_START_DATE = __main__.SESSION_START_DATE
SESSION_END_DATE = __main__.SESSION_END_DATE
_scroll_table = __main__._scroll_table
download_file = __main__.download_file
PATHS = __main__.PATHS

AWIN_TX_CSV = str(PATHS["output"] / "awin_transactions.csv")
df_awin_tx = None


# ── Awin transaction API ────────────────────────────────────────
def fetch_awin_transactions(advertiser_id, start_date, end_date,
                            date_type="transaction", timezone="UTC",
                            status=None, publisher_id=None):
    """
    Fetch transactions from Awin API.
    Handles the 31-day max window by chunking automatically.
    """
    awin_key = os.environ.get("AWAPI")
    if not awin_key:
        raise ValueError("No AWAPI key found in environment.")

    all_transactions = []
    chunk_start = datetime.strptime(start_date, "%Y-%m-%d")
    chunk_end_limit = datetime.strptime(end_date, "%Y-%m-%d")

    chunk_num = 0
    while chunk_start < chunk_end_limit:
        chunk_end = min(chunk_start + timedelta(days=30), chunk_end_limit)
        chunk_num += 1

        sd_str = chunk_start.strftime("%Y-%m-%dT00:00:00")
        ed_str = chunk_end.strftime("%Y-%m-%dT23:59:59")

        print(
            f"  \U0001f4e6 Chunk {chunk_num}: "
            f"{chunk_start.strftime('%Y-%m-%d')} \u2192 {chunk_end.strftime('%Y-%m-%d')}",
            end="",
        )

        url = f"https://api.awin.com/advertisers/{advertiser_id}/transactions/"
        params = {
            "accessToken": awin_key,
            "startDate": sd_str,
            "endDate": ed_str,
            "dateType": date_type,
            "timezone": timezone,
        }
        if status:
            params["status"] = status
        if publisher_id:
            params["publisherId"] = str(publisher_id)

        resp = requests.get(url, params=params)
        if resp.status_code != 200:
            print(f" \u274c Error {resp.status_code}")
            raise Exception(f"Awin API error {resp.status_code}: {resp.text[:300]}")

        data = resp.json()
        print(f" \u2192 {len(data)} transactions")
        all_transactions.extend(data)

        chunk_start = chunk_end + timedelta(days=1)
        if chunk_start < chunk_end_limit:
            time.sleep(0.5)  # rate limit courtesy

    return all_transactions


def process_awin_transactions(raw):
    if not raw:
        return pd.DataFrame()

    rows = []
    for tx in raw:
        rows.append({
            "Transaction ID": tx.get("id"),
            "Advertiser ID": tx.get("advertiserId"),
            "Advertiser Name": tx.get("advertiserName"),
            "Publisher ID": tx.get("publisherId"),
            "Publisher Name": tx.get("siteName", ""),
            "Publisher URL": tx.get("publisherUrl", ""),
            "Click Ref": tx.get("clickRef", ""),
            "Order Ref": tx.get("orderRef", ""),
            "Transaction Date": tx.get("transactionDate", ""),
            "Validation Date": tx.get("validationDate", ""),
            "Type": tx.get("type", ""),
            "Status": tx.get("status", ""),
            "Sale Amount": tx.get("saleAmount", {}).get("amount", 0),
            "Currency": tx.get("saleAmount", {}).get("currency", ""),
            "Commission Amount": tx.get("commissionAmount", {}).get("amount", 0),
            "Click Device": tx.get("clickDevice", ""),
            "Transaction Device": tx.get("transactionDevice", ""),
            "Lapse Time (s)": tx.get("lapseTime", 0),
        })

    df = pd.DataFrame(rows)

    # Numeric types
    for c in ["Sale Amount", "Commission Amount", "Lapse Time (s)"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    df["Sale Amount"] = df["Sale Amount"].round(2)
    df["Commission Amount"] = df["Commission Amount"].round(2)

    # Extract publisher domain from URL
    def _pub_domain(url):
        if not url:
            return ""
        if not str(url).startswith("http"):
            url = "https://" + str(url)
        try:
            return urlparse(url).netloc.lower().lstrip("www.")
        except Exception:
            return ""

    df["Publisher Domain"] = df["Publisher URL"].apply(_pub_domain)
    df = df.sort_values("Transaction Date", ascending=False).reset_index(drop=True)
    return df


# ── Widgets ──────────────────────────────────────────────────────
tx_header = widgets.HTML(
    '<div class="peec-header">\U0001f4ca Awin Transaction Report</div>'
    '<div class="peec-sub">Pull individual transactions with publisher URLs for domain matching'
    f' &nbsp;|&nbsp; Advertiser: <b>{ADVERTISER_ID}</b>'
    f' &nbsp;|&nbsp; {SESSION_START_DATE} to {SESSION_END_DATE}</div>'
)

tx_status = widgets.Dropdown(
    options=["All", "pending", "approved", "declined", "deleted"],
    value="All", description="Status:",
    style={"description_width": "60px"}, layout=widgets.Layout(width="200px"),
)

tx_pull_btn = widgets.Button(
    description="  Pull Transactions", button_style="info",
    icon="cloud-download", layout=widgets.Layout(width="200px", height="36px"),
)
tx_dl_btn = widgets.Button(
    description="  \u2b07 Download CSV", button_style="success",
    layout=widgets.Layout(width="160px", height="36px"),
)
tx_stats = widgets.HTML("")
tx_status_msg = widgets.HTML("")


def on_tx_pull(b):
    global df_awin_tx
    tx_stats.value = ""
    tx_status_msg.value = "\u23f3 Fetching transactions..."
    sd = SESSION_START_DATE
    ed = SESSION_END_DATE
    adv = ADVERTISER_ID
    status = None if tx_status.value == "All" else tx_status.value

    try:
        raw = fetch_awin_transactions(adv, sd, ed, status=status)
        df = process_awin_transactions(raw)

        if df.empty:
            tx_status_msg.value = "\u26a0\ufe0f No transactions returned."
            return

        df_awin_tx = df
        __main__.df_awin_tx = df_awin_tx
        df.to_csv(AWIN_TX_CSV, index=False)

        total_rev = df["Sale Amount"].sum()
        total_comm = df["Commission Amount"].sum()
        unique_pubs = df["Publisher ID"].nunique()
        unique_domains = df["Publisher Domain"].replace("", pd.NA).dropna().nunique()

        tx_stats.value = (
            f'<div>'
            f'<span class="peec-stat">\U0001f4dd Transactions: <b>{len(df):,}</b></span>'
            f'<span class="peec-stat">\U0001f465 Publishers: <b>{unique_pubs:,}</b></span>'
            f'<span class="peec-stat">\U0001f310 Unique Pub Domains: <b>{unique_domains:,}</b></span>'
            f'<span class="peec-stat">\U0001f4b0 Revenue: <b>\u00a3{total_rev:,.2f}</b></span>'
            f'<span class="peec-stat">\U0001f4b7 Commission: <b>\u00a3{total_comm:,.2f}</b></span>'
            f'</div>'
        )
        tx_status_msg.value = (
            f'\u2705 Pulled {len(df):,} transactions. '
            f'CSV saved to output folder.'
        )

    except Exception as e:
        tx_status_msg.value = f"\u274c Error: {e}"


def on_tx_dl(b):
    if df_awin_tx is None or df_awin_tx.empty:
        return
    download_file(AWIN_TX_CSV)


tx_pull_btn.on_click(on_tx_pull)
tx_dl_btn.on_click(on_tx_dl)

display(
    tx_header,
    tx_status,
    widgets.HBox(
        [tx_pull_btn, tx_dl_btn],
        layout=widgets.Layout(margin="8px 0 10px 0"),
    ),
    tx_stats,
    tx_status_msg,
)
