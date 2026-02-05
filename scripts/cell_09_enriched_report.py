# cell_09_enriched_report.py — Domain Match + Enriched Report (consolidated)
# Matches PEEC domains to Awin publisher domains, pulls publisher report
# for accurate names, adds AI model data, and applies exclude filter.
# Produces: df_enriched

import os
import __main__
import requests
import pandas as pd
import ipywidgets as widgets
from IPython.display import display, HTML

# ── Prerequisites ────────────────────────────────────────────────
for _r in ["df_domain_result", "df_awin_tx", "df_detail",
           "_normalise_host", "_scroll_table", "download_file",
           "PATHS", "ADVERTISER_ID", "SESSION_START_DATE", "SESSION_END_DATE"]:
    if not hasattr(__main__, _r) or getattr(__main__, _r) is None:
        raise RuntimeError(f"Missing '{_r}'. Run earlier cells first.")

if not os.environ.get("AWAPI"):
    raise RuntimeError("AWAPI not set in environment. Run the Session Config cell first.")

df_domain_result = __main__.df_domain_result
df_awin_tx = __main__.df_awin_tx
df_detail = __main__.df_detail
_normalise_host = __main__._normalise_host
_scroll_table = __main__._scroll_table
download_file = __main__.download_file
PATHS = __main__.PATHS
ADVERTISER_ID = __main__.ADVERTISER_ID
SESSION_START_DATE = __main__.SESSION_START_DATE
SESSION_END_DATE = __main__.SESSION_END_DATE

ENRICHED_CSV = str(PATHS["output"] / "peec_awin_enriched.csv")
PUB_REPORT_CSV = str(PATHS["output"] / "awin_publisher_report.csv")
df_enriched = None


# ── Awin publisher report (for publisher names) ──────────────────
def _fetch_publisher_report(advertiser_id, start_date, end_date):
    """Fetch publisher performance report from Awin API."""
    awin_key = os.environ.get("AWAPI")
    url = f"https://api.awin.com/advertisers/{advertiser_id}/reports/publisher"
    params = {
        "accessToken": awin_key,
        "startDate": start_date,
        "endDate": end_date,
        "dateType": "transaction",
        "timezone": "UTC",
    }
    resp = requests.get(url, params=params)
    if resp.status_code != 200:
        raise Exception(f"Awin publisher report error {resp.status_code}: {resp.text[:300]}")
    return resp.json()


def _process_publisher_report(raw):
    """Process raw publisher report into a DataFrame and save as CSV."""
    if not raw:
        return pd.DataFrame()
    df = pd.DataFrame(raw)

    int_cols = ["impressions", "clicks", "totalNo", "confirmedNo", "pendingNo", "declinedNo"]
    float_cols = ["totalValue", "totalComm", "confirmedValue", "confirmedComm",
                  "pendingValue", "pendingComm", "declinedValue", "declinedComm"]
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    for c in float_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0).round(2)

    if "totalValue" in df.columns and "totalNo" in df.columns:
        df["AOV"] = (df["totalValue"] / df["totalNo"].replace(0, pd.NA)).round(2)
    if "totalNo" in df.columns and "clicks" in df.columns:
        df["Conv Rate %"] = ((df["totalNo"] / df["clicks"].replace(0, pd.NA)) * 100).round(2)

    rename = {
        "publisherId": "Publisher ID", "publisherName": "Publisher Name",
        "impressions": "Impressions", "clicks": "Clicks",
        "totalNo": "Sales", "totalValue": "Revenue", "totalComm": "Commission",
        "confirmedNo": "Confirmed Sales", "confirmedValue": "Confirmed Revenue",
        "confirmedComm": "Confirmed Comm",
        "pendingNo": "Pending Sales", "pendingValue": "Pending Revenue",
        "declinedNo": "Declined Sales",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    keep = ["Publisher ID", "Publisher Name", "Impressions", "Clicks", "Sales",
            "Revenue", "Commission", "AOV", "Conv Rate %",
            "Confirmed Sales", "Confirmed Revenue", "Confirmed Comm",
            "Pending Sales", "Pending Revenue", "Declined Sales"]
    keep = [c for c in keep if c in df.columns]
    df = df[keep].sort_values("Revenue", ascending=False).reset_index(drop=True)
    return df


# ── Helpers ──────────────────────────────────────────────────────
def _parse_exclude_keywords(text):
    """Parse comma-separated exclude keywords into a list of lowercase strings."""
    if not text or not text.strip():
        return []
    return [kw.strip().lower() for kw in text.split(",") if kw.strip()]


def _row_excluded(row, exclude_keywords):
    """Check if a row should be excluded based on domain keywords."""
    if not exclude_keywords:
        return False
    peec_d = str(row.get("Peec Domain", "")).lower()
    awin_d = str(row.get("Awin Domain", "")).lower()
    return any(kw in peec_d or kw in awin_d for kw in exclude_keywords)


def _build_model_lookup(detail_df):
    """
    Build a dict: domain -> "cla, goo, ope, per, ..."
    Each model ID is truncated to 3 characters, de-duped, and sorted.
    """
    if detail_df is None or detail_df.empty:
        return {}
    tmp = detail_df.dropna(subset=["Model"])[["Domain", "Model"]].copy()
    tmp["_code"] = tmp["Model"].astype(str).str[:3]
    tmp = tmp[["Domain", "_code"]].drop_duplicates().sort_values(["Domain", "_code"])
    return tmp.groupby("Domain")["_code"].agg(", ".join).to_dict()


# ── Widgets ──────────────────────────────────────────────────────
enrich_table = widgets.HTML("")
enrich_status_msg = widgets.HTML("")
enrich_stats = widgets.HTML("")
enrich_dl_btn = widgets.Button(
    description="  \u2b07 Download CSV", button_style="success",
    layout=widgets.Layout(width="160px", height="36px"),
)
enrich_run_btn = widgets.Button(
    description="  Build Enriched Report", button_style="info",
    icon="bar-chart", layout=widgets.Layout(width="240px", height="36px"),
)

enrich_domain_type = widgets.SelectMultiple(
    options=["All"], value=["All"], description="Domain types:",
    style={"description_width": "100px"},
    layout=widgets.Layout(width="300px", height="80px"),
)
enrich_exclude = widgets.Text(
    description="Exclude domains:",
    placeholder="e.g. google, facebook, pinterest, reddit",
    style={"description_width": "120px"},
    layout=widgets.Layout(width="600px"),
)


def run_enrich(b=None):
    global df_enriched
    enrich_stats.value = ""
    enrich_table.value = ""
    enrich_status_msg.value = ""

    if df_domain_result is None or df_domain_result.empty:
        enrich_status_msg.value = "\u26a0\ufe0f Run the Domain Report first."
        return
    if df_awin_tx is None or df_awin_tx.empty:
        enrich_status_msg.value = "\u26a0\ufe0f Run the Awin Transaction Report first."
        return

    # ── Step 1: Aggregate Awin transactions by publisher domain ──
    enrich_status_msg.value = "\u23f3 Building Awin publisher domain summary..."

    awin_domains = (
        df_awin_tx[df_awin_tx["Publisher Domain"] != ""]
        .groupby("Publisher Domain", as_index=False)
        .agg(
            Publisher_ID=("Publisher ID", "first"),
            Publisher_Name=("Publisher Name", "first"),
            Awin_Transactions=("Transaction ID", "count"),
            Awin_Revenue=("Sale Amount", "sum"),
            Awin_Commission=("Commission Amount", "sum"),
        )
    )
    awin_domains.columns = [
        "Awin Domain", "Publisher ID", "Publisher Name",
        "Awin Transactions", "Awin Revenue", "Awin Commission",
    ]
    awin_domains["Awin Revenue"] = awin_domains["Awin Revenue"].round(2)
    awin_domains["Awin Commission"] = awin_domains["Awin Commission"].round(2)
    awin_domains["Awin AOV"] = (
        awin_domains["Awin Revenue"]
        / awin_domains["Awin Transactions"].replace(0, pd.NA)
    ).round(2)

    # ── Step 2: Normalise hostnames and match ────────────────────
    awin_domains["_awin_host"] = awin_domains["Awin Domain"].apply(_normalise_host)
    peec = df_domain_result.copy()
    peec["_peec_host"] = peec["Domain"].apply(_normalise_host)

    enrich_status_msg.value = (
        f"\u23f3 Matching {len(peec)} Peec domains against "
        f"{len(awin_domains)} Awin publisher domains..."
    )

    awin_lookup = {}
    for _, a in awin_domains.iterrows():
        host = a["_awin_host"]
        if host and len(host) >= 3:
            awin_lookup.setdefault(host, []).append(a)

    matches = []
    for _, p in peec.iterrows():
        ph = p["_peec_host"]
        if not ph or len(ph) < 3:
            continue
        if ph in awin_lookup:
            for a in awin_lookup[ph]:
                matches.append({
                    "Peec Domain": p["Domain"],
                    "Domain Type": p.get("Domain Type", ""),
                    "Peec Citations": p.get("Total Citations", 0),
                    "Peec Avg Pos": p.get("Avg Citation Pos", 0),
                    "Peec Unique Pages": p.get("Unique Pages", 0),
                    "Peec Models Present": p.get("Models Present", 0),
                    "Awin Domain": a["Awin Domain"],
                    "Publisher ID": a["Publisher ID"],
                    "Publisher Name": a["Publisher Name"],
                    "Awin Transactions": a["Awin Transactions"],
                    "Awin Revenue": a["Awin Revenue"],
                    "Awin Commission": a["Awin Commission"],
                    "Awin AOV": a["Awin AOV"],
                })

    if not matches:
        peec_hosts = sorted(peec["_peec_host"].unique())[:30]
        awin_hosts = sorted(awin_domains["_awin_host"].unique())[:30]
        enrich_status_msg.value = "\u26a0\ufe0f No domain matches found."
        enrich_table.value = (
            "<div><b>Peec normalised hosts (first 30):</b><br>"
            + "<br>".join(f"&nbsp;&nbsp;{s}" for s in peec_hosts)
            + "<br><br><b>Awin publisher normalised hosts (first 30):</b><br>"
            + "<br>".join(f"&nbsp;&nbsp;{s}" for s in awin_hosts)
            + "<br><br>Compare the lists above for near-misses.</div>"
        )
        return

    merged = pd.DataFrame(matches)

    # ── Populate domain type filter ──────────────────────────────
    available_types = sorted(merged["Domain Type"].dropna().unique().tolist())
    enrich_domain_type.options = ["All"] + available_types

    # ── Step 3: Pull publisher report for accurate names ─────────
    enrich_status_msg.value = "\u23f3 Fetching Awin publisher report for publisher names..."
    try:
        raw_pub = _fetch_publisher_report(
            ADVERTISER_ID, SESSION_START_DATE, SESSION_END_DATE
        )
        df_pub = _process_publisher_report(raw_pub)
        if not df_pub.empty:
            df_pub.to_csv(PUB_REPORT_CSV, index=False)

            # Build name lookup and backfill
            pub_name_lookup = dict(
                zip(
                    df_pub["Publisher ID"].astype(int),
                    df_pub["Publisher Name"],
                )
            )
            merged["Publisher ID"] = merged["Publisher ID"].astype(int)
            merged["Publisher Name"] = merged["Publisher ID"].map(pub_name_lookup).fillna(
                merged["Publisher Name"]
            )
    except Exception as e:
        pass  # Fall back to publisher names from transaction data

    # ── Step 4: Add Models column from df_detail ─────────────────
    model_lookup = _build_model_lookup(df_detail)
    merged["Models"] = merged["Peec Domain"].map(model_lookup).fillna("")

    # ── Step 5: Apply filters ────────────────────────────────────
    selected_types = list(enrich_domain_type.value)
    if selected_types and "All" not in selected_types:
        merged = merged[merged["Domain Type"].isin(selected_types)]

    exclude_keywords = _parse_exclude_keywords(enrich_exclude.value)
    excluded_count = 0
    if exclude_keywords:
        mask = merged.apply(lambda row: _row_excluded(row, exclude_keywords), axis=1)
        excluded_count = mask.sum()
        merged = merged[~mask].reset_index(drop=True)

    # ── Step 6: Final output ─────────────────────────────────────
    output_cols = [
        "Peec Domain",
        "Domain Type",
        "Peec Citations",
        "Peec Avg Pos",
        "Peec Unique Pages",
        "Peec Models Present",
        "Models",
        "Awin Domain",
        "Publisher ID",
        "Publisher Name",
        "Awin Transactions",
        "Awin Revenue",
        "Awin Commission",
        "Awin AOV",
    ]
    output_cols = [c for c in output_cols if c in merged.columns]
    merged = merged[output_cols]
    merged = merged.sort_values("Peec Citations", ascending=False).reset_index(drop=True)

    df_enriched = merged
    __main__.df_enriched = df_enriched
    merged.to_csv(ENRICHED_CSV, index=False)

    # ── Stats ────────────────────────────────────────────────────
    matched_domains = merged["Peec Domain"].nunique()
    matched_pubs = merged["Publisher ID"].nunique()
    total_citations = merged["Peec Citations"].sum()
    total_revenue = merged["Awin Revenue"].sum()

    excluded_note = (
        f' &nbsp;|&nbsp; \U0001f6ab Excluded: <b>{excluded_count}</b>'
        if excluded_count else ""
    )

    enrich_stats.value = (
        f'<div>'
        f'<span class="peec-stat">\U0001f517 Matched Domains: <b>{matched_domains}</b>{excluded_note}</span>'
        f'<span class="peec-stat">\U0001f465 Unique Publishers: <b>{matched_pubs}</b></span>'
        f'<span class="peec-stat">\U0001f4dd Citations: <b>{total_citations:,.0f}</b></span>'
        f'<span class="peec-stat">\U0001f4b0 Awin Revenue: <b>\u00a3{total_revenue:,.2f}</b></span>'
        f'</div>'
    )

    enrich_status_msg.value = (
        f"\u2705 Matched {matched_domains} domains across {matched_pubs} publishers. "
        f"CSV saved to output folder."
    )

    enrich_table.value = (
        '<div class="peec-scroll">'
        + merged.to_html(index=True, escape=False, max_cols=None, max_rows=None)
        + "</div>"
    )


def on_enrich_dl(b):
    if df_enriched is None or df_enriched.empty:
        return
    download_file(ENRICHED_CSV)


enrich_run_btn.on_click(run_enrich)
enrich_dl_btn.on_click(on_enrich_dl)

display(
    widgets.HTML(
        '<div class="peec-header">\U0001f4ca Enriched Report \u2014 Domain Match + Citations + Transactions</div>'
        '<div class="peec-sub">Matches Peec citation domains to Awin publisher domains, '
        "enriches with AI model data and transaction metrics</div>"
    ),
    widgets.HTML(
        '<div class="peec-section" style="margin-bottom:8px">'
        "\u2139\ufe0f <b>Models</b> = AI models citing this domain (3-char codes) &nbsp;|&nbsp; "
        "<b>Publisher Name</b> = from Awin publisher report</div>"
    ),
    enrich_domain_type,
    widgets.HTML('<div style="font-size:11px;color:#888;margin:-4px 0 4px 0">'
                 'Ctrl+click to select types to include (none selected = show all)</div>'),
    enrich_exclude,
    widgets.HBox(
        [enrich_run_btn, enrich_dl_btn],
        layout=widgets.Layout(margin="8px 0 10px 0"),
    ),
    enrich_stats,
    enrich_status_msg,
    enrich_table,
)
