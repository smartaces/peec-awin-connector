# cell_08_domain_match.py — Peec <> Awin domain matching
# Normalised-hostname approach: strip protocol, www., paths, then exact match.
# Produces: df_matched

import __main__
import pandas as pd
import ipywidgets as widgets
from IPython.display import display, HTML

# ── Prerequisites ────────────────────────────────────────────────
for _r in ["df_domain_result", "df_awin_tx", "_normalise_host",
           "_scroll_table", "download_file", "PATHS"]:
    if not hasattr(__main__, _r) or getattr(__main__, _r) is None:
        raise RuntimeError(f"Missing '{_r}'. Run earlier cells first.")

df_domain_result = __main__.df_domain_result
df_awin_tx = __main__.df_awin_tx
_normalise_host = __main__._normalise_host
_scroll_table = __main__._scroll_table
download_file = __main__.download_file
PATHS = __main__.PATHS

MATCH_CSV = str(PATHS["output"] / "peec_awin_domain_match.csv")
df_matched = None

# ── Widgets ──────────────────────────────────────────────────────
match_output = widgets.Output()
match_stats = widgets.HTML("")
match_dl_btn = widgets.Button(
    description="  \u2b07 Download CSV", button_style="success",
    layout=widgets.Layout(width="160px", height="36px"),
)
match_run_btn = widgets.Button(
    description="  Run Domain Match", button_style="info",
    icon="link", layout=widgets.Layout(width="200px", height="36px"),
)


def run_match(b=None):
    global df_matched
    with match_output:
        match_output.clear_output()
        match_stats.value = ""

        if df_domain_result is None or df_domain_result.empty:
            print("\u26a0\ufe0f Run the Domain Report first.")
            return
        if df_awin_tx is None or df_awin_tx.empty:
            print("\u26a0\ufe0f Run the Awin Transaction Report first.")
            return

        print("\u23f3 Building Awin publisher domain summary...")

        # ── Aggregate Awin transactions by publisher domain ──────
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

        # ── Normalise hostnames ──────────────────────────────────
        awin_domains["_awin_host"] = awin_domains["Awin Domain"].apply(_normalise_host)
        peec = df_domain_result.copy()
        peec["_peec_host"] = peec["Domain"].apply(_normalise_host)

        print(
            f"\u23f3 Matching {len(peec)} Peec domains against "
            f"{len(awin_domains)} Awin publisher domains..."
        )

        # ── Build lookup dict: normalised host -> Awin row(s) ────
        awin_lookup = {}
        for _, a in awin_domains.iterrows():
            host = a["_awin_host"]
            if host and len(host) >= 3:
                awin_lookup.setdefault(host, []).append(a)

        # ── Match: Peec host == Awin host (exact after normalisation)
        matches = []
        for _, p in peec.iterrows():
            ph = p["_peec_host"]
            if not ph or len(ph) < 3:
                continue
            if ph in awin_lookup:
                for a in awin_lookup[ph]:
                    matches.append({
                        # Peec
                        "Peec Domain": p["Domain"],
                        "Domain Type": p.get("Domain Type", ""),
                        "Peec Citations": p.get("Total Citations", 0),
                        "Peec Avg Pos": p.get("Avg Citation Pos", 0),
                        "Peec Unique Pages": p.get("Unique Pages", 0),
                        "Peec Models Present": p.get("Models Present", 0),
                        # Awin
                        "Awin Domain": a["Awin Domain"],
                        "Publisher ID": a["Publisher ID"],
                        "Publisher Name": a["Publisher Name"],
                        "Awin Transactions": a["Awin Transactions"],
                        "Awin Revenue": a["Awin Revenue"],
                        "Awin Commission": a["Awin Commission"],
                        "Awin AOV": a["Awin AOV"],
                        # Debug
                        "Peec Host": ph,
                        "Awin Host": a["_awin_host"],
                    })

        match_output.clear_output()

        if not matches:
            print("\u26a0\ufe0f No domain matches found.\n")
            print("Peec normalised hosts (first 30):")
            for s in sorted(peec["_peec_host"].unique())[:30]:
                print(f"  {s}")
            print(f"\nAwin publisher normalised hosts (first 30):")
            for s in sorted(awin_domains["_awin_host"].unique())[:30]:
                print(f"  {s}")
            print("\nCompare the lists above for near-misses.")
            return

        df_m = pd.DataFrame(matches)
        df_m = df_m.sort_values("Peec Citations", ascending=False).reset_index(drop=True)
        df_matched = df_m
        __main__.df_matched = df_matched
        df_m.to_csv(MATCH_CSV, index=False)

        peec_matched = df_m["Peec Domain"].nunique()
        awin_matched = df_m["Awin Domain"].nunique()

        match_stats.value = (
            f'<div>'
            f'<span class="peec-stat">\U0001f517 Matched Pairs: <b>{len(df_m):,}</b></span>'
            f'<span class="peec-stat">\U0001f310 Peec Domains: <b>{peec_matched}</b> / {len(peec)}</span>'
            f'<span class="peec-stat">\U0001f465 Awin Domains: <b>{awin_matched}</b> / {len(awin_domains)}</span>'
            f'<span class="peec-stat">\U0001f4dd Citations (matched): <b>{df_m["Peec Citations"].sum():,.0f}</b></span>'
            f'<span class="peec-stat">\U0001f4b0 Revenue (matched): <b>\u00a3{df_m["Awin Revenue"].sum():,.2f}</b></span>'
            f'</div>'
        )
        display(_scroll_table(df_m))


def on_match_dl(b):
    if df_matched is None or df_matched.empty:
        return
    download_file(MATCH_CSV)


match_run_btn.on_click(run_match)
match_dl_btn.on_click(on_match_dl)

display(
    widgets.HTML(
        '<div class="peec-header">\U0001f517 Peec \u2194 Awin Domain Match</div>'
        '<div class="peec-sub">Matches Peec citation domains to Awin publisher domains '
        "via normalised hostname (exact match, www. stripped)</div>"
    ),
    widgets.HBox(
        [match_run_btn, match_dl_btn],
        layout=widgets.Layout(margin="0 0 10px 0"),
    ),
    match_stats,
    match_output,
)
