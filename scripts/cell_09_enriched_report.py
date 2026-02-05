# cell_09_enriched_report.py — Enriched report: matched domains + transaction metrics + models
# Simplified: uses transaction-level data only (no publisher performance report).
# Produces: df_enriched

import __main__
import pandas as pd
import ipywidgets as widgets
from IPython.display import display, HTML

# ── Prerequisites ────────────────────────────────────────────────
for _r in ["df_matched", "df_detail", "_scroll_table", "download_file", "PATHS"]:
    if not hasattr(__main__, _r) or getattr(__main__, _r) is None:
        raise RuntimeError(f"Missing '{_r}'. Run earlier cells first.")

df_matched = __main__.df_matched
df_detail = __main__.df_detail
_scroll_table = __main__._scroll_table
download_file = __main__.download_file
PATHS = __main__.PATHS

ENRICHED_CSV = str(PATHS["output"] / "peec_awin_enriched.csv")
df_enriched = None

# ── Widgets ──────────────────────────────────────────────────────
enrich_output = widgets.Output()
enrich_stats = widgets.HTML("")
enrich_dl_btn = widgets.Button(
    description="  \u2b07 Download CSV", button_style="success",
    layout=widgets.Layout(width="160px", height="36px"),
)
enrich_run_btn = widgets.Button(
    description="  Build Enriched Report", button_style="info",
    icon="bar-chart", layout=widgets.Layout(width="240px", height="36px"),
)

enrich_exclude = widgets.Text(
    description="Exclude domains:",
    placeholder="e.g. google, facebook, pinterest, reddit",
    style={"description_width": "120px"},
    layout=widgets.Layout(width="600px"),
)


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
    Build a dict: domain -> "gpt-4, claud, gemin, ..."
    Each model ID is truncated to 5 characters, de-duped, and sorted.
    """
    if detail_df is None or detail_df.empty:
        return {}
    grouped = (
        detail_df.dropna(subset=["Model"])
        .groupby("Domain")["Model"]
        .apply(lambda models: ", ".join(
            sorted(set(str(m)[:5] for m in models))
        ))
    )
    return grouped.to_dict()


def run_enrich(b=None):
    global df_enriched
    with enrich_output:
        enrich_output.clear_output()
        enrich_stats.value = ""

        if df_matched is None or df_matched.empty:
            print("\u26a0\ufe0f Run the Domain Match cell first.")
            return

        print("\u23f3 Building enriched report...")

        merged = df_matched.copy()

        # ── Add Models column from df_detail ─────────────────────
        model_lookup = _build_model_lookup(df_detail)
        merged["Models"] = merged["Peec Domain"].map(model_lookup).fillna("")

        if model_lookup:
            print(
                f"\U0001f916 Mapped models for "
                f"{sum(1 for v in model_lookup.values() if v):,} domains"
            )
        else:
            print(
                "\u2139\ufe0f df_detail not available \u2014 Models column will be empty. "
                "Run the Peec Citation Data pull cell to populate it."
            )

        # ── Apply keyword exclude filter ─────────────────────────
        exclude_keywords = _parse_exclude_keywords(enrich_exclude.value)
        excluded_count = 0
        if exclude_keywords:
            mask = merged.apply(lambda row: _row_excluded(row, exclude_keywords), axis=1)
            excluded_count = mask.sum()
            merged = merged[~mask].reset_index(drop=True)
            print(f"\U0001f6ab Excluded {excluded_count} rows matching: {', '.join(exclude_keywords)}")

        # ── Build final output column order ──────────────────────
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

        enrich_output.clear_output()

        # ── Stats ────────────────────────────────────────────────
        matched_pubs = merged["Publisher ID"].nunique()
        total_citations = merged["Peec Citations"].sum()
        total_revenue = merged["Awin Revenue"].sum()

        excluded_note = (
            f' &nbsp;|&nbsp; \U0001f6ab Excluded: <b>{excluded_count}</b>'
            if excluded_count else ""
        )

        enrich_stats.value = (
            f'<div>'
            f'<span class="peec-stat">\U0001f517 Enriched Rows: <b>{len(merged):,}</b>{excluded_note}</span>'
            f'<span class="peec-stat">\U0001f465 Unique Publishers: <b>{matched_pubs}</b></span>'
            f'<span class="peec-stat">\U0001f4dd Citations: <b>{total_citations:,.0f}</b></span>'
            f'<span class="peec-stat">\U0001f4b0 Awin Revenue: <b>\u00a3{total_revenue:,.2f}</b></span>'
            f'</div>'
        )

        display(_scroll_table(merged))


def on_enrich_dl(b):
    if df_enriched is None or df_enriched.empty:
        return
    download_file(ENRICHED_CSV)


enrich_run_btn.on_click(run_enrich)
enrich_dl_btn.on_click(on_enrich_dl)

display(
    widgets.HTML(
        '<div class="peec-header">\U0001f4ca Enriched Report \u2014 Citations + Transaction Data</div>'
        '<div class="peec-sub">Combines matched domains with Awin transaction metrics '
        "and AI model citation data</div>"
    ),
    widgets.HTML(
        '<div class="peec-section" style="margin-bottom:8px">'
        "\u2139\ufe0f <b>Awin Domain</b> = publisher domain from transaction URL &nbsp;|&nbsp; "
        "<b>Models</b> = AI models citing this domain (truncated to 5 chars)</div>"
    ),
    enrich_exclude,
    widgets.HBox(
        [enrich_run_btn, enrich_dl_btn],
        layout=widgets.Layout(margin="8px 0 10px 0"),
    ),
    enrich_stats,
    enrich_output,
)
