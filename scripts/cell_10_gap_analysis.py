# cell_10_gap_analysis.py — Gap analysis: PEEC-cited domains NOT in Awin
# Identifies potential recruitment targets.
# Produces: df_gap

import __main__
import pandas as pd
import ipywidgets as widgets
from IPython.display import display, HTML

# ── Prerequisites ────────────────────────────────────────────────
for _r in ["df_detail", "df_domain_result", "df_enriched",
           "_scroll_table", "download_file", "PATHS"]:
    if not hasattr(__main__, _r) or getattr(__main__, _r) is None:
        raise RuntimeError(f"Missing '{_r}'. Run earlier cells first.")

df_detail = __main__.df_detail
df_domain_result = __main__.df_domain_result
df_enriched = __main__.df_enriched
_scroll_table = __main__._scroll_table
download_file = __main__.download_file
PATHS = __main__.PATHS

GAP_CSV = str(PATHS["output"] / "peec_awin_gap_analysis.csv")
df_gap = None

# ── Widgets ──────────────────────────────────────────────────────
gap_table = widgets.Output(layout=widgets.Layout(
    max_height='600px', overflow_y='auto', overflow_x='auto',
    border='1px solid #e0e0e0',
))
gap_status_msg = widgets.HTML("")
gap_stats = widgets.HTML("")
gap_dl_btn = widgets.Button(
    description="  \u2b07 Download CSV", button_style="success",
    layout=widgets.Layout(width="160px", height="36px"),
)
gap_run_btn = widgets.Button(
    description="  Run Gap Analysis", button_style="warning",
    icon="search", layout=widgets.Layout(width="200px", height="36px"),
)

gap_domain_type = widgets.Dropdown(
    options=["All"], value="All", description="Domain type:",
    style={"description_width": "90px"}, layout=widgets.Layout(width="260px"),
)
gap_domain_search = widgets.Text(
    description="Domain contains:",
    placeholder="e.g. example, shop",
    style={"description_width": "120px"},
    layout=widgets.Layout(width="400px"),
)
gap_exclude = widgets.Text(
    description="Exclude domains:",
    placeholder="e.g. google, wikipedia, youtube, reddit",
    style={"description_width": "120px"},
    layout=widgets.Layout(width="600px"),
)


def _parse_keywords(text):
    if not text or not text.strip():
        return []
    return [kw.strip().lower() for kw in text.split(",") if kw.strip()]


def run_gap(b=None):
    global df_gap
    gap_stats.value = ""
    with gap_table:
        gap_table.clear_output(wait=True)
    gap_status_msg.value = ""

    if df_detail is None or df_detail.empty:
        gap_status_msg.value = "\u26a0\ufe0f Run the Peec Citation Data pull cell first."
        return
    if df_domain_result is None or df_domain_result.empty:
        gap_status_msg.value = "\u26a0\ufe0f Run the Domain-Level Report cell first."
        return
    if df_enriched is None or df_enriched.empty:
        gap_status_msg.value = "\u26a0\ufe0f Run the Enriched Report cell first."
        return

    gap_status_msg.value = "\u23f3 Identifying Peec domains not matched to Awin publishers..."

    # ── Get matched domains from enriched report ─────────────
    matched_domains = set(df_enriched["Peec Domain"].str.lower().unique())

    # ── Get all Peec domains ─────────────────────────────────
    all_peec = df_domain_result.copy()
    all_peec["_domain_lower"] = all_peec["Domain"].str.lower()

    # Filter to unmatched only
    gap_domains = all_peec[~all_peec["_domain_lower"].isin(matched_domains)].copy()
    gap_domains = gap_domains.drop(columns=["_domain_lower"])

    if gap_domains.empty:
        gap_status_msg.value = "\U0001f389 Every Peec-cited domain is matched to an Awin publisher!"
        return

    # ── Populate domain type filter ──────────────────────────
    available_types = sorted(gap_domains["Domain Type"].dropna().unique().tolist())
    gap_domain_type.options = ["All"] + available_types
    if gap_domain_type.value not in gap_domain_type.options:
        gap_domain_type.value = "All"

    # ── Apply domain type filter ─────────────────────────────
    if gap_domain_type.value != "All":
        gap_domains = gap_domains[gap_domains["Domain Type"] == gap_domain_type.value]

    # ── Apply domain keyword include filter ──────────────────
    include_kws = _parse_keywords(gap_domain_search.value)
    if include_kws:
        mask = gap_domains["Domain"].str.lower().apply(
            lambda d: any(kw in d for kw in include_kws)
        )
        gap_domains = gap_domains[mask]

    # ── Apply domain keyword exclude filter ──────────────────
    exclude_kws = _parse_keywords(gap_exclude.value)
    excluded_count = 0
    if exclude_kws:
        mask = gap_domains["Domain"].str.lower().apply(
            lambda d: any(kw in d for kw in exclude_kws)
        )
        excluded_count = mask.sum()
        gap_domains = gap_domains[~mask]

    if gap_domains.empty:
        gap_stats.value = '<div class="peec-stat">\u26a0\ufe0f No domains match current filters</div>'
        gap_status_msg.value = ""
        return

    # ── Build URL-level detail for gap domains ───────────────
    gap_domain_set = set(gap_domains["Domain"].str.lower())

    detail = df_detail.copy()
    detail["_domain_lower"] = detail["Domain"].str.lower()
    detail = detail[detail["_domain_lower"].isin(gap_domain_set)].copy()
    detail = detail.drop(columns=["_domain_lower"])

    if detail.empty:
        gap_status_msg.value = "\u26a0\ufe0f No URL-level detail found for gap domains."
        return

    # Aggregate: one row per URL
    url_agg = (
        detail.groupby("URL", as_index=False)
        .agg(
            Full_URL=("Full URL", "first"),
            Domain=("Domain", "first"),
            Title=("Title", "first"),
            Page_Type=("Page Type", "first"),
            Domain_Type=("Domain Type", "first"),
            Citations=("usage_count", "sum"),
            Avg_Pos=("citation_avg", "mean"),
            Models=("Model", lambda x: ", ".join(
                sorted(set(str(m)[:5] for m in x.dropna()))
            )),
            Model_Count=("Model", "nunique"),
            Prompt_Count=("Prompt", "nunique"),
        )
    )
    url_agg.columns = [
        "URL", "Full URL", "Domain", "Title", "Page Type",
        "Domain Type", "Citations", "Avg Pos",
        "Models", "Model Count", "Prompt Count",
    ]
    url_agg["Avg Pos"] = url_agg["Avg Pos"].round(2)

    # Bring in domain-level totals for context
    domain_totals = gap_domains.set_index("Domain")[["Total Citations"]].rename(
        columns={"Total Citations": "Domain Total Citations"}
    )
    url_agg = url_agg.merge(
        domain_totals, left_on="Domain", right_index=True, how="left",
    )

    # Sort: highest domain citation total first, then highest URL citations
    url_agg = url_agg.sort_values(
        ["Domain Total Citations", "Citations"],
        ascending=[False, False],
    ).reset_index(drop=True)

    # ── Build display version with clickable links ───────────
    def _make_link(full_url):
        if not full_url:
            return ""
        truncated = full_url[:60] + "..." if len(str(full_url)) > 60 else full_url
        return (
            f'<a href="{full_url}" target="_blank" title="{full_url}">'
            f"{truncated}</a>"
        )

    display_df = url_agg[[
        "Domain", "Domain Type", "Domain Total Citations",
        "Title", "Citations", "Avg Pos",
        "Models", "Model Count", "Prompt Count",
    ]].copy()
    display_df["Link"] = url_agg["Full URL"].apply(_make_link)

    # ── Save CSV (full URLs, no HTML) ────────────────────────
    csv_df = url_agg[[
        "Domain", "Domain Type", "Domain Total Citations",
        "Title", "Citations", "Avg Pos",
        "Models", "Model Count", "Prompt Count",
        "Full URL",
    ]].copy()
    df_gap = csv_df
    __main__.df_gap = df_gap
    csv_df.to_csv(GAP_CSV, index=False)

    # ── Stats ────────────────────────────────────────────────
    n_domains = display_df["Domain"].nunique()
    n_urls = len(display_df)
    total_citations = display_df["Citations"].sum()
    domain_total_citations = (
        display_df.drop_duplicates("Domain")["Domain Total Citations"].sum()
    )

    excluded_note = (
        f' &nbsp;|&nbsp; \U0001f6ab Excluded: <b>{excluded_count}</b>'
        if excluded_count else ""
    )

    gap_stats.value = (
        f'<div>'
        f'<span class="peec-stat">\U0001f50d Gap Domains: <b>{n_domains:,}</b>{excluded_note}</span>'
        f'<span class="peec-stat">\U0001f517 Gap URLs: <b>{n_urls:,}</b></span>'
        f'<span class="peec-stat">\U0001f4dd Domain Citations: <b>{domain_total_citations:,.0f}</b></span>'
        f'<span class="peec-stat">\U0001f4c4 URL Citations: <b>{total_citations:,.0f}</b></span>'
        f'</div>'
    )

    gap_status_msg.value = (
        f"\u2705 Found {n_domains} gap domains across {n_urls} URLs. "
        f"CSV saved to output folder."
    )

    with gap_table:
        gap_table.clear_output(wait=True)
        display(HTML(
            '<div class="peec-scroll" style="max-height:none;overflow:visible;">'
            + display_df.to_html(index=True, escape=False, max_cols=None, max_rows=None)
            + "</div>"
        ))


def on_gap_dl(b):
    if df_gap is None or df_gap.empty:
        return
    download_file(GAP_CSV)


gap_run_btn.on_click(run_gap)
gap_dl_btn.on_click(on_gap_dl)

display(
    widgets.HTML(
        '<div class="peec-header">\U0001f50d Gap Analysis \u2014 AI-Cited Domains NOT in Awin</div>'
        '<div class="peec-sub">Identifies domains and pages cited by AI models where you have '
        "no Awin publisher relationship \u2014 potential recruitment targets</div>"
    ),
    widgets.HTML('<div class="peec-section">Filters</div>'),
    widgets.HBox([gap_domain_type], layout=widgets.Layout(margin="0 0 4px 0")),
    gap_domain_search,
    gap_exclude,
    widgets.HBox(
        [gap_run_btn, gap_dl_btn],
        layout=widgets.Layout(margin="8px 0 10px 0"),
    ),
    gap_stats,
    gap_status_msg,
    gap_table,
)
