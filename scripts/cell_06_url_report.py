# cell_06_url_report.py — URL / page-level aggregation report
# Produces: df_url_result

import __main__
import pandas as pd
import ipywidgets as widgets
from IPython.display import display, HTML, Javascript

# ── Prerequisites ────────────────────────────────────────────────
for _r in ["df_detail", "_scroll_table", "download_file", "PATHS"]:
    if not hasattr(__main__, _r) or getattr(__main__, _r) is None:
        raise RuntimeError(f"Missing '{_r}'. Run earlier cells first.")

df_detail = __main__.df_detail
_scroll_table = __main__._scroll_table
download_file = __main__.download_file
PATHS = __main__.PATHS

URL_CSV = str(PATHS["output"] / "peec_url_report.csv")
df_url_result = None

# ── Filters ──────────────────────────────────────────────────────
u_page_type = widgets.Dropdown(
    options=["All"], value="All", description="Page type:",
    style={"description_width": "90px"}, layout=widgets.Layout(width="260px"),
)
u_domain_type = widgets.Dropdown(
    options=["All"], value="All", description="Domain type:",
    style={"description_width": "90px"}, layout=widgets.Layout(width="260px"),
)
u_model = widgets.Dropdown(
    options=["All"], value="All", description="Model:",
    style={"description_width": "90px"}, layout=widgets.Layout(width="260px"),
)
u_prompt_search = widgets.Text(
    description="Prompt contains:", placeholder="e.g. best product",
    style={"description_width": "120px"}, layout=widgets.Layout(width="380px"),
)
u_title_search = widgets.Text(
    description="Title contains:", placeholder="e.g. review",
    style={"description_width": "120px"}, layout=widgets.Layout(width="380px"),
)
u_url_search = widgets.Text(
    description="URL contains:", placeholder="e.g. example.com",
    style={"description_width": "120px"}, layout=widgets.Layout(width="380px"),
)

u_dl_btn = widgets.Button(
    description="  \u2b07 Download CSV", button_style="success",
    layout=widgets.Layout(width="160px", height="36px"),
)
u_stats = widgets.HTML("")
u_table = widgets.Output()


def _run_url_report():
    global df_url_result
    u_stats.value = ""
    with u_table:
        u_table.clear_output(wait=True)

    if df_detail is None:
        with u_table:
            u_table.clear_output(wait=True)
            display(HTML("\u26a0\ufe0f Pull data first."))
        return

    df = df_detail.copy()

    # Pre-aggregation filters
    if u_model.value != "All":
        df = df[df["Model"] == u_model.value]
    pq = u_prompt_search.value.strip().lower()
    if pq:
        df = df[df["Prompt"].astype(str).str.lower().str.contains(pq, na=False)]
    tq = u_title_search.value.strip().lower()
    if tq:
        df = df[df["Title"].astype(str).str.lower().str.contains(tq, na=False)]
    if u_page_type.value != "All":
        df = df[df["Page Type"] == u_page_type.value]
    if u_domain_type.value != "All":
        df = df[df["Domain Type"] == u_domain_type.value]

    if df.empty:
        u_stats.value = '<div class="peec-stat">\u26a0\ufe0f No rows match filters</div>'
        return

    # Aggregate: one row per URL
    agg = (
        df.groupby("URL", as_index=False)
        .agg(
            Full_URL=("Full URL", "first"),
            Domain=("Domain", "first"),
            Title=("Title", "first"),
            Page_Type=("Page Type", "first"),
            Domain_Type=("Domain Type", "first"),
            Avg_Citation_Pos=("citation_avg", "mean"),
            Total_Citations=("usage_count", "sum"),
            Models_Present=("Model", "nunique"),
            Prompt_Count=("Prompt", "nunique"),
        )
    )
    agg.columns = [
        "URL", "Full URL", "Domain", "Title", "Page Type",
        "Domain Type", "Avg Citation Pos", "Total Citations",
        "Models Present", "Prompt Count",
    ]
    agg["Avg Citation Pos"] = agg["Avg Citation Pos"].round(2)

    # URL text filter (post-agg)
    uq = u_url_search.value.strip().lower()
    if uq:
        agg = agg[agg["URL"].str.lower().str.contains(uq, na=False)]

    agg = agg.sort_values("Total Citations", ascending=False).reset_index(drop=True)

    # Save full data to CSV
    csv_df = agg[["Domain", "Title", "Page Type", "Domain Type",
                   "Avg Citation Pos", "Total Citations", "Models Present",
                   "Prompt Count", "Full URL"]].copy()
    df_url_result = csv_df.copy()
    __main__.df_url_result = df_url_result
    csv_df.to_csv(URL_CSV, index=False)

    u_stats.value = (
        f'<div>'
        f'<span class="peec-stat">\U0001f517 URLs: <b>{len(agg):,}</b></span>'
        f'<span class="peec-stat">\U0001f310 Domains: <b>{agg["Domain"].nunique():,}</b></span>'
        f'<span class="peec-stat">\U0001f522 Total Citations: <b>{agg["Total Citations"].sum():,.0f}</b></span>'
        f'<span class="peec-stat">\U0001f4cd Avg Citation Pos: <b>{agg["Avg Citation Pos"].mean():.1f}</b></span>'
        f'</div>'
    )

    # Build display version: clickable truncated link as last column
    display_df = agg[["Domain", "Title", "Page Type", "Domain Type",
                      "Avg Citation Pos", "Total Citations", "Models Present",
                      "Prompt Count"]].copy()

    def _make_link(idx):
        full = agg.loc[idx, "Full URL"] if idx < len(agg) else ""
        if not full:
            return ""
        truncated = full[:70] + "..." if len(full) > 70 else full
        return f'<a href="{full}" target="_blank" title="{full}">{truncated}</a>'

    display_df["Link"] = [_make_link(i) for i in range(len(agg))]

    with u_table:
        u_table.clear_output(wait=True)
        display(HTML(
            '<div class="peec-scroll">'
            + display_df.to_html(index=True, escape=False, max_cols=None, max_rows=None)
            + "</div>"
        ))
        display(Javascript(
            'document.querySelectorAll(".peec-scroll").forEach(function(el){el.scrollTop=0});'
        ))


def _init_url_filters():
    if df_detail is None:
        return
    u_page_type.options = ["All"] + sorted(df_detail["Page Type"].dropna().unique().tolist())
    u_page_type.value = "All"
    u_domain_type.options = ["All"] + sorted(df_detail["Domain Type"].dropna().unique().tolist())
    u_domain_type.value = "All"
    u_model.options = ["All"] + sorted(df_detail["Model"].dropna().unique().tolist())
    u_model.value = "All"


def _on_u_filter(change):
    _run_url_report()


def _on_u_dl(b):
    if df_url_result is None or df_url_result.empty:
        return
    download_file(URL_CSV)


u_dl_btn.on_click(_on_u_dl)

_init_url_filters()

display(
    widgets.HTML(
        '<div class="peec-header">\U0001f517 URL / Page-Level Report</div>'
        '<div class="peec-sub">One row per URL \u2014 citations summed across models (filter to drill down)</div>'
    ),
    widgets.HTML('<div class="peec-section">Filters</div>'),
    widgets.HBox([u_page_type, u_domain_type, u_model], layout=widgets.Layout(margin="0 0 4px 0")),
    widgets.HBox([u_prompt_search, u_title_search], layout=widgets.Layout(margin="0 0 4px 0")),
    u_url_search,
    u_dl_btn,
    u_stats,
    u_table,
)

_run_url_report()

# Attach filter observers AFTER initial run to prevent double-trigger
u_page_type.observe(_on_u_filter, names="value")
u_domain_type.observe(_on_u_filter, names="value")
u_model.observe(_on_u_filter, names="value")
u_prompt_search.observe(_on_u_filter, names="value")
u_title_search.observe(_on_u_filter, names="value")
u_url_search.observe(_on_u_filter, names="value")
