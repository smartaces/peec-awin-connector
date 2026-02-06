# cell_05_domain_report.py — Domain-level aggregation report
# Produces: df_domain_result

import __main__
import pandas as pd
import ipywidgets as widgets
from IPython.display import display, HTML

# ── Prerequisites ────────────────────────────────────────────────
for _r in ["df_detail", "_scroll_table", "download_file", "PATHS"]:
    if not hasattr(__main__, _r) or getattr(__main__, _r) is None:
        raise RuntimeError(f"Missing '{_r}'. Run earlier cells first.")

df_detail = __main__.df_detail
_scroll_table = __main__._scroll_table
download_file = __main__.download_file
PATHS = __main__.PATHS

DOMAIN_CSV = str(PATHS["output"] / "peec_domain_report.csv")
df_domain_result = None

# ── Filters ──────────────────────────────────────────────────────
d_page_type = widgets.Dropdown(
    options=["All"], value="All", description="Page type:",
    style={"description_width": "90px"}, layout=widgets.Layout(width="260px"),
)
d_domain_type = widgets.Dropdown(
    options=["All"], value="All", description="Domain type:",
    style={"description_width": "90px"}, layout=widgets.Layout(width="260px"),
)
d_model = widgets.Dropdown(
    options=["All"], value="All", description="Model:",
    style={"description_width": "90px"}, layout=widgets.Layout(width="260px"),
)
d_prompt_search = widgets.Text(
    description="Prompt contains:", placeholder="e.g. best product",
    style={"description_width": "120px"}, layout=widgets.Layout(width="380px"),
)
d_domain_search = widgets.Text(
    description="Domain contains:", placeholder="e.g. example",
    style={"description_width": "120px"}, layout=widgets.Layout(width="380px"),
)

d_dl_btn = widgets.Button(
    description="  \u2b07 Download CSV", button_style="success",
    layout=widgets.Layout(width="160px", height="36px"),
)
d_stats = widgets.HTML("")
d_table = widgets.Output(layout=widgets.Layout(
    max_height='600px', overflow_y='auto', overflow_x='auto',
    border='1px solid #e0e0e0',
))


def _run_domain_report():
    global df_domain_result
    d_stats.value = ""
    with d_table:
        d_table.clear_output(wait=True)

    if df_detail is None:
        with d_table:
            d_table.clear_output(wait=True)
            display(HTML("\u26a0\ufe0f Pull data first."))
        return

    df = df_detail.copy()

    # Pre-aggregation filters
    if d_model.value != "All":
        df = df[df["Model"] == d_model.value]
    pq = d_prompt_search.value.strip().lower()
    if pq:
        df = df[df["Prompt"].astype(str).str.lower().str.contains(pq, na=False)]
    if d_page_type.value != "All":
        df = df[df["Page Type"] == d_page_type.value]

    if df.empty:
        d_stats.value = '<div class="peec-stat">\u26a0\ufe0f No rows match filters</div>'
        return

    # Aggregate
    agg = (
        df.groupby("Domain", as_index=False)
        .agg(
            Domain_Type=("Domain Type", "first"),
            Total_Citations=("usage_count", "sum"),
            Avg_Citation_Pos=("citation_avg", "mean"),
            Unique_Pages=("URL", "nunique"),
            Unique_Subdomains=("Subdomain", "nunique"),
            Models_Present=("Model", "nunique"),
            Prompts_Appearing_In=("Prompt", "nunique"),
        )
    )
    agg.columns = [
        "Domain", "Domain Type", "Total Citations", "Avg Citation Pos",
        "Unique Pages", "Unique Subdomains", "Models Present", "Prompts Appearing In",
    ]
    agg["Avg Citation Pos"] = agg["Avg Citation Pos"].round(2)

    # Post-aggregation filters
    if d_domain_type.value != "All":
        agg = agg[agg["Domain Type"] == d_domain_type.value]
    dq = d_domain_search.value.strip().lower()
    if dq:
        agg = agg[agg["Domain"].str.lower().str.contains(dq, na=False)]

    agg = agg.sort_values("Total Citations", ascending=False).reset_index(drop=True)
    df_domain_result = agg
    __main__.df_domain_result = df_domain_result
    agg.to_csv(DOMAIN_CSV, index=False)

    d_stats.value = (
        f'<div>'
        f'<span class="peec-stat">\U0001f310 Domains: <b>{len(agg):,}</b></span>'
        f'<span class="peec-stat">\U0001f522 Total Citations: <b>{agg["Total Citations"].sum():,.0f}</b></span>'
        f'<span class="peec-stat">\U0001f4cd Avg Citation Pos: <b>{agg["Avg Citation Pos"].mean():.1f}</b></span>'
        f'<span class="peec-stat">\U0001f4c4 Total Unique Pages: <b>{agg["Unique Pages"].sum():,.0f}</b></span>'
        f'</div>'
    )
    with d_table:
        d_table.clear_output(wait=True)
        display(HTML(
            '<div class="peec-scroll" style="max-height:none;overflow:visible;">'
            + agg.to_html(index=True, escape=False, max_cols=None, max_rows=None)
            + "</div>"
        ))


def _init_domain_filters():
    if df_detail is None:
        return
    d_page_type.options = ["All"] + sorted(df_detail["Page Type"].dropna().unique().tolist())
    d_page_type.value = "All"
    d_domain_type.options = ["All"] + sorted(df_detail["Domain Type"].dropna().unique().tolist())
    d_domain_type.value = "All"
    d_model.options = ["All"] + sorted(df_detail["Model"].dropna().unique().tolist())
    d_model.value = "All"


def _on_d_filter(change):
    _run_domain_report()


def _on_d_dl(b):
    if df_domain_result is None or df_domain_result.empty:
        return
    download_file(DOMAIN_CSV)


d_dl_btn.on_click(_on_d_dl)

_init_domain_filters()

display(
    widgets.HTML(
        '<div class="peec-header">\U0001f310 Domain-Level Report</div>'
        '<div class="peec-sub">One row per domain \u2014 citations summed across models (filter to drill down)</div>'
    ),
    widgets.HTML('<div class="peec-section">Filters</div>'),
    widgets.HBox([d_page_type, d_domain_type, d_model], layout=widgets.Layout(margin="0 0 4px 0")),
    widgets.HBox([d_prompt_search, d_domain_search], layout=widgets.Layout(margin="0 0 8px 0")),
    d_dl_btn,
    d_stats,
    d_table,
)

_run_domain_report()

# Attach filter observers AFTER initial run to prevent double-trigger
d_page_type.observe(_on_d_filter, names="value")
d_domain_type.observe(_on_d_filter, names="value")
d_model.observe(_on_d_filter, names="value")
d_prompt_search.observe(_on_d_filter, names="value")
d_domain_search.observe(_on_d_filter, names="value")
