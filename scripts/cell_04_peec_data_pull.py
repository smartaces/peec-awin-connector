# cell_04_peec_data_pull.py — Pull PEEC citation data
# Uses session date range and project. Produces: df_detail

import __main__
import pandas as pd
import ipywidgets as widgets
from IPython.display import display

# ── Prerequisites ────────────────────────────────────────────────
for _r in ["peec", "prompt_lookup", "SESSION_START_DATE", "SESSION_END_DATE",
           "PROJECT_ID", "PROJECT_NAME", "_build_row", "_scroll_table", "PATHS"]:
    if not hasattr(__main__, _r) or getattr(__main__, _r) is None:
        raise RuntimeError(f"Missing '{_r}'. Run earlier cells first.")

peec = __main__.peec
_build_row = __main__._build_row
_scroll_table = __main__._scroll_table
PROJECT_ID = __main__.PROJECT_ID
PROJECT_NAME = __main__.PROJECT_NAME
PATHS = __main__.PATHS

# ── State ────────────────────────────────────────────────────────
df_detail = None

# ── Widgets ──────────────────────────────────────────────────────
header = widgets.HTML(
    '<div class="peec-header">\U0001f4ca Peec AI \u2014 Citation Data</div>'
    f'<div class="peec-sub">Project: <b>{PROJECT_NAME}</b> &nbsp;|&nbsp; '
    f'Date range: <b>{__main__.SESSION_START_DATE}</b> to <b>{__main__.SESSION_END_DATE}</b></div>'
)

pull_btn = widgets.Button(
    description="  Pull Data", button_style="info",
    icon="cloud-download", layout=widgets.Layout(width="160px", height="36px"),
)
pull_stats = widgets.HTML("")
pull_output = widgets.Output()


def on_pull(b):
    global df_detail
    with pull_output:
        pull_output.clear_output()
        pull_stats.value = ""
        sd = __main__.SESSION_START_DATE
        ed = __main__.SESSION_END_DATE

        print("\u23f3 Fetching domain classifications...")
        domains_report = peec.report_domains(
            start_date=sd, end_date=ed, project_id=PROJECT_ID,
        )
        domain_class = {
            r["domain"]: r.get("classification", "Unknown")
            for r in domains_report.get("data", []) if r.get("domain")
        }

        print("\u23f3 Fetching URL report (prompt \u00d7 model breakdown)...")
        report = peec.report_urls(
            start_date=sd, end_date=ed, project_id=PROJECT_ID,
            dimensions=["prompt_id", "model_id"],
        )

        rows = report.get("data", [])
        if not rows:
            print("\u26a0\ufe0f No data returned for this date range.")
            return

        df = pd.DataFrame([_build_row(r) for r in rows])
        df["Domain Type"] = df["Domain"].map(domain_class).fillna("Unknown")
        df_detail = df
        __main__.df_detail = df_detail

        pull_output.clear_output()
        pull_stats.value = (
            f'<div>'
            f'<span class="peec-stat">\u2705 Pulled <b>{len(df):,}</b> raw rows</span>'
            f'<span class="peec-stat">\U0001f310 <b>{df["Domain"].nunique():,}</b> domains</span>'
            f'<span class="peec-stat">\U0001f517 <b>{df["URL"].nunique():,}</b> unique URLs</span>'
            f'<span class="peec-stat">\U0001f916 <b>{df["Model"].nunique():,}</b> models</span>'
            f'</div><div class="peec-section">Now run the Domain or URL report cells below \u2193</div>'
        )


pull_btn.on_click(on_pull)

display(header, pull_btn, pull_output, pull_stats)
