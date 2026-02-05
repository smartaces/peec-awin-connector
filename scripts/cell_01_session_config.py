# cell_01_session_config.py — Unified session configuration
# Sets: API keys, PEEC project, date range, Awin advertiser ID
# All downstream cells read from these shared globals.

import os
import sys
import __main__
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv
import ipywidgets as widgets
import requests
from IPython.display import display, HTML

# ── Environment detection ────────────────────────────────────────
try:
    from google.colab import userdata  # type: ignore
except ImportError:
    userdata = None

IN_COLAB = "google.colab" in sys.modules
__main__.IN_COLAB = IN_COLAB

load_dotenv()


# ── API key loader ───────────────────────────────────────────────
def _load_key(secret_name, env_var, label):
    """Load an API key: Colab secrets -> .env -> env var."""
    key = None

    if IN_COLAB:
        try:
            from google.colab import userdata as _ud  # type: ignore
            key = _ud.get(secret_name)
            if key:
                print(f"\u2705 Loaded {label} key from Colab secret '{secret_name}'.")
        except Exception as exc:
            print(f"\u26a0\ufe0f Could not read Colab secret '{secret_name}': {exc}")

    if not key:
        key = os.getenv(env_var)
        if key:
            print(f"\u2705 Loaded {label} key from environment variable '{env_var}'.")

    if not key:
        raise RuntimeError(
            f"{label} API key not found. "
            f"In Colab, add it via Settings \u2192 Secrets as '{secret_name}'. "
            f"Locally, set {env_var} in a .env file or export it."
        )

    os.environ[env_var] = key


_load_key("PEEC_API_KEY", "PEEC_API_KEY", "Peec AI")
_load_key("AWAPI", "AWAPI", "Awin")
print("\U0001f510 Both API keys configured.\n")


# ── Fetch PEEC projects for dropdown ─────────────────────────────
PEEC_BASE = "https://api.peec.ai/customer/v1"
_headers = {
    "X-API-Key": os.environ["PEEC_API_KEY"],
    "Content-Type": "application/json",
}

print("\u23f3 Loading PEEC projects...")
_projects_resp = requests.get(
    f"{PEEC_BASE}/projects",
    headers=_headers,
    params={"limit": 1000, "offset": 0},
)
_projects_resp.raise_for_status()
_project_list = _projects_resp.json()["data"]
print(f"\u2705 Found {len(_project_list)} project(s).\n")

_project_options = {
    f"{p['name']} ({p['status']})": p["id"] for p in _project_list
}


# ── Session config widgets ───────────────────────────────────────
_cfg_header = widgets.HTML(
    '<div class="peec-header">\u2699\ufe0f Session Configuration</div>'
    '<div class="peec-sub">Set your parameters once \u2014 all data pulls will use these values.</div>'
)

_project_dd = widgets.Dropdown(
    options=_project_options,
    description="PEEC Project:",
    style={"description_width": "110px"},
    layout=widgets.Layout(width="450px"),
)

_adv_id = widgets.Text(
    description="Awin Advertiser ID:",
    value="",
    placeholder="e.g. 4567",
    style={"description_width": "140px"},
    layout=widgets.Layout(width="300px"),
)

_start_picker = widgets.DatePicker(
    description="Start date:",
    value=date(2026, 1, 1),
    style={"description_width": "80px"},
    layout=widgets.Layout(width="240px"),
)
_end_picker = widgets.DatePicker(
    description="End date:",
    value=date.today(),
    style={"description_width": "80px"},
    layout=widgets.Layout(width="240px"),
)

_confirm_btn = widgets.Button(
    description="  Confirm Settings",
    button_style="success",
    icon="check",
    layout=widgets.Layout(width="200px", height="36px"),
)
_cfg_status = widgets.HTML("")
_cfg_output = widgets.Output()


def _on_confirm(b):
    with _cfg_output:
        _cfg_output.clear_output()

        sd = _start_picker.value
        ed = _end_picker.value
        if sd is None or ed is None:
            print("\u26a0\ufe0f Please select both start and end dates.")
            return
        if sd > ed:
            print("\u26a0\ufe0f Start date must be before end date.")
            return

        adv_text = _adv_id.value.strip()
        if not adv_text or not adv_text.isdigit() or int(adv_text) == 0:
            print("\u26a0\ufe0f Please enter a valid Awin Advertiser ID.")
            return

        # Set globals on __main__ so all subsequent cells can access them
        __main__.SESSION_START_DATE = str(sd)
        __main__.SESSION_END_DATE = str(ed)
        __main__.ADVERTISER_ID = int(adv_text)
        __main__.PROJECT_ID = _project_dd.value
        __main__.PROJECT_NAME = _project_dd.label

        _cfg_status.value = (
            f'<div style="margin-top:8px">'
            f'<span class="peec-stat">\u2705 Project: <b>{__main__.PROJECT_NAME}</b></span>'
            f'<span class="peec-stat">\U0001f4c5 {__main__.SESSION_START_DATE} \u2192 {__main__.SESSION_END_DATE}</span>'
            f'<span class="peec-stat">\U0001f4e2 Advertiser: <b>{__main__.ADVERTISER_ID}</b></span>'
            f'</div>'
        )
        print(f"\u2705 Session configured.")


_confirm_btn.on_click(_on_confirm)

display(
    _cfg_header,
    _project_dd,
    _adv_id,
    widgets.HBox(
        [_start_picker, _end_picker],
        layout=widgets.Layout(margin="0 0 8px 0"),
    ),
    _confirm_btn,
    _cfg_output,
    _cfg_status,
)
