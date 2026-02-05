# cell_03_peec_client.py — PeecClient, shared helpers, lookup tables
# Produces globals: peec, prompt_lookup, tag_lookup, topic_lookup,
#   _extract_domain, _extract_subdomain, _build_row, _scroll_table,
#   _normalise_host, download_file

import os
import re
import shutil
import __main__
from pathlib import Path
from urllib.parse import urlparse

import requests
import pandas as pd
from IPython.display import HTML

# ── Prerequisites ────────────────────────────────────────────────
_required = ["PROJECT_ID", "PROJECT_NAME", "IN_COLAB", "PATHS"]
for _r in _required:
    if not hasattr(__main__, _r) or getattr(__main__, _r) is None:
        raise RuntimeError(f"Missing '{_r}'. Run the Session Config cell first.")

PROJECT_ID = __main__.PROJECT_ID
PROJECT_NAME = __main__.PROJECT_NAME
IN_COLAB = __main__.IN_COLAB
PATHS = __main__.PATHS

PEEC_BASE = "https://api.peec.ai/customer/v1"


# ══════════════════════════════════════════════════════════════════
# PeecClient
# ══════════════════════════════════════════════════════════════════
class PeecClient:
    """Lightweight wrapper around the Peec AI Customer API."""

    def __init__(self, api_key=None):
        self.api_key = api_key or os.environ["PEEC_API_KEY"]
        self.headers = {
            "X-API-Key": self.api_key,
            "Content-Type": "application/json",
        }

    # ── project / lookup endpoints (GET) ─────────────────────────
    def get_projects(self, limit=1000, offset=0):
        return requests.get(
            f"{PEEC_BASE}/projects",
            headers=self.headers,
            params={"limit": limit, "offset": offset},
        ).json()

    def get_brands(self, project_id=None, limit=1000, offset=0):
        params = {"limit": limit, "offset": offset}
        if project_id:
            params["project_id"] = project_id
        return requests.get(f"{PEEC_BASE}/brands", headers=self.headers, params=params).json()

    def get_prompts(self, project_id=None, limit=1000, offset=0):
        params = {"limit": limit, "offset": offset}
        if project_id:
            params["project_id"] = project_id
        return requests.get(f"{PEEC_BASE}/prompts", headers=self.headers, params=params).json()

    def get_tags(self, project_id=None, limit=1000, offset=0):
        params = {"limit": limit, "offset": offset}
        if project_id:
            params["project_id"] = project_id
        return requests.get(f"{PEEC_BASE}/tags", headers=self.headers, params=params).json()

    def get_topics(self, project_id=None, limit=1000, offset=0):
        params = {"limit": limit, "offset": offset}
        if project_id:
            params["project_id"] = project_id
        return requests.get(f"{PEEC_BASE}/topics", headers=self.headers, params=params).json()

    def get_models(self, project_id=None, limit=1000, offset=0):
        params = {"limit": limit, "offset": offset}
        if project_id:
            params["project_id"] = project_id
        return requests.get(f"{PEEC_BASE}/models", headers=self.headers, params=params).json()

    def get_chats(self, start_date, end_date, project_id=None, limit=1000, offset=0):
        params = {"limit": limit, "offset": offset, "start_date": start_date, "end_date": end_date}
        if project_id:
            params["project_id"] = project_id
        return requests.get(f"{PEEC_BASE}/chats", headers=self.headers, params=params).json()

    def get_chat(self, chat_id, project_id=None):
        params = {}
        if project_id:
            params["project_id"] = project_id
        return requests.get(
            f"{PEEC_BASE}/chats/{chat_id}/content", headers=self.headers, params=params
        ).json()

    # ── report endpoints (POST) ──────────────────────────────────
    def _report(self, endpoint, start_date, end_date, dimensions=None,
                project_id=None, limit=1000, offset=0):
        payload = {"limit": limit, "offset": offset,
                   "start_date": start_date, "end_date": end_date}
        if dimensions:
            payload["dimensions"] = dimensions
        if project_id:
            payload["project_id"] = project_id
        return requests.post(
            f"{PEEC_BASE}/reports/{endpoint}", headers=self.headers, json=payload
        ).json()

    def report_brands(self, start_date, end_date, **kwargs):
        return self._report("brands", start_date, end_date, **kwargs)

    def report_domains(self, start_date, end_date, **kwargs):
        return self._report("domains", start_date, end_date, **kwargs)

    def report_urls(self, start_date, end_date, **kwargs):
        return self._report("urls", start_date, end_date, **kwargs)


# ══════════════════════════════════════════════════════════════════
# Shared helpers
# ══════════════════════════════════════════════════════════════════
def _extract_domain(url):
    if not url:
        return ""
    if not url.startswith("http"):
        url = "https://" + url
    try:
        return urlparse(url).netloc.lower().lstrip("www.")
    except Exception:
        return url.split("/")[0].lower()


def _extract_subdomain(url):
    if not url:
        return ""
    if not url.startswith("http"):
        url = "https://" + url
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return url.split("/")[0].lower()


def _build_row(r):
    raw_url = r.get("urlNormalized") or r.get("url", "")
    return {
        "URL": raw_url,
        "Full URL": r.get("url", ""),
        "Domain": _extract_domain(raw_url),
        "Subdomain": _extract_subdomain(raw_url),
        "Title": r.get("title"),
        "Page Type": r.get("classification"),
        "Prompt": prompt_lookup.get(
            (r.get("prompt") or {}).get("id", ""),
            (r.get("prompt") or {}).get("id", ""),
        ),
        "Prompt ID": (r.get("prompt") or {}).get("id"),
        "Model": (r.get("model") or {}).get("id"),
        "citation_avg": r.get("citation_avg", 0),
        "usage_count": r.get("usage_count", 0),
    }


def _scroll_table(df):
    """Render full dataframe inside a scrollable container with sticky headers."""
    return HTML(
        '<div class="peec-scroll">'
        + df.to_html(index=True, escape=False, max_cols=None, max_rows=None)
        + "</div>"
    )


def _normalise_host(domain):
    """
    Normalise a domain / URL to its bare hostname for matching.

    Steps:
      1. Lowercase & strip whitespace
      2. Remove protocol  (http:// or https://)
      3. Remove www. prefix
      4. Remove any path, query-string, or fragment
      5. Remove trailing dots / slashes

    Examples:
      https://www.spacenk.com/uk/brands  ->  spacenk.com
      rebeccajones.substack.com           ->  rebeccajones.substack.com
      www.substack.com                    ->  substack.com
    """
    s = str(domain).lower().strip()
    s = re.sub(r"^https?://", "", s)
    s = re.sub(r"^www\.", "", s)
    s = s.split("/")[0].split("?")[0].split("#")[0]
    s = s.rstrip(".")
    return s


def download_file(filepath, filename=None):
    """
    Download / save a file.
    On Colab: triggers browser download via colab_files.download().
    Locally: copies to PATHS['output'] (if not already there) and prints the path.
    """
    filepath = str(filepath)
    if filename is None:
        filename = Path(filepath).name

    if IN_COLAB:
        from google.colab import files as colab_files  # type: ignore
        colab_files.download(filepath)
    else:
        dest = Path(PATHS["output"]) / filename
        src = Path(filepath).resolve()
        if src != dest.resolve():
            shutil.copy2(filepath, dest)
        print(f"\u2705 Saved: {dest}")


# ══════════════════════════════════════════════════════════════════
# Fetch lookups & instantiate client
# ══════════════════════════════════════════════════════════════════
print(f"\u23f3 Loading lookups for {PROJECT_NAME}...")

peec = PeecClient()

_prompts_raw = peec.get_prompts(project_id=PROJECT_ID)["data"]
_tags_raw = peec.get_tags(project_id=PROJECT_ID)["data"]
_topics_raw = peec.get_topics(project_id=PROJECT_ID)["data"]
_models_raw = peec.get_models(project_id=PROJECT_ID)["data"]

prompt_lookup = {
    p["id"]: p["messages"][0]["content"] if p.get("messages") else p["id"]
    for p in _prompts_raw
}
tag_lookup = {t["id"]: t["name"] for t in _tags_raw}
topic_lookup = {t["id"]: t["name"] for t in _topics_raw}

print(
    f"\u2705 Ready \u2014 {len(prompt_lookup)} prompts, {len(tag_lookup)} tags, "
    f"{len(topic_lookup)} topics, {len(_models_raw)} models"
)

# ── Export to __main__ ───────────────────────────────────────────
__main__.peec = peec
__main__.prompt_lookup = prompt_lookup
__main__.tag_lookup = tag_lookup
__main__.topic_lookup = topic_lookup
__main__._extract_domain = _extract_domain
__main__._extract_subdomain = _extract_subdomain
__main__._build_row = _build_row
__main__._scroll_table = _scroll_table
__main__._normalise_host = _normalise_host
__main__.download_file = download_file
