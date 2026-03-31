from urllib.parse import urlencode
import pandas as pd
from datetime import datetime
from .config import PRODUCTIVE_BASE_URL, HEADERS
from .http_utils import get_paginated
import requests

# ------------------------------------------------------------
# URL builder
# ------------------------------------------------------------
def _build_url(endpoint: str, params: dict | None = None) -> str:
    base = f"{PRODUCTIVE_BASE_URL}/{endpoint}"
    if params:
        return f"{base}?{urlencode(params)}"
    return base


# ------------------------------------------------------------
# Full-table extractor (unchanged)
# ------------------------------------------------------------
def extract_table(endpoint: str, params: dict | None = None) -> pd.DataFrame:
    rows = []
    url = _build_url(endpoint, params)
    for res in get_paginated(url):
        for item in res.get("data", []):
            rows.append(_flatten_item(item))
    return pd.DataFrame(rows)


# ------------------------------------------------------------
# Incremental extractor (cleaned + hardened)
# ------------------------------------------------------------
def extract_table_incremental(
    endpoint: str,
    since_iso: str,
    updated_field: str = "updated_at",
    try_server_filter: bool = True
) -> pd.DataFrame:
    """
    Incremental extraction:
    - Normalizes all timestamps to UTC
    - Avoids sorting (Productive rejects sorting on many endpoints)
    - Attempts server-side filtering; falls back safely
    """

    # Convert starting timestamp safely
    since_dt = pd.to_datetime(since_iso, utc=True, errors="coerce")

    # ----------------------------
    # 1. Build params (NO SORTING)
    # ----------------------------
    params = {}

    # Attempt server-side filtering first
    if try_server_filter:
        params[f"filter[{updated_field}]"] = f"gte:{since_iso}"

    url = _build_url(endpoint, params)

    # ----------------------------
    # 2. Test request to see if API accepts filter
    # ----------------------------
    try:
        resp = requests.get(url, headers=HEADERS, timeout=60)
        if resp.status_code == 400 and try_server_filter:
            # Remove unsupported filter
            params.pop(f"filter[{updated_field}]", None)
            url = _build_url(endpoint, params)
        else:
            resp.raise_for_status()
    except requests.HTTPError:
        # Fallback = no server-side filter
        params.pop(f"filter[{updated_field}]", None)
        url = _build_url(endpoint, params)

    # ----------------------------
    # 3. Client-side filtering
    # ----------------------------
    rows = []
    for res in get_paginated(url):
        for item in res.get("data", []):
            row = _flatten_item(item)
            raw_ts = row.get(updated_field)

            # Normalize timestamp
            ts = pd.to_datetime(raw_ts, utc=True, errors="coerce")

            if ts is not None and pd.notna(ts) and ts > since_dt:
                rows.append(row)

    df = pd.DataFrame(rows)
    return df


# ------------------------------------------------------------
# JSON:API → flat dict
# ------------------------------------------------------------
def _flatten_item(item: dict) -> dict:
    attrs = item.get("attributes", {}) or {}
    row = {"id": item.get("id")}

    for k, v in attrs.items():
        if k != "custom_fields":
            row[k] = v

    # include raw custom field IDs
    cf = attrs.get("custom_fields", {}) or {}
    for cfid, value in cf.items():
        row[cfid] = value

    return row


# ------------------------------------------------------------
# Apply custom field + people + option lookups
# ------------------------------------------------------------
def apply_lookups(df: pd.DataFrame, cf_map: dict, opt_map: dict, people_map: dict) -> pd.DataFrame:
    if df.empty:
        return df

    # Rename CF columns by name
    df = df.rename(columns=cf_map)

    def repl(v):
        if isinstance(v, list):
            return [opt_map.get(str(x), people_map.get(str(x), x)) for x in v]
        return opt_map.get(str(v), people_map.get(str(v), v))

    for col in df.columns:
        if col not in ("id", "name", "updated_at", "created_at"):
            df[col] = df[col].apply(repl)

    return df