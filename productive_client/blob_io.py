import io
import json
import pandas as pd
from datetime import datetime, timezone
from azure.storage.blob import BlobServiceClient
from .config import AZURE_STORAGE_CONNECTION_STRING, AZURE_BLOB_CONTAINER, STATE_BLOB

_blob_service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
_container_client = _blob_service.get_container_client(AZURE_BLOB_CONTAINER)


# ------------------------------------------------------------
# Internal: get blob client
# ------------------------------------------------------------
def _get_blob_client(name: str):
    return _blob_service.get_blob_client(container=AZURE_BLOB_CONTAINER, blob=name)


# ------------------------------------------------------------
# Read incremental state from Blob
# ------------------------------------------------------------
def read_state() -> str:
    """Return last_run ISO string stored in Blob, or a safe default."""
    bc = _get_blob_client(STATE_BLOB)
    try:
        content = bc.download_blob().readall().decode("utf-8")
        data = json.loads(content)
        return data.get("last_run", "1970-01-01T00:00:00Z")
    except Exception:
        return "1970-01-01T00:00:00Z"


# ------------------------------------------------------------
# Write incremental state to Blob
# ------------------------------------------------------------
def write_state(now_iso: str | None = None):
    if not now_iso:
        now_iso = datetime.now(timezone.utc).isoformat()
    bc = _get_blob_client(STATE_BLOB)
    bc.upload_blob(json.dumps({"last_run": now_iso}), overwrite=True)


# ------------------------------------------------------------
# NEW FUNCTION #1
# Check whether a blob exists before loading/writing
# ------------------------------------------------------------
def blob_file_exists(blob_name: str) -> bool:
    try:
        _get_blob_client(blob_name).get_blob_properties()
        return True
    except Exception:
        return False


# ------------------------------------------------------------
# NEW FUNCTION #2
# Write a full DataFrame to Blob (first/full load)
# ------------------------------------------------------------
def write_full_blob(blob_name: str, df: pd.DataFrame):
    out = io.StringIO()
    df.to_csv(out, index=False)
    _get_blob_client(blob_name).upload_blob(out.getvalue(), overwrite=True)


# ------------------------------------------------------------
# Append + merge incremental changes into blob CSV
# ------------------------------------------------------------
def append_merge_csv(blob_name: str, df_new: pd.DataFrame, key_col: str = "id", updated_col: str = "updated_at"):
    """
    Append new rows to existing CSV in Blob.
    If blob does not exist, create it even if df_new is empty.
    """

    bc = _get_blob_client(blob_name)

    # ----------------------
    # Case A — Blob exists
    # ----------------------
    try:
        existing = bc.download_blob().readall().decode("utf-8")
        df_old = pd.read_csv(io.StringIO(existing))
        blob_exists = True
    except Exception:
        df_old = pd.DataFrame()
        blob_exists = False

    # ----------------------
    # Case B — No new data
    # ----------------------
    if df_new is None or df_new.empty:
        if not blob_exists:
            # create an empty blob so the table exists in the container
            out = io.StringIO()
            df_old.to_csv(out, index=False)
            bc.upload_blob(out.getvalue(), overwrite=True)
        return

    # ----------------------
    # Case C — Merge new + old
    # ----------------------
    df_all = pd.concat([df_old, df_new], ignore_index=True)

    # Normalize timestamps to UTC
    if updated_col in df_all.columns:
        df_all[updated_col] = pd.to_datetime(df_all[updated_col], utc=True, errors="coerce")
        df_all = df_all.sort_values(by=[key_col, updated_col])
        df_all = df_all.drop_duplicates(subset=[key_col], keep="last")

    # Save final merged CSV
    out = io.StringIO()
    df_all.to_csv(out, index=False)
    bc.upload_blob(out.getvalue(), overwrite=True)