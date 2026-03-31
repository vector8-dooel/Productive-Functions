import logging
from datetime import datetime, timezone
from .lookups import get_custom_fields, get_custom_field_options, get_people
from .extractors import extract_table, extract_table_incremental, apply_lookups
from .blob_io import (
    read_state,
    write_state,
    append_merge_csv,
    write_full_blob,
    blob_file_exists
)
from .config import TABLES
import pandas as pd

def run_incremental_pipeline():

    # Global lookups (do once per run)
    cf_map = get_custom_fields()
    opt_map = get_custom_field_options()
    people_map = get_people()

    since_iso = read_state()

    for endpoint in TABLES:
        print(f"Processing table: {endpoint}")

        blob_name = f"{endpoint}.csv"
        blob_exists = blob_file_exists(blob_name)

        # ---------------------------------------------------------
        # CASE 1 — FIRST RUN → ALWAYS DO FULL LOAD
        # ---------------------------------------------------------
        if not blob_exists:
            print(f" → First run for {endpoint}: Performing full load.")
            df_full = extract_table(endpoint)
            df_full = apply_lookups(df_full, cf_map, opt_map, people_map)
            write_full_blob(blob_name, df_full)
            continue

        # ---------------------------------------------------------
        # CASE 2 — Determine timestamp fields for incremental
        # ---------------------------------------------------------
        # Get one page to inspect available fields
        sample_df = extract_table(endpoint, params={"page[size]": 1})

        timestamp_field = None
        if "updated_at" in sample_df.columns:
            timestamp_field = "updated_at"
        elif "created_at" in sample_df.columns:
            timestamp_field = "created_at"

        # ---------------------------------------------------------
        # CASE 3 — TRUE INCREMENTAL (updated_at exists)
        # ---------------------------------------------------------
        if timestamp_field == "updated_at":
            print(f" → Using REAL incremental for {endpoint} (updated_at).")

            df_inc = extract_table_incremental(endpoint, since_iso, updated_field="updated_at")
            df_inc = apply_lookups(df_inc, cf_map, opt_map, people_map)

            # Merge new rows into existing blob
            append_merge_csv(blob_name, df_inc)
            continue

        # ---------------------------------------------------------
        # CASE 4 — NEW ROWS ONLY (created_at), plus diff-merge
        # ---------------------------------------------------------
        if timestamp_field == "created_at":
            print(f" → Using created_at incremental + diff merge for {endpoint}.")

            df_inc = extract_table_incremental(endpoint, since_iso, updated_field="created_at")
            df_inc = apply_lookups(df_inc, cf_map, opt_map, people_map)

            # Still need diff-merge because created_at doesn't reflect updates
            append_merge_csv(blob_name, df_inc)
            continue

        # ---------------------------------------------------------
        # CASE 5 — NO TIMESTAMPS → ALWAYS FULL LOAD + DIFF MERGE
        # ---------------------------------------------------------
        print(f" → No timestamps available for {endpoint}. Using full diff-merge.")

        df_full = extract_table(endpoint)
        df_full = apply_lookups(df_full, cf_map, opt_map, people_map)

        append_merge_csv(blob_name, df_full)

    # Update last run timestamp
    write_state()
    print("All tables processed.")