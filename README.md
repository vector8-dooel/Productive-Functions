This repository contains code that creates an azure function (python v2 model) named productive-tables-function which extracts and makes transformations to the productive API tables listed below and ingests them in the azure blob container named productive-data. The data is refreshed daily at 6am UTC and the instead of refreshing the entire table, new data is appended from the previous day

The tables ingested are projects,deals,deal_statuses,companies,subsidiaries,invoices,services,service_types,custom_fields and pipelines

The solution supports:

<li>🔄 Incremental refresh (for endpoints with timestamps)</li>
<li>🧩 Full diff‑merge (for endpoints with no timestamp fields)</li>
<li>💾 Blob merge logic (dedupe by ID + latest row wins)</li>
<li>⏱ Automated daily refresh via Timer Trigger (6am utc by productive_incremental_timer)</li>
<li>🛠 Retry logic for rate limits and API instability</li>
<li>☁️ Works fully serverless in Azure (no local dependencies)</li>
</p>

<h2>📁 Project Structure</h2>

<pre style="background:#f6f8fa; padding:16px; border-radius:6px;">
productive-functions/
│
├── function_app.py               # Azure Function entry file (Python v2)
├── host.json                     # Function host configuration
├── requirements.txt              # Python dependencies
├── local.settings.json           # Local dev settings (ignored in Azure)
│
└── productive_client/            # Core ETL logic
    ├── __init__.py
    ├── config.py                 # Env configuration + API keys
    ├── http_utils.py             # Pagination + retry logic
    ├── lookups.py                # Lookup tables (fields, options, people)
    ├── extractors.py             # Universal + incremental extractors
    ├── pipeline.py               # Orchestrator for full/incremental loads
    └── blob_io.py                # Blob I/O + merge + state management
</pre>


