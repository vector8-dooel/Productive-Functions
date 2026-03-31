import os

# Productive API
PRODUCTIVE_BASE_URL = os.environ.get("PRODUCTIVE_BASE_URL", "https://api.productive.io/api/v2")
PRODUCTIVE_AUTH_TOKEN = os.environ["PRODUCTIVE_AUTH_TOKEN"]
PRODUCTIVE_ORG_ID = os.environ["PRODUCTIVE_ORG_ID"]

HEADERS = {
    "Accept": "text/xml,application/xml,application/xhtml+xml,text/html;q=0.9,text/plain;q=0.8,image/png,*/*;q=0.5",
    "X-Auth-Token": PRODUCTIVE_AUTH_TOKEN,
    "X-Organization-Id": PRODUCTIVE_ORG_ID,
    "Content-Type": "application/vnd.api+json; charset=utf-8"
}

# Azure Blob
AZURE_STORAGE_CONNECTION_STRING = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
AZURE_BLOB_CONTAINER = os.environ.get("AZURE_BLOB_CONTAINER", "productive-data")
STATE_BLOB = os.environ.get("STATE_BLOB", "state.json")

# Tables to process
TABLES = [t.strip() for t in os.environ.get("TABLES", "projects,deals,deal_statuses,companies,subsidiaries,invoices,services,service_types,custom_fields,pipelines").split(",")]

# Timer schedule (cron expression)
CRON_SCHEDULE = os.environ.get("CRON_SCHEDULE", "0 0 6 * * *")  # default 06:00 daily