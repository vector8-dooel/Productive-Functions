from .config import PRODUCTIVE_BASE_URL
from .http_utils import get_paginated

def get_custom_fields():
    fields = {}
    for res in get_paginated(f"{PRODUCTIVE_BASE_URL}/custom_fields"):
        for item in res.get("data", []):
            fields[item["id"]] = item["attributes"]["name"]
    return fields

def get_custom_field_options():
    lookup = {}
    for res in get_paginated(f"{PRODUCTIVE_BASE_URL}/custom_field_options"):
        for item in res.get("data", []):
            lookup[item["id"]] = item["attributes"]["name"]
    return lookup

def get_people():
    lookup = {}
    for res in get_paginated(f"{PRODUCTIVE_BASE_URL}/people"):
        for p in res.get("data", []):
            pid = p["id"]
            attrs = p.get("attributes", {})
            full = f"{attrs.get('first_name','')} {attrs.get('last_name','')}".strip()
            lookup[pid] = full
    return lookup