import time
import random
import requests
from typing import Generator
from .config import HEADERS

def get_paginated(url: str, max_retries: int = 5, backoff: float = 1.8) -> Generator[dict, None, None]:

    while url:
        for attempt in range(1, max_retries + 1):
            try:
                resp = requests.get(url, headers=HEADERS, timeout=120)

                # --------------------------
                # Retry cases
                # --------------------------
                if resp.status_code in (429, 408):
                    # rate limited or timeout — safe to retry
                    sleep_s = backoff ** attempt + random.uniform(0, 0.5)
                    time.sleep(sleep_s)
                    continue

                if resp.status_code >= 500:
                    # server error — retry
                    sleep_s = backoff ** attempt + random.uniform(0, 0.5)
                    time.sleep(sleep_s)
                    continue

                # --------------------------
                # Fail on real client errors
                # --------------------------
                resp.raise_for_status()

                # --------------------------
                # Success
                # --------------------------
                data = resp.json()
                yield data

                url = data.get("links", {}).get("next")
                break

            except requests.exceptions.Timeout:
                # Timeout → retry
                sleep_s = backoff ** attempt + random.uniform(0, 0.5)
                time.sleep(sleep_s)
                continue

            except requests.exceptions.RequestException as e:
                # Hard failure after retries
                if attempt == max_retries:
                    raise RuntimeError(f"Failed to fetch page: {e}")

                # Retry for early attempts
                sleep_s = backoff ** attempt + random.uniform(0, 0.5)
                time.sleep(sleep_s)
                continue