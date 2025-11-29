#!/usr/bin/env python3
import os
import json
import time
import random
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError


BASE_URL = "https://peeringdb.com/api"

ENDPOINTS = [
    "org",
    "fac",
    "net",
    "ix",
    "campus",
    "carrier",
    "netfac",
    "ixfac",
    "carrierfac",
]

OUT_DIR = Path("downloads")
OUT_DIR.mkdir(exist_ok=True)

MAX_RETRIES = 5


def fetch_json(url: str, headers: dict, retries: int = MAX_RETRIES) -> dict:
    """Fetch JSON from PeeringDB with retry/backoff."""
    for attempt in range(1, retries + 1):
        try:
            req = Request(url, headers=headers)
            with urlopen(req, timeout=30) as resp:
                return json.load(resp)

        except (HTTPError, URLError, TimeoutError) as e:
            wait = 1.5 ** attempt + random.uniform(0, 0.4)
            print(f"[-] Attempt {attempt} failed for {url}: {e} — retrying in {wait:.1f}s")
            time.sleep(wait)

    print(f"[!] Failed after {retries} attempts: {url}")
    return {}


def main():
    api_key = os.environ.get("PEERINGDB")

    if not api_key:
        raise ValueError("PEERINGDB environment variable is missing!")

    headers = {"Authorization": "Api-Key " + api_key}

    print(f"📡 Downloading {len(ENDPOINTS)} PeeringDB datasets...")

    for ep in ENDPOINTS:
        url = f"{BASE_URL}/{ep}"
        print(f"[+] Fetching {url} ...")

        data = fetch_json(url, headers=headers)

        out_path = OUT_DIR / f"peeringdb_{ep}.json"
        out_path.write_text(
            json.dumps(data, indent=2, ensure_ascii=False),
            encoding="utf-8"
        )

        print(f"    → Saved to {out_path}")

    print("✅ All PeeringDB datasets downloaded.")


if __name__ == "__main__":
    main()
