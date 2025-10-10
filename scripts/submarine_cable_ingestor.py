#!/usr/bin/env python3
import json
import time
import random
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

BASE_URL = "https://www.submarinecablemap.com/api/v3"
CABLE_GEO_URL = f"{BASE_URL}/cable/cable-geo.json"
OUTPUT_FILE = Path("cable-geo-enriched.json")

DETAIL_KEYS = [
    "length",
    "landing_points",
    "owners",
    "suppliers",
    "rfs",
    "rfs_year",
    "is_planned",
    "notes",
    "url",
]

MAX_WORKERS = 50
MAX_RETRIES = 3


def fetch_json(url: str, retries: int = MAX_RETRIES) -> dict:
    for attempt in range(1, retries + 1):
        try:
            req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(req, timeout=20) as resp:
                return json.load(resp)
        except (HTTPError, URLError, TimeoutError) as e:
            wait = 1.5 ** attempt + random.uniform(0, 0.3)
            print(f"[-] [{attempt}] Failed {url}: {e} - retrying in {wait:.1f}s")
            time.sleep(wait)
    print(f"[-] Failed after {retries} attempts: {url}")
    return {}


def enrich_feature(feature: dict) -> dict:
    cable_id = feature.get("properties", {}).get("id")
    if not cable_id:
        return feature

    detail_url = f"{BASE_URL}/cable/{cable_id}.json"
    details = fetch_json(detail_url)
    if not details:
        return feature

    enriched = {}
    for key in DETAIL_KEYS:
        if key not in details:
            continue
        if key == "landing_points":
            enriched[key] = [lp.get("name") for lp in details["landing_points"] if lp.get("name")]
        else:
            enriched[key] = details[key]

    feature["properties"].update(enriched)
    return feature


def main():
    print("Fetching base cable-geo.json ...")
    cable_geo = fetch_json(CABLE_GEO_URL)
    features = cable_geo.get("features", [])
    print(f"Found {len(features)} cables. Fetching details with {MAX_WORKERS} threads...")

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(enrich_feature, f): f for f in features}
        for i, future in enumerate(as_completed(futures), 1):
            feature = future.result()
            results.append(feature)
            if i % 20 == 0:
                print(f"[+] Processed {i}/{len(features)}")

    cable_geo["features"] = results
    OUTPUT_FILE.write_text(json.dumps(cable_geo, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[>] Enriched dataset saved to {OUTPUT_FILE.resolve()}")


if __name__ == "__main__":
    main()
