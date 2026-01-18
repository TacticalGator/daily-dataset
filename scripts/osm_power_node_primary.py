#!/usr/bin/env python3
import os
import time
import json
import urllib.parse
import requests
from datetime import datetime

# === CONFIG ===
DOWNLOAD_DIR = "downloads"
NODE_TAGS = [
    "transformer",
    "switch",
    "terminal",
    "converter",
    "connection",
    "transition",
    "compensator",
    "inverter",
    "cable_distribution",
    "cable_distribution_cabinet"
]

OVERPASS_ENDPOINTS = [
    "http://overpass-api.de/api/interpreter",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
    "https://overpass.private.coffee/api/interpreter",
    "https://overpass.osm.jp/api/interpreter"
]

TIMEOUT = 1800  # seconds

# === UTILITIES ===
def log(msg: str):
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{now}] {msg}", flush=True)

def build_query(tag: str) -> str:
    """Build Overpass QL query for a single node tag."""
    return f'[out:json][timeout:{TIMEOUT}];node["power"="{tag}"];out meta;'

def download_tag(tag: str) -> dict:
    """Download a single tag from multiple endpoints."""
    for endpoint in OVERPASS_ENDPOINTS:
        log(f"🔗 Trying {endpoint} for '{tag}'...")
        try:
            query = build_query(tag)
            url = f"{endpoint}?data={urllib.parse.quote(query)}"
            response = requests.get(url, timeout=TIMEOUT)

            if response.status_code != 200:
                log(f"  ⚡ HTTP {response.status_code} returned, skipping to next endpoint...")
                continue

            data = response.json()
            elements = data.get("elements", [])
            log(f"  ✅ Finished download: {len(elements)} elements retrieved ({len(response.content) / 1024:.1f} KB)")
            return data

        except requests.exceptions.RequestException as e:
            log(f"  ❌ Request failed: {e}, skipping to next endpoint...")

        except json.JSONDecodeError as e:
            log(f"  ❌ JSON decode error: {e}, skipping to next endpoint...")

    log(f"❌ All endpoints failed for '{tag}'")
    return None

# === MAIN ===
if __name__ == "__main__":
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    for tag in NODE_TAGS:
        data = download_tag(tag)
        if data:
            filename = os.path.join(DOWNLOAD_DIR, f"osm_power_node_{tag}.json")
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            log(f"💾 Saved '{tag}' to {filename}\n")
        else:
            log(f"⚠️ '{tag}' skipped, no valid data\n")
