#!/usr/bin/env python3
"""
OSM Power Infrastructure Fetcher & Converter

Fetches power infrastructure data (nodes and ways) from Overpass API and
converts directly to GeoJSON without intermediate disk writes.
Optimized for GitHub Actions free tier.
"""

import os
import json
import requests
from datetime import datetime
from typing import Any
from osm2geojson import json2geojson

# === CONFIG ===============================================================

OUTPUT_DIR = "downloads"

# Node-primary tags (no transformation needed)
NODE_POWER_TAGS = [
    "transformer",
    "switch",
    "terminal",
    "converter",
    "connection",
    "transition",
    "compensator",
    "inverter",
    "cable_distribution",
    "cable_distribution_cabinet",
]

# Way-primary tags (transformation needed)
WAY_POWER_TAGS = [
    "line",
    "minor_line",
    "cable",
    "switchgear",
    "substation",
]

OVERPASS_ENDPOINTS = [
    "https://overpass.private.coffee/api/interpreter",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
    "http://overpass-api.de/api/interpreter",
    "https://overpass.osm.jp/api/interpreter",
]

TIMEOUT = 3600  # seconds

# === UTILITIES ============================================================


def log(msg: str) -> None:
    """Print a timestamped log message."""
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{now}] {msg}", flush=True)


def build_node_query(tag: str) -> str:
    """Build Overpass QL query for node elements with a given power tag."""
    return f'[out:json][timeout:{TIMEOUT}];node["power"="{tag}"];out meta;'


def build_way_query(tag: str) -> str:
    """Build Overpass QL query for way elements with a given power tag."""
    return f'[out:json][timeout:{TIMEOUT}];way["power"="{tag}"];out geom;'


def fetch_overpass(query: str) -> dict[str, Any] | None:
    """
    Try each Overpass endpoint until one returns valid data.
    Returns raw OSM JSON dict or None if all endpoints fail.
    """
    for endpoint in OVERPASS_ENDPOINTS:
        log(f"  🔗 Trying {endpoint}...")
        try:
            response = requests.post(
                endpoint,
                data={"data": query},
                timeout=TIMEOUT,
            )

            if response.status_code != 200:
                log(f"    ⚡ HTTP {response.status_code}, skipping...")
                continue

            data = response.json()
            elements = data.get("elements", [])

            if not elements:
                log("    ⚠️ 0 elements returned, trying next...")
                continue

            log(f"    ✅ {len(elements):,} elements ({len(response.content) / 1024:.1f} KB)")
            return data

        except requests.exceptions.RequestException as e:
            log(f"    ❌ Request failed: {e}")
        except json.JSONDecodeError as e:
            log(f"    ❌ JSON decode error: {e}")

    return None


def convert_to_geojson(osm_data: dict[str, Any]) -> dict[str, Any]:
    """Convert raw OSM JSON to GeoJSON using osm2geojson."""
    return json2geojson(osm_data)


def transform_geojson(geojson: dict[str, Any]) -> dict[str, Any]:
    """
    Clean up GeoJSON properties (for ways only):
      - Remove 'nodes' key (internal OSM reference)
      - Remove 'type' key (redundant with geometry type)
    """
    features = []
    for feature in geojson.get("features", []):
        props = feature.get("properties", {}).copy()
        props.pop("nodes", None)
        props.pop("type", None)

        features.append({
            "type": "Feature",
            "properties": props,
            "geometry": feature.get("geometry"),
        })

    return {"type": "FeatureCollection", "features": features}


def save_geojson(geojson: dict[str, Any], filepath: str) -> None:
    """Write GeoJSON to disk with compact formatting."""
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, separators=(",", ":"))


# === PROCESSORS ===========================================================


def process_node_tag(tag: str) -> bool:
    """
    Pipeline for a node power tag (3 steps, no transformation):
      1. Fetch from Overpass API
      2. Convert to GeoJSON
      3. Save to disk

    Returns True on success, False on failure.
    """
    label = f"node:{tag}"
    output_file = os.path.join(OUTPUT_DIR, f"osm_power_node_{tag}.geojson")

    log(f"{'=' * 60}")
    log(f"Processing '{label}'")
    log(f"{'=' * 60}")

    # Step 1: Fetch
    log(f"[1/3] Fetching '{label}' from Overpass API...")
    query = build_node_query(tag)
    osm_data = fetch_overpass(query)

    if osm_data is None:
        log(f"  ❌ All endpoints failed for '{label}', skipping\n")
        return False

    # Step 2: Convert
    log(f"[2/3] Converting to GeoJSON...")
    geojson = convert_to_geojson(osm_data)
    del osm_data  # Free memory immediately

    # Step 3: Save
    feature_count = len(geojson.get("features", []))
    log(f"[3/3] Saving {feature_count:,} features → {output_file}")
    save_geojson(geojson, output_file)
    del geojson  # Free memory before next iteration

    log(f"  ✅ Done\n")
    return True


def process_way_tag(tag: str) -> bool:
    """
    Pipeline for a way power tag (4 steps, with transformation):
      1. Fetch from Overpass API
      2. Convert to GeoJSON
      3. Transform (clean properties)
      4. Save to disk

    Returns True on success, False on failure.
    """
    label = f"way:{tag}"
    output_file = os.path.join(OUTPUT_DIR, f"osm_power_way_{tag}.geojson")

    log(f"{'=' * 60}")
    log(f"Processing '{label}'")
    log(f"{'=' * 60}")

    # Step 1: Fetch
    log(f"[1/4] Fetching '{label}' from Overpass API...")
    query = build_way_query(tag)
    osm_data = fetch_overpass(query)

    if osm_data is None:
        log(f"  ❌ All endpoints failed for '{label}', skipping\n")
        return False

    # Step 2: Convert
    log(f"[2/4] Converting to GeoJSON...")
    geojson = convert_to_geojson(osm_data)
    del osm_data  # Free memory immediately

    # Step 3: Transform
    log(f"[3/4] Transforming (cleaning properties)...")
    geojson = transform_geojson(geojson)

    # Step 4: Save
    feature_count = len(geojson.get("features", []))
    log(f"[4/4] Saving {feature_count:,} features → {output_file}")
    save_geojson(geojson, output_file)
    del geojson  # Free memory before next iteration

    log(f"  ✅ Done\n")
    return True


# === MAIN =================================================================


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    total_tags = len(NODE_POWER_TAGS) + len(WAY_POWER_TAGS)
    log("Starting OSM Power Infrastructure Pipeline")
    log(f"Node tags: {NODE_POWER_TAGS}")
    log(f"Way tags: {WAY_POWER_TAGS}")
    log(f"Total: {total_tags} tags to process\n")

    results = {"success": [], "failed": []}

    # Process node-primary tags first (smaller datasets)
    log("=" * 60)
    log("PHASE 1: NODE-PRIMARY TAGS")
    log("=" * 60 + "\n")

    for tag in NODE_POWER_TAGS:
        if process_node_tag(tag):
            results["success"].append(f"node:{tag}")
        else:
            results["failed"].append(f"node:{tag}")

    # Process way-primary tags (larger datasets)
    log("=" * 60)
    log("PHASE 2: WAY-PRIMARY TAGS")
    log("=" * 60 + "\n")

    for tag in WAY_POWER_TAGS:
        if process_way_tag(tag):
            results["success"].append(f"way:{tag}")
        else:
            results["failed"].append(f"way:{tag}")

    # Summary
    log("=" * 60)
    log("SUMMARY")
    log("=" * 60)
    log(f"✅ Success: {len(results['success'])}/{total_tags}")
    if results["failed"]:
        log(f"❌ Failed: {results['failed']}")


if __name__ == "__main__":
    main()
