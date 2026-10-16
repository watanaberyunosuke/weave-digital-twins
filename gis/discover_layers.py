#!/usr/bin/env python3
"""Find the item IDs to paste into a region config.

Searches whichever public catalogues the region config names under
`provider:` — an ArcGIS Online owner and/or a CKAN organization. The
catalogue endpoints are config, not code, so a new region needs no change
here.

    export GIS_REGION_CONFIG=gis/regions/my-region.yml
    python gis/discover_layers.py                     # list everything
    python gis/discover_layers.py --search reservoir  # filter
    python gis/discover_layers.py --arcgis-owner someutility   # ad hoc
"""

from __future__ import annotations

import argparse
import sys

import requests
import yaml

from load_gis import resolve_config_path

ARCGIS_SEARCH = "https://www.arcgis.com/sharing/rest/search"


def arcgis_search(owner: str, term: str, limit: int = 100) -> list[dict]:
    query = f'owner:{owner} AND (type:"Feature Service")'
    if term:
        query += f' AND ({term})'
    out, start = [], 1
    while start > 0 and len(out) < limit:
        r = requests.get(ARCGIS_SEARCH, timeout=30, params={
            "q": query, "f": "json", "num": 100, "start": start,
            "sortField": "title", "sortOrder": "asc",
        })
        r.raise_for_status()
        payload = r.json()
        out.extend(payload.get("results", []))
        start = payload.get("nextStart", -1)
    return out[:limit]


def ckan_search(api_url: str, organization: str, term: str,
                rows: int = 100) -> list[dict]:
    q = f"organization:{organization}" if organization else ""
    if term:
        q = f"{q} AND {term}" if q else term
    r = requests.get(api_url, params={"q": q, "rows": rows}, timeout=30)
    r.raise_for_status()
    return r.json().get("result", {}).get("results", [])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", help="region config (default: $GIS_REGION_CONFIG)")
    ap.add_argument("--search", default="", help="filter term, e.g. reservoir")
    ap.add_argument("--source", choices=["arcgis", "ckan", "both"], default="both")
    ap.add_argument("--arcgis-owner", help="override the config's ArcGIS owner")
    ap.add_argument("--ckan-api-url", help="override the config's CKAN endpoint")
    ap.add_argument("--ckan-organization", help="override the config's CKAN org")
    args = ap.parse_args()

    with open(resolve_config_path(args.config)) as fh:
        provider = (yaml.safe_load(fh) or {}).get("provider") or {}
    arcgis = provider.get("arcgis") or {}
    ckan = provider.get("ckan") or {}

    owner = args.arcgis_owner or arcgis.get("owner")
    hub_url = arcgis.get("hub_url")
    ckan_api = args.ckan_api_url or ckan.get("api_url")
    ckan_org = args.ckan_organization or ckan.get("organization")

    if args.source in ("arcgis", "both"):
        print("=" * 70)
        print("ArcGIS Online — paste item_id values into your region config")
        print("=" * 70)
        if not owner:
            print("  (no provider.arcgis.owner in config — skipping)")
        else:
            try:
                for item in arcgis_search(owner, args.search):
                    print(f"\n{item.get('title')}")
                    print(f"  item_id: {item.get('id')}")
                    print(f"  url:     {item.get('url')}")
            except Exception as exc:
                print(f"  ! ArcGIS search failed: {exc}")
                if hub_url:
                    print(f"  Fallback: open a dataset page on {hub_url} and read")
                    print("  the item ID out of the thumbnail URL in the page source.")

    if args.source in ("ckan", "both"):
        print("\n" + "=" * 70)
        print("CKAN — direct download resources (CSV, GeoJSON, SHP)")
        print("=" * 70)
        if not ckan_api:
            print("  (no provider.ckan.api_url in config — skipping)")
        else:
            try:
                for ds in ckan_search(ckan_api, ckan_org, args.search):
                    print(f"\n{ds.get('title')}")
                    for res in ds.get("resources", []):
                        print(f"  [{res.get('format')}] {res.get('url')}")
            except Exception as exc:
                print(f"  ! CKAN search failed: {exc}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
