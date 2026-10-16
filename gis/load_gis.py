#!/usr/bin/env python3
"""Load public GIS network layers into PostGIS.

Resolves ArcGIS Online item IDs to FeatureServer URLs at runtime, pages
through features as GeoJSON, and writes both a verbatim copy (asset.gis_raw)
and a conformed copy (asset.pipe / asset.facility).

Which network — and where it is — comes entirely from a region config under
gis/regions/. Nothing here is region-specific.

    export GIS_REGION_CONFIG=gis/regions/my-region.yml
    python gis/load_gis.py --all
    python gis/load_gis.py --layer water_main
    python gis/load_gis.py --layer water_main --limit 500     # quick test
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import sys
import time

import requests
import yaml
import psycopg
from psycopg.types.json import Jsonb

ARCGIS_ITEM_API = "https://www.arcgis.com/sharing/rest/content/items/{item_id}"
PAGE_SIZE = 1000
DSN = os.environ.get("TWIN_DSN", "postgresql://weave:weave@localhost:5432/weave_twin")
HERE = os.path.dirname(os.path.abspath(__file__))
REGION_DIR = os.path.join(HERE, "regions")


def resolve_config_path(explicit: str | None) -> str:
    """--config, else $GIS_REGION_CONFIG, else fail with the available ones."""
    path = explicit or os.environ.get("GIS_REGION_CONFIG")
    if not path:
        available = sorted(
            os.path.relpath(p) for p in glob.glob(os.path.join(REGION_DIR, "*.yml"))
        )
        sys.exit(
            "no region config: pass --config PATH or set GIS_REGION_CONFIG.\n"
            "available:\n  " + "\n  ".join(available or ["(none — copy TEMPLATE.yml)"])
        )
    if not os.path.exists(path):
        sys.exit(f"region config not found: {path}")
    return path


def load_config(path: str) -> dict:
    with open(path) as fh:
        cfg = yaml.safe_load(fh) or {}
    if not cfg.get("layers"):
        sys.exit(f"{path} defines no layers")
    return cfg


def resolve_service_url(item_id: str) -> str:
    """Item ID -> FeatureServer base URL."""
    r = requests.get(ARCGIS_ITEM_API.format(item_id=item_id),
                     params={"f": "json"}, timeout=30)
    r.raise_for_status()
    meta = r.json()
    if "error" in meta:
        raise RuntimeError(f"ArcGIS error for {item_id}: {meta['error']}")
    url = meta.get("url")
    if not url:
        raise RuntimeError(f"Item {item_id} has no service URL (type={meta.get('type')})")
    return url.rstrip("/")


def fetch_features(service_url: str, layer_index: int, extent: dict | None,
                   limit: int | None = None):
    """Page through a FeatureServer layer, yielding GeoJSON features.

    An extent of None queries the whole service — correct for a small network,
    slow for a large one.
    """
    query_url = f"{service_url}/{layer_index}/query"
    offset, fetched = 0, 0

    while True:
        params = {
            "where": "1=1",
            "outFields": "*",
            "inSR": 4326,
            "outSR": 4326,
            "returnGeometry": "true",
            "resultOffset": offset,
            "resultRecordCount": PAGE_SIZE,
            "f": "geojson",
        }
        if extent:
            params.update({
                "geometry": json.dumps({
                    "xmin": extent["xmin"], "ymin": extent["ymin"],
                    "xmax": extent["xmax"], "ymax": extent["ymax"],
                    "spatialReference": {"wkid": 4326},
                }),
                "geometryType": "esriGeometryEnvelope",
                "spatialRel": "esriSpatialRelIntersects",
            })

        resp = requests.get(query_url, params=params, timeout=120)
        resp.raise_for_status()
        payload = resp.json()

        if "error" in payload:
            raise RuntimeError(f"Query error: {payload['error']}")

        features = payload.get("features", [])
        if not features:
            return

        for feat in features:
            yield feat
            fetched += 1
            if limit and fetched >= limit:
                return

        if len(features) < PAGE_SIZE:
            return
        offset += PAGE_SIZE
        time.sleep(0.3)          # be polite to a free public service


def pick(props: dict, candidates: list[str]):
    """GIS attribute names are inconsistent; try several."""
    lowered = {k.lower(): v for k, v in props.items()}
    for cand in candidates:
        if cand.lower() in lowered:
            val = lowered[cand.lower()]
            if val not in (None, "", " "):
                return val
    return None


def to_number(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def load_layer(conn, name: str, cfg: dict, extent: dict | None, hints: dict,
               source_id: str, limit: int | None = None) -> int:
    item_id = cfg.get("item_id")
    if not item_id:
        print(f"  ! {name}: no item_id set — run discover_layers.py")
        return 0

    print(f"  resolving {name} ({item_id}) ...")
    service_url = resolve_service_url(item_id)
    print(f"    service: {service_url}")

    target = cfg.get("target", "raw_only")
    count = 0

    with conn.cursor() as cur:
        for feat in fetch_features(service_url, cfg.get("layer", 0), extent, limit):
            props = feat.get("properties") or {}
            geom = feat.get("geometry")
            if not geom:
                continue

            fid = str(feat.get("id") or props.get("OBJECTID") or count)
            geojson = json.dumps(geom)

            cur.execute(
                """INSERT INTO asset.gis_raw (layer, source_fid, props, geom)
                   VALUES (%s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))
                   ON CONFLICT (layer, source_fid)
                   DO UPDATE SET props = EXCLUDED.props,
                                 geom = EXCLUDED.geom,
                                 loaded_at = now()""",
                (name, fid, Jsonb(props), geojson),
            )

            if target == "pipe" and geom.get("type") in ("LineString", "MultiLineString"):
                cur.execute(
                    """INSERT INTO asset.pipe
                         (pipe_id, layer, owner, asset_name, diameter_mm,
                          material, length_m, geom, source)
                       VALUES (%s, %s, %s, %s, %s, %s,
                               ST_Length(ST_GeomFromGeoJSON(%s)::geography),
                               ST_LineMerge(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)),
                               %s)
                       ON CONFLICT (pipe_id) DO UPDATE SET
                         diameter_mm = EXCLUDED.diameter_mm,
                         material = EXCLUDED.material,
                         geom = EXCLUDED.geom""",
                    (
                        f"{name}:{fid}", name, cfg.get("owner"),
                        pick(props, hints["asset_name"]),
                        to_number(pick(props, hints["diameter_mm"])),
                        pick(props, hints["material"]),
                        geojson, geojson, source_id,
                    ),
                )

            elif target == "facility" and geom.get("type") in ("Point", "MultiPoint", "Polygon"):
                cur.execute(
                    """INSERT INTO asset.facility
                         (facility_id, facility_type, name, capacity_ml, props, geom, source)
                       VALUES (%s, %s, %s, %s, %s,
                               ST_Centroid(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)),
                               %s)
                       ON CONFLICT (facility_id) DO UPDATE SET
                         name = EXCLUDED.name, geom = EXCLUDED.geom""",
                    (
                        f"{name}:{fid}", cfg.get("facility_type", name),
                        pick(props, hints["asset_name"]),
                        to_number(pick(props, hints["capacity_ml"])),
                        Jsonb(props), geojson, source_id,
                    ),
                )

            count += 1
            if count % 1000 == 0:
                conn.commit()
                print(f"    {count} features ...")

    conn.commit()
    print(f"    {name}: {count} features loaded")
    return count


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", help="region config (default: $GIS_REGION_CONFIG)")
    ap.add_argument("--layer", help="single layer name from the region config")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--limit", type=int, help="stop after N features (testing)")
    ap.add_argument("--build-topology", action="store_true",
                    help="run pgRouting topology build after loading")
    args = ap.parse_args()

    config_path = resolve_config_path(args.config)
    cfg = load_config(config_path)
    region = cfg.get("region") or {}
    layers = cfg["layers"]
    extent = region.get("extent")
    source_id = region.get("source_id") or "public_gis"
    hints = cfg["attribute_hints"]

    print(f"region: {region.get('name') or '(unnamed)'}  [{config_path}]")
    if not extent:
        print("  no extent set — querying services unfiltered")

    if args.layer:
        if args.layer not in layers:
            ap.error(f"no layer {args.layer!r} in {config_path}")
        selected = {args.layer: layers[args.layer]}
    elif args.all:
        selected = layers
    else:
        ap.error("pass --layer NAME or --all")

    total = 0
    with psycopg.connect(DSN) as conn:
        for name, layer_cfg in selected.items():
            try:
                total += load_layer(conn, name, layer_cfg, extent, hints,
                                    source_id, args.limit)
            except Exception as exc:                      # keep going on failure
                print(f"  ! {name} failed: {exc}")

        if args.build_topology:
            print("building pgRouting topology (slow on large networks) ...")
            with conn.cursor() as cur:
                cur.execute("SELECT asset.build_topology()")
                print(f"  {cur.fetchone()[0]}")
            conn.commit()

    print(f"\ntotal features: {total}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
