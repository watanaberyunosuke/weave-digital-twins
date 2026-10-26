#!/usr/bin/env python3
"""Load public GIS network layers into PostGIS.

Resolves ArcGIS Online item IDs to FeatureServer URLs at runtime (or reads a
WFS 2.0 feature type or an Overpass query directly), pages through features
as GeoJSON, and writes
both a verbatim copy (asset.gis_raw) and a conformed copy (asset.pipe /
asset.facility).

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


def paged(fetch_page, limit: int | None = None):
    """Yield features page by page until a short page, an empty one or the limit.

    `fetch_page(offset)` returns one page's features; the services differ in
    how they're asked for a page, not in how paging ends.
    """
    offset, fetched = 0, 0
    while True:
        features = fetch_page(offset)
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


def fetch_features(service_url: str, layer_index: int, extent: dict | None,
                   limit: int | None = None):
    """Page through a FeatureServer layer, yielding GeoJSON features.

    An extent of None queries the whole service — correct for a small network,
    slow for a large one.
    """
    query_url = f"{service_url}/{layer_index}/query"
    params = {
        "where": "1=1",
        "outFields": "*",
        "inSR": 4326,
        "outSR": 4326,
        "returnGeometry": "true",
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

    def page(offset: int) -> list:
        resp = requests.get(query_url, params={**params, "resultOffset": offset},
                            timeout=120)
        resp.raise_for_status()
        payload = resp.json()
        if "error" in payload:
            raise RuntimeError(f"Query error: {payload['error']}")
        return payload.get("features", [])

    return paged(page, limit)


def fetch_wfs_features(wfs_url: str, type_name: str, extent: dict | None,
                       cql_filter: str | None = None,
                       geometry_field: str = "geom", limit: int | None = None):
    """Page through a WFS 2.0 feature type as GeoJSON (GeoServer's dialect).

    The extent goes into the CQL filter as a BBOX rather than the `bbox`
    parameter, because GeoServer refuses requests that carry both. CQL takes
    the 'EPSG:4326' code in lon/lat order, unlike the WFS 2.0 bbox axis order.
    """
    clauses = [f"({cql_filter})"] if cql_filter else []
    if extent:
        clauses.append(
            f"BBOX({geometry_field}, {extent['xmin']}, {extent['ymin']}, "
            f"{extent['xmax']}, {extent['ymax']}, 'EPSG:4326')"
        )
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": type_name,
        "outputFormat": "application/json",
        "srsName": "EPSG:4326",
        "count": PAGE_SIZE,
    }
    if clauses:
        params["cql_filter"] = " AND ".join(clauses)

    def page(offset: int) -> list:
        resp = requests.get(wfs_url, params={**params, "startIndex": offset},
                            timeout=120)
        resp.raise_for_status()
        # GeoServer reports errors as an XML exception report with HTTP 200.
        if not resp.headers.get("Content-Type", "").startswith("application/json"):
            raise RuntimeError(f"WFS error: {resp.text[:300]}")
        return resp.json().get("features", [])

    return paged(page, limit)


def osm_geometry(element: dict) -> dict | None:
    """GeoJSON geometry for an Overpass element returned with `out geom`."""
    if element["type"] == "node":
        return {"type": "Point", "coordinates": [element["lon"], element["lat"]]}
    if element["type"] == "way" and element.get("geometry"):
        coords = [[p["lon"], p["lat"]] for p in element["geometry"]]
        if len(coords) >= 4 and coords[0] == coords[-1]:
            return {"type": "Polygon", "coordinates": [coords]}
        return {"type": "LineString", "coordinates": coords}
    if element["type"] == "relation" and element.get("bounds"):
        # A multipolygon's parts would need assembling; its bounds centre is
        # enough to place a site.
        b = element["bounds"]
        return {"type": "Point", "coordinates": [(b["minlon"] + b["maxlon"]) / 2,
                                                 (b["minlat"] + b["maxlat"]) / 2]}
    return None


def fetch_overpass_features(overpass_url: str, query: str,
                            limit: int | None = None):
    """Run an Overpass QL query (ending in `out geom;`) and yield GeoJSON.

    The query, area filter included, lives in the region config. Overpass
    answers a whole query at once, so there is no paging; the extent is left
    to the query because Overpass areas are tighter than a bounding box.
    """
    # The public instances refuse requests without an identifying agent.
    resp = requests.post(overpass_url, data={"data": query}, timeout=300,
                         headers={"User-Agent": "weave-digital-twins gis loader"})
    resp.raise_for_status()
    for n, element in enumerate(resp.json().get("elements", [])):
        if limit and n >= limit:
            return
        geom = osm_geometry(element)
        if geom:
            yield {"id": f"{element['type']}/{element['id']}",
                   "properties": element.get("tags", {}), "geometry": geom}


def layer_features(name: str, cfg: dict, extent: dict | None,
                   limit: int | None):
    """The layer's features, from Overpass, a WFS feature type or an ArcGIS item."""
    if cfg.get("overpass_query"):
        url = cfg.get("overpass_url", "https://overpass-api.de/api/interpreter")
        print(f"  querying {name} from {url} ...")
        return fetch_overpass_features(url, cfg["overpass_query"], limit)

    if cfg.get("wfs_url"):
        print(f"  reading {name} ({cfg['type_name']}) from {cfg['wfs_url']} ...")
        return fetch_wfs_features(cfg["wfs_url"], cfg["type_name"], extent,
                                  cfg.get("cql_filter"),
                                  cfg.get("geometry_field", "geom"), limit)

    item_id = cfg["item_id"]
    print(f"  resolving {name} ({item_id}) ...")
    service_url = resolve_service_url(item_id)
    print(f"    service: {service_url}")
    return fetch_features(service_url, cfg.get("layer", 0), extent, limit)


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


def part_ids(pipe_id: str, parts: int | None) -> set[str]:
    """The asset.pipe ids a feature conformed into `parts` pipes was given."""
    if not parts:
        return set()
    if parts == 1:
        return {pipe_id}
    return {f"{pipe_id}:{n}" for n in range(1, parts + 1)}


def drop_stale_pipes(cur, stale: list[str]) -> int:
    """Remove pipes a reloaded feature no longer produces.

    Without this, a feature that changes how many parts it splits into keeps
    its old pipes next to the new ones, so asset.pipe no longer matches
    asset.gis_raw. Synthetic meters may point at an old part; they keep their
    row and lose only the reference, which the seeder sets again. Returns how
    many meters were released.
    """
    cur.execute("UPDATE ref.water_meter SET nearest_pipe = NULL"
                " WHERE nearest_pipe = ANY(%s)", (stale,))
    released = cur.rowcount
    cur.execute("DELETE FROM asset.pipe WHERE pipe_id = ANY(%s)", (stale,))
    return released


def upsert_pipe(cur, name: str, fid: str, cfg: dict, props: dict, hints: dict,
                geojson: str, source_id: str, previous_parts: int | None) -> int:
    """Conform one line feature into asset.pipe; returns meters released.

    ST_LineMerge leaves a MultiLineString when the parts don't meet end to
    end, and asset.pipe holds LineStrings only. Each part becomes its own
    pipe, <layer>:<fid>:<n>; a feature that merges to one line keeps the
    plain <layer>:<fid> id.
    """
    pipe_id = f"{name}:{fid}"
    cur.execute(
        """INSERT INTO asset.pipe
             (pipe_id, layer, owner, asset_name, diameter_mm,
              material, length_m, geom, source)
           SELECT CASE WHEN count(*) OVER () = 1 THEN %(pipe_id)s
                       ELSE %(pipe_id)s || ':' || part.path[1] END,
                  %(layer)s, %(owner)s, %(asset_name)s,
                  %(diameter_mm)s, %(material)s,
                  ST_Length(part.geom::geography), part.geom,
                  %(source)s
             FROM ST_Dump(ST_LineMerge(ST_SetSRID(
                    ST_GeomFromGeoJSON(%(geojson)s), 4326))) AS part
           ON CONFLICT (pipe_id) DO UPDATE SET
             diameter_mm = EXCLUDED.diameter_mm,
             material = EXCLUDED.material,
             length_m = EXCLUDED.length_m,
             geom = EXCLUDED.geom
           RETURNING pipe_id""",
        {
            "pipe_id": pipe_id, "layer": name,
            "owner": cfg.get("owner"),
            "asset_name": pick(props, hints["asset_name"]),
            "diameter_mm": to_number(pick(props, hints["diameter_mm"])),
            "material": pick(props, hints["material"]),
            "geojson": geojson, "source": source_id,
        },
    )
    current = {row[0] for row in cur.fetchall()}
    stale = part_ids(pipe_id, previous_parts) - current
    return drop_stale_pipes(cur, sorted(stale)) if stale else 0


def load_layer(conn, name: str, cfg: dict, extent: dict | None, hints: dict,
               source_id: str, limit: int | None = None) -> int:
    if not (cfg.get("item_id") or cfg.get("wfs_url") or cfg.get("overpass_query")):
        print(f"  ! {name}: no item_id set — run discover_layers.py")
        return 0

    target = cfg.get("target", "raw_only")
    count = 0
    released = 0

    with conn.cursor() as cur:
        for feat in layer_features(name, cfg, extent, limit):
            props = feat.get("properties") or {}
            geom = feat.get("geometry")
            if not geom:
                continue

            fid = str(feat.get("id") or props.get("OBJECTID") or count)
            geojson = json.dumps(geom)

            # The previous raw geometry says how many pipes this feature was
            # conformed into last time. Every part of the statement reads the
            # same snapshot, so `previous` sees the row before it is updated.
            cur.execute(
                """WITH previous AS (
                       SELECT CASE WHEN ST_GeometryType(geom) IN
                                            ('ST_LineString', 'ST_MultiLineString')
                                   THEN ST_NumGeometries(ST_LineMerge(geom)) END AS parts
                         FROM asset.gis_raw
                        WHERE layer = %(layer)s AND source_fid = %(fid)s
                   ), upsert AS (
                       INSERT INTO asset.gis_raw (layer, source_fid, props, geom)
                       VALUES (%(layer)s, %(fid)s, %(props)s,
                               ST_SetSRID(ST_GeomFromGeoJSON(%(geojson)s), 4326))
                       ON CONFLICT (layer, source_fid)
                       DO UPDATE SET props = EXCLUDED.props,
                                     geom = EXCLUDED.geom,
                                     loaded_at = now()
                   )
                   SELECT (SELECT parts FROM previous)""",
                {"layer": name, "fid": fid, "props": Jsonb(props), "geojson": geojson},
            )
            previous_parts = cur.fetchone()[0]

            if target == "pipe" and geom.get("type") in ("LineString", "MultiLineString"):
                released += upsert_pipe(cur, name, fid, cfg, props, hints, geojson,
                                        source_id, previous_parts)

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
    if released:
        print(f"    {released} meters lost their nearest pipe to a re-split feature;"
              " re-run seed_synthetic_network.py to reassign them")
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
                # A failed statement aborts the transaction; without the
                # rollback every later layer and the topology build fail too.
                conn.rollback()
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
