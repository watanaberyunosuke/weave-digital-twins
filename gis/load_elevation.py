#!/usr/bin/env python3
"""Sample a DEM onto asset.node.elevation_m.

Without elevations every junction sits at the model's 50 m default, so every
modelled pressure is reservoir head minus 50 and the residual check compares
synthetic readings against a constant. This fills the nodes from an ArcGIS
elevation tile service named in the region config:

    elevation:
      tile_service: https://<host>/arcgis/rest/services/<dem>/ImageServer
      level: 14

Tile-only services (no getSamples or identify) are the common case for
public DEMs, so the nodes are grouped by tile, each tile is fetched once and
decoded from LERC, and every node is interpolated from the 257 x 257 grid.
The extra row and column repeat the neighbouring tile's first, which makes
the grid one of point samples, so bilinear interpolation needs no tile
stitching.

    python gis/load_elevation.py            # nodes with no elevation yet
    python gis/load_elevation.py --all      # re-sample every node
"""

from __future__ import annotations

import argparse
import math
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

import imagecodecs
import numpy as np
import psycopg
import requests

from load_gis import DSN, load_config, resolve_config_path

ORIGIN = 20037508.342787            # Web Mercator half-extent
EARTH_RADIUS = 6378137.0
TILE = 256
WEB_MERCATOR = {102100, 3857}
WORKERS = 4


def service_info(url: str, level: int) -> None:
    """Fail early on a service the sampling below can't read."""
    r = requests.get(url, params={"f": "json"}, timeout=30)
    r.raise_for_status()
    info = r.json()
    if "error" in info:
        sys.exit(f"! elevation service error: {info['error']}")
    tiles = info.get("tileInfo") or {}
    wkid = (tiles.get("spatialReference") or {}).get("latestWkid") or \
        (tiles.get("spatialReference") or {}).get("wkid")
    if tiles.get("format") != "LERC" or wkid not in WEB_MERCATOR or tiles.get("rows") != TILE:
        sys.exit(f"! {url} is not a Web Mercator LERC tile service "
                 f"(format={tiles.get('format')}, wkid={wkid}, rows={tiles.get('rows')})")
    max_lod = info.get("maxLOD", len(tiles.get("lods", [])) - 1)
    if level > max_lod:
        sys.exit(f"! level {level} is beyond the service's maxLOD {max_lod}")


def tile_position(lon: float, lat: float, level: int) -> tuple[int, int, float, float]:
    """(row, col) of the tile holding a point, and its pixel offset inside it."""
    x = math.radians(lon) * EARTH_RADIUS
    y = math.log(math.tan(math.pi / 4 + math.radians(lat) / 2)) * EARTH_RADIUS
    res = 2 * ORIGIN / TILE / 2 ** level
    px, py = (x + ORIGIN) / res, (ORIGIN - y) / res
    return int(py // TILE), int(px // TILE), px % TILE, py % TILE


def bilinear(grid: np.ndarray, valid: np.ndarray | None, fx: float, fy: float):
    """Interpolate the point grid; None if any of the four samples is nodata."""
    c, r = int(fx), int(fy)
    dx, dy = fx - c, fy - r
    if valid is not None and not valid[r:r + 2, c:c + 2].all():
        return None
    z = grid[r:r + 2, c:c + 2].astype(float)
    return float(z[0, 0] * (1 - dx) * (1 - dy) + z[0, 1] * dx * (1 - dy)
                 + z[1, 0] * (1 - dx) * dy + z[1, 1] * dx * dy)


def nodes_by_tile(cur, level: int, every_node: bool) -> dict:
    """Group the nodes to sample by the tile that holds them."""
    cur.execute("SELECT node_id, ST_X(geom), ST_Y(geom) FROM asset.node"
                + ("" if every_node else " WHERE elevation_m IS NULL"))
    by_tile: dict[tuple[int, int], list[tuple[int, float, float]]] = defaultdict(list)
    for node_id, lon, lat in cur.fetchall():
        row, col, fx, fy = tile_position(lon, lat, level)
        by_tile[(row, col)].append((node_id, fx, fy))
    return by_tile


def fetch_tile(url: str, level: int, key: tuple[int, int]):
    """(key, decoded grid and mask or None if outside the DEM, error or None).

    A tile that fails or lies outside the DEM leaves its nodes unset (the
    model's default) instead of stopping the run.
    """
    row, col = key
    try:
        resp = requests.get(f"{url}/tile/{level}/{row}/{col}", timeout=60)
        if resp.status_code == 404:
            return key, None, None
        resp.raise_for_status()
        return key, imagecodecs.lerc_decode(resp.content, masks=True), None
    except Exception as exc:
        return key, None, exc


def sample_tiles(url: str, level: int, by_tile: dict):
    """Interpolate every node from its tile; (samples, missing, failed)."""
    samples: list[tuple[int, float]] = []
    missing = failed = 0
    # Each tile takes seconds to arrive, so a few are fetched at once, as a
    # map viewer would; more would lean on a free public service.
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        results = pool.map(lambda key: fetch_tile(url, level, key), sorted(by_tile))
        for i, (key, decoded, exc) in enumerate(results, 1):
            members = by_tile[key]
            if exc is not None:
                print(f"  ! tile {level}/{key[0]}/{key[1]} failed: {exc}")
                failed += len(members)
            elif decoded is None:
                missing += len(members)
            else:
                grid, valid = decoded
                values = [(node_id, bilinear(grid, valid, fx, fy))
                          for node_id, fx, fy in members]
                samples += [(n, round(z, 2)) for n, z in values if z is not None]
                missing += sum(1 for _, z in values if z is None)
            if i % 100 == 0:
                print(f"  {i}/{len(by_tile)} tiles ...")
    return samples, missing, failed


def write_elevations(cur, samples: list[tuple[int, float]]) -> int:
    cur.execute("CREATE TEMP TABLE node_elevation (node_id BIGINT, elevation_m DOUBLE PRECISION)")
    with cur.copy("COPY node_elevation FROM STDIN") as copy:
        for row in samples:
            copy.write_row(row)
    cur.execute("""UPDATE asset.node n SET elevation_m = e.elevation_m
                   FROM node_elevation e WHERE e.node_id = n.node_id""")
    return cur.rowcount


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", help="region config (default: $GIS_REGION_CONFIG)")
    ap.add_argument("--all", action="store_true",
                    help="re-sample every node, not only those with no elevation")
    ap.add_argument("--level", type=int, help="override the config's tile level")
    args = ap.parse_args()

    cfg = load_config(resolve_config_path(args.config))
    elevation = cfg.get("elevation") or {}
    url = (elevation.get("tile_service") or "").rstrip("/")
    if not url:
        print("no elevation.tile_service in the region config — nodes keep the "
              "model's default elevation")
        return
    level = args.level if args.level is not None else int(elevation.get("level", 14))
    service_info(url, level)

    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        by_tile = nodes_by_tile(cur, level, args.all)
        nodes = sum(len(v) for v in by_tile.values())
        print(f"sampling {nodes} nodes from {len(by_tile)} tiles at level {level}")
        samples, missing, failed = sample_tiles(url, level, by_tile)
        updated = write_elevations(cur, samples)
        conn.commit()

    print(f"\n{updated} nodes given an elevation; {missing} outside the DEM or on "
          f"nodata, {failed} on tiles that failed to fetch")


if __name__ == "__main__":
    main()
