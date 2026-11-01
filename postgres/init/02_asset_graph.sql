-- Weave — asset graph (real topology from public GIS)
-- Loaded by gis/load_gis.py from whichever catalogue the active region config
-- in gis/regions/ points at.

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pgrouting;
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE SCHEMA IF NOT EXISTS asset;
CREATE SCHEMA IF NOT EXISTS twin;
CREATE SCHEMA IF NOT EXISTS ref;

-- ============================================================ WATER: real
-- Raw GIS layers land here verbatim; attributes vary by layer so the source
-- properties are kept as JSONB rather than forced into a fixed schema.

CREATE TABLE asset.gis_raw (
    layer        TEXT NOT NULL,
    source_fid   TEXT NOT NULL,
    props        JSONB,
    geom         GEOMETRY(Geometry, 4326),
    loaded_at    TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (layer, source_fid)
);
CREATE INDEX ON asset.gis_raw USING GIST (geom);
CREATE INDEX ON asset.gis_raw (layer);

-- Conformed pipe network. Edges of the graph.
CREATE TABLE asset.pipe (
    pipe_id       TEXT PRIMARY KEY,
    layer         TEXT,                 -- which GIS layer it came from
    owner         TEXT,                 -- asset owner, per the source layer
    asset_name    TEXT,
    diameter_mm   NUMERIC,
    material      TEXT,
    length_m      NUMERIC,
    geom          GEOMETRY(LineString, 4326) NOT NULL,
    source        TEXT,                 -- region.source_id from the region config
    start_node    BIGINT,               -- populated by build_topology()
    end_node      BIGINT
);
CREATE INDEX ON asset.pipe USING GIST (geom);

-- Nodes of the graph, derived from pipe endpoints by build_topology().
CREATE TABLE asset.node (
    node_id    BIGINT PRIMARY KEY,
    geom       GEOMETRY(Point, 4326) NOT NULL,
    node_type  TEXT DEFAULT 'junction', -- junction | reservoir | tank | source
    elevation_m NUMERIC
);
CREATE INDEX ON asset.node USING GIST (geom);

-- Point assets: reservoirs, storage dams, pump sets.
CREATE TABLE asset.facility (
    facility_id   TEXT PRIMARY KEY,
    facility_type TEXT NOT NULL,        -- service_reservoir | storage_dam | pump_set
    name          TEXT,
    capacity_ml   NUMERIC,
    props         JSONB,
    geom          GEOMETRY(Point, 4326) NOT NULL,
    source        TEXT
);
CREATE INDEX ON asset.facility USING GIST (geom);
CREATE INDEX ON asset.facility (facility_type);

-- ==================================================== WATER: synthetic DMAs
-- District Metered Areas are not public. We derive plausible ones by
-- clustering the real pipe network, so synthetic meters hang off real assets
-- rather than floating at random coordinates.

CREATE TABLE asset.dma (
    dma_id       TEXT PRIMARY KEY,
    name         TEXT,
    pipe_count   INTEGER,
    total_length_m NUMERIC,
    geom         GEOMETRY(Polygon, 4326),
    centroid     GEOMETRY(Point, 4326),
    synthetic    BOOLEAN DEFAULT true
);
CREATE INDEX ON asset.dma USING GIST (geom);

-- Synthetic meter and logger registry, seeded from real DMAs.
CREATE TABLE ref.water_meter (
    meter_id      TEXT PRIMARY KEY,
    dma_id        TEXT REFERENCES asset.dma(dma_id),
    meter_type    TEXT,                 -- residential | commercial | bulk
    nearest_pipe  TEXT REFERENCES asset.pipe(pipe_id),
    geom          GEOMETRY(Point, 4326),
    synthetic     BOOLEAN DEFAULT true
);
CREATE INDEX ON ref.water_meter USING GIST (geom);
CREATE INDEX ON ref.water_meter (dma_id);

CREATE TABLE ref.pressure_logger (
    logger_id     TEXT PRIMARY KEY,
    dma_id        TEXT REFERENCES asset.dma(dma_id),
    node_id       BIGINT,
    geom          GEOMETRY(Point, 4326),
    synthetic     BOOLEAN DEFAULT true
);
CREATE INDEX ON ref.pressure_logger USING GIST (geom);

-- ====================================================== WATER: real gauges
-- Public gauge network. Real identifiers, real locations.
CREATE TABLE ref.hydrometry_site (
    site_id      TEXT PRIMARY KEY,
    site_name    TEXT,
    provider     TEXT,                  -- HYDRO_PROVIDER of the feeding poller
    variable     TEXT,                  -- flow | level | rainfall | quality
    unit         TEXT,
    geom         GEOMETRY(Point, 4326),
    props        JSONB
);
CREATE INDEX ON ref.hydrometry_site USING GIST (geom);

-- Real reservoir storage gauges (ingest/storage_poller.py). facility_id is the
-- nearest supply asset within STORAGE_MATCH_M, a best guess kept with its
-- distance; a gauge with no asset nearby still stands on its own point.
CREATE TABLE ref.storage_site (
    station_no   TEXT PRIMARY KEY,
    storage_name TEXT,
    facility_id  TEXT,
    match_m      DOUBLE PRECISION,
    source       TEXT,
    geom         GEOMETRY(Point, 4326),
    synthetic    BOOLEAN DEFAULT false
);
CREATE INDEX storage_site_geom_idx ON ref.storage_site USING GIST (geom);

-- =========================================================== ELECTRICITY
CREATE TABLE asset.substation (
    substation_id TEXT PRIMARY KEY,
    name          TEXT,
    operator      TEXT,
    voltage_kv    NUMERIC,
    source        TEXT,                 -- 'osm'
    props         JSONB,
    geom          GEOMETRY(Point, 4326)
);
CREATE INDEX ON asset.substation USING GIST (geom);

CREATE TABLE asset.power_line (
    line_id     TEXT PRIMARY KEY,
    voltage_kv  NUMERIC,
    operator    TEXT,
    source      TEXT,
    geom        GEOMETRY(LineString, 4326)
);
CREATE INDEX ON asset.power_line USING GIST (geom);

-- Synthetic connection-point meters, hung off real substations.
CREATE TABLE ref.electricity_meter (
    meter_id      TEXT PRIMARY KEY,
    substation_id TEXT REFERENCES asset.substation(substation_id),
    customer_type TEXT,                 -- residential | commercial | industrial
    has_solar     BOOLEAN,
    geom          GEOMETRY(Point, 4326),
    synthetic     BOOLEAN DEFAULT true
);
CREATE INDEX ON ref.electricity_meter USING GIST (geom);

-- ============================================================= TRANSPORT
CREATE TABLE asset.transit_stop (
    stop_id    TEXT PRIMARY KEY,
    stop_name  TEXT,
    mode       TEXT,
    geom       GEOMETRY(Point, 4326),
    source     TEXT DEFAULT 'gtfs'
);
CREATE INDEX ON asset.transit_stop USING GIST (geom);

-- ============================================== derive graph from pipe geom
-- Snaps pipe endpoints onto shared nodes: an endpoint within `tolerance`
-- (degrees, as the geometry is 4326) of a node joins it, otherwise it founds a
-- new node, first come first served, as pgr_createTopology did. That function
-- was removed in pgRouting 4, and its replacement, pgr_extractVertices, only
-- merges exactly equal points, while real GIS endpoints that should meet are
-- routinely a few millimetres apart. So the snapping is done here in PostGIS.
--
-- Endpoints are compared with nodes, never with each other, so every endpoint
-- ends up within tolerance of its own node and a second run finds nothing new
-- (a cluster-based snap can chain points further apart than that, and would
-- then re-split them on the next run). Existing nodes are matched first and
-- kept, so node ids, elevations and node types survive re-runs and incremental
-- loads; to re-snap at a different tolerance, empty asset.node first.
-- Junctions no pipe touches any more are removed, because run_sim.py turns
-- every node into an EPANET junction and an unconnected one fails the solve.
--
-- Volumes initialised before this change still carry pgr_createTopology's
-- id, source_node and target_node columns on asset.pipe; nothing reads them.
CREATE OR REPLACE FUNCTION asset.build_topology(tolerance DOUBLE PRECISION DEFAULT 0.00001)
RETURNS TEXT LANGUAGE plpgsql AS $$
DECLARE
    ep       RECORD;
    hit      BIGINT;
    next_id  BIGINT;
    created  INTEGER := 0;
    removed  INTEGER := 0;
BEGIN
    -- New ids come from max()+1, so two concurrent runs would collide. Plain
    -- reads (the console, run_sim.py) are not blocked by this mode.
    LOCK TABLE asset.node IN EXCLUSIVE MODE;

    -- Normally gone with the previous call's transaction; this covers two
    -- calls in one.
    IF to_regclass('pg_temp.build_topology_endpoint') IS NOT NULL THEN
        DROP TABLE pg_temp.build_topology_endpoint;
    END IF;
    CREATE TEMP TABLE build_topology_endpoint ON COMMIT DROP AS
    SELECT pipe_id, true AS is_start, ST_StartPoint(geom) AS pt, NULL::BIGINT AS node_id
    FROM asset.pipe
    UNION ALL
    SELECT pipe_id, false, ST_EndPoint(geom), NULL::BIGINT
    FROM asset.pipe;
    DELETE FROM build_topology_endpoint WHERE pt IS NULL;     -- empty geometry
    ALTER TABLE build_topology_endpoint ADD PRIMARY KEY (pipe_id, is_start);
    ANALYZE build_topology_endpoint;

    -- Set-based while nodes are not being created, which on a re-run is
    -- every endpoint.
    UPDATE build_topology_endpoint e SET node_id = (
        SELECT n.node_id FROM asset.node n
        WHERE ST_DWithin(n.geom, e.pt, tolerance)
        ORDER BY n.geom <-> e.pt, n.node_id
        LIMIT 1);

    -- One at a time for the rest, so each endpoint sees the nodes founded by
    -- those before it. The fixed order keeps node ids reproducible.
    SELECT COALESCE(max(node_id), 0) + 1 INTO next_id FROM asset.node;
    FOR ep IN
        SELECT pipe_id, is_start, pt FROM build_topology_endpoint
        WHERE node_id IS NULL
        ORDER BY pipe_id, is_start DESC
    LOOP
        SELECT n.node_id INTO hit FROM asset.node n
        WHERE ST_DWithin(n.geom, ep.pt, tolerance)
        ORDER BY n.geom <-> ep.pt, n.node_id
        LIMIT 1;
        IF hit IS NULL THEN
            INSERT INTO asset.node (node_id, geom) VALUES (next_id, ep.pt);
            hit := next_id;
            next_id := next_id + 1;
            created := created + 1;
        END IF;
        UPDATE build_topology_endpoint SET node_id = hit
        WHERE pipe_id = ep.pipe_id AND is_start = ep.is_start;
    END LOOP;

    -- Only changed rows are written, so a re-run leaves asset.pipe untouched.
    UPDATE asset.pipe p
    SET start_node = s.node_id, end_node = e.node_id
    FROM build_topology_endpoint s
    JOIN build_topology_endpoint e ON e.pipe_id = s.pipe_id AND NOT e.is_start
    WHERE s.is_start AND s.pipe_id = p.pipe_id
      AND (p.start_node, p.end_node) IS DISTINCT FROM (s.node_id, e.node_id);

    DELETE FROM asset.node n
    WHERE n.node_type = 'junction'
      AND NOT EXISTS (SELECT 1 FROM build_topology_endpoint e
                      WHERE e.node_id = n.node_id);
    GET DIAGNOSTICS removed = ROW_COUNT;

    RETURN format('%s pipes in graph, %s nodes (%s new, %s removed)',
                  (SELECT count(*) FROM asset.pipe WHERE start_node IS NOT NULL),
                  (SELECT count(*) FROM asset.node), created, removed);
END;
$$;
