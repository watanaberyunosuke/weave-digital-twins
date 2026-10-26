-- Weave — telemetry (silver/gold), Timescale hypertables
-- Real feeds and synthetic feeds are kept in separate tables and every row
-- carries is_synthetic, so no analysis can silently mix them.

-- ================================================== WATER: real hydrometry
CREATE TABLE twin.hydrometry (
    ts           TIMESTAMPTZ NOT NULL,
    site_id      TEXT        NOT NULL,
    variable     TEXT        NOT NULL,   -- flow_cumecs | level_m | rainfall_mm
    value        DOUBLE PRECISION,
    quality_code TEXT,
    provider     TEXT,
    is_synthetic BOOLEAN DEFAULT false
);
SELECT create_hypertable('twin.hydrometry', 'ts', chunk_time_interval => INTERVAL '7 days');
CREATE INDEX ON twin.hydrometry (site_id, variable, ts DESC);

-- Real: bulk storage volumes from the water authority (batch loaded)
CREATE TABLE twin.storage_level (
    ts             TIMESTAMPTZ NOT NULL,
    storage_name   TEXT NOT NULL,
    volume_ml      DOUBLE PRECISION,
    capacity_ml    DOUBLE PRECISION,
    percent_full   DOUBLE PRECISION,
    is_synthetic   BOOLEAN DEFAULT false,
    PRIMARY KEY (ts, storage_name)
);

-- ============================================= WATER: synthetic meter data
CREATE TABLE twin.meter_reading (
    ts            TIMESTAMPTZ NOT NULL,
    meter_id      TEXT        NOT NULL,
    dma_id        TEXT,
    litres        DOUBLE PRECISION,
    flow_lpm      DOUBLE PRECISION,
    battery_pct   INTEGER,
    is_synthetic  BOOLEAN DEFAULT true
);
SELECT create_hypertable('twin.meter_reading', 'ts', chunk_time_interval => INTERVAL '1 day');
CREATE INDEX ON twin.meter_reading (dma_id, ts DESC);
CREATE INDEX ON twin.meter_reading (meter_id, ts DESC);

CREATE TABLE twin.pressure_reading (
    ts            TIMESTAMPTZ NOT NULL,
    logger_id     TEXT        NOT NULL,
    dma_id        TEXT,
    pressure_m    DOUBLE PRECISION,
    flow_ls       DOUBLE PRECISION,
    is_synthetic  BOOLEAN DEFAULT true
);
SELECT create_hypertable('twin.pressure_reading', 'ts', chunk_time_interval => INTERVAL '1 day');
CREATE INDEX ON twin.pressure_reading (dma_id, ts DESC);

-- Gold: per-DMA water balance. Inflow minus consumption is the leak signal.
CREATE TABLE twin.dma_balance (
    window_start   TIMESTAMPTZ NOT NULL,
    dma_id         TEXT NOT NULL,
    inflow_l       DOUBLE PRECISION,
    consumption_l  DOUBLE PRECISION,
    loss_l         DOUBLE PRECISION,
    loss_pct       DOUBLE PRECISION,
    min_pressure_m DOUBLE PRECISION,
    meter_count    BIGINT,
    PRIMARY KEY (window_start, dma_id)
);

-- Latest state per DMA for the map layer.
CREATE TABLE twin.dma_latest (
    dma_id         TEXT PRIMARY KEY,
    ts             TIMESTAMPTZ,
    inflow_l       DOUBLE PRECISION,
    consumption_l  DOUBLE PRECISION,
    loss_pct       DOUBLE PRECISION,
    min_pressure_m DOUBLE PRECISION,
    status         TEXT               -- OK | WATCH | SUSPECTED_LEAK | UNKNOWN
);

-- ========================================================== ELECTRICITY
-- Real: wholesale market regional dispatch
CREATE TABLE twin.market_dispatch (
    ts            TIMESTAMPTZ NOT NULL,
    region_id     TEXT        NOT NULL,
    price_mwh     DOUBLE PRECISION,
    total_demand_mw DOUBLE PRECISION,
    net_interchange_mw DOUBLE PRECISION,
    scheduled_generation_mw DOUBLE PRECISION,
    semi_scheduled_generation_mw DOUBLE PRECISION,
    is_synthetic  BOOLEAN DEFAULT false,
    PRIMARY KEY (ts, region_id)
);

-- Synthetic: electricity interval meter data
CREATE TABLE twin.electricity_interval (
    ts            TIMESTAMPTZ NOT NULL,
    meter_id      TEXT        NOT NULL,
    substation_id TEXT,
    kwh_import    DOUBLE PRECISION,
    kwh_export    DOUBLE PRECISION,
    voltage_v     DOUBLE PRECISION,
    is_synthetic  BOOLEAN DEFAULT true
);
SELECT create_hypertable('twin.electricity_interval', 'ts', chunk_time_interval => INTERVAL '1 day');
CREATE INDEX ON twin.electricity_interval (substation_id, ts DESC);

-- ============================================================ TRANSPORT
CREATE TABLE twin.vehicle_position (
    ts               TIMESTAMPTZ NOT NULL,
    vehicle_id       TEXT        NOT NULL,
    route_id         TEXT,
    trip_id          TEXT,
    lat              DOUBLE PRECISION,
    lon              DOUBLE PRECISION,
    bearing          INTEGER,
    occupancy_status TEXT,
    is_synthetic     BOOLEAN DEFAULT false,
    geom             GEOMETRY(Point, 4326)
                     GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(lon, lat), 4326)) STORED
);
SELECT create_hypertable('twin.vehicle_position', 'ts', chunk_time_interval => INTERVAL '1 hour');
CREATE INDEX ON twin.vehicle_position (vehicle_id, ts DESC);

CREATE TABLE twin.vehicle_latest (
    vehicle_id       TEXT PRIMARY KEY,
    ts               TIMESTAMPTZ,
    route_id         TEXT,
    lat              DOUBLE PRECISION,
    lon              DOUBLE PRECISION,
    bearing          INTEGER,
    occupancy_status TEXT,
    geom             GEOMETRY(Point, 4326)
                     GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(lon, lat), 4326)) STORED
);
CREATE INDEX ON twin.vehicle_latest USING GIST (geom);

-- ====================================================== simulation output
CREATE TABLE twin.sim_result (
    run_id       TEXT NOT NULL,
    ts           TIMESTAMPTZ NOT NULL,
    element_type TEXT,                  -- node | link
    element_id   TEXT,
    variable     TEXT,                  -- pressure | head | flow | velocity
    value        DOUBLE PRECISION,
    PRIMARY KEY (run_id, ts, element_type, element_id, variable)
);

-- A row here makes a DMA's residual baseline start afresh from reset_at
-- (run_sim.py --reset-baseline); '*' resets every DMA. run_sim.py creates it
-- on volumes initialised before it existed.
CREATE TABLE twin.residual_baseline_reset (
    dma_id   TEXT PRIMARY KEY,
    reset_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE twin.sim_residual (
    ts           TIMESTAMPTZ NOT NULL,
    dma_id       TEXT,
    element_id   TEXT,
    variable     TEXT,
    modelled     DOUBLE PRECISION,
    observed     DOUBLE PRECISION,
    residual     DOUBLE PRECISION,
    z_score      DOUBLE PRECISION,
    flagged      BOOLEAN
);
SELECT create_hypertable('twin.sim_residual', 'ts', chunk_time_interval => INTERVAL '7 days');
CREATE INDEX ON twin.sim_residual (dma_id, ts DESC);
