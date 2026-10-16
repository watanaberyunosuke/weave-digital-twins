-- Weave — Kafka sources
-- Real feeds and synthetic feeds, all carrying the same envelope.

SET 'sql-client.execution.result-mode' = 'tableau';
SET 'parallelism.default' = '1';

-- ============================================================ REAL: water
CREATE TABLE IF NOT EXISTS hydrometry_raw (
    source       STRING,
    site_id      STRING,
    variable     STRING,
    unit         STRING,
    value        DOUBLE,
    quality_code STRING,
    provider     STRING,
    is_synthetic BOOLEAN,
    event_ts     STRING,
    kafka_ts     TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
    WATERMARK FOR kafka_ts AS kafka_ts - INTERVAL '60' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'water.hydrometry.raw.v1',
    'properties.bootstrap.servers' = 'broker:29092',
    'properties.group.id' = 'flink-twin',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

-- ====================================================== REAL: electricity
CREATE TABLE IF NOT EXISTS market_dispatch_raw (
    source                       STRING,
    region_id                    STRING,
    settlement_ts                STRING,
    price_mwh                    DOUBLE,
    total_demand_mw              DOUBLE,
    net_interchange_mw           DOUBLE,
    scheduled_generation_mw      DOUBLE,
    semi_scheduled_generation_mw DOUBLE,
    is_synthetic                 BOOLEAN,
    kafka_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
    WATERMARK FOR kafka_ts AS kafka_ts - INTERVAL '60' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'electricity.market-dispatch.raw.v1',
    'properties.bootstrap.servers' = 'broker:29092',
    'properties.group.id' = 'flink-twin',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

-- ===================================================== SYNTHETIC: meters
CREATE TABLE IF NOT EXISTS meter_raw (
    meter_id    STRING,
    dma_id      STRING,
    litres      DOUBLE,
    flow_lpm    DOUBLE,
    battery_pct INT,
    event_ts    BIGINT,
    kafka_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
    WATERMARK FOR kafka_ts AS kafka_ts - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'water.meter-readings.raw.v1',
    'properties.bootstrap.servers' = 'broker:29092',
    'properties.group.id' = 'flink-twin',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE IF NOT EXISTS pressure_raw (
    logger_id  STRING,
    dma_id     STRING,
    pressure_m DOUBLE,
    flow_ls    DOUBLE,
    event_ts   BIGINT,
    kafka_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
    WATERMARK FOR kafka_ts AS kafka_ts - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'water.pressure-readings.raw.v1',
    'properties.bootstrap.servers' = 'broker:29092',
    'properties.group.id' = 'flink-twin',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE IF NOT EXISTS electricity_meter_raw (
    meter_id   STRING,
    kwh_import DOUBLE,
    kwh_export DOUBLE,
    voltage_v  DOUBLE,
    event_ts   BIGINT,
    kafka_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
    WATERMARK FOR kafka_ts AS kafka_ts - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'electricity.meter-interval.raw.v1',
    'properties.bootstrap.servers' = 'broker:29092',
    'properties.group.id' = 'flink-twin',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);
