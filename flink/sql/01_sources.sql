-- Weave — Kafka sources
-- Real feeds and synthetic feeds, all carrying the same envelope.
--
-- Rows are dated by the source's own event_ts, not by the Kafka timestamp, so
-- a late or replayed message keeps the time it was measured. The exception is
-- pressure: Datagen's event_ts is a counter, not a time, so it stays on the
-- Kafka timestamp until the truth-network generator (HDH-124) emits real ones.

SET 'sql-client.execution.result-mode' = 'tableau';
SET 'parallelism.default' = '1';

-- ============================================================ REAL: water
CREATE TABLE IF NOT EXISTS hydrometry_raw (
    source       STRING,
    site_id      STRING,
    variable     STRING,
    unit         STRING,
    `value`      DOUBLE,
    quality_code STRING,
    provider     STRING,
    is_synthetic BOOLEAN,
    -- UTC with a trailing Z (ingest/common.py iso_z). Flink's ISO-8601 reader
    -- rejects a +00:00 offset, and with ignore-parse-errors that would read as
    -- NULL, so the pipeline drops rows without one.
    event_ts     TIMESTAMP_LTZ(3),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '60' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'water.hydrometry.raw.v1',
    'properties.bootstrap.servers' = 'broker:29092',
    'properties.group.id' = 'flink-twin',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'json.ignore-parse-errors' = 'true'
);

-- ===================================================== SYNTHETIC: meters
CREATE TABLE IF NOT EXISTS meter_raw (
    meter_id    STRING,
    dma_id      STRING,
    litres      DOUBLE,
    flow_lpm    DOUBLE,
    battery_pct INT,
    event_ts    BIGINT,                      -- end of the reading's interval, ms
    event_time AS TO_TIMESTAMP_LTZ(event_ts, 3),
    -- No delay: a run's rows all share one interval end, and the watermark
    -- reaching it is what closes the previous 15-minute window.
    WATERMARK FOR event_time AS event_time
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
