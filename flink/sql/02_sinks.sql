-- Weave — Postgres sinks
-- Flink SQL has no env substitution, so the connection details are literal.
-- If you change them in docker-compose.override.yml, change them here too.

CREATE TABLE IF NOT EXISTS hydrometry_sink (
    ts           TIMESTAMP(3),
    site_id      STRING,
    variable     STRING,
    value        DOUBLE,
    quality_code STRING,
    provider     STRING,
    is_synthetic BOOLEAN
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/weave_twin',
    'table-name' = 'twin.hydrometry',
    'username' = 'weave', 'password' = 'weave',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '5s'
);

CREATE TABLE IF NOT EXISTS market_sink (
    ts                           TIMESTAMP(3),
    region_id                    STRING,
    price_mwh                    DOUBLE,
    total_demand_mw              DOUBLE,
    net_interchange_mw           DOUBLE,
    scheduled_generation_mw      DOUBLE,
    semi_scheduled_generation_mw DOUBLE,
    is_synthetic                 BOOLEAN,
    PRIMARY KEY (ts, region_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/weave_twin',
    'table-name' = 'twin.market_dispatch',
    'username' = 'weave', 'password' = 'weave'
);

CREATE TABLE IF NOT EXISTS meter_sink (
    ts           TIMESTAMP(3),
    meter_id     STRING,
    dma_id       STRING,
    litres       DOUBLE,
    flow_lpm     DOUBLE,
    battery_pct  INT,
    is_synthetic BOOLEAN
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/weave_twin',
    'table-name' = 'twin.meter_reading',
    'username' = 'weave', 'password' = 'weave',
    'sink.buffer-flush.max-rows' = '500',
    'sink.buffer-flush.interval' = '2s'
);

CREATE TABLE IF NOT EXISTS pressure_sink (
    ts           TIMESTAMP(3),
    logger_id    STRING,
    dma_id       STRING,
    pressure_m   DOUBLE,
    flow_ls      DOUBLE,
    is_synthetic BOOLEAN
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/weave_twin',
    'table-name' = 'twin.pressure_reading',
    'username' = 'weave', 'password' = 'weave',
    'sink.buffer-flush.max-rows' = '200',
    'sink.buffer-flush.interval' = '2s'
);

CREATE TABLE IF NOT EXISTS electricity_meter_sink (
    ts           TIMESTAMP(3),
    meter_id     STRING,
    kwh_import   DOUBLE,
    kwh_export   DOUBLE,
    voltage_v    DOUBLE,
    is_synthetic BOOLEAN
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/weave_twin',
    'table-name' = 'twin.electricity_interval',
    'username' = 'weave', 'password' = 'weave',
    'sink.buffer-flush.max-rows' = '500',
    'sink.buffer-flush.interval' = '2s'
);

-- Gold: per-DMA water balance, the leak signal.
CREATE TABLE IF NOT EXISTS dma_balance_sink (
    window_start   TIMESTAMP(3),
    dma_id         STRING,
    consumption_l  DOUBLE,
    min_pressure_m DOUBLE,
    meter_count    BIGINT,
    PRIMARY KEY (window_start, dma_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/weave_twin',
    'table-name' = 'twin.dma_balance',
    'username' = 'weave', 'password' = 'weave'
);
