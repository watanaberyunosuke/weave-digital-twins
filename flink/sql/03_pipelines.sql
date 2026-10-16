-- Weave — pipelines
-- One statement set. Watch at http://localhost:9081

EXECUTE STATEMENT SET
BEGIN

INSERT INTO hydrometry_sink
SELECT CAST(kafka_ts AS TIMESTAMP(3)), site_id, variable, `value`,
       quality_code, provider, false
FROM hydrometry_raw
WHERE `value` IS NOT NULL;

INSERT INTO market_sink
SELECT CAST(kafka_ts AS TIMESTAMP(3)), region_id, price_mwh, total_demand_mw,
       net_interchange_mw, scheduled_generation_mw,
       semi_scheduled_generation_mw, false
FROM market_dispatch_raw
WHERE region_id IS NOT NULL;

INSERT INTO meter_sink
SELECT CAST(kafka_ts AS TIMESTAMP(3)), meter_id, dma_id, litres, flow_lpm,
       battery_pct, true
FROM meter_raw
WHERE litres >= 0;

INSERT INTO pressure_sink
SELECT CAST(kafka_ts AS TIMESTAMP(3)), logger_id, dma_id, pressure_m, flow_ls, true
FROM pressure_raw
WHERE pressure_m BETWEEN 0 AND 120;

INSERT INTO electricity_meter_sink
SELECT CAST(kafka_ts AS TIMESTAMP(3)), meter_id, kwh_import, kwh_export, voltage_v, true
FROM electricity_meter_raw
WHERE voltage_v BETWEEN 180 AND 280;

-- Gold: 15-minute consumption per DMA, joined to pressure minima.
INSERT INTO dma_balance_sink
SELECT m.window_start,
       m.dma_id,
       m.consumption_l,
       p.min_pressure_m,
       m.meter_count
FROM (
    SELECT window_start, window_end, dma_id,
           sum(litres) AS consumption_l,
           count(DISTINCT meter_id) AS meter_count
    FROM TABLE(TUMBLE(TABLE meter_raw, DESCRIPTOR(kafka_ts), INTERVAL '15' MINUTES))
    GROUP BY window_start, window_end, dma_id
) m
LEFT JOIN (
    SELECT window_start, window_end, dma_id,
           min(pressure_m) AS min_pressure_m
    FROM TABLE(TUMBLE(TABLE pressure_raw, DESCRIPTOR(kafka_ts), INTERVAL '15' MINUTES))
    GROUP BY window_start, window_end, dma_id
) p
  ON m.dma_id = p.dma_id
 AND m.window_start = p.window_start
 AND m.window_end = p.window_end;

END;
