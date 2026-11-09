-- Weave — pipelines
-- One statement set. Watch at http://localhost:9081

-- The Postgres keys make a replayed message an upsert, but the window below sums
-- messages, not rows, so a repeat would count twice in dma_balance. Keep the first
-- message per (meter, event time); the state is dropped after the TTL, which is
-- longer than any replay the producers make.
SET 'table.exec.state.ttl' = '2 h';

CREATE TEMPORARY VIEW meter_first AS
SELECT meter_id, dma_id, litres, event_time
FROM (
    SELECT meter_id, dma_id, litres, event_time,
           ROW_NUMBER() OVER (PARTITION BY meter_id, event_time ORDER BY event_time ASC) AS rn
    FROM meter_raw
    WHERE litres >= 0 AND event_ts IS NOT NULL AND meter_id IS NOT NULL
)
WHERE rn = 1;

EXECUTE STATEMENT SET
BEGIN

-- Event time from the source. A row without a readable event_ts is dropped
-- rather than dated by its arrival, which is the false-recent reading this
-- pipeline exists to avoid.
INSERT INTO hydrometry_sink
SELECT CAST(event_ts AS TIMESTAMP(3)), site_id, variable, `value`,
       quality_code, provider, false
FROM hydrometry_raw
WHERE `value` IS NOT NULL AND event_ts IS NOT NULL AND site_id IS NOT NULL AND variable IS NOT NULL;

INSERT INTO meter_sink
SELECT CAST(event_time AS TIMESTAMP(3)), meter_id, dma_id, litres, flow_lpm,
       battery_pct, true
FROM meter_raw
WHERE litres >= 0 AND event_ts IS NOT NULL AND meter_id IS NOT NULL;

-- Pressure is still dated by arrival: Datagen's event_ts is a counter (HDH-124).
INSERT INTO pressure_sink
SELECT CAST(kafka_ts AS TIMESTAMP(3)), logger_id, dma_id, pressure_m, flow_ls, true
FROM pressure_raw
WHERE pressure_m BETWEEN 0 AND 120 AND logger_id IS NOT NULL;

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
    FROM TABLE(TUMBLE(TABLE meter_first, DESCRIPTOR(event_time), INTERVAL '15' MINUTES))
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
