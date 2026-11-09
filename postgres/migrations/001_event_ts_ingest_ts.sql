-- HDH-75: separate when a reading was measured from when it arrived, and key
-- each reading so a replay is an upsert. postgres/init/ runs only on an empty
-- volume, so this brings an existing one to the same shape. Safe to re-run.
--
--   docker exec -i weave-postgres psql -U weave -d weave_twin -v ON_ERROR_STOP=1 \
--     < postgres/migrations/001_event_ts_ingest_ts.sql
--
-- Stop the Flink pipelines first (a running sink still writes the old column
-- names), and start them again from the updated flink/sql/ files afterwards.
--
-- Rows that arrived before this migration have no recorded arrival for
-- meter_reading and pressure_reading, so their ingest_ts stays NULL (unknown)
-- rather than being filled with a time that would be wrong.

BEGIN;

DO $$
DECLARE
    t text;
BEGIN
    -- ts -> event_ts (hypertable dimension columns can be renamed in place).
    FOREACH t IN ARRAY ARRAY['hydrometry', 'meter_reading', 'pressure_reading'] LOOP
        IF EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_schema = 'twin' AND table_name = t AND column_name = 'ts') THEN
            EXECUTE format('ALTER TABLE twin.%I RENAME COLUMN ts TO event_ts', t);
        END IF;
    END LOOP;

    -- ingested_at -> ingest_ts on hydrometry, which already recorded arrival.
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = 'twin' AND table_name = 'hydrometry' AND column_name = 'ingested_at') THEN
        ALTER TABLE twin.hydrometry RENAME COLUMN ingested_at TO ingest_ts;
    END IF;
END
$$;

ALTER INDEX IF EXISTS twin.hydrometry_ingested_at_idx RENAME TO hydrometry_ingest_ts_idx;

ALTER TABLE twin.meter_reading ADD COLUMN IF NOT EXISTS ingest_ts TIMESTAMPTZ;
ALTER TABLE twin.meter_reading ALTER COLUMN ingest_ts SET DEFAULT now();
ALTER TABLE twin.pressure_reading ADD COLUMN IF NOT EXISTS ingest_ts TIMESTAMPTZ;
ALTER TABLE twin.pressure_reading ALTER COLUMN ingest_ts SET DEFAULT now();

-- A unique index cannot be built over exact repeats, so remove them first,
-- keeping one row of each. Only rows identical in key and event_ts go.
DELETE FROM twin.hydrometry a USING twin.hydrometry b
WHERE a.tableoid = b.tableoid AND a.ctid > b.ctid
  AND a.site_id = b.site_id AND a.variable = b.variable AND a.event_ts = b.event_ts;
DELETE FROM twin.meter_reading a USING twin.meter_reading b
WHERE a.tableoid = b.tableoid AND a.ctid > b.ctid
  AND a.meter_id = b.meter_id AND a.event_ts = b.event_ts;
DELETE FROM twin.pressure_reading a USING twin.pressure_reading b
WHERE a.tableoid = b.tableoid AND a.ctid > b.ctid
  AND a.logger_id = b.logger_id AND a.event_ts = b.event_ts;

CREATE UNIQUE INDEX IF NOT EXISTS hydrometry_key ON twin.hydrometry (site_id, variable, event_ts);
CREATE UNIQUE INDEX IF NOT EXISTS meter_reading_key ON twin.meter_reading (meter_id, event_ts);
CREATE UNIQUE INDEX IF NOT EXISTS pressure_reading_key ON twin.pressure_reading (logger_id, event_ts);

COMMIT;
