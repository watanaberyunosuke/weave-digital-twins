-- Weave — read-only role for GIS clients
-- QGIS, ArcGIS Pro, GDAL and the OGC API Features service (featureserv)
-- connect as this role, so nothing outside the pipeline can change the twin.
-- Runs only on an empty volume; on an existing one, apply it by hand:
--   docker exec -i weave-postgres psql -U weave -d weave_twin < postgres/init/04_gis_reader.sql

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'weave_reader') THEN
        CREATE ROLE weave_reader LOGIN PASSWORD 'weave_reader';
    END IF;
END
$$;

-- Session-level guard as well as grants: a client that runs DDL or DML
-- through a raw SQL window still gets refused.
ALTER ROLE weave_reader SET default_transaction_read_only = on;

GRANT CONNECT ON DATABASE weave_twin TO weave_reader;
GRANT USAGE ON SCHEMA asset, ref, twin TO weave_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA asset, ref, twin TO weave_reader;

-- Tables created later by the pipeline (pgRouting vertices, new twin tables)
-- become readable without another grant. TimescaleDB copies a hypertable's
-- grants to each new chunk.
ALTER DEFAULT PRIVILEGES FOR ROLE weave IN SCHEMA asset, ref, twin
    GRANT SELECT ON TABLES TO weave_reader;

-- DMA outlines with their current model status, so a GIS client can colour
-- DMAs the way the console does; twin.dma_latest itself has no geometry.
CREATE OR REPLACE VIEW asset.dma_status AS
SELECT d.dma_id, d.name, d.pipe_count, d.total_length_m, d.synthetic,
       l.ts AS status_ts, COALESCE(l.status, 'UNKNOWN') AS status,
       l.loss_pct, l.min_pressure_m, d.geom
FROM asset.dma d
LEFT JOIN twin.dma_latest l USING (dma_id);

GRANT SELECT ON asset.dma_status TO weave_reader;
