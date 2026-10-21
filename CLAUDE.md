# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Weave is a framework for building digital twins of utility networks (water, electricity, transport). It loads **real** public GIS topology into PostGIS, derives **synthetic** meters onto that real geometry, and runs both real and synthetic telemetry through one Kafka/Flink pipeline into TimescaleDB, where an EPANET model's output is compared against observations to flag anomalies.

This repo is **an overlay, not a standalone stack**. It is copied over `weave-stack/`, a copy of `cp-all-in-one-community/` from Confluent's `cp-all-in-one` (branch `8.2.0-post`), which supplies `broker`, `connect`, `flink-jobmanager`, `flink-taskmanager` and `flink-sql-client`. [docker-compose.override.yml](docker-compose.override.yml) only adds mounts to those and defines the Weave-specific services (`postgres`, `ingest-market`, `ingest-hydrometry`, `gis-loader`, `simulator`, `console`, `featureserv`, and the ELK services `elasticsearch`, `kibana`, `elk-setup`, `logstash`, `filebeat`). Running compose from this directory alone will not work. The override pins the compose project to `weave`, so the directory can be named anything without orphaning the `weave_pgdata` volume.

## Commands

Everything runs in containers; there is no local build and no linter. Syntax-check before running, and run the unit tests (standard library only; the EPANET solve test skips unless `wntr` is installed):

```bash
python3 -m py_compile gis/*.py ingest/*.py scripts/seed_synthetic_network.py model/*.py console/app.py
python3 -m unittest discover -s model -p 'test_*.py'
bash -n scripts/*.sh
node --check console/static/app.js
```

Full bring-up (from inside `weave-stack/` after the overlay is copied in):

```bash
cp .env.example .env                     # then edit for your region
./scripts/download-flink-connectors.sh   # populates flink/lib/ (gitignored, must exist before compose up)
docker compose up -d broker connect flink-jobmanager flink-taskmanager \
    flink-sql-client postgres ingest-market ingest-hydrometry gis-loader simulator console featureserv
./scripts/bootstrap.sh                   # topics -> GIS load -> seed synthetic -> connectors -> build model -> ELK
docker exec -it flink-sql-client sql-client.sh
  SOURCE '/sql/01_sources.sql'; SOURCE '/sql/02_sinks.sql'; SOURCE '/sql/03_pipelines.sql';
```

Individual steps (each is what `bootstrap.sh` invokes, and each is safe to re-run):

```bash
# Find ArcGIS/CKAN item ids for a new region config
docker exec gis-loader python gis/discover_layers.py --search reservoir

# Load GIS. --limit is the way to iterate — a full load of a large network is slow
docker exec gis-loader python gis/load_gis.py --layer water_main --limit 500
docker exec gis-loader python gis/load_gis.py --all --build-topology

# Re-derive DMAs/meters (TRUNCATEs asset.dma, ref.water_meter, ref.pressure_logger) and rewrite schemas/
docker exec gis-loader python scripts/seed_synthetic_network.py --dmas 40 --meters-per-dma 25

./scripts/register-connectors.sh          # re-registers Datagen connectors (DELETEs then POSTs each)
docker exec simulator python run_sim.py --build   # PostGIS -> /app/out/network.inp
docker exec simulator python run_sim.py --once    # one solve + residuals; --loop is the container default

./scripts/elk-up.sh                       # existing cluster if .env's ELASTICSEARCH_URL answers, else local-elk profile
./scripts/setup-elk.sh                    # re-apply ILM, index templates, Kibana data views (elk-setup runs it on up)
curl 'localhost:9200/_cat/indices/twin-*,weave-logs-*?v'

psql postgresql://weave:weave@localhost:5432/weave_twin
```

Console `:8090` and OGC API Features `:9000` (both loopback only), Kibana `:5601` and Elasticsearch `:9200` (the local cluster: security off, loopback only), Flink `:9081`, Connect `:8083`, Postgres `:5432`.

## Architecture

**Region isolation is the core constraint.** No place name appears in any code, SQL, schema or container definition. A region lives in exactly two files: a config under [gis/regions/](gis/regions/) (layers, catalogue, bounding box) and `.env` (feed endpoints, gauge/market ids). Adding a region must never require a code change. Pollers under [ingest/](ingest/) are adapters for the *shape* of an API (a Kisters hydrometry webservice, a wholesale market summary), not for a place — [ingest/common.py:21](ingest/common.py#L21) `require_env` exists to force region settings to be config rather than baked-in defaults. When editing, do not introduce coordinates, gauge ids or agency names outside those two files.

**Real vs synthetic never mixes silently.** Every telemetry table and every Kafka message carries `is_synthetic`, and real/synthetic land in separate tables. Registry tables carry `synthetic BOOLEAN DEFAULT true`. The Flink pipelines hardcode the flag per source ([flink/sql/03_pipelines.sql](flink/sql/03_pipelines.sql)). Preserve this on any new table or pipeline.

**Coherence of synthetic data comes from ordering.** [scripts/seed_synthetic_network.py](scripts/seed_synthetic_network.py) is the hinge: it clusters real `asset.pipe` rows into DMAs (`ST_ClusterKMeans`), places meters *on* real pipe geometry (`ST_LineInterpolatePoint`), then writes Avro schemas into `schemas/` whose `arg.properties.options` are drawn from the ids it just created. So Datagen can only emit ids that exist in the database. `schemas/` is therefore a **generated directory** — it is empty in the repo, and `register-connectors.sh` fails loudly if the seeder hasn't run.

### Data flow

```
public GIS catalogue ─(gis/load_gis.py)─> asset.gis_raw + asset.pipe/facility
                                              │
                              asset.build_topology()  ── snap ends ──> asset.node, pipe.start_node/end_node
                                              │
                     seed_synthetic_network.py ├─> asset.dma, ref.water_meter, ref.pressure_logger
                                               └─> schemas/*.avsc ──> Datagen connectors ──┐
                                                                                           ▼
real feeds (ingest/*.py) ──────────────────────────────────────────> Kafka topics ──> Flink SQL ──> twin.* (Timescale)
                                                                                           │
                                     model/run_sim.py: PostGIS -> EPANET .inp -> solve -> twin.sim_result
                                                                                           │
                                     compute_residuals(): modelled vs observed -> twin.sim_residual, twin.dma_latest
                                                                                           │
                          Logstash (JDBC poll, 15 s) ── twin.* last 7 days ──> Elasticsearch ──> Kibana
                          Filebeat ── container logs ──> Logstash ────────────────────┘
```

### Schemas in Postgres

- `asset` — real topology (`gis_raw` verbatim JSONB, conformed `pipe`/`facility`/`substation`/`power_line`/`transit_stop`, graph `node`) plus derived `dma`.
- `ref` — registries of things that emit telemetry (`water_meter`, `pressure_logger`, `electricity_meter` synthetic; `hydrometry_site` real).
- `twin` — telemetry hypertables and simulation output.

[postgres/init/](postgres/init/) runs **only on an empty volume**. Schema changes require `docker compose down -v` for `postgres` (destroying loaded GIS) or a manual migration.

`asset.build_topology()` ([postgres/init/02_asset_graph.sql:177](postgres/init/02_asset_graph.sql#L177)) snaps pipe endpoints within `tolerance` onto shared `asset.node` rows in plain PostGIS, because pgRouting 4 removed `pgr_createTopology`. Existing nodes keep their ids across re-runs and junctions no pipe touches are pruned; to re-snap at a new tolerance, empty `asset.node` first. The first run is slow on large networks (about 15 s per 180k pipes) and nothing downstream works without it — `run_sim.py --build` exits if `pipe.start_node` is unpopulated.

### Flink layer

Three files sourced in order: `01_sources.sql` (Kafka sources, all JSON, all `latest-offset`, watermarks on the Kafka timestamp), `02_sinks.sql` (JDBC sinks to `twin.*`), `03_pipelines.sql` (one `EXECUTE STATEMENT SET` — bronze passthroughs plus a 15-minute tumbling DMA balance).

Flink SQL has **no environment substitution**, so the Postgres URL and credentials are literal in `02_sinks.sql`. Changing them in `docker-compose.override.yml` means changing them there too. Connector JARs are downloaded into `flink/lib/` by `download-flink-connectors.sh` (Flink minor version auto-detected from the running jobmanager; override with `FLINK_MINOR`).

### ELK layer

Kibana can only read Elasticsearch, so Elasticsearch holds a **derived, disposable copy** of `twin.*`. Nothing writes to it except Logstash, and losing the `esdata` volume loses nothing: Logstash rebuilds the window from Postgres. Postgres stays the system of record because pgRouting, the spatial seeding and `run_sim.py` all need SQL.

- **Bounded to 7 days.** ILM (`twin-retention`) deletes each daily `twin-<table>-YYYY.MM.dd` index at 7 days, and every JDBC query in [elk/logstash/pipeline/twin.conf](elk/logstash/pipeline/twin.conf) is floored at `now() - 7 days`, so a fresh Logstash never backfills more than ILM keeps. `twin-dma-latest` is a single current-state index with no retention. `twin.sim_result` is deliberately not mirrored because of its volume.
- **No duplicates.** Polls overlap (5 minutes, or 1 hour for `dma_balance`) to catch rows Flink commits late. That is safe only because each `doc_id` is the row's natural key, computed in the SQL, and `@timestamp` is the row's own time, so a re-read overwrites the same document in the same daily index. Keep both properties on any new input.
- **Adding a twin table** means adding a `jdbc` input with a natural-key `doc_id` and `[@metadata][table]`, plus a data view in [scripts/setup-elk.sh](scripts/setup-elk.sh). Mappings come from the `twin-mappings` component template (strings → keyword, numbers → double), not from Logstash.
- **Existing cluster or Weave's own.** [scripts/elk-up.sh](scripts/elk-up.sh) is the only supported way to start ELK. When `.env` sets `ELASTICSEARCH_URL` and it answers, only `elk-setup`, `logstash` and `filebeat` start, and they join `ELK_NETWORK` so the cluster's TLS hostname resolves as its certificate names it. When it's unset or not answering, the `local-elk` profile starts `elasticsearch` and `kibana`, and the script blanks the external settings for that run. A bare `docker compose up logstash` skips that decision and reads `.env` as-is. The probe runs in a container on the same network with the same CA and credentials, so TLS or auth failures stop the script instead of silently falling back.
- The saved JDBC positions belong to one target cluster. [elk/logstash/reset-on-target-change.sh](elk/logstash/reset-on-target-change.sh) is the image entrypoint: when `ES_URL` differs from the last run, it deletes the positions so the new cluster gets the full 7-day backfill instead of only rows newer than the switch.
- Logstash outputs always carry `user`/`password`/`ssl_certificate_authorities`. For the local cluster these are empty (security off), and the CA falls back to the system bundle, because the option is path-validated even though plain http ignores it.
- Unlike Flink SQL, Logstash does substitute `${ENV}`, so its Postgres credentials come from `docker-compose.override.yml`.
- Filebeat reads only containers with a compose project label, and it excludes the ELK services so their own failures can't loop back through them. Logs are kept for 7 days. It reads Docker's JSON log files, so every service in the overlay (upstream ones included) sets `logging: *json-logs` (json-file, 3 × 10 MB); an engine defaulting to the `local` driver writes a binary format Filebeat silently can't read. Give any new service the same anchor. Autodiscover conditions match label keys in their dotted form (`docker.container.labels.com.docker.compose.project`); the dedotted form silently matches nothing.
- Elasticsearch uses absolute disk watermarks (2gb / 1500mb / 1gb). On Docker Desktop its data path is the whole shared VM disk, and the default 90% watermark leaves every shard unassigned, so Kibana hangs at startup.

### Console

[console/](console/) is a FastAPI app serving one static page (no build step; Leaflet is vendored into the image at build time so the page makes no third-party requests). It shows pipeline state, the network map with DMA status, telemetry freshness, real feeds and flagged residuals, and runs the bootstrap steps.

- **It never writes itself.** Reads use read-only Postgres transactions plus the Connect, Flink and Kafka endpoints. Every operation is an allow-listed entry in `ACTIONS` whose `build_command()` is the exact documented command (`scripts/*.sh`, or `docker exec gis-loader|simulator|flink-sql-client ...`), so the console cannot drift from the CLI. Add an operation there, not as new logic.
- **Step status is derived from what each step leaves behind** (topics, row counts, `schemas/*.avsc`, connector state, `network.inp`, Flink jobs, latest residual), so steps run from the CLI show up too.
- **One job at a time**, because seeding truncates what loading and model building read. Jobs live in memory; killing the console doesn't stop a `docker exec` already started.
- **It holds the Docker socket**, so it binds to loopback, rejects other `Host` headers, and requires an `X-Weave-Console` header on POSTs so other sites in the browser can't trigger operations. Keep all three.
- Starting the Flink pipelines is refused while any Flink job runs: a second statement set writes every row twice, and the telemetry hypertables have no key to deduplicate on.
- **Basemaps** are a server-side catalogue (`BUILTIN_BASEMAPS` in `console/app.py`: OSM, CARTO, Esri, OpenTopoMap, an optional `CONSOLE_TILE_URL`, and Google via the official Map Tiles API when `GOOGLE_MAPS_API_KEY` and `GOOGLE_MAPS_REGION` are set). None is drawn unless `CONSOLE_BASEMAP` or the viewer picks one, because every tile request tells the provider which area is in view. Dark mode swaps CARTO to its dark variant and inverts other light tiles in CSS; imagery and Google tiles are never altered (Google's terms forbid it). Google attribution is fetched per viewport through `/api/basemaps/google-attribution`; the Google logo its terms also require is not drawn yet.
- **Theme** is Auto/Light/Dark from the header toggle (`data-theme` on `<html>`, remembered per browser). Every colour is a token defined on `:root` and redefined for dark under both `prefers-color-scheme` and `[data-theme="dark"]`; map overlays read tokens from `#map`, which carries its own when the basemap is dark.
- Static files are served `Cache-Control: no-cache`; there is no build step to fingerprint names, so without it browsers keep stale JS/CSS after a rebuild.

### GIS clients

Desktop GIS reaches the twin two ways, both as the read-only role `weave_reader` ([postgres/init/04_gis_reader.sql](postgres/init/04_gis_reader.sql): grants plus `default_transaction_read_only`, and default privileges so new tables are covered):

- **PostGIS direct** on `localhost:5432`, for SQL and the raw attributes.
- **OGC API Features** from `featureserv` (pg_featureserv) on `127.0.0.1:9000`, which publishes every spatial table the role can see. It returns no "next" link, so a client treats the first page as the whole layer: the QGIS connection file sets `pagesize` to `FEATURESERV_PAGE_SIZE` (50,000) and `PGFS_PAGING_LIMITMAX` must be at least that. A layer larger than that is silently truncated over OGC API; use PostGIS for it.
- `asset.dma_status` is a view joining DMA outlines to `twin.dma_latest`, because the status table has no geometry.
- The console's GIS clients tab serves QGIS connection files (`/api/gis/qgis/*.xml`, imported via Browser > right-click > Load Connections). Both formats were checked by importing them into QGIS 4.2.3 and loading every feature.
- `04_gis_reader.sql` runs only on an empty volume; apply it by hand to an existing one.

### Model layer

[model/run_sim.py](model/run_sim.py) builds the EPANET network from real geometry — reservoirs snapped to their nearest node, then pipes and the junctions they touch — but takes demands from synthetic meter readings. That split is deliberate.

- **What gets modelled.** [model/subgraph.py](model/subgraph.py) walks out from the reservoirs' nodes, in node-id then pipe-id order, keeping pipes until `SIM_MAX_PIPES`. Every kept pipe is connected to supply by construction, and the same graph always gives the same model; a plain `LIMIT` gave an arbitrary subset with unsupplied fragments. The build prints how many pipes were left out (over the cap, in components no reservoir feeds, zero-length after snapping) and exits with a diagnostic when there is no reservoir or no supplied pipe.
- **Element ids map back to assets.** Pipes are `P<pipe_id>`, reservoirs `R<facility_id>` with connector `L<facility_id>`, junctions `N<node_id>`, so `twin.sim_result` joins straight back to the asset tables. An id EPANET can't take (over 31 characters, or containing whitespace, `;` or `"`) becomes `<prefix>#` plus 16 hex of its SHA-1, recomputable with `element_name()`. Keep both properties when changing the build; per-pipe outputs depend on them.

Expect the first build to be messy: real GIS has fragments, missing diameters and no elevations, and the code compensates with `COALESCE` defaults (elevation 50 m, diameter 150 mm, reservoir head 80 m) and by skipping pipes that fail to add.

## Conventions

- Comments explain *why* a choice was made, not what the line does; keep that register. The prose in the docstrings and SQL headers is part of the deliverable.
- Loaders keep going on failure: a failed layer prints and continues, a failed poll logs and retries, a pipe that won't add is skipped. Don't convert these to hard exits.
- GIS attribute names are inconsistent across sources; resolve them through `attribute_hints` in the region config via `pick()`, never by hardcoding a column name.
- Branches are `<type>/hdh-<n>-<short-description>`, lowercase and hyphen-separated: `feature/` new capability, `enhancement/` improvement to existing behaviour, `fix/` defect, `chore/` maintenance, `docs/` documentation only. The Linear id stays in the name because that is how a pull request links to its ticket. [.githooks/pre-push](.githooks/pre-push) rejects pushes from any other branch name except `main`; enable it once per clone with `git config core.hooksPath .githooks`.
- [.github/workflows/ci.yml](.github/workflows/ci.yml) is the only gate before a Dependabot patch update merges itself, so when a component gains a module or a test, add its import or test there too. Keep `import wntr` explicit: the EPANET test skips rather than fails when wntr can't load.
- Weave services whose command is a plain process (Python, `sleep`, uvicorn) set `init: true`. As PID 1 they ignore SIGTERM and never reap orphans; a zombie in the console made `docker compose stop` fail and left Docker Desktop holding the stack's ports, so the next start hung until Docker Desktop was restarted.
- Topic names are `<domain>.<subject>.raw.v1`, created with explicit retention in [scripts/create-topics.sh](scripts/create-topics.sh).
- Treat this as critical-infrastructure data: public sources still reveal network vulnerability in aggregate, and the region config plus `.env` are the files that identify the target region.
