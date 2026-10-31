# Weave — utility network digital twin framework

A framework for building digital twins of water, electricity and transport networks using public network topology data and synthetic telemetry to fill gaps where real meter data isn't publicly available.

**The core principle:** Never fake the network. Use real public GIS layers for topology (pipes, mains, substations, transit stops), and generate meters *onto* that real geometry rather than floating them at random coordinates.

## Why it works

Most utilities publish their bulk-supply network and major assets as open GIS: water authority pipes and storage dams, OpenStreetMap's transmission lines and substations, transport authorities' GTFS feeds. What stays closed is distribution reticulation and meter data — the retail domain, which is commercial and privacy-sensitive.

Weave takes the real network and generates plausible meter readings *anchored to it*: clustering real pipes into synthetic District Metered Areas, placing meters along real pipe geometry, then deriving Datagen schemas whose ID fields are drawn from the ids just created. The result is coherent synthetic telemetry that can be spatially joined back to real assets and used to test the whole detection and simulation machinery end to end.

## What you get

- **Asset graph:** Real network topology in PostGIS with pgRouting for connectivity queries
- **Telemetry ingestion:** Real feeds (AEMO, WMIS, BoM, GTFS-RT, etc.) and synthetic feeds (Datagen) in one Kafka/Flink pipeline
- **Network model:** EPANET hydraulic solver (water) or pandapower (electricity) running on a schedule against live telemetry
- **Anomaly detection:** Residual-based leak/burst/outage alerts by comparing modelled to observed
- **What-if scenarios:** Run the model against changed conditions to plan interventions

## Customizing for your region

No place name appears anywhere in the code, the schema, the SQL or the
container definitions. A region lives in exactly two files:

1. **`gis/regions/<your-region>.yml`** — network layers, their catalogue, and the
   bounding box to load. Copy `gis/regions/TEMPLATE.yml` to start;
   `melbourne-water.example.yml` is a filled-in one to read.
2. **`.env`** — real-feed endpoints and the gauge/market ids to poll. Copy
   `.env.example`.

The pollers under `ingest/` are adapters for a *shape* of API (a Kisters
hydrometry webservice, a wholesale market summary report) rather than for a
place — endpoints and ids are env config. If your region publishes something
structurally different, the adapter is the only thing you rewrite.

Everything else — Kafka topics, Flink SQL, schema, network model building,
residual calculation — stays the same.

## Quick start

Needs Docker Compose 2.24.4 or later. Every port is published on
`127.0.0.1` only, because none of the Kafka, Connect, Flink or monitoring
endpoints asks for credentials; from another machine, use an SSH tunnel
(`ssh -L 9021:localhost:9021 <host>`).

```bash
# Take Confluent's compose stack into weave-stack, then overlay Weave
git clone --depth 1 -b 8.2.0-post https://github.com/confluentinc/cp-all-in-one.git
cp -R cp-all-in-one/cp-all-in-one weave-stack
git clone https://github.com/watanaberyunosuke/weave-digital-twins.git
# rsync rather than cp -r weave/*, which skips dotfiles such as .env.example.
# .env is excluded so overlaying from a working copy never carries its
# credentials across, and re-running the overlay keeps the stack's own .env.
rsync -a --exclude .git --exclude .env weave-digital-twins/ weave-stack/
cd weave-stack

# Point it at a region
cp .env.example .env          # then edit: endpoints, ids, GIS_REGION_CONFIG
./scripts/init-secrets.sh     # console tokens; asks for the admin password

# Download Flink connectors
./scripts/download-flink-connectors.sh

# Start the stack
docker compose up -d broker connect control-center flink-jobmanager \
    flink-taskmanager flink-sql-client postgres ingest-market \
    ingest-hydrometry gis-loader simulator console featureserv

# Bootstrap (load GIS, derive synthetic assets, start generators, bring up ELK)
./scripts/bootstrap.sh

# Start Flink SQL pipelines
docker exec -it flink-sql-client sql-client.sh
  SOURCE '/sql/01_sources.sql';
  SOURCE '/sql/02_sinks.sql';
  SOURCE '/sql/03_pipelines.sql';
```

Visit the console at `http://localhost:8090`, Kibana at `http://localhost:5601`, Control Center at `http://localhost:9021` and Flink at `http://localhost:9081`.

**Control Center** manages Kafka: the broker, topics, consumer lag, the
Datagen connectors and Schema Registry subjects. It does not show Flink,
which needs Confluent Manager for Apache Flink on Kubernetes; manage Flink
jobs in the Flink UI. Control Center and Confluent Server are under the
Confluent Enterprise License: without a key they run a 30-day trial, and a
free developer licence covers a single-broker cluster such as this one
indefinitely ([licences](https://docs.confluent.io/platform/current/installation/license.html)).

The console shows the twin on a map (DMAs coloured by status, real pipes,
facilities and gauges), telemetry freshness with each table labelled real or
synthetic, the real feeds, and flagged residuals. Its pipeline panel runs the
same steps as `bootstrap.sh`, one at a time, showing the exact command and its
output, so you can also bootstrap from there instead of the terminal.

**Public console.** `docker compose --profile public up -d console-public`
serves the same page read-only on `:8091`, for publishing. Its demo login,
open to everyone with the password `demo`, runs the safe steps for real and
simulates the rest. The admin login on the same page runs every step live
once `./scripts/init-secrets.sh` has filled the console secrets into `.env`
(it asks for the admin password and keeps only its hash).

The map has a basemap picker (OpenStreetMap, CARTO, Esri imagery and
topographic, OpenTopoMap, and Google Maps when `.env` holds a Map Tiles API
key) and a light/dark theme toggle. No basemap is drawn until you pick one,
because tile requests tell the provider which area you are looking at.

**Desktop GIS.** QGIS, ArcGIS Pro and GDAL can open the twin's layers
read-only, either straight from PostGIS (`localhost:5432`, user
`weave_reader`) or over OGC API Features at `http://localhost:9000`. The
console's *GIS clients* tab has the details, copy buttons and QGIS
connection files to import.


Kibana reads Elasticsearch, not Postgres, so Logstash mirrors the `twin.*`
tables into it every 15 seconds and Filebeat ships the containers' logs
alongside. That copy is bounded to the last 7 days; Postgres remains the
system of record, and the full history lives there.

`scripts/elk-up.sh` (run by bootstrap) reuses an existing Elasticsearch and
Kibana when `.env` points at one that answers, and otherwise starts Weave's
own. If it finds a cluster already running that `.env` doesn't mention, it
stops and prints the lines to add.

## Real/synthetic split

Every table and message carries `is_synthetic` so you never accidentally mix them:

| Layer | Real (public) | Synthetic (generated) |
|---|---|---|
| Network topology | GIS mains, substations, transit stops | — |
| Telemetry | Your region's public feeds | Smart meters, pressure loggers |

Synthetic data is *coherent*: every meter ID exists, every meter sits on real geometry, every zone is derived from real assets.

## Key components

**`gis/regions/*.yml`** — The only files that know where the twin is: layers, catalogue, extent

**`gis/load_gis.py`** — Resolves public data catalogue URLs at runtime, pages features into PostGIS, builds pgRouting topology

**`scripts/seed_synthetic_network.py`** — Clusters real network into zones, places meters on real geometry, generates Datagen schemas

**`ingest/*.py`** — Real-feed adapters. Endpoints and ids come from `.env`; swap the parser if your region's API has a different shape.

**`model/run_sim.py`** — Builds network model from PostGIS, runs solves on a schedule, computes residuals

**`flink/sql/*.sql`** — ETL. Real and synthetic flows merge transparently; downstream analysis knows which is which.

**`console/`** — Web page for viewing the twin and running the pipeline steps; operations are the documented commands, nothing more

**`elk/`** — Logstash pipelines that mirror Postgres into Elasticsearch for Kibana, and the Filebeat config for container logs. `scripts/setup-elk.sh` sets the 7-day retention, the mappings and the Kibana data views.

## Extending it

**New region:** Copy `gis/regions/TEMPLATE.yml`, fill in your network's public data sources, set the feed endpoints in `.env`, run bootstrap. No code change.

**New utility:** Add schema tables, write a poller, add Flink pipeline, wire into the simulator if there's a model for it.

**Swap the model:** EPANET for water is the default. Use OpenDSS or pandapower for electricity, SUMO for transport. The schema is model-agnostic.

## Contributing

Work is tracked in Linear, one branch per issue, named
`<type>/hdh-<n>-<short-description>` where type is `feature`, `enhancement`,
`fix`, `chore` or `docs` (for example `fix/hdh-79-deterministic-supplied-subgraph`).
Enable the hook that checks branch names before a push:

```bash
git config core.hooksPath .githooks
```

Agent sessions are recorded with [Entire](https://entire.io), which links each Claude Code or
Cursor session to the commits it produced. `.entire/settings.json` enables it, the agent hooks
are in `.claude/settings.json` and `.cursor/hooks.json`, and its git hooks are in `.githooks/`
(the branch-name check is `pre-push.pre-entire` and runs after Entire's `pre-push`). Install the
CLI (`brew install entireio/tap/entire` or `curl -fsSL https://entire.io/install.sh | bash`) and
run `entire enable` once per clone; `entire status` shows the state, and `entire checkpoint` and
`entire search` browse past sessions. Every hook does nothing if the CLI is not installed.

The repository is public, so `push_sessions` is `false` and telemetry is off: checkpoints stay
as git refs in your clone and are never pushed here. To share them, send them to a separate
private repository with `entire enable --checkpoint-remote github:<owner>/<private-repo>`.

CI ([.github/workflows/ci.yml](.github/workflows/ci.yml)) runs the syntax checks, installs each
component's `requirements.txt` on Python 3.12 and imports its code. Dependabot patch updates
merge themselves once CI passes; minor and major updates wait for review.

Dependencies are pinned in each component's `requirements.in`. The `requirements.txt` beside
it is a hash-locked file generated from it, covering transitive packages too, and the images
and CI install it with `--require-hashes`. After editing a `requirements.in`, regenerate the
locks with [scripts/lock-requirements.sh](scripts/lock-requirements.sh) (its header has the
`docker run` line); CI fails if a lock no longer matches its input.

## Known limits

- **Topology build is slow** on large networks; use `--limit` to test first
- **First model build won't be clean** — real GIS has fragments, missing attributes, no elevations; expect iteration
- **This is critical-infrastructure data** — even public sources reveal network vulnerability in aggregate; decide where it runs, and note that your region config and `.env` are the files that identify it

## References

- [WNTR — Water Network Tool for Resilience](https://wntr.readthedocs.io/)
- [pgRouting — shortest path, service areas](https://pgrouting.org/)
- [Confluent Datagen](https://docs.confluent.io/kafka-connect-datagen/current/overview.html)
- [Flink SQL CDC](https://nightlies.apache.org/flink/flink-cdc-docs-master/)

License: Open source. Check each data source's own license before operational use.
