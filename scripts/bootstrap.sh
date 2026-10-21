#!/usr/bin/env bash
# End-to-end bootstrap. Assumes docker compose is already up.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

if [[ ! -f .env ]]; then
  echo "! no .env — cp .env.example .env and set your region first"
  exit 1
fi

echo "== 1. topics =="
./scripts/create-topics.sh

echo "== 2. load real GIS network =="
docker exec gis-loader python gis/load_gis.py --all --build-topology
docker exec gis-loader python gis/load_elevation.py

echo "== 3. derive synthetic DMAs + meters from real network =="
docker exec gis-loader python scripts/seed_synthetic_network.py \
  --dmas 40 --meters-per-dma 25

echo "== 4. start synthetic generators =="
./scripts/register-connectors.sh

echo "== 5. build the EPANET model =="
docker exec simulator python run_sim.py --build

echo "== 6. ELK read copy (existing cluster, or Weave's own) =="
./scripts/elk-up.sh

cat <<'MSG'

Done. Next:
  docker exec -it flink-sql-client sql-client.sh
    SOURCE '/sql/01_sources.sql';
    SOURCE '/sql/02_sinks.sql';
    SOURCE '/sql/03_pipelines.sql';

  Kibana:   http://localhost:5601
  Flink:    http://localhost:9081
MSG
