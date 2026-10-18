#!/usr/bin/env bash
# Register the synthetic-feed Datagen connectors.
# Requires schemas/ to exist — run seed_synthetic_network.py first.
#
# Streaming is the default: the connector files ask for ~100M iterations, which
# is effectively endless. For a bounded batch, override per run (env vars, not
# edits to connectors/*.json, so the committed configs stay the streaming ones):
#   DATAGEN_ITERATIONS=10000 DATAGEN_MAX_INTERVAL=10 ./scripts/register-connectors.sh
# Start the Flink pipelines first: its sources read latest-offset, so records
# produced before the job starts are never seen.
set -euo pipefail
CONNECT_URL=${CONNECT_URL:-http://localhost:8083}
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../connectors" && pwd)"
SCHEMAS="$(cd "$(dirname "${BASH_SOURCE[0]}")/../schemas" && pwd)"

# Empty means "keep the connector file's own value".
DATAGEN_ITERATIONS=${DATAGEN_ITERATIONS:-}
DATAGEN_MAX_INTERVAL=${DATAGEN_MAX_INTERVAL:-}
for v in DATAGEN_ITERATIONS DATAGEN_MAX_INTERVAL; do
  if [[ -n "${!v}" && ! "${!v}" =~ ^[1-9][0-9]*$ ]]; then
    echo "! $v must be a positive integer (got '${!v}')"; exit 1
  fi
done

if ! ls "$SCHEMAS"/*.avsc >/dev/null 2>&1; then
  echo "! no schemas found in $SCHEMAS"
  echo "  run: docker exec gis-loader python scripts/seed_synthetic_network.py"
  exit 1
fi

echo "Waiting for Connect at $CONNECT_URL ..."
until curl -s -o /dev/null "$CONNECT_URL/connectors"; do sleep 3; done

for f in "$DIR"/datagen-*.json; do
  name=$(python3 -c "import json;print(json.load(open('$f'))['name'])")
  schema=$(python3 -c "import json,os;print(os.path.basename(json.load(open('$f'))['config']['schema.filename']))")
  if [[ ! -f "$SCHEMAS/$schema" ]]; then
    echo "-> $name  SKIPPED (no $schema)"; continue
  fi
  curl -s -X DELETE "$CONNECT_URL/connectors/$name" >/dev/null || true
  body=$(python3 - "$f" "$DATAGEN_ITERATIONS" "$DATAGEN_MAX_INTERVAL" <<'PY'
import json, sys
cfg = json.load(open(sys.argv[1]))
if sys.argv[2]:
    cfg["config"]["iterations"] = int(sys.argv[2])
if sys.argv[3]:
    cfg["config"]["max.interval"] = int(sys.argv[3])
print(json.dumps(cfg))
PY
)
  code=$(curl -s -o /tmp/r.json -w '%{http_code}' -X POST "$CONNECT_URL/connectors" \
         -H 'Content-Type: application/json' --data "$body")
  [[ "$code" == "201" ]] && echo "-> $name  registered" \
    || { echo "-> $name  FAILED ($code)"; cat /tmp/r.json; echo; }
done
