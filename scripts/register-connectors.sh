#!/usr/bin/env bash
# Register the synthetic-feed Datagen connectors.
# Requires schemas/ to exist — run seed_synthetic_network.py first.
set -euo pipefail
CONNECT_URL=${CONNECT_URL:-http://localhost:8083}
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../connectors" && pwd)"
SCHEMAS="$(cd "$(dirname "${BASH_SOURCE[0]}")/../schemas" && pwd)"

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
  code=$(curl -s -o /tmp/r.json -w '%{http_code}' -X POST "$CONNECT_URL/connectors" \
         -H 'Content-Type: application/json' --data @"$f")
  [[ "$code" == "201" ]] && echo "-> $name  registered" \
    || { echo "-> $name  FAILED ($code)"; cat /tmp/r.json; echo; }
done
