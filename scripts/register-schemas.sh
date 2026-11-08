#!/usr/bin/env bash
# Register each topic's contract (kafka/contracts/<topic>.json) in Schema
# Registry under <topic>-value, with schema evolution on.
#
# The topics stay plain JSON on the wire: Flink SQL's json format can't read
# the registry's framing (a magic byte and schema id before each message),
# and every producer, Datagen included, would have to change with it. So the
# registry holds the contract and checks each change to it, and the console
# compares live messages with the registered version and warns on the
# Alerts tab. Nothing is rejected at the broker.
#
# SCHEMA_COMPATIBILITY (default BACKWARD) is set on each subject before the
# contract is posted, so a change the registry finds incompatible is refused
# and reported here, and the console flags that the repository's contract
# differs from the registered one. Posting an unchanged contract returns the
# existing version, so re-running is safe.
#
#   ./scripts/register-schemas.sh
#   SCHEMA_COMPATIBILITY=FULL ./scripts/register-schemas.sh
set -euo pipefail
SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL:-http://localhost:8081}
SCHEMA_COMPATIBILITY=${SCHEMA_COMPATIBILITY:-BACKWARD}
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../kafka/contracts" && pwd)"
CT="Content-Type: application/vnd.schemaregistry.v1+json"

case "$SCHEMA_COMPATIBILITY" in
  BACKWARD|BACKWARD_TRANSITIVE|FORWARD|FORWARD_TRANSITIVE|FULL|FULL_TRANSITIVE|NONE) ;;
  *) echo "! SCHEMA_COMPATIBILITY must be a Schema Registry compatibility level (got '$SCHEMA_COMPATIBILITY')"; exit 1 ;;
esac

# Bounded, unlike the Connect wait in register-connectors.sh: the console
# runs this as a job and holds its one-job lock until the script ends.
echo "Waiting for Schema Registry at $SCHEMA_REGISTRY_URL ..."
for _ in $(seq 1 20); do
  curl -s -o /dev/null "$SCHEMA_REGISTRY_URL/subjects" && break
  sleep 3
done
if ! curl -s -o /dev/null "$SCHEMA_REGISTRY_URL/subjects"; then
  echo "! Schema Registry is not answering at $SCHEMA_REGISTRY_URL"
  exit 1
fi

refused=0
for f in "$DIR"/*.json; do
  topic=$(basename "$f" .json)
  subject="$topic-value"
  curl -s -X PUT -H "$CT" --data "{\"compatibility\":\"$SCHEMA_COMPATIBILITY\"}" \
    "$SCHEMA_REGISTRY_URL/config/$subject" >/dev/null
  body=$(python3 -c 'import json,sys; print(json.dumps({"schemaType": "JSON", "schema": open(sys.argv[1]).read()}))' "$f")
  out=$(curl -s -w '\n%{http_code}' -X POST -H "$CT" --data "$body" "$SCHEMA_REGISTRY_URL/subjects/$subject/versions")
  code=${out##*$'\n'}
  resp=${out%$'\n'*}
  if [[ $code == 200 ]]; then
    version=$(curl -s "$SCHEMA_REGISTRY_URL/subjects/$subject/versions/latest" \
      | python3 -c 'import json,sys; print(json.load(sys.stdin).get("version", "?"))')
    echo "-> $subject  version $version ($SCHEMA_COMPATIBILITY)"
  elif [[ $code == 409 ]]; then
    # Loaders keep going: one refused contract shouldn't stop the others.
    echo "! $subject  refused as incompatible under $SCHEMA_COMPATIBILITY; the registered version stays"
    echo "  $resp"
    refused=$((refused + 1))
  else
    echo "! $subject  HTTP $code: $resp"
    refused=$((refused + 1))
  fi
done
exit $((refused > 0))
