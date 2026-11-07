#!/usr/bin/env bash
# Seed synthetic telemetry only when it is missing or has gone stale.
#
# Checks the newest row in twin.meter_reading and twin.pressure_reading. A feed
# with no rows, or whose newest row is older than SEED_STALE_S, gets one bounded
# batch: meters through `synthetic_meters.py --once`, pressure through a bounded
# Datagen registration (DATAGEN_ITERATIONS / DATAGEN_MAX_INTERVAL).
#
# This is not the gap fill. ingest-backfill only fills a gap once a live row
# follows it, so it cannot help an empty table or a feed that stopped. Rows
# seeded here travel the live path (Kafka, Flink, Timescale), so Flink must be
# running first: its sources read latest-offset and earlier records are lost.
#
#   ./scripts/seed-if-needed.sh              # seed what is missing or stale
#   ./scripts/seed-if-needed.sh --check      # report only, change nothing
#   SEED_STALE_S=900 SEED_ITERATIONS=2000 ./scripts/seed-if-needed.sh
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

STALE_S=${SEED_STALE_S:-600}               # older than this counts as a gap
ITERATIONS=${SEED_ITERATIONS:-5000}        # pressure records per batch
MAX_INTERVAL=${SEED_MAX_INTERVAL:-20}      # ms between pressure records
FLINK_URL=${FLINK_URL:-http://localhost:9081}
CHECK_ONLY=0
[[ "${1:-}" == "--check" ]] && CHECK_ONLY=1

for v in STALE_S ITERATIONS MAX_INTERVAL; do
  [[ "${!v}" =~ ^[1-9][0-9]*$ ]] || { echo "! $v must be a positive integer (got '${!v}')"; exit 1; }
done

# Seconds since the newest row, or "none" when the table is empty.
age_of() {
  docker exec weave-postgres psql -U weave -d weave_twin -Atc \
    "SELECT COALESCE(EXTRACT(EPOCH FROM now() - max(ts))::bigint::text, 'none') FROM twin.$1"
}

needs_seed() { # $1 = table; prints the reason and returns 0 when seeding is due
  local age; age=$(age_of "$1")
  if [[ "$age" == "none" ]]; then echo "$1: empty"; return 0; fi
  if (( age > STALE_S )); then echo "$1: newest row ${age}s old (limit ${STALE_S}s)"; return 0; fi
  echo "$1: fresh (${age}s)" >&2; return 1
}

flink_running() {
  curl -s "$FLINK_URL/jobs/overview" | grep -q '"state":"RUNNING"'
}

seed_meters=0; seed_pressure=0
if reason=$(needs_seed meter_reading); then echo "-> $reason"; seed_meters=1; fi
if reason=$(needs_seed pressure_reading); then echo "-> $reason"; seed_pressure=1; fi

if (( !seed_meters && !seed_pressure )); then echo "Nothing to seed."; exit 0; fi
if (( CHECK_ONLY )); then echo "Check only: seeding would run."; exit 0; fi

if ! flink_running; then
  echo "! no Flink job is running at $FLINK_URL; seeded records would never reach twin.*"
  echo "  start the pipelines first (see CLAUDE.md: SOURCE '/sql/03_pipelines.sql')"
  exit 1
fi

if (( seed_meters )); then
  docker exec ingest-meters python synthetic_meters.py --once
fi
if (( seed_pressure )); then
  DATAGEN_ITERATIONS=$ITERATIONS DATAGEN_MAX_INTERVAL=$MAX_INTERVAL ./scripts/register-connectors.sh
fi
echo "Seed triggered. Rows reach twin.* once Flink commits; a short batch may not close the"
echo "15-minute dma_balance window until a later event arrives."
