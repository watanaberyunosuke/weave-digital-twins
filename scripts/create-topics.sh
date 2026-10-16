#!/usr/bin/env bash
set -euo pipefail
BROKER=${BROKER:-broker:29092}
create() {
  docker exec broker kafka-topics --bootstrap-server "$BROKER" \
    --create --if-not-exists --topic "$1" --partitions "${2:-1}" \
    --replication-factor 1 --config retention.ms="$3" >/dev/null
  echo "  $1"
}
echo "Creating topics ..."
# real
create water.hydrometry.raw.v1          1 2592000000   # 30 days
create electricity.market-dispatch.raw.v1 1 2592000000 # 30 days
create transport.vehicle-positions.raw.v1 1 172800000  # 48 hours
# synthetic
create water.meter-readings.raw.v1      1 604800000    # 7 days
create water.pressure-readings.raw.v1   1 604800000    # 7 days
create electricity.meter-interval.raw.v1 1 604800000   # 7 days
