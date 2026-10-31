#!/usr/bin/env bash
set -euo pipefail
LIB="$(cd "$(dirname "${BASH_SOURCE[0]}")/../flink" && pwd)/lib"
mkdir -p "$LIB"
DETECTED=$(docker exec flink-jobmanager sh -c 'ls /opt/flink/lib/flink-dist*.jar 2>/dev/null' 2>/dev/null \
  | sed -E 's/.*flink-dist[-_]?(scala_[0-9.]+-)?([0-9]+\.[0-9]+).*/\2/' || true)
# This runs before the first `compose up`, when there is no jobmanager to
# ask, so fall back to the image tag in Confluent's compose file.
COMPOSE_FILE_BASE="$(dirname "$LIB")/../docker-compose.yml"
if [[ -z $DETECTED && -f $COMPOSE_FILE_BASE ]]; then
  DETECTED=$(sed -nE 's/^[[:space:]]*image:[[:space:]]*[^[:space:]]*flink[^:]*:([0-9]+\.[0-9]+).*/\1/p' \
    "$COMPOSE_FILE_BASE" | head -n 1)
fi
FLINK_MINOR=${FLINK_MINOR:-${DETECTED:-1.20}}
echo "Targeting Flink ${FLINK_MINOR} (override with FLINK_MINOR=...)"
# Connector releases are built per Flink minor; 1.19 stops at the older lines.
case $FLINK_MINOR in
  1.19) KAFKA_DEFAULT=3.3.0; JDBC_DEFAULT=3.2.0 ;;
  *)    KAFKA_DEFAULT=3.4.0; JDBC_DEFAULT=3.3.0 ;;
esac
KAFKA_CONNECTOR=${KAFKA_CONNECTOR:-${KAFKA_DEFAULT}-${FLINK_MINOR}}
JDBC_CONNECTOR=${JDBC_CONNECTOR:-${JDBC_DEFAULT}-${FLINK_MINOR}}
# Some Flink images already bundle the Kafka connector, and a second copy
# makes the 'kafka' table factory ambiguous, so only fetch it when absent.
BUNDLED_KAFKA=$(docker exec flink-jobmanager sh -c 'ls /opt/flink/lib/flink-sql-connector-kafka*.jar 2>/dev/null' 2>/dev/null || true)
# Without a running jobmanager, Confluent's flink-kafka images are known to
# bundle the connector; their name says so.
if [[ -z $BUNDLED_KAFKA && -f $COMPOSE_FILE_BASE ]] \
    && grep -Eq '^[[:space:]]*image:[[:space:]]*[^[:space:]]*flink-kafka:' "$COMPOSE_FILE_BASE"; then
  BUNDLED_KAFKA="the flink-kafka image's Kafka connector"
fi
PG_DRIVER=${PG_DRIVER:-42.7.4}
MVN=https://repo.maven.apache.org/maven2
get() { local out="$LIB/$(basename "$1")"
  [[ -f "$out" ]] && { echo "  have $(basename "$out")"; return; }
  echo "  fetching $(basename "$out")"
  curl -fsSL "$1" -o "$out" || { echo "  !! failed: $1"; rm -f "$out"; }; }
if [[ -n $BUNDLED_KAFKA ]]; then
  echo "  image bundles $(basename "$BUNDLED_KAFKA") — not fetching another"
else
  get "$MVN/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR}/flink-sql-connector-kafka-${KAFKA_CONNECTOR}.jar"
fi
get "$MVN/org/apache/flink/flink-connector-jdbc/${JDBC_CONNECTOR}/flink-connector-jdbc-${JDBC_CONNECTOR}.jar"
get "$MVN/org/postgresql/postgresql/${PG_DRIVER}/postgresql-${PG_DRIVER}.jar"
ls -lh "$LIB"
