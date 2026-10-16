#!/usr/bin/env bash
set -euo pipefail
LIB="$(cd "$(dirname "${BASH_SOURCE[0]}")/../flink" && pwd)/lib"
mkdir -p "$LIB"
DETECTED=$(docker exec flink-jobmanager sh -c 'ls /opt/flink/lib/flink-dist*.jar 2>/dev/null' 2>/dev/null \
  | sed -E 's/.*flink-dist[-_]?(scala_[0-9.]+-)?([0-9]+\.[0-9]+).*/\2/' || true)
FLINK_MINOR=${FLINK_MINOR:-${DETECTED:-1.20}}
echo "Targeting Flink ${FLINK_MINOR} (override with FLINK_MINOR=...)"
KAFKA_CONNECTOR=${KAFKA_CONNECTOR:-3.4.0-${FLINK_MINOR}}
JDBC_CONNECTOR=${JDBC_CONNECTOR:-3.3.0-${FLINK_MINOR}}
PG_DRIVER=${PG_DRIVER:-42.7.4}
MVN=https://repo.maven.apache.org/maven2
get() { local out="$LIB/$(basename "$1")"
  [[ -f "$out" ]] && { echo "  have $(basename "$out")"; return; }
  echo "  fetching $(basename "$out")"
  curl -fsSL "$1" -o "$out" || { echo "  !! failed: $1"; rm -f "$out"; }; }
get "$MVN/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR}/flink-sql-connector-kafka-${KAFKA_CONNECTOR}.jar"
get "$MVN/org/apache/flink/flink-connector-jdbc/${JDBC_CONNECTOR}/flink-connector-jdbc-${JDBC_CONNECTOR}.jar"
get "$MVN/org/postgresql/postgresql/${PG_DRIVER}/postgresql-${PG_DRIVER}.jar"
ls -lh "$LIB"
