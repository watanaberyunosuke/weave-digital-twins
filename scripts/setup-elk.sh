#!/bin/sh
# ILM, index templates and Kibana data views for the ELK read copy.
# Runs as the one-shot elk-setup service before Logstash starts (POSIX sh,
# because that image has no bash). Idempotent — safe to re-run from the host:
#   ES_URL=... ES_USER=... ES_PASSWORD=... ES_CA=... ./scripts/setup-elk.sh
# ES_USER/ES_CA are empty for the local cluster; an existing cluster needs a
# user that may manage ILM, templates and Kibana saved objects.
set -eu
ES=${ES_URL:-http://localhost:9200}
KB=${KIBANA_URL:-}
ES=${ES%/} KB=${KB%/}   # paths below start with /, and ES rejects //_ilm

req() {  # curl with the cluster's credentials and CA, failing on HTTP errors
  if [ -n "${ES_USER:-}" ]; then set -- -u "$ES_USER:${ES_PASSWORD:-}" "$@"; fi
  if [ -n "${ES_CA:-}" ]; then set -- --cacert "$ES_CA" "$@"; fi
  curl -sS --fail-with-body -o /dev/null "$@"
}

es_put() {  # path, JSON body on stdin
  req -X PUT "$ES$1" -H 'Content-Type: application/json' --data-binary @-
  echo "  PUT $1"
}

echo "== ILM: the ES copy is bounded; Postgres keeps the history =="
for policy in twin-retention logs-retention; do
  es_put "/_ilm/policy/$policy" <<'JSON'
{"policy": {"phases": {
  "hot":    {"actions": {}},
  "delete": {"min_age": "7d", "actions": {"delete": {}}}
}}}
JSON
done

echo "== index templates =="
# Zero replicas: the copy is disposable (rebuilt from Postgres), and a
# replica on a single-node cluster would only hold every index at yellow.
# Twin fields are ids, statuses and measurements — never free text — so
# strings map to keyword. Numbers map to double because the first reading of
# a series can be a whole number, and a guessed long would truncate every
# later decimal.
es_put /_component_template/twin-mappings <<'JSON'
{"template": {
  "settings": {"number_of_replicas": 0},
  "mappings": {
    "dynamic_templates": [
      {"strings_as_keyword": {"match_mapping_type": "string", "mapping": {"type": "keyword"}}},
      {"longs_as_double":    {"match_mapping_type": "long",   "mapping": {"type": "double"}}},
      {"doubles_as_double":  {"match_mapping_type": "double", "mapping": {"type": "double"}}}
    ],
    "properties": {
      "@timestamp":   {"type": "date"},
      "ts":           {"type": "date"},
      "window_start": {"type": "date"},
      "is_synthetic": {"type": "boolean"},
      "flagged":      {"type": "boolean"},
      "geom":         {"type": "geo_shape", "ignore_malformed": true}
    }
  }
}}
JSON

es_put /_index_template/twin <<'JSON'
{"index_patterns": ["twin-*"], "priority": 200,
 "composed_of": ["twin-mappings"],
 "template": {"settings": {"index.lifecycle.name": "twin-retention"}}}
JSON

# Current state, overwritten in place — it must not age out with the
# time-series indices, so it gets its own higher-priority template without ILM.
es_put /_index_template/twin-dma-latest <<'JSON'
{"index_patterns": ["twin-dma-latest"], "priority": 300,
 "composed_of": ["twin-mappings"]}
JSON

es_put /_index_template/weave-logs <<'JSON'
{"index_patterns": ["weave-logs-*"], "priority": 200,
 "template": {"settings": {"number_of_replicas": 0,
                           "index.lifecycle.name": "logs-retention"}}}
JSON

echo "== Kibana data views =="
if [ -z "$KB" ]; then
  echo "  skipped: no KIBANA_URL — the indices are ready for any Kibana pointed at $ES"
  exit 0
fi
data_view() {  # id, index pattern, display name
  req -X POST "$KB/api/data_views/data_view" \
       -H 'kbn-xsrf: true' -H 'Content-Type: application/json' \
       --data-binary "{\"override\": true, \"data_view\": {\"id\": \"$1\", \"title\": \"$2\", \"name\": \"$3\", \"timeFieldName\": \"@timestamp\"}}"
  echo "  $1"
}
data_view twin-hydrometry           'twin-hydrometry-*'           'Hydrometry (real)'
data_view twin-market-dispatch      'twin-market-dispatch-*'      'Market dispatch (real)'
data_view twin-meter-reading        'twin-meter-reading-*'        'Water meters (synthetic)'
data_view twin-pressure-reading     'twin-pressure-reading-*'     'Pressure loggers (synthetic)'
data_view twin-electricity-interval 'twin-electricity-interval-*' 'Electricity meters (synthetic)'
data_view twin-dma-balance          'twin-dma-balance-*'          'DMA balance'
data_view twin-sim-residual         'twin-sim-residual-*'         'Model residuals'
data_view twin-dma-latest           'twin-dma-latest'             'DMA status (current)'
data_view weave-logs                'weave-logs-*'                'Container logs'
