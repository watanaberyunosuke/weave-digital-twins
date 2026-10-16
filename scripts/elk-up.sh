#!/usr/bin/env bash
# Bring up the ELK read copy. Uses an existing Elasticsearch when one answers
# at ELASTICSEARCH_URL (.env); otherwise starts Weave's own via the local-elk
# profile. The choice is made fresh on every run, so re-running is safe.
#
# The probe runs from a container on ELK_NETWORK with the configured CA and
# credentials — the exact path Logstash will take — so "reachable" here means
# reachable for the pipeline, not merely from the host.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
COMPOSE=${COMPOSE:-docker compose}   # overridable, e.g. to target a test project
CURL_IMAGE=curlimages/curl:8.16.0

env_get() {  # shell environment first, then .env — the same precedence compose uses
  local v="${!1:-}"
  if [[ -z $v && -f .env ]]; then
    v=$(sed -n "s/^$1=//p" .env | tail -1 | sed -E "s/^\"(.*)\"$/\1/; s/^'(.*)'$/\1/")
  fi
  printf '%s' "$v"
}

ES_URL=$(env_get ELASTICSEARCH_URL)
NET=$(env_get ELK_NETWORK)
USER_=$(env_get ELASTICSEARCH_USERNAME)
PASS=$(env_get ELASTICSEARCH_PASSWORD)
CA=$(env_get ELASTICSEARCH_CA)

probe() {  # prints "<http code> <curl exit code>"
  docker network inspect "$NET" >/dev/null 2>&1 || { echo "000 7"; return; }
  local args=(-s -o /dev/null -w '%{http_code} %{exitcode}' --max-time 5)
  [[ -n $USER_ ]] && args+=(-u "$USER_:$PASS")
  [[ -n $CA ]] && args+=(--cacert "/certs/$CA")
  docker run --rm --network "$NET" -v "$PWD/elk/certs:/certs:ro" "$CURL_IMAGE" \
    "${args[@]}" "$ES_URL" || true
}

# Something already on :9200 that isn't ours is almost certainly a cluster the
# user meant to reuse; starting a second one would clash on the port and
# split the data, so explain how to point at it instead.
explain_existing() {
  local c=$1 net host scheme kb
  net=$(docker inspect "$c" --format '{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}' | awk '{print $1}')
  host=$(docker inspect "$c" --format '{{.Config.Hostname}}')
  scheme=http
  curl -sk -o /dev/null --max-time 3 https://localhost:9200 && scheme=https
  kb=$(docker ps --filter "network=$net" --filter publish=5601 --format '{{.Names}}' | head -1)
  if [[ -n $ES_URL ]]; then
    echo "! and Elasticsearch is already running here (container '$c'), so Weave's own"
    echo "  would clash with it on :9200. To use that one instead, set in .env:"
  else
    echo "! Elasticsearch is already running here (container '$c'), but .env has no"
    echo "  ELASTICSEARCH_URL, so Weave can't use it. To reuse it, set in .env:"
  fi
  cat <<MSG

    ELK_NETWORK=$net
    ELASTICSEARCH_URL=$scheme://$host:9200
    ELASTICSEARCH_USERNAME=elastic
    ELASTICSEARCH_PASSWORD=<its password>
MSG
  [[ $scheme == https ]] && echo "    ELASTICSEARCH_CA=http_ca.crt"
  [[ -n $kb ]] && echo "    KIBANA_URL=http://$kb:5601"
  if [[ $scheme == https ]]; then
    cat <<MSG

  and copy its CA (the hostname above is the one its certificate names):

    docker cp $c:/usr/share/elasticsearch/config/certs/http_ca.crt elk/certs/
MSG
  fi
  echo
  echo "  Then re-run ./scripts/elk-up.sh."
}

mode=local
if [[ -n $ES_URL ]]; then
  [[ -n $NET ]] || { echo "! ELASTICSEARCH_URL is set but ELK_NETWORK is not — set the Docker network the cluster is on"; exit 1; }
  read -r code rc < <(probe) || true
  code=${code:-000} rc=${rc:-7}
  case "$code/$rc" in
    200/*) mode=existing ;;
    401/*|403/*) echo "! $ES_URL rejected the credentials ($code) — check ELASTICSEARCH_USERNAME/PASSWORD"; exit 1 ;;
    000/6|000/7|000/28)
      echo "! nothing answering at $ES_URL on network $NET — starting Weave's own cluster instead" ;;
    000/*) echo "! could not connect to $ES_URL (curl exit $rc) — a TLS problem? check ELASTICSEARCH_CA and that the URL's host is one the certificate names"; exit 1 ;;
    *) echo "! $ES_URL answered HTTP $code"; exit 1 ;;
  esac
fi

if [[ $mode == existing ]]; then
  echo "== ELK: using existing cluster at $ES_URL (network $NET) =="
  $COMPOSE up -d elk-setup logstash filebeat
else
  holder=$(docker ps --filter publish=9200 --format '{{.Names}}' | grep -vx elasticsearch | head -1 || true)
  if [[ -n $holder ]]; then explain_existing "$holder"; exit 1; fi
  echo "== ELK: starting Weave's own Elasticsearch + Kibana (local-elk) =="
  # Blank the external settings for this run so a configured-but-down cluster
  # doesn't leak into the local one's connection.
  ELASTICSEARCH_URL= ELASTICSEARCH_USERNAME= ELASTICSEARCH_PASSWORD= ELASTICSEARCH_CA= \
  ELK_NETWORK= KIBANA_URL=http://kibana:5601 \
    $COMPOSE --profile local-elk up -d elasticsearch kibana elk-setup logstash filebeat
fi
