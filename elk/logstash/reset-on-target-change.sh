#!/bin/sh
# The jdbc inputs' saved positions describe what one cluster already holds.
# Pointed at a different cluster (existing <-> Weave's own, or another URL),
# they would make it skip every row read before the switch — so forget them,
# and the 7-day floor in twin.conf backfills the new target. Runs before
# Logstash starts, so nothing can rewrite the files underneath it.
state=/usr/share/logstash/data
target=${ES_URL%/}
if [ "$(cat "$state/elk_target" 2>/dev/null)" != "$target" ]; then
  rm -f "$state"/jdbc_last_run_*
  printf '%s' "$target" > "$state/elk_target"
  echo "elk target is now $target: jdbc positions reset, backfilling the last 7 days"
fi
exec /usr/local/bin/docker-entrypoint "$@"
