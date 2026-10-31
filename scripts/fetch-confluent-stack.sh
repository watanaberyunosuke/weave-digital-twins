#!/usr/bin/env bash
# Puts Confluent's compose stack into the repository root, so the clone is
# the stack: docker-compose.yml (broker, Connect, Control Center with
# Prometheus and Alertmanager, Flink) and the vol/ config it mounts.
# docker-compose.override.yml layers Weave onto it.
#
# Both are git-ignored, because they are Confluent's files, not this repo's.
# Re-running replaces them with a fresh copy of the pinned branch; the
# override, .env and the data volumes are untouched.
#
#   ./scripts/fetch-confluent-stack.sh
#   CP_BRANCH=8.2.0-post ./scripts/fetch-confluent-stack.sh
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

CP_REPO=${CP_REPO:-https://github.com/confluentinc/cp-all-in-one.git}
CP_BRANCH=${CP_BRANCH:-8.2.0-post}

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

echo "Fetching $CP_REPO ($CP_BRANCH) ..."
git clone -q --depth 1 -b "$CP_BRANCH" "$CP_REPO" "$tmp/cp"
src="$tmp/cp/cp-all-in-one"
if [[ ! -f $src/docker-compose.yml ]]; then
  echo "! $CP_BRANCH has no cp-all-in-one/docker-compose.yml"
  exit 1
fi

cp "$src/docker-compose.yml" docker-compose.yml
rm -rf vol
if [[ -d $src/vol ]]; then
  cp -R "$src/vol" vol
  echo "  wrote docker-compose.yml and vol/ from $CP_BRANCH"
else
  echo "  wrote docker-compose.yml from $CP_BRANCH"
fi
