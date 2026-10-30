#!/usr/bin/env bash
# Compile each component's requirements.in into a hash-locked requirements.txt.
#
# The Dockerfiles and CI install with --require-hashes, so every package,
# transitive ones included, is pinned and checked against PyPI's hashes. A
# lock is resolved for Python 3.12 on Linux, the base of every image, so run
# this in that image rather than on the host:
#
#   docker run --rm -v "$PWD":/w -w /w python:3.12-slim bash -c \
#     'pip install -q --require-hashes -r .github/ci/requirements.txt && scripts/lock-requirements.sh'
#
# Existing pins are kept, so re-running on an unchanged input changes nothing
# (CI relies on that to catch a stale lock). Extra arguments go to every
# pip-compile, e.g. --upgrade-package urllib3 to move one transitive pin.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

for dir in model ingest console .github/ci; do
  echo "-> $dir"
  (cd "$ROOT/$dir" && pip-compile --quiet --generate-hashes --allow-unsafe \
     --strip-extras --no-emit-index-url --output-file=requirements.txt "$@" \
     requirements.in)
done
