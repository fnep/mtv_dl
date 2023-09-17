#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset
if [[ "${TRACE-0}" == "1" ]]; then set -o xtrace; fi

docker run --rm -it \
  -v "$(pwd)/mtv_dl.py:/source/mtv_dl.py" \
  -v "$(pwd)/poetry.lock:/source/poetry.lock" \
  -v "$(pwd)/pyproject.toml:/source/pyproject.toml" \
  -v "$(pwd)/requirements.txt:/source/requirements.txt" \
  -v "$(pwd)/requirements-dev.txt:/source/requirements-dev.txt" \
  python:3.10 bash -c "\
    apt-get update; \
    pip install poetry; \
    cd /source; \
    pip install --no-cache-dir --upgrade pip; \
    poetry update; \
    poetry export -f requirements.txt --without dev --output /source/requirements.txt; \
    poetry export -f requirements.txt --with dev --output /source/requirements-dev.txt;"
