#!/usr/bin/env bash
# Run baseline capture inside a Python 3.9 container.
# Usage: bash run_capture.sh
set -euo pipefail

TAP_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

echo "==> Running baseline capture in python:3.9-slim"
echo "    Tap dir: $TAP_DIR"

docker run --rm \
  -v "$TAP_DIR":/tap \
  -w /tap \
  -e TAP_GOOGLEPLAY_KEY_FILE="${TAP_GOOGLEPLAY_KEY_FILE:?}" \
  -e TAP_GOOGLEPLAY_BUCKET_NAME="${TAP_GOOGLEPLAY_BUCKET_NAME:?}" \
  -e TAP_GOOGLEPLAY_PACKAGE_NAME="${TAP_GOOGLEPLAY_PACKAGE_NAME:?}" \
  -e TAP_GOOGLEPLAY_START_DATE="${TAP_GOOGLEPLAY_START_DATE:-2024-01-01T00:00:00Z}" \
  -e AES_SECRET_KEY="peliqan-test-key" \
  python:3.9-slim \
  bash -c "
    apt-get update -qq && apt-get install -y -qq git > /dev/null
    python -m venv /venv
    /venv/bin/pip install -e . -q
    /venv/bin/python test/regression/capture.py
  "

echo ""
echo "==> Baseline written to test/regression/baseline/"
