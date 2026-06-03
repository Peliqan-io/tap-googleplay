"""
Shared fixtures and utilities for tap-googleplay regression tests.
"""
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

REGRESSION_DIR = Path(__file__).parent
BASELINE_DIR = REGRESSION_DIR / "baseline"
TAP_CMD = str(Path(sys.executable).parent / "tap-googleplay")

REQUIRED_ENV = [
    "TAP_GOOGLEPLAY_BUCKET_NAME",
    "TAP_GOOGLEPLAY_PACKAGE_NAME",
]


def build_config():
    missing = [v for v in REQUIRED_ENV if not os.getenv(v)]
    if missing:
        pytest.skip(f"Missing required env vars: {missing}")

    key_file_raw = os.getenv("TAP_GOOGLEPLAY_KEY_FILE")
    if not key_file_raw:
        pytest.skip("Missing TAP_GOOGLEPLAY_KEY_FILE env var")

    key_file = json.loads(key_file_raw)

    return {
        "key_file": key_file,
        "bucket_name": os.environ["TAP_GOOGLEPLAY_BUCKET_NAME"],
        "package_name": os.environ["TAP_GOOGLEPLAY_PACKAGE_NAME"],
        "start_date": os.getenv("TAP_GOOGLEPLAY_START_DATE", "2024-01-01T00:00:00Z"),
    }


def _tap_env():
    env = os.environ.copy()
    env.setdefault("AES_SECRET_KEY", "peliqan-test-key")
    return env


def discover_catalog(config_path):
    """Run tap-googleplay in --discover mode and return the catalog dict."""
    result = subprocess.run(
        [TAP_CMD, "--config", config_path, "--discover"],
        capture_output=True, text=True, env=_tap_env()
    )
    if result.returncode != 0:
        raise RuntimeError(f"discover failed: {result.stderr[-2000:]}")
    return json.loads(result.stdout)


def select_all_streams(catalog):
    for stream in catalog.get("streams", []):
        for entry in stream.get("metadata", []):
            entry.setdefault("metadata", {})["selected"] = True
    return catalog


def write_json_tmp(data):
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    json.dump(data, tmp)
    tmp.close()
    return tmp.name


@pytest.fixture(scope="session")
def config_file():
    config = build_config()
    path = write_json_tmp(config)
    yield path
    os.unlink(path)


@pytest.fixture(scope="session")
def catalog_file(config_file):
    catalog = discover_catalog(config_file)
    select_all_streams(catalog)
    path = write_json_tmp(catalog)
    yield path
    os.unlink(path)


def run_tap(config_path, catalog_path=None):
    cmd = [TAP_CMD, "--config", config_path]
    if catalog_path:
        cmd += ["--catalog", catalog_path]
    result = subprocess.run(cmd, capture_output=True, text=True, env=_tap_env())
    return result.stdout, result.stderr, result.returncode


def parse_messages(stdout):
    schemas, records, states = {}, {}, []
    for line in stdout.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        t = msg.get("type")
        if t == "SCHEMA":
            schemas[msg["stream"]] = msg["schema"]
            records.setdefault(msg["stream"], [])
        elif t == "RECORD":
            records.setdefault(msg["stream"], []).append(msg["record"])
        elif t == "STATE":
            states.append(msg["value"])
    return schemas, records, states
