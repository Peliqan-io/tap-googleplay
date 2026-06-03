#!/usr/bin/env python3
"""
Capture tap-googleplay outputs for regression comparison.

Exercises discover(), csv_to_list(), load_schemas(), and runs existing
unit tests — no GCS credentials needed.

Usage:
    python capture.py --output baseline   # on 3.9 (master)
    python capture.py --output current    # on 3.11 (migration branch)
"""
import argparse
import codecs
import json
import subprocess
import sys
from pathlib import Path


def capture_discovery():
    """Run discover() and return the catalog."""
    from tap_googleplay import discover
    return discover()


def capture_schemas():
    """Load raw schemas and return them."""
    from tap_googleplay import load_schemas
    return load_schemas()


def capture_csv_parsing():
    """Exercise csv_to_list() with various inputs."""
    from tap_googleplay import csv_to_list

    results = {}

    # 1. Simple CSV
    simple = "Date,Package Name,OS Version,Active Device Installs\n2024-01-15,com.test,12,1000\n2024-01-15,com.test,13,2000"
    data, headers = csv_to_list(simple)
    results["simple"] = {"data": data, "headers": headers}

    # 2. Empty rows
    with_empty = "Date,Name\n2024-01-01,Test\n\n2024-01-02,Test2\n"
    data, headers = csv_to_list(with_empty)
    results["with_empty_rows"] = {"data": data, "headers": headers}

    # 3. Short rows (fewer columns than header)
    short = "A,B,C\n1,2,3\n4,5\n6"
    data, headers = csv_to_list(short)
    results["short_rows"] = {"data": data, "headers": headers}

    # 4. Special characters
    special = "Name,Value\ntest's,100\n\"quoted,value\",200"
    data, headers = csv_to_list(special)
    results["special_chars"] = {"data": data, "headers": headers}

    # 5. Google Play style headers (spaces → underscores, lowered)
    gplay = "Date,Package Name,Daily Device Installs,Daily User Installs\n2024-02-01,com.app,500,450"
    data, headers = csv_to_list(gplay)
    results["google_play_headers"] = {"data": data, "headers": headers}

    # 6. Use the test fixture if available
    fixture_path = Path(__file__).parent.parent.parent / "test" / "inputs" / "simple.csv"
    if fixture_path.exists():
        content = fixture_path.read_text()
        data, headers = csv_to_list(content)
        results["fixture_simple"] = {"data": data, "headers": headers}

    # 7. UTF-16 fixture
    utf16_path = Path(__file__).parent.parent.parent / "test" / "inputs" / "sample_installs_utf16.csv"
    if utf16_path.exists():
        raw = utf16_path.read_bytes()
        bom = codecs.BOM_UTF16_LE
        if raw.startswith(bom):
            raw = raw[len(bom):]
        decoded = raw.decode('utf-16le')
        data, headers = csv_to_list(decoded)
        results["fixture_utf16"] = {"data": data, "headers": headers}

    return results


def run_existing_tests():
    """Run the existing unit tests and capture results."""
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "test/", "-v", "--tb=short"],
        capture_output=True, text=True, timeout=120
    )
    return {
        "exit_code": result.returncode,
        "stdout_tail": result.stdout[-2000:] if result.stdout else "",
        "stderr_tail": result.stderr[-500:] if result.stderr else "",
        "passed": result.stdout.count(" PASSED") if result.stdout else 0,
        "failed": result.stdout.count(" FAILED") if result.stdout else 0,
    }


def main():
    parser = argparse.ArgumentParser(description="Capture tap-googleplay regression output")
    parser.add_argument("--output", required=True, choices=["baseline", "current"])
    args = parser.parse_args()

    regression_dir = Path(__file__).parent
    output_dir = regression_dir / args.output
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Python version: {sys.version}")
    print(f"Output dir:     {output_dir}")

    # 1. Discovery
    print("\nCapturing discovery...")
    catalog = capture_discovery()
    (output_dir / "catalog.json").write_text(
        json.dumps(catalog, indent=2, sort_keys=True)
    )
    streams = [s["stream"] for s in catalog.get("streams", [])]
    print(f"  Streams: {streams}")

    # 2. Raw schemas
    print("Capturing schemas...")
    schemas = capture_schemas()
    (output_dir / "schemas.json").write_text(
        json.dumps(schemas, indent=2, sort_keys=True)
    )
    print(f"  Schema files: {list(schemas.keys())}")

    # 3. CSV parsing
    print("Capturing CSV parsing...")
    csv_results = capture_csv_parsing()
    (output_dir / "csv_parsing.json").write_text(
        json.dumps(csv_results, indent=2, sort_keys=True)
    )
    print(f"  Test cases: {list(csv_results.keys())}")

    # 4. Existing tests
    print("Running existing unit tests...")
    test_results = run_existing_tests()
    (output_dir / "unit_test_results.json").write_text(
        json.dumps(test_results, indent=2)
    )
    print(f"  Passed: {test_results['passed']}, Failed: {test_results['failed']}, Exit: {test_results['exit_code']}")

    # 5. Meta
    meta = {
        "python_version": sys.version.split()[0],
        "streams": streams,
        "csv_test_cases": len(csv_results),
        "unit_tests_passed": test_results["passed"],
        "unit_tests_failed": test_results["failed"],
    }
    (output_dir / "meta.json").write_text(json.dumps(meta, indent=2))

    print(f"\nCapture complete. Files written to: {output_dir}/")


if __name__ == "__main__":
    main()
