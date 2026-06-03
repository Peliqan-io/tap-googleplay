# tap-googleplay — Python 3.9 → 3.11 Regression Tests

## Strategy

1. Exercise `discover()`, `load_schemas()`, `csv_to_list()` on both Python versions
2. Run existing unit tests on both versions
3. Compare all outputs

No GCS credentials needed.

## Usage

```bash
cd tap-googleplay

git checkout master
docker run --rm -v "$(pwd)":/tap -w /tap python:3.9-slim \
  bash -c 'apt-get update -qq && apt-get install -y -qq git > /dev/null 2>&1 && pip install -e ".[dev]" -q 2>/dev/null && python test/regression/capture.py --output baseline'

git checkout python-311-migration
docker run --rm -v "$(pwd)":/tap -w /tap python:3.11-slim \
  bash -c 'apt-get update -qq && apt-get install -y -qq git > /dev/null 2>&1 && pip install -e ".[dev]" -q 2>/dev/null && python test/regression/capture.py --output current'

python3 -m pytest test/regression/test_regression.py -v
```
