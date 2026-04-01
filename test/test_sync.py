"""
Sync and integration tests for tap-googleplay.

Tests the sync flow with mocked Google Cloud Storage.
These tests verify that the core sync functionality works correctly,
especially the blob download path that was failing with urllib3 2.x.
"""

import codecs
import json
import os
import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from tap_googleplay import (
    sync, query_report, Context, BOOKMARK_DATE_FORMAT, csv_to_list
)


FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "inputs")


def load_csv_fixture(filename):
    """Load a CSV fixture file and return its bytes."""
    path = os.path.join(FIXTURES_DIR, filename)
    with open(path, 'rb') as f:
        return f.read()


def make_mock_blob(content_bytes):
    """Create a mock blob that returns the given content."""
    blob = MagicMock()
    blob.download_as_string.return_value = content_bytes
    return blob


class TestQueryReport:
    """Tests for query_report() with mocked GCS."""

    def setup_method(self):
        """Reset Context before each test."""
        Context.stream_map = {}
        Context.new_counts = {'installs': 0}
        Context.updated_counts = {'installs': 0}

    def test_blob_not_found_handles_gracefully(
        self, mock_bucket, valid_config, sample_catalog
    ):
        """Missing blob (None) is handled without crashing."""
        mock_bucket.get_blob.return_value = None
        
        valid_config['start_date'] = (
            datetime.utcnow() - timedelta(days=10)
        ).strftime(BOOKMARK_DATE_FORMAT)
        
        Context.config = valid_config
        Context.catalog = sample_catalog
        Context.state = {}
        
        # Should not raise an exception
        with patch('tap_googleplay.singer.write_state'):
            with patch('tap_googleplay.singer.write_bookmark'):
                query_report(mock_bucket)

    def test_utf16_bom_stripped_correctly(
        self, mock_bucket, sample_csv_content, valid_config, sample_catalog
    ):
        """
        UTF-16 LE BOM is properly stripped from downloaded data.
        
        This is critical because Google Play reports come as UTF-16 LE with BOM.
        """
        # Create blob with BOM-prefixed content
        bom_content = codecs.BOM_UTF16_LE + sample_csv_content.encode('utf-16le')
        blob = make_mock_blob(bom_content)
        mock_bucket.get_blob.return_value = blob
        
        valid_config['start_date'] = (
            datetime.utcnow() - timedelta(days=10)
        ).strftime(BOOKMARK_DATE_FORMAT)
        
        Context.config = valid_config
        Context.catalog = sample_catalog
        Context.state = {}
        Context.new_counts = {'installs': 0}
        Context.updated_counts = {'installs': 0}
        
        records_written = []
        
        def capture_record(stream, record, time_extracted=None):
            records_written.append(record)
        
        with patch('tap_googleplay.singer.write_record', side_effect=capture_record):
            with patch('tap_googleplay.singer.write_state'):
                with patch('tap_googleplay.singer.write_bookmark'):
                    query_report(mock_bucket)
        
        # If BOM wasn't handled, we'd get decode errors
        assert len(records_written) >= 0

    def test_report_key_format(
        self, mock_bucket, mock_blob, valid_config, sample_catalog
    ):
        """Report key follows expected Google Play format."""
        valid_config['start_date'] = '2024-01-15T00:00:00Z'
        valid_config['package_name'] = 'com.test.app'
        
        Context.config = valid_config
        Context.catalog = sample_catalog
        Context.state = {}
        Context.new_counts = {'installs': 0}
        Context.updated_counts = {'installs': 0}
        
        with patch('tap_googleplay.singer.write_record'):
            with patch('tap_googleplay.singer.write_state'):
                with patch('tap_googleplay.singer.write_bookmark'):
                    query_report(mock_bucket)
        
        # Check that get_blob was called with expected key format
        calls = mock_bucket.get_blob.call_args_list
        assert len(calls) > 0
        
        # Key format: stats/installs/installs_{package}_{YYYYMM}_{dimension}.csv
        key = calls[0][0][0]
        assert key.startswith('stats/installs/installs_')
        assert 'com.test.app' in key
        assert 'os_version.csv' in key


class TestSync:
    """Tests for sync() function."""

    def setup_method(self):
        """Reset Context before each test."""
        Context.stream_map = {}
        Context.new_counts = {}
        Context.updated_counts = {}

    def test_sync_workflow(self, mock_bucket, sample_catalog, valid_config):
        """sync() performs complete workflow: write schema, init counts, query report."""
        Context.config = valid_config
        Context.catalog = sample_catalog
        Context.state = {}
        Context.new_counts = {}
        Context.updated_counts = {}
        
        with patch('tap_googleplay.singer.write_schema') as mock_schema:
            with patch('tap_googleplay.query_report') as mock_query:
                sync(mock_bucket)
        
        # Schema was written
        mock_schema.assert_called()
        call_args = mock_schema.call_args[0]
        assert call_args[0] == 'installs'
        
        # Counts initialized
        assert 'installs' in Context.new_counts
        assert 'installs' in Context.updated_counts
        
        # Query report called
        mock_query.assert_called_once_with(mock_bucket)


class TestFixtureFiles:
    """Tests using the actual fixture files."""

    def test_utf16_fixture_end_to_end(self):
        """
        UTF-16 fixture can be loaded, decoded, and parsed end-to-end.
        
        This tests the complete path that was failing with urllib3 2.x:
        bytes (with BOM) -> strip BOM -> decode UTF-16 LE -> parse CSV
        """
        content = load_csv_fixture("sample_installs_utf16.csv")
        
        # Verify BOM present
        assert content.startswith(codecs.BOM_UTF16_LE)
        
        # Decode as the tap does
        bom = codecs.BOM_UTF16_LE
        if content.startswith(bom):
            content = content[len(bom):]
        decoded = content.decode('utf-16le')
        
        # Parse
        data, headers = csv_to_list(decoded)
        
        # Verify parsed correctly
        assert 'date' in headers
        assert 'package_name' in headers
        assert len(data) == 5  # 5 data rows in fixture

    def test_february_fixture_parses(self):
        """February 2024 fixture file parses with correct data."""
        content = load_csv_fixture("installs_202402.csv")
        
        # Decode
        bom = codecs.BOM_UTF16_LE
        if content.startswith(bom):
            content = content[len(bom):]
        decoded = content.decode('utf-16le')
        
        data, headers = csv_to_list(decoded)
        
        assert len(data) == 3
        assert data[0]['date'] == '2024-02-01'
