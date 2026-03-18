"""
Unit tests for CSV parsing in tap-googleplay.

Tests the csv_to_list() function which parses Google Play CSV reports.
"""

import codecs
import os
import pytest

from tap_googleplay import csv_to_list


FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "inputs")


def load_csv_fixture(filename):
    """Load a CSV fixture file and return its bytes."""
    path = os.path.join(FIXTURES_DIR, filename)
    with open(path, 'rb') as f:
        return f.read()


class TestCsvToList:
    """Tests for csv_to_list() function."""

    def test_valid_csv_returns_data_and_headers(self):
        """Basic CSV parsing returns tuple of (data, headers)."""
        content = "Date,Package Name,Value\n2024-01-01,com.app,100"
        data, headers = csv_to_list(content)
        
        assert headers == ['date', 'package_name', 'value']
        assert len(data) == 1
        assert data[0]['date'] == '2024-01-01'
        assert data[0]['package_name'] == 'com.app'
        assert data[0]['value'] == '100'

    def test_header_normalization_lowercase(self):
        """Headers are lowercased."""
        content = "DATE,PACKAGE_NAME\n2024-01-01,com.app"
        data, headers = csv_to_list(content)
        
        assert headers == ['date', 'package_name']

    def test_header_normalization_spaces_to_underscores(self):
        """Spaces in headers are replaced with underscores."""
        content = "Daily Device Installs,OS Version\n100,Android 14"
        data, headers = csv_to_list(content)
        
        assert headers == ['daily_device_installs', 'os_version']

    def test_empty_rows_skipped(self):
        """Empty rows in CSV are skipped."""
        content = "Date,Value\n2024-01-01,100\n\n2024-01-02,200\n"
        data, headers = csv_to_list(content)
        
        assert len(data) == 2
        assert data[0]['date'] == '2024-01-01'
        assert data[1]['date'] == '2024-01-02'

    def test_whitespace_trimmed_from_values(self):
        """Whitespace is trimmed from values."""
        content = "Date,Value\n 2024-01-01 , 100 "
        data, headers = csv_to_list(content)
        
        assert data[0]['date'] == '2024-01-01'
        assert data[0]['value'] == '100'

    def test_row_shorter_than_header_handled(self):
        """Rows with fewer columns than header are handled gracefully."""
        content = "Date,Value,Extra\n2024-01-01,100"
        data, headers = csv_to_list(content)
        
        assert len(data) == 1
        assert 'date' in data[0]
        assert 'value' in data[0]
        # 'extra' key should not exist since row was short
        assert 'extra' not in data[0]

    def test_multiple_rows_parsed(self):
        """Multiple rows are parsed correctly."""
        content = (
            "Date,Package Name,Value\n"
            "2024-01-01,com.app,100\n"
            "2024-01-02,com.app,200\n"
            "2024-01-03,com.app,300\n"
        )
        data, headers = csv_to_list(content)
        
        assert len(data) == 3
        assert data[0]['value'] == '100'
        assert data[1]['value'] == '200'
        assert data[2]['value'] == '300'

    def test_google_play_report_format(self, sample_csv_content):
        """Parse CSV in Google Play report format."""
        data, headers = csv_to_list(sample_csv_content)
        
        assert 'date' in headers
        assert 'package_name' in headers
        assert 'os_version' in headers
        assert 'active_device_installs' in headers
        assert 'daily_device_installs' in headers
        
        assert len(data) == 2
        assert data[0]['package_name'] == 'com.example.app'

    def test_fixture_file_content(self, simple_csv_file_path):
        """Parse the simple CSV fixture file."""
        with open(simple_csv_file_path, 'r') as f:
            content = f.read()
        
        data, headers = csv_to_list(content)
        
        assert headers == ['date', 'package_name', 'value']
        assert len(data) == 2

    def test_utf16_fixture_after_decode(self):
        """Parse the UTF-16 fixture after proper decoding."""
        # Load the UTF-16 LE file
        fixture_path = os.path.join(FIXTURES_DIR, "sample_installs_utf16.csv")
        with open(fixture_path, 'rb') as f:
            raw_bytes = f.read()
        
        # Strip BOM and decode (as the tap does)
        bom = codecs.BOM_UTF16_LE
        if raw_bytes.startswith(bom):
            raw_bytes = raw_bytes[len(bom):]
        content = raw_bytes.decode('utf-16le')
        
        data, headers = csv_to_list(content)
        
        assert 'date' in headers
        assert 'package_name' in headers
        assert 'os_version' in headers
        assert len(data) == 5  # 5 data rows in fixture

    def test_quoted_values_with_commas(self):
        """CSV values containing commas are handled (quoted)."""
        content = 'Name,Description\n"App, Inc.","A great app, really"'
        data, headers = csv_to_list(content)
        
        assert len(data) == 1
        assert data[0]['name'] == 'App, Inc.'
        assert data[0]['description'] == 'A great app, really'

    def test_empty_csv_returns_empty_data(self):
        """CSV with only header returns empty data list."""
        content = "Date,Value\n"
        data, headers = csv_to_list(content)
        
        assert headers == ['date', 'value']
        assert len(data) == 0
