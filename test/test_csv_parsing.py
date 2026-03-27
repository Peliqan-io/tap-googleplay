"""
Unit tests for CSV parsing in tap-googleplay.

Tests the csv_to_list() function which parses Google Play CSV reports.
"""

import codecs
import os
import pytest

from tap_googleplay import csv_to_list


FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "inputs")


class TestCsvToList:
    """Tests for csv_to_list() function."""

    def test_valid_csv_returns_data_and_headers(self):
        """Basic CSV parsing returns tuple of (data, headers) with proper normalization."""
        content = "Date,Package Name,Value\n2024-01-01,com.app,100"
        data, headers = csv_to_list(content)
        
        assert headers == ['date', 'package_name', 'value']
        assert len(data) == 1
        assert data[0]['date'] == '2024-01-01'
        assert data[0]['package_name'] == 'com.app'
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

    def test_utf16_fixture_after_decode(self):
        """
        Parse the UTF-16 fixture after proper decoding.
        
        This tests the critical path: UTF-16 LE with BOM -> decode -> parse.
        Google Play reports are delivered in this format.
        """
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
