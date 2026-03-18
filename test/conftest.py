"""
Shared pytest fixtures for tap-googleplay tests.
"""

import codecs
import json
import os
import pytest
from unittest.mock import MagicMock, patch


FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "inputs")


# ---------------------------------------------------------------------------
# CSV Content Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_csv_content():
    """Plain CSV content (before encoding)."""
    return (
        "Date,Package Name,OS Version,Active Device Installs,Daily Device Installs\n"
        "2024-01-15,com.example.app,Android 14,1000,50\n"
        "2024-01-15,com.example.app,Android 13,800,30\n"
    )


@pytest.fixture
def sample_csv_bytes_utf16(sample_csv_content):
    """UTF-16 LE encoded CSV with BOM (as Google Play provides)."""
    return codecs.BOM_UTF16_LE + sample_csv_content.encode('utf-16le')


@pytest.fixture
def sample_csv_file_path():
    """Path to the UTF-16 LE sample CSV fixture file."""
    return os.path.join(FIXTURES_DIR, "sample_installs_utf16.csv")


@pytest.fixture
def simple_csv_file_path():
    """Path to simple ASCII CSV fixture file."""
    return os.path.join(FIXTURES_DIR, "simple.csv")


# ---------------------------------------------------------------------------
# Configuration Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def valid_config():
    """Valid tap configuration."""
    return {
        'key_file': {
            'type': 'service_account',
            'project_id': 'test-project',
            'private_key_id': 'key123',
            'private_key': '-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----\n',
            'client_email': 'test@test-project.iam.gserviceaccount.com',
            'client_id': '123456789',
        },
        'start_date': '2024-01-01T00:00:00Z',
        'bucket_name': 'pubsite_prod_rev_12345',
        'package_name': 'com.example.app'
    }


@pytest.fixture
def sample_state():
    """Sample state with bookmark."""
    return {
        'bookmarks': {
            'installs': {
                'start_date': '2024-06-01T00:00:00Z'
            }
        }
    }


# ---------------------------------------------------------------------------
# Catalog Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_catalog():
    """Sample catalog for sync tests."""
    return {
        'streams': [{
            'stream': 'installs',
            'tap_stream_id': 'installs',
            'schema': {
                'type': ['null', 'object'],
                'properties': {
                    'date': {'type': ['null', 'string'], 'format': 'date-time'},
                    'package_name': {'type': ['null', 'string']},
                    'dimension_name': {'type': ['null', 'string']},
                    'dimension_value': {'type': ['null', 'string']},
                    'active_device_installs': {'type': ['null', 'number']},
                    'daily_device_installs': {'type': ['null', 'number']},
                    'daily_device_uninstalls': {'type': ['null', 'number']},
                    'daily_device_upgrades': {'type': ['null', 'number']},
                    'total_user_installs': {'type': ['null', 'number']},
                    'daily_user_installs': {'type': ['null', 'number']},
                    'daily_user_uninstalls': {'type': ['null', 'number']},
                    'install_events': {'type': ['null', 'number']},
                    'update_events': {'type': ['null', 'number']},
                    'uninstall_events': {'type': ['null', 'number']},
                }
            },
            'key_properties': ['date', 'package_name', 'dimension_name', 'dimension_value'],
            'metadata': []
        }]
    }


# ---------------------------------------------------------------------------
# Mock GCS Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_gcs_client():
    """Mocked Google Cloud Storage client."""
    with patch('tap_googleplay.storage.Client') as MockClient:
        client_instance = MagicMock()
        MockClient.from_service_account_info.return_value = client_instance
        yield client_instance


@pytest.fixture
def mock_bucket(mock_gcs_client):
    """Mocked GCS bucket."""
    bucket = MagicMock()
    mock_gcs_client.get_bucket.return_value = bucket
    return bucket


@pytest.fixture
def mock_blob(mock_bucket, sample_csv_bytes_utf16):
    """Mocked blob that returns sample CSV data."""
    blob = MagicMock()
    blob.download_as_string.return_value = sample_csv_bytes_utf16
    mock_bucket.get_blob.return_value = blob
    return blob


# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------

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
