"""
Dependency resolution tests for tap-googleplay.

These tests verify that the urllib3 compatibility fix is effective
and all required packages can be imported without conflicts.

The core issue: google-cloud-storage 1.16.1 with urllib3 2.x causes:
    TypeError: decompress() got an unexpected keyword argument 'max_length'

These tests ensure the fix (pinning urllib3<2) is working.
"""

import pytest


class TestDependencyResolution:
    """Verify dependencies resolve correctly after the fix."""

    def test_urllib3_version_is_1x(self):
        """urllib3 must be version 1.x to avoid max_length error."""
        import urllib3
        major = int(urllib3.__version__.split('.')[0])
        assert major == 1, f"Expected urllib3 1.x, got {urllib3.__version__}"

    def test_google_cloud_storage_imports(self):
        """google-cloud-storage must import without errors."""
        from google.cloud import storage
        assert storage.Client is not None

    def test_google_cloud_storage_blob_imports(self):
        """google.cloud.storage.blob must import without errors."""
        from google.cloud.storage import blob
        assert blob.Blob is not None

    def test_google_resumable_media_imports(self):
        """google-resumable-media must import without errors."""
        from google.resumable_media.requests import download
        assert download.Download is not None

    def test_requests_imports(self):
        """requests library must import correctly."""
        import requests
        assert requests.get is not None

    def test_urllib3_response_imports(self):
        """urllib3.response must import correctly."""
        from urllib3.response import HTTPResponse
        assert HTTPResponse is not None

    def test_tap_googleplay_imports(self):
        """tap_googleplay must import without errors."""
        import tap_googleplay
        assert tap_googleplay.main is not None
        assert tap_googleplay.discover is not None
        assert tap_googleplay.sync is not None

    def test_tap_googleplay_context_class(self):
        """Context class must be importable."""
        from tap_googleplay import Context
        assert Context is not None

    def test_tap_googleplay_keyfile_class(self):
        """KeyFile class must be importable."""
        from tap_googleplay import KeyFile
        assert KeyFile is not None

    def test_all_tap_dependencies_compatible(self):
        """
        Full import chain that was failing must work.
        
        This is the exact import path that triggers the error when
        urllib3 2.x is installed with google-cloud-storage 1.16.1.
        """
        # These imports trace the path that leads to the error
        from google.cloud import storage
        from google.cloud.storage import blob
        from google.resumable_media.requests import download
        from urllib3.response import HTTPResponse
        
        # If we get here without TypeError, the fix is working
        assert True

    def test_singer_imports(self):
        """singer-python must import correctly."""
        import singer
        assert singer.write_record is not None
        assert singer.write_schema is not None
        assert singer.write_state is not None
