"""
Dependency resolution tests for tap-googleplay.

These tests verify that the urllib3 compatibility fix is effective.

The core issue: google-cloud-storage 1.16.1 with urllib3 2.x causes:
    TypeError: decompress() got an unexpected keyword argument 'max_length'

This test ensures the fix (pinning urllib3<2) is working.
"""

import pytest


class TestDependencyResolution:
    """Verify urllib3 version constraint is enforced."""

    def test_urllib3_version_is_1x(self):
        """
        urllib3 must be version 1.x to avoid max_length error.
        
        This is the critical test for the dependency fix.
        google-cloud-storage 1.16.1 is incompatible with urllib3 2.x
        because it calls decompress(max_length=...) which was removed in urllib3 2.0.
        """
        import urllib3
        major = int(urllib3.__version__.split('.')[0])
        assert major == 1, f"Expected urllib3 1.x, got {urllib3.__version__}"
