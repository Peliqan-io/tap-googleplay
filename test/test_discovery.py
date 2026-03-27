"""
Tests for discovery mode in tap-googleplay.

Tests the discover() and load_schemas() functions.
"""

import json
import pytest

from tap_googleplay import discover, load_schemas, Context, get_bookmark


class TestLoadSchemas:
    """Tests for load_schemas() function."""

    def test_installs_schema_required_fields(self):
        """installs schema contains all required Google Play report fields."""
        schemas = load_schemas()
        
        assert 'installs' in schemas
        props = schemas['installs']['properties']
        
        required_fields = [
            'date',
            'package_name',
            'dimension_name',
            'dimension_value',
            'active_device_installs',
            'daily_device_installs',
            'daily_device_uninstalls',
            'daily_device_upgrades',
            'total_user_installs',
            'daily_user_installs',
            'daily_user_uninstalls',
            'install_events',
            'update_events',
            'uninstall_events',
        ]
        
        for field in required_fields:
            assert field in props, f"Missing required field: {field}"


class TestDiscover:
    """Tests for discover() function."""

    def test_discover_returns_valid_catalog(self):
        """discover() returns a valid Singer catalog structure."""
        catalog = discover()
        
        assert isinstance(catalog, dict)
        assert 'streams' in catalog
        assert isinstance(catalog['streams'], list)
        
        stream_names = [s['stream'] for s in catalog['streams']]
        assert 'installs' in stream_names

    def test_installs_stream_structure(self):
        """installs stream has correct Singer catalog structure."""
        catalog = discover()
        installs = next(s for s in catalog['streams'] if s['stream'] == 'installs')
        
        assert installs['tap_stream_id'] == 'installs'
        assert 'schema' in installs
        assert 'key_properties' in installs
        assert 'metadata' in installs
        
        expected_keys = ['date', 'package_name', 'dimension_name', 'dimension_value']
        assert installs['key_properties'] == expected_keys


class TestGetBookmark:
    """Tests for get_bookmark() function."""

    def test_returns_state_bookmark_if_present(self, sample_state):
        """get_bookmark prioritizes state bookmark over config start_date."""
        Context.state = sample_state
        Context.config = {'start_date': '2020-01-01T00:00:00Z'}
        
        bookmark = get_bookmark('installs')
        assert bookmark == '2024-06-01T00:00:00Z'

    def test_returns_config_start_date_if_no_state(self):
        """get_bookmark falls back to config start_date when no state."""
        Context.state = {}
        Context.config = {'start_date': '2024-01-01T00:00:00Z'}
        
        bookmark = get_bookmark('installs')
        assert bookmark == '2024-01-01T00:00:00Z'


class TestContextClass:
    """Tests for Context class methods."""

    def test_get_catalog_entry_caching(self, sample_catalog):
        """get_catalog_entry builds stream_map cache correctly."""
        Context.catalog = sample_catalog
        Context.stream_map = {}
        
        entry = Context.get_catalog_entry('installs')
        assert entry is not None
        assert entry['stream'] == 'installs'
        
        # Second call should use cache
        entry2 = Context.get_catalog_entry('installs')
        assert entry2 is entry

    def test_is_selected_with_metadata(self):
        """is_selected correctly parses Singer metadata."""
        catalog = {
            'streams': [{
                'stream': 'installs',
                'tap_stream_id': 'installs',
                'schema': {},
                'key_properties': [],
                'metadata': [
                    {'breadcrumb': [], 'metadata': {'selected': True}}
                ]
            }]
        }
        Context.catalog = catalog
        Context.stream_map = {}
        
        assert Context.is_selected('installs') is True
